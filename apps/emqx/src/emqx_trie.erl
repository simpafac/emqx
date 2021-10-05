%%--------------------------------------------------------------------
%% Copyright (c) 2017-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_trie).

-include("emqx.hrl").

%% Mnesia bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

%% Trie APIs
-export([ insert/1
        , match/1
        , delete/1
        ]).

-export([ empty/0
        , lock_tables/0
        ]).

-export([is_compact/0, set_compact/1]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-define(TRIE, emqx_trie).
-define(PREFIX(Prefix), {Prefix, 0}).
-define(TOPIC(Topic), {Topic, 1}).

-record(?TRIE,
        { key :: ?TOPIC(binary()) | ?PREFIX(binary())
        , count = 0 :: non_neg_integer()
        }).

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

%% @doc Create or replicate topics table.
-spec(mnesia(boot | copy) -> ok).
mnesia(boot) ->
    %% Optimize storage
    StoreProps = [{ets, [{read_concurrency, true},
                         {write_concurrency, true}
                        ]}],
    ok = ekka_mnesia:create_table(?TRIE, [
                {rlog_shard, ?ROUTE_SHARD},
                {ram_copies, [node()]},
                {record_name, ?TRIE},
                {attributes, record_info(fields, ?TRIE)},
                {type, ordered_set},
                {storage_properties, StoreProps}]);
mnesia(copy) ->
    %% Copy topics table
    ok = ekka_mnesia:copy_table(?TRIE, ram_copies).

%%--------------------------------------------------------------------
%% Topics APIs
%%--------------------------------------------------------------------

%% @doc Insert a topic filter into the trie.
-spec(insert(emqx_types:topic()) -> ok).
insert(Topic) when is_binary(Topic) ->
    insert(Topic, ?TRIE).

insert(Topic, Trie) when is_binary(Topic) ->
    {TopicKey, PrefixKeys} = make_keys(Topic),
    case mnesia:wread({Trie, TopicKey}) of
        [_] -> ok; %% already inserted
        [] -> lists:foreach(fun(Key) -> insert_key(Key, Trie) end, [TopicKey | PrefixKeys])
    end.

%% @doc Delete a topic filter from the trie.
-spec(delete(emqx_types:topic()) -> ok).
delete(Topic) when is_binary(Topic) ->
    delete(Topic, ?TRIE).

delete(Topic, Trie) when is_binary(Topic) ->
    {TopicKey, PrefixKeys} = make_keys(Topic),
    case [] =/= mnesia:wread({Trie, TopicKey}) of
        true -> lists:foreach(fun(Key) -> delete_key(Key, Trie) end, [TopicKey | PrefixKeys]);
        false -> ok
    end.

%% @doc Find trie nodes that matchs the topic name.
-spec(match(emqx_types:topic()) -> list(emqx_types:topic())).
match(Topic) when is_binary(Topic) ->
    match(Topic, ?TRIE).
match(Topic, Trie) when is_binary(Topic) ->
    Words = emqx_topic:words(Topic),
    case emqx_topic:wildcard(Words) of
        true ->
            %% In MQTT spec, clients are not allowed to
            %% publish messages to a wildcard topic.
            %% Here we refuse to match wildcard topic.
            %%
            %% NOTE: this does not imply emqx allows clients
            %% publishing to wildcard topics.
            %% Such clients will get disconnected.
            [];
        false ->
            do_match(Words, Trie)
    end.

%% @doc Is the trie empty?
-spec(empty() -> boolean()).
empty() -> empty(?TRIE).

empty(Trie) -> ets:first(Trie) =:= '$end_of_table'.

-spec lock_tables() -> ok.
lock_tables() ->
    mnesia:write_lock_table(?TRIE).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

make_keys(Topic) ->
    Words = emqx_topic:words(Topic),
    {?TOPIC(Topic), [?PREFIX(Prefix) || Prefix <- make_prefixes(Words)]}.

compact(Words) ->
    case is_compact() of
        true -> do_compact(Words);
        false -> Words
    end.

%% join split words into compacted segments
%% each segment ends with one wildcard word
%% e.g.
%% a/b/c/+/d/# => [a/b/c/+, d/#]
%% a/+/+/b => [a/+, +, b]
%% a/+/+/+/+/b => [a/+, +, +, +, b]
do_compact(Words) ->
    do_compact(Words, empty, []).

do_compact([], empty, Acc) -> lists:reverse(Acc);
do_compact([], Seg, Acc) -> lists:reverse([Seg | Acc]);
do_compact([Word | Words], Seg, Acc) when Word =:= '+' orelse Word =:= '#' ->
    do_compact(Words, empty, [join(Seg, Word) | Acc]);
do_compact([Word | Words], Seg, Acc) ->
    do_compact(Words, join(Seg, Word), Acc).

join(empty, '+') -> <<"+">>;
join(empty, '#') -> <<"#">>;
join(empty, '') -> <<>>;
join(empty, Word) -> Word;
join(Prefix, Word) -> emqx_topic:join([Prefix, Word]).

make_prefixes(Words) ->
    lists:map(fun emqx_topic:join/1,
              make_prefixes(compact(Words), [], [])).

make_prefixes([_LastWord], _Prefix, Acc) ->
    lists:map(fun lists:reverse/1, Acc);
make_prefixes([H | T], Prefix0, Acc0) ->
    Prefix = [H | Prefix0],
    Acc = [Prefix | Acc0],
    make_prefixes(T, Prefix, Acc).

insert_key(Key, Trie) ->
    T = case mnesia:wread({Trie, Key}) of
            [#?TRIE{count = C} = T1] ->
                T1#?TRIE{count = C + 1};
             [] ->
                #?TRIE{key = Key, count = 1}
         end,
    ok = mnesia:write(Trie, T, write).

delete_key(Key, Trie) ->
    case mnesia:wread({Trie, Key}) of
        [#?TRIE{count = C} = T] when C > 1 ->
            ok = mnesia:write(Trie, T#?TRIE{count = C - 1}, write);
        [_] ->
            ok = mnesia:delete(Trie, Key, write);
        [] ->
            ok
    end.

%% micro-optimization: no need to lookup when topic is not wildcard
%% because we only insert wildcards to emqx_trie
lookup_topic(_Topic,_Trie, false) -> [];
lookup_topic(Topic, Trie, true) -> lookup_topic(Topic, Trie).

lookup_topic(Topic, Trie) when is_binary(Topic) ->
    case ets:lookup(Trie, ?TOPIC(Topic)) of
        [#?TRIE{count = C}] -> [Topic || C > 0];
        [] -> []
    end.

has_prefix(empty, _Trie) -> true; %% this is the virtual tree root
has_prefix(Prefix, Trie) ->
    case ets:lookup(Trie, ?PREFIX(Prefix)) of
        [#?TRIE{count = C}] -> C > 0;
        [] -> false
    end.

do_match([<<"$", _/binary>> = Prefix | Words], Trie) ->
    %% For topics having dollar sign prefix,
    %% we do not match root level + or #,
    %% fast forward to the next level.
    case Words =:= [] of
        true -> lookup_topic(Prefix, Trie);
        false -> []
    end ++ do_match(Words, Prefix, Trie);
do_match(Words, Trie) ->
    do_match(Words, empty, Trie).

do_match(Words, Prefix, Trie) ->
    case is_compact() of
        true -> match_compact(Words, Prefix, Trie, false, []);
        false -> match_no_compact(Words, Prefix, Trie, false, [])
    end.

match_no_compact([], Topic, Trie, IsWildcard, Acc) ->
    'match_#'(Topic, Trie) ++ %% try match foo/+/# or foo/bar/#
    lookup_topic(Topic, Trie, IsWildcard) ++ %% e.g. foo/+
    Acc;
match_no_compact([Word | Words], Prefix, Trie, IsWildcard, Acc0) ->
    case has_prefix(Prefix, Trie) of
        true ->
            Acc1 = 'match_#'(Prefix, Trie) ++ Acc0,
            Acc = match_no_compact(Words, join(Prefix, '+'), Trie, true, Acc1),
            match_no_compact(Words, join(Prefix, Word), Trie, IsWildcard, Acc);
        false ->
            %% non-compact paths in database
            %% if there is no prefix matches the current topic prefix
            %% we can simpliy return from here
            %% e.g. a/b/c/+ results in
            %%  - a
            %%  - a/b
            %%  - a/b/c
            %%  - a/b/c/+
            %% if the input topic is to match 'a/x/y',
            %% then at the second level, we lookup prefix a/x,
            %% no such prefix to be found, meaning there is no point
            %% searching for 'a/x/y', 'a/x/+' or 'a/x/#'
            Acc0
    end.

match_compact([], Topic, Trie, IsWildcard, Acc) ->
    'match_#'(Topic, Trie) ++ %% try match foo/bar/#
    lookup_topic(Topic, Trie, IsWildcard) ++ %% try match foo/bar
    Acc;
match_compact([Word | Words], Prefix, Trie, IsWildcard, Acc0) ->
    Acc1 = 'match_#'(Prefix, Trie) ++ Acc0,
    Acc = match_compact(Words, join(Prefix, Word), Trie, IsWildcard, Acc1),
    WildcardPrefix = join(Prefix, '+'),
    %% go deeper to match current_prefix/+ only when:
    %% 1. current word is the last
    %% OR
    %% 2. there is a prefix = 'current_prefix/+'
    case Words =:= [] orelse has_prefix(WildcardPrefix, Trie) of
        true -> match_compact(Words, WildcardPrefix, Trie, true, Acc);
        false -> Acc
    end.

'match_#'(Prefix, Trie) ->
    MlTopic = join(Prefix, '#'),
    lookup_topic(MlTopic, Trie).

is_compact() ->
    emqx:get_config([broker, perf, trie_compaction], true).

set_compact(Bool) ->
    emqx_config:put([broker, perf, trie_compaction], Bool).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

make_keys_test_() ->
    [{"no compact", fun() -> with_compact_flag(false, fun make_keys_no_compact/0) end},
     {"compact", fun() -> with_compact_flag(true, fun make_keys_compact/0) end}
    ].

make_keys_no_compact() ->
    ?assertEqual({?TOPIC(<<"#">>), []}, make_keys(<<"#">>)),
    ?assertEqual({?TOPIC(<<"a/+">>),
                  [?PREFIX(<<"a">>)]}, make_keys(<<"a/+">>)),
    ?assertEqual({?TOPIC(<<"+">>), []}, make_keys(<<"+">>)).

make_keys_compact() ->
    ?assertEqual({?TOPIC(<<"#">>), []}, make_keys(<<"#">>)),
    ?assertEqual({?TOPIC(<<"a/+">>), []}, make_keys(<<"a/+">>)),
    ?assertEqual({?TOPIC(<<"+">>), []}, make_keys(<<"+">>)),
    ?assertEqual({?TOPIC(<<"a/+/c">>),
                  [?PREFIX(<<"a/+">>)]}, make_keys(<<"a/+/c">>)).

words(T) -> emqx_topic:words(T).

make_prefixes_t(Topic) -> make_prefixes(words(Topic)).

with_compact_flag(IsCompact, F) ->
    OldV = is_compact(),
    set_compact(IsCompact),
    try F()
    after set_compact(OldV)
    end.

make_prefixes_test_() ->
    [{"no compact", fun() -> with_compact_flag(false, fun make_prefixes_no_compact/0) end},
     {"compact", fun() -> with_compact_flag(true, fun make_prefixes_compact/0) end}
    ].

make_prefixes_no_compact() ->
    ?assertEqual([<<"a/b">>, <<"a">>], make_prefixes_t(<<"a/b/+">>)),
    ?assertEqual([<<"a/b/+/c">>, <<"a/b/+">>, <<"a/b">>, <<"a">>],
                 make_prefixes_t(<<"a/b/+/c/#">>)).

make_prefixes_compact() ->
    ?assertEqual([], make_prefixes_t(<<"a/b/+">>)),
    ?assertEqual([<<"a/b/+">>], make_prefixes_t(<<"a/b/+/c/#">>)).

do_compact_test() ->
    ?assertEqual([<<"/+">>], do_compact(words(<<"/+">>))),
    ?assertEqual([<<"/#">>], do_compact(words(<<"/#">>))),
    ?assertEqual([<<"a/b/+">>, <<"c">>], do_compact(words(<<"a/b/+/c">>))),
    ?assertEqual([<<"a/+">>, <<"+">>, <<"b">>], do_compact(words(<<"a/+/+/b">>))),
    ?assertEqual([<<"a/+">>, <<"+">>, <<"+">>, <<"+">>, <<"b">>],
                 do_compact(words(<<"a/+/+/+/+/b">>))),
    ok.

clear_tables() -> ekka_mnesia:clear_table(?TRIE).

-endif. % TEST
