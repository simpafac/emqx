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
-module(emqx_persistent_session_gc).

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        ]).

-ifdef(TEST).
-export([ session_gc_worker/2
        ]).
-endif.

-define(SERVER, ?MODULE).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    process_flag(trap_exit, true),
    {ok, start_session_gc_timer(#{})}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({timeout, Ref, session_gc_timeout}, State) ->
    State1 = session_gc_timeout(Ref, State),
    {noreply, State1};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Session messages GC
%%--------------------------------------------------------------------

start_session_gc_timer(State) ->
    Interval = emqx_config:get([persistent_session_store, session_message_gc_interval]),
    State#{ session_gc_timer => erlang:start_timer(Interval, self(), session_gc_timeout)}.

session_gc_timeout(Ref, #{ session_gc_timer := R } = State) when R =:= Ref ->
    %% Prevent overlapping processes.
    GCPid = maps:get(gc_pid, State, undefined),
    case GCPid =/= undefined andalso erlang:is_process_alive(GCPid) of
        true  -> start_session_gc_timer(State);
        false -> start_session_gc_timer(State#{ gc_pid => proc_lib:spawn_link(fun session_gc_worker/0)})
    end;
session_gc_timeout(_Ref, State) ->
    State.

session_gc_worker() ->
    ok = emqx_persistent_session:gc_session_messages(fun session_gc_worker/2).

%% TODO: Maybe these should be configurable?
-define(MARKER_GRACE_PERIOD, 60000000).
-define(ABANDONED_GRACE_PERIOD, 300000000).

session_gc_worker(Tag, Key) ->
    case Tag of
        delete ->
            emqx_persistent_session:delete_session_message(Key);
        marker ->
            TS = emqx_persistent_session:session_message_info(timestamp, Key),
            case TS + ?MARKER_GRACE_PERIOD < erlang:system_time(microsecond) of
                true  ->
                    emqx_persistent_session:delete_session_message(Key);
                false -> ok
            end;
        abandoned ->
            TS = emqx_persistent_session:session_message_info(timestamp, Key),
            case TS + ?ABANDONED_GRACE_PERIOD < erlang:system_time(microsecond) of
                true  ->
                    emqx_persistent_session:delete_session_message(Key);
                false -> ok
            end
    end.

%%--------------------------------------------------------------------
%% Message GC
%%--------------------------------------------------------------------
