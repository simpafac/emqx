%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_persistent_session_SUITE).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("../src/emqx_persistent_session.hrl").

-compile(export_all).
-compile(nowarn_export_all).

%%--------------------------------------------------------------------
%% SUITE boilerplate
%%--------------------------------------------------------------------

all() ->
    [ {group, persistent_store_enabled}
    , {group, persistent_store_disabled}
    ].

%% A persistent session can be resumed in two ways:
%%    1. The old connection process is still alive, and the session is taken
%%       over by the new connection.
%%    2. The old session process has died (e.g., because of node down).
%%       The new process resumes the session from the stored state, and finds
%%       any subscribed messages from the persistent message store.
%%
%% We want to test both ways, both with the db backend enabled and disabled.
%%
%% In addition, we test both tcp and quic connections.

groups() ->
    TCs = emqx_ct:all(?MODULE),
    SnabbkaffeTCs = [TC || TC <- TCs, is_snabbkaffe_tc(TC)],
    GCTests  = [TC || TC <- TCs, is_gc_tc(TC)],
    OtherTCs = (TCs -- SnabbkaffeTCs) -- GCTests,
    [ {persistent_store_enabled, [ {group, no_kill_connection_process}
                                 , {group,    kill_connection_process}
                                 , {group, snabbkaffe}
                                 , {group, gc_tests}
                                 ]}
    , {persistent_store_disabled, [ {group, no_kill_connection_process}
                                  ]}
    , {no_kill_connection_process, [], [{group, tcp}, {group, quic}]}
    , {   kill_connection_process, [], [{group, tcp}, {group, quic}]}
    , {snabbkaffe, [], [{group, tcp_snabbkaffe}, {group, quic_snabbkaffe}]}
    , {tcp,  [], OtherTCs}
    , {quic, [], OtherTCs}
    , {tcp_snabbkaffe,  [], SnabbkaffeTCs}
    , {quic_snabbkaffe, [], SnabbkaffeTCs}
    , {gc_tests, [], GCTests}
    ].

is_snabbkaffe_tc(TC) ->
    re:run(atom_to_list(TC), "^t_snabbkaffe_") /= nomatch.

is_gc_tc(TC) ->
    re:run(atom_to_list(TC), "^t_gc_") /= nomatch.

init_per_group(persistent_store_enabled, Config) ->
    %% Start Apps
    emqx_ct_helpers:boot_modules(all),
    meck:new(emqx_config, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_config, get, fun(?is_enabled_key) -> true;
                                   (Other) -> meck:passthrough([Other])
                                end),
    emqx_ct_helpers:start_apps([], fun set_special_confs/1),
    Config;
init_per_group(persistent_store_disabled, Config) ->
    %% Start Apps
    emqx_ct_helpers:boot_modules(all),
    meck:new(emqx_config, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_config, get, fun(?is_enabled_key) -> false;
                                   (Other) -> meck:passthrough([Other])
                                end),
    emqx_ct_helpers:start_apps([], fun set_special_confs/1),
    Config;
init_per_group(Group, Config) when Group == tcp; Group == tcp_snabbkaffe ->
    [ {port, 1883}, {conn_fun, connect}| Config];
init_per_group(Group, Config) when Group == quic; Group == quic_snabbkaffe ->
    [ {port, 14567}, {conn_fun, quic_connect} | Config];
init_per_group(no_kill_connection_process, Config) ->
    [ {kill_connection_process, false} | Config];
init_per_group(kill_connection_process, Config) ->
    [ {kill_connection_process, true} | Config];
init_per_group(snabbkaffe, Config) ->
    [ {kill_connection_process, true} | Config];
init_per_group(gc_tests, Config) ->
    %% We need to make sure the system does not interfere with this test group.
    emqx_ct_helpers:stop_apps([]),
    Ets = gc_tests_store,
    Pid = spawn(fun() ->
                        ets:new(Ets, [named_table, public, ordered_set]),
                        receive stop -> ok end
                end),
    meck:new(mnesia, [non_strict, passthrough, no_history, no_link]),
    meck:expect(mnesia, dirty_first, fun(emqx_session_msg) -> ets:first(Ets);
                                        (X) -> meck:passtrhough(X)
                                     end),
    meck:expect(mnesia, dirty_next, fun(emqx_session_msg, X) -> ets:next(Ets, X);
                                       (Tab, X) -> meck:passtrough([Tab, X])
                                    end),
    [{store_owner, Pid}, {store, Ets} | Config].

init_per_suite(Config) ->
    Config.

set_special_confs(emqx) ->
    Path = emqx_ct_helpers:deps_path(emqx, "test/emqx_SUITE_data/loaded_plugins"),
    application:set_env(emqx, plugins_loaded_file, Path);
set_special_confs(_) ->
    ok.

end_per_suite(_Config) ->
    ok.

end_per_group(gc_tests, Config) ->
    meck:unload(mnesia),
    ?config(store_owner, Config) ! stop,
    ok;
end_per_group(persistent_store_enabled, _Config) ->
    meck:unload(emqx_config),
    emqx_ct_helpers:stop_apps([]);
end_per_group(persistent_store_disabled, _Config) ->
    meck:unload(emqx_config),
    emqx_ct_helpers:stop_apps([]);
end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    Config1 = preconfig_per_testcase(TestCase, Config),
    case is_gc_tc(TestCase) of
        true ->
            Store = ?config(store, Config),
            ets:delete_all_objects(Store);
        false ->
            skip
    end,
    case erlang:function_exported(?MODULE, TestCase, 2) of
        true -> ?MODULE:TestCase(init, Config1);
        _ -> Config1
    end.

end_per_testcase(TestCase, Config) ->
    case is_snabbkaffe_tc(TestCase) of
        true  -> snabbkaffe:stop();
        false -> skip
    end,
    case erlang:function_exported(?MODULE, TestCase, 2) of
        true -> ?MODULE:TestCase('end', Config);
        false -> ok
    end,
    Config.

preconfig_per_testcase(TestCase, Config) ->
    {BaseName, Config1} =
        case ?config(tc_group_properties, Config) of
            [] ->
                %% We are running a single testcase
                {atom_to_binary(TestCase),
                 init_per_group(tcp, init_per_group(kill_connection_process, Config))};
            [_|_] = Props->
                Path = lists:reverse(?config(tc_group_path, Config) ++ Props),
                Pre0 = [atom_to_list(N) || {name, N} <- lists:flatten(Path)],
                Pre1 = lists:join("_", Pre0 ++ [atom_to_binary(TestCase)]),
                {iolist_to_binary(Pre1),
                 Config}
        end,
    [ {topic, iolist_to_binary([BaseName, "/foo"])}
    , {stopic, iolist_to_binary([BaseName, "/+"])}
    , {stopic_alt, iolist_to_binary([BaseName, "/foo"])}
    , {client_id, BaseName}
    | Config1].

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

client_info(Key, Client) ->
    maps:get(Key, maps:from_list(emqtt:info(Client)), undefined).

receive_messages(Count) ->
    receive_messages(Count, []).

receive_messages(0, Msgs) ->
    Msgs;
receive_messages(Count, Msgs) ->
    receive
        {publish, Msg} ->
            receive_messages(Count-1, [Msg|Msgs]);
        _Other ->
            receive_messages(Count, Msgs)
    after 1000 ->
        Msgs
    end.

maybe_kill_connection_process(ClientId, Config) ->
    case ?config(kill_connection_process, Config) of
        true ->
            [ConnectionPid] = emqx_cm:lookup_channels(ClientId),
            ?assert(is_pid(ConnectionPid)),
            ConnectionPid ! die_if_test,
            ok;
        false ->
            ok
    end.

snabbkaffe_sync_publish(Topic, Payloads, Config) ->
    Fun = fun(Client, Payload) ->
                  ?wait_async_action( {ok, _} = emqtt:publish(Client, Topic, Payload, 2)
                                    , #{?snk_kind := ps_persist_msg, payload := Payload}
                                    )
          end,
    do_publish(Payloads, Fun, Config).

publish(Topic, Payloads, Config) ->
    Fun = fun(Client, Payload) ->
                  {ok, _} = emqtt:publish(Client, Topic, Payload, 2)
          end,
    do_publish(Payloads, Fun, Config).

do_publish(Payloads = [_|_], PublishFun, Config) ->
    %% Publish from another process to avoid connection confusion.
    {Pid, Ref} =
        spawn_monitor(
          fun() ->
                  ConnFun = ?config(conn_fun, Config),
                  {ok, Client} = emqtt:start_link([ {proto_ver, v5}
                                                  | Config]),
                  {ok, _} = emqtt:ConnFun(Client),
                  lists:foreach(fun(Payload) -> PublishFun(Client, Payload)end, Payloads),
                  ok = emqtt:disconnect(Client)
          end),
    receive
        {'DOWN', Ref, process, Pid, normal} -> ok;
        {'DOWN', Ref, process, Pid, What} -> error({failed_publish, What})
    end;
do_publish(Payload, PublishFun, Config) ->
    do_publish([Payload], PublishFun, Config).

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

%% [MQTT-3.1.2-23]
t_connect_session_expiry_interval(Config) ->
    ConnFun = ?config(conn_fun, Config),
    Topic = ?config(topic, Config),
    STopic = ?config(stopic, Config),
    Payload = <<"test message">>,
    ClientId = ?config(client_id, Config),

    {ok, Client1} = emqtt:start_link([ {clientid, ClientId},
                                       {proto_ver, v5},
                                       {properties, #{'Session-Expiry-Interval' => 30}}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client1),
    {ok, _, [2]} = emqtt:subscribe(Client1, STopic, qos2),
    ok = emqtt:disconnect(Client1),

    maybe_kill_connection_process(ClientId, Config),

    publish(Topic, Payload, Config),

    {ok, Client2} = emqtt:start_link([ {clientid, ClientId},
                                       {proto_ver, v5},
                                       {properties, #{'Session-Expiry-Interval' => 30}},
                                       {clean_start, false}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client2),
    [Msg | _ ] = receive_messages(1),
    ?assertEqual({ok, iolist_to_binary(Topic)}, maps:find(topic, Msg)),
    ?assertEqual({ok, iolist_to_binary(Payload)}, maps:find(payload, Msg)),
    ?assertEqual({ok, 2}, maps:find(qos, Msg)),
    ok = emqtt:disconnect(Client2).

t_without_client_id(Config) ->
    process_flag(trap_exit, true), %% Emqtt client dies
    ConnFun = ?config(conn_fun, Config),
    {ok, Client0} = emqtt:start_link([ {proto_ver, v5},
                                       {properties, #{'Session-Expiry-Interval' => 30}},
                                       {clean_start, false}
                                     | Config]),
    {error, {client_identifier_not_valid, _}} = emqtt:ConnFun(Client0),
    ok.

t_assigned_clientid_persistent_session(Config) ->
    ConnFun = ?config(conn_fun, Config),
    {ok, Client1} = emqtt:start_link([ {proto_ver, v5},
                                       {properties, #{'Session-Expiry-Interval' => 30}},
                                       {clean_start, true}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client1),

    AssignedClientId = client_info(clientid, Client1),
    ok = emqtt:disconnect(Client1),

    maybe_kill_connection_process(AssignedClientId, Config),

    {ok, Client2} = emqtt:start_link([ {clientid, AssignedClientId},
                                       {proto_ver, v5},
                                       {clean_start, false}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client2),
    ?assertEqual(1, client_info(session_present, Client2)),
    ok = emqtt:disconnect(Client2).

t_cancel_on_disconnect(Config) ->
    %% Open a persistent session, but cancel the persistence when
    %% shutting down the connection.
    ConnFun = ?config(conn_fun, Config),
    ClientId = ?config(client_id, Config),

    {ok, Client1} = emqtt:start_link([ {proto_ver, v5},
                                       {clientid, ClientId},
                                       {properties, #{'Session-Expiry-Interval' => 30}},
                                       {clean_start, true}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client1),
    ok = emqtt:disconnect(Client1, 0, #{'Session-Expiry-Interval' => 0}),

    {ok, Client2} = emqtt:start_link([ {clientid, ClientId},
                                       {proto_ver, v5},
                                       {clean_start, false},
                                       {properties, #{'Session-Expiry-Interval' => 30}}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client2),
    ?assertEqual(0, client_info(session_present, Client2)),
    ok = emqtt:disconnect(Client2).

t_persist_on_disconnect(Config) ->
    %% Open a non-persistent session, but add the persistence when
    %% shutting down the connection. This is a protocol error, and
    %% should not convert the session into a persistent session.
    ConnFun = ?config(conn_fun, Config),
    ClientId = ?config(client_id, Config),

    {ok, Client1} = emqtt:start_link([ {proto_ver, v5},
                                       {clientid, ClientId},
                                       {properties, #{'Session-Expiry-Interval' => 0}},
                                       {clean_start, true}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client1),

    %% Strangely enough, the disconnect is reported as successful by emqtt.
    ok = emqtt:disconnect(Client1, 0, #{'Session-Expiry-Interval' => 30}),

    {ok, Client2} = emqtt:start_link([ {clientid, ClientId},
                                       {proto_ver, v5},
                                       {clean_start, false},
                                       {properties, #{'Session-Expiry-Interval' => 30}}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client2),
    %% The session should not be known, since it wasn't persisted because of the
    %% changed expiry interval in the disconnect call.
    ?assertEqual(0, client_info(session_present, Client2)),
    ok = emqtt:disconnect(Client2).

t_process_dies_session_expires(Config) ->
    %% Emulate an error in the connect process,
    %% or that the node of the process goes down.
    %% A persistent session should eventually expire.
    ConnFun = ?config(conn_fun, Config),
    ClientId = ?config(client_id, Config),
    {ok, Client1} = emqtt:start_link([ {proto_ver, v5},
                                       {clientid, ClientId},
                                       {properties, #{'Session-Expiry-Interval' => 1}},
                                       {clean_start, true}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client1),
    ok = emqtt:disconnect(Client1),

    maybe_kill_connection_process(ClientId, Config),

    timer:sleep(1000),

    {ok, Client2} = emqtt:start_link([ {proto_ver, v5},
                                       {clientid, ClientId},
                                       {clean_start, false}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client2),
    ?assertEqual(0, client_info(session_present, Client2)),
    emqtt:disconnect(Client2).

t_publish_while_client_is_gone(Config) ->
    %% A persistent session should receive messages in its
    %% subscription even if the process owning the session dies.
    ConnFun = ?config(conn_fun, Config),
    Topic = ?config(topic, Config),
    STopic = ?config(stopic, Config),
    Payload1 = <<"hello1">>,
    Payload2 = <<"hello2">>,
    ClientId = ?config(client_id, Config),
    {ok, Client1} = emqtt:start_link([ {proto_ver, v5},
                                       {clientid, ClientId},
                                       {properties, #{'Session-Expiry-Interval' => 30}},
                                       {clean_start, true}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client1),
    {ok, _, [2]} = emqtt:subscribe(Client1, STopic, qos2),

    ok = emqtt:disconnect(Client1),
    maybe_kill_connection_process(ClientId, Config),

    ok = publish(Topic, [Payload1, Payload2], Config),

    {ok, Client2} = emqtt:start_link([ {proto_ver, v5},
                                       {clientid, ClientId},
                                       {properties, #{'Session-Expiry-Interval' => 30}},
                                       {clean_start, false}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client2),
    [Msg1] = receive_messages(1),
    [Msg2] = receive_messages(1),
    ?assertEqual({ok, iolist_to_binary(Payload1)}, maps:find(payload, Msg1)),
    ?assertEqual({ok, 2}, maps:find(qos, Msg1)),
    ?assertEqual({ok, iolist_to_binary(Payload2)}, maps:find(payload, Msg2)),
    ?assertEqual({ok, 2}, maps:find(qos, Msg2)),

    ok = emqtt:disconnect(Client2).

t_clean_start_drops_subscriptions(Config) ->
    %% 1. A persistent session is started and disconnected.
    %% 2. While disconnected, a message is published and persisted.
    %% 3. When connecting again, the clean start flag is set, the subscription is renewed,
    %%    then we disconnect again.
    %% 4. Finally, a new connection is made with clean start set to false.
    %% The original message should not be delivered.

    ConnFun = ?config(conn_fun, Config),
    Topic = ?config(topic, Config),
    STopic = ?config(stopic, Config),
    Payload1 = <<"hello1">>,
    Payload2 = <<"hello2">>,
    Payload3 = <<"hello3">>,
    ClientId = ?config(client_id, Config),

    %% 1.
    {ok, Client1} = emqtt:start_link([ {proto_ver, v5},
                                       {clientid, ClientId},
                                       {properties, #{'Session-Expiry-Interval' => 30}},
                                       {clean_start, true}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client1),
    {ok, _, [2]} = emqtt:subscribe(Client1, STopic, qos2),

    ok = emqtt:disconnect(Client1),
    maybe_kill_connection_process(ClientId, Config),

    %% 2.
    ok = publish(Topic, Payload1, Config),

    %% 3.
    {ok, Client2} = emqtt:start_link([ {proto_ver, v5},
                                       {clientid, ClientId},
                                       {properties, #{'Session-Expiry-Interval' => 30}},
                                       {clean_start, true}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client2),
    ?assertEqual(0, client_info(session_present, Client2)),
    {ok, _, [2]} = emqtt:subscribe(Client2, STopic, qos2),

    ok = publish(Topic, Payload2, Config),
    [Msg1] = receive_messages(1),
    ?assertEqual({ok, iolist_to_binary(Payload2)}, maps:find(payload, Msg1)),

    ok = emqtt:disconnect(Client2),
    maybe_kill_connection_process(ClientId, Config),

    %% 4.
    {ok, Client3} = emqtt:start_link([ {proto_ver, v5},
                                       {clientid, ClientId},
                                       {properties, #{'Session-Expiry-Interval' => 30}},
                                       {clean_start, false}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client3),

    ok = publish(Topic, Payload3, Config),
    [Msg2] = receive_messages(1),
    ?assertEqual({ok, iolist_to_binary(Payload3)}, maps:find(payload, Msg2)),

    ok = emqtt:disconnect(Client3).


t_multiple_subscription_matches(Config) ->
    ConnFun = ?config(conn_fun, Config),
    Topic = ?config(topic, Config),
    STopic1 = ?config(stopic, Config),
    STopic2 = ?config(stopic_alt, Config),
    Payload = <<"test message">>,
    ClientId = ?config(client_id, Config),

    {ok, Client1} = emqtt:start_link([ {clientid, ClientId},
                                       {proto_ver, v5},
                                       {properties, #{'Session-Expiry-Interval' => 30}}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client1),
    {ok, _, [2]} = emqtt:subscribe(Client1, STopic1, qos2),
    {ok, _, [2]} = emqtt:subscribe(Client1, STopic2, qos2),
    ok = emqtt:disconnect(Client1),

    maybe_kill_connection_process(ClientId, Config),

    publish(Topic, Payload, Config),

    {ok, Client2} = emqtt:start_link([ {clientid, ClientId},
                                       {proto_ver, v5},
                                       {properties, #{'Session-Expiry-Interval' => 30}},
                                       {clean_start, false}
                                     | Config]),
    {ok, _} = emqtt:ConnFun(Client2),

    %% We will receive the same message twice because it matches two subscriptions.
    [Msg1, Msg2] = receive_messages(2),
    ?assertEqual({ok, iolist_to_binary(Topic)}, maps:find(topic, Msg1)),
    ?assertEqual({ok, iolist_to_binary(Payload)}, maps:find(payload, Msg1)),
    ?assertEqual({ok, iolist_to_binary(Topic)}, maps:find(topic, Msg2)),
    ?assertEqual({ok, iolist_to_binary(Payload)}, maps:find(payload, Msg2)),
    ?assertEqual({ok, 2}, maps:find(qos, Msg1)),
    ?assertEqual({ok, 2}, maps:find(qos, Msg2)),
    ok = emqtt:disconnect(Client2).



%%--------------------------------------------------------------------
%% Snabbkaffe helpers
%%--------------------------------------------------------------------

check_snabbkaffe_vanilla(Trace) ->
    ResumeTrace = [T || #{?snk_kind := K} = T <- Trace,
                        re:run(atom_to_list(K), "^ps_") /= nomatch],
    ?assertMatch([_|_], ResumeTrace),
    [_Sid] = lists:usort(?projection(sid, ResumeTrace)),
    %% Check internal flow of the emqx_cm resuming
    ?assert(?strict_causality(#{ ?snk_kind := ps_resuming },
                              #{ ?snk_kind := ps_initial_pendings },
                              ResumeTrace)),
    ?assert(?strict_causality(#{ ?snk_kind := ps_initial_pendings },
                              #{ ?snk_kind := ps_persist_pendings },
                              ResumeTrace)),
    ?assert(?strict_causality(#{ ?snk_kind := ps_persist_pendings },
                              #{ ?snk_kind := ps_notify_writers },
                              ResumeTrace)),
    ?assert(?strict_causality(#{ ?snk_kind := ps_notify_writers },
                              #{ ?snk_kind := ps_resume_session },
                              ResumeTrace)),
    ?assert(?strict_causality(#{ ?snk_kind := ps_resume_session },
                              #{ ?snk_kind := ps_marker_pendings },
                              ResumeTrace)),
    ?assert(?strict_causality(#{ ?snk_kind := ps_marker_pendings },
                              #{ ?snk_kind := ps_resume_end },
                              ResumeTrace)),

    %% Check flow between worker and emqx_cm
    ?assert(?strict_causality(#{ ?snk_kind := ps_notify_writers },
                              #{ ?snk_kind := ps_worker_started },
                              ResumeTrace)),
    ?assert(?strict_causality(#{ ?snk_kind := ps_marker_pendings },
                              #{ ?snk_kind := ps_worker_resume_end },
                              ResumeTrace)),
    ?assert(?strict_causality(#{ ?snk_kind := ps_worker_resume_end },
                              #{ ?snk_kind := ps_worker_shutdown },
                              ResumeTrace)).

%%--------------------------------------------------------------------
%% Snabbkaffe tests
%%--------------------------------------------------------------------

t_snabbkaffe_vanilla_stages(Config) ->
    %% Test that all stages of session resume works ok in the simplest case
    process_flag(trap_exit, true),
    ConnFun = ?config(conn_fun, Config),
    ClientId = ?config(client_id, Config),
    EmqttOpts = [ {proto_ver, v5},
                  {clientid, ClientId},
                  {properties, #{'Session-Expiry-Interval' => 30}}
                | Config],
    {ok, Client1} = emqtt:start_link([{clean_start, true}|EmqttOpts]),
    {ok, _} = emqtt:ConnFun(Client1),
    ok = emqtt:disconnect(Client1),
    maybe_kill_connection_process(ClientId, Config),

    ?check_trace(
       begin
           {ok, Client2} = emqtt:start_link([{clean_start, false}|EmqttOpts]),
           {ok, _} = emqtt:ConnFun(Client2),
           ok = emqtt:disconnect(Client2)
       end,
       fun(ok, Trace) ->
               check_snabbkaffe_vanilla(Trace)
       end),
    ok.

t_snabbkaffe_pending_messages(Config) ->
    %% Make sure there are pending messages are fetched during the init stage.
    process_flag(trap_exit, true),
    ConnFun = ?config(conn_fun, Config),
    ClientId = ?config(client_id, Config),
    Topic = ?config(topic, Config),
    STopic = ?config(stopic, Config),
    Payloads = [<<"test", (integer_to_binary(X))/binary>> || X <- [1,2,3,4,5]],
    EmqttOpts = [ {proto_ver, v5},
                  {clientid, ClientId},
                  {properties, #{'Session-Expiry-Interval' => 30}}
                | Config],
    {ok, Client1} = emqtt:start_link([{clean_start, true} | EmqttOpts]),
    {ok, _} = emqtt:ConnFun(Client1),
    {ok, _, [2]} = emqtt:subscribe(Client1, STopic, qos2),
    ok = emqtt:disconnect(Client1),
    maybe_kill_connection_process(ClientId, Config),


    ?check_trace(
       begin
           snabbkaffe_sync_publish(Topic, Payloads, Config),
           {ok, Client2} = emqtt:start_link([{clean_start, false} | EmqttOpts]),
           {ok, _} = emqtt:ConnFun(Client2),
           Msgs = receive_messages(length(Payloads)),
           ReceivedPayloads = [P || #{ payload := P } <- Msgs],
           ?assertEqual(lists:sort(ReceivedPayloads), lists:sort(Payloads)),
           ok = emqtt:disconnect(Client2)
       end,
       fun(ok, Trace) ->
               check_snabbkaffe_vanilla(Trace),
               %% Check that all messages was delivered from the DB
               [Delivers1] = ?projection(msgs, ?of_kind(ps_persist_pendings_msgs, Trace)),
               [Delivers2] = ?projection(msgs, ?of_kind(ps_marker_pendings_msgs, Trace)),
               Delivers = Delivers1 ++ Delivers2,
               ?assertEqual(length(Payloads), length(Delivers)),
               %% Check for no duplicates
               ?assertEqual(lists:usort(Delivers), lists:sort(Delivers))
       end),
    ok.

t_snabbkaffe_buffered_messages(Config) ->
    %% Make sure to buffer messages during startup.
    process_flag(trap_exit, true),
    ConnFun = ?config(conn_fun, Config),
    ClientId = ?config(client_id, Config),
    Topic = ?config(topic, Config),
    STopic = ?config(stopic, Config),
    Payloads1 = [<<"test", (integer_to_binary(X))/binary>> || X <- [1, 2, 3]],
    Payloads2 = [<<"test", (integer_to_binary(X))/binary>> || X <- [4, 5, 6]],
    EmqttOpts = [ {proto_ver, v5},
                  {clientid, ClientId},
                  {properties, #{'Session-Expiry-Interval' => 30}}
                | Config],
    {ok, Client1} = emqtt:start_link([{clean_start, true} | EmqttOpts]),
    {ok, _} = emqtt:ConnFun(Client1),
    {ok, _, [2]} = emqtt:subscribe(Client1, STopic, qos2),
    ok = emqtt:disconnect(Client1),
    maybe_kill_connection_process(ClientId, Config),

    publish(Topic, Payloads1, Config),

    ?check_trace(
       begin
           %% Make the resume init phase wait until the first message is delivered.
           ?force_ordering( #{ ?snk_kind := ps_worker_deliver },
                            #{ ?snk_kind := ps_resume_end }),
           spawn_link(fun() ->
                              ?block_until(#{ ?snk_kind := ps_marker_pendings_msgs }, infinity, 5000),
                              publish(Topic, Payloads2, Config)
                      end),
           {ok, Client2} = emqtt:start_link([{clean_start, false} | EmqttOpts]),
           {ok, _} = emqtt:ConnFun(Client2),
           Msgs = receive_messages(length(Payloads1) + length(Payloads2) + 1),
           ReceivedPayloads = [P || #{ payload := P } <- Msgs],
           ?assertEqual(lists:sort(Payloads1 ++ Payloads2),
                        lists:sort(ReceivedPayloads)),
           ok = emqtt:disconnect(Client2)
       end,
       fun(ok, Trace) ->
               check_snabbkaffe_vanilla(Trace),
               %% Check that some messages was buffered in the writer process
               [Msgs] = ?projection(msgs, ?of_kind(ps_writer_pendings, Trace)),
               ?assertMatch(X when 0 < X andalso X =< length(Payloads2),
                            length(Msgs))
       end),
    ok.

%%--------------------------------------------------------------------
%% GC tests
%%--------------------------------------------------------------------

-define(MARKER, 3).
-define(DELIVERED, 2).
-define(UNDELIVERED, 1).
-define(ABANDONED, 0).

msg_id() ->
    emqx_guid:gen().

delivered_msg(MsgId, SessionID, STopic) ->
    {SessionID, MsgId, STopic, ?DELIVERED}.

undelivered_msg(MsgId, SessionID, STopic) ->
    {SessionID, MsgId, STopic, ?UNDELIVERED}.

marker_msg(MarkerID, SessionID) ->
    {SessionID, MarkerID, <<>>, ?MARKER}.

guid(MicrosecondsAgo) ->
    %% Make a fake GUID and set a timestamp.
    << TS:64, Tail:64 >> = emqx_guid:gen(),
    << (TS - MicrosecondsAgo) : 64, Tail:64 >>.

abandoned_session_msg(SessionID) ->
    abandoned_session_msg(SessionID, 0).

abandoned_session_msg(SessionID, MicrosecondsAgo) ->
    TS = erlang:system_time(microsecond),
    {SessionID, <<>>, <<(TS - MicrosecondsAgo) : 64>>, ?ABANDONED}.

fresh_gc_delete_fun() ->
    Ets = ets:new(gc_collect, [ordered_set]),
    fun(delete,  Key) -> ets:insert(Ets, {Key}), ok;
       (collect, <<>>) -> List = ets:match(Ets, {'$1'}), ets:delete(Ets), lists:append(List);
       (_, _Key) -> ok
    end.

fresh_gc_callbacks_fun() ->
    Ets = ets:new(gc_collect, [ordered_set]),
    fun(collect, <<>>) -> List = ets:match(Ets, {'$1'}), ets:delete(Ets), lists:append(List);
       (Tag, Key) -> ets:insert(Ets, {{Key, Tag}}), ok
    end.

get_gc_delete_messages() ->
    Fun = fresh_gc_delete_fun(),
    emqx_persistent_session:gc_session_messages(Fun),
    Fun(collect, <<>>).

get_gc_callbacks() ->
    Fun = fresh_gc_callbacks_fun(),
    emqx_persistent_session:gc_session_messages(Fun),
    Fun(collect, <<>>).


t_gc_all_delivered(Config) ->
    Store = ?config(store, Config),
    STopic = ?config(stopic, Config),
    SessionId = emqx_guid:gen(),
    MsgIds = [msg_id() || _ <- lists:seq(1, 5)],
    Delivered = [delivered_msg(X, SessionId, STopic) || X <- MsgIds],
    Undelivered = [undelivered_msg(X, SessionId, STopic) || X <- MsgIds],
    SortedContent = lists:usort(Delivered ++ Undelivered),
    ets:insert(Store, [{X, <<>>} || X <- SortedContent]),
    GCMessages = get_gc_delete_messages(),
    ?assertEqual(SortedContent, GCMessages),
    ok.

t_gc_some_undelivered(Config) ->
    Store = ?config(store, Config),
    STopic = ?config(stopic, Config),
    SessionId = emqx_guid:gen(),
    MsgIds = [msg_id() || _ <- lists:seq(1, 10)],
    Delivered = [delivered_msg(X, SessionId, STopic) || X <- MsgIds],
    {Delivered1,_Delivered2} = split(Delivered),
    Undelivered = [undelivered_msg(X, SessionId, STopic) || X <- MsgIds],
    {Undelivered1, Undelivered2} = split(Undelivered),
    Content = Delivered1 ++ Undelivered1 ++ Undelivered2,
    ets:insert(Store, [{X, <<>>} || X <- Content]),
    Expected = lists:usort(Delivered1 ++ Undelivered1),
    GCMessages = get_gc_delete_messages(),
    ?assertEqual(Expected, GCMessages),
    ok.

t_gc_with_markers(Config) ->
    Store = ?config(store, Config),
    STopic = ?config(stopic, Config),
    SessionId = emqx_guid:gen(),
    MsgIds1 = [msg_id() || _ <- lists:seq(1, 10)],
    MarkerId = msg_id(),
    MsgIds = [msg_id() || _ <- lists:seq(1, 4)] ++ MsgIds1,
    Delivered = [delivered_msg(X, SessionId, STopic) || X <- MsgIds],
    {Delivered1,_Delivered2} = split(Delivered),
    Undelivered = [undelivered_msg(X, SessionId, STopic) || X <- MsgIds],
    {Undelivered1, Undelivered2} = split(Undelivered),
    Markers = [marker_msg(MarkerId, SessionId)],
    Content = Delivered1 ++ Undelivered1 ++ Undelivered2 ++ Markers,
    ets:insert(Store, [{X, <<>>} || X <- Content]),
    Expected = lists:usort(Delivered1 ++ Undelivered1),
    GCMessages = get_gc_delete_messages(),
    ?assertEqual(Expected, GCMessages),
    ok.

t_gc_abandoned_some_undelivered(Config) ->
    Store = ?config(store, Config),
    STopic = ?config(stopic, Config),
    SessionId = emqx_guid:gen(),
    MsgIds = [msg_id() || _ <- lists:seq(1, 10)],
    Delivered = [delivered_msg(X, SessionId, STopic) || X <- MsgIds],
    {Delivered1,_Delivered2} = split(Delivered),
    Undelivered = [undelivered_msg(X, SessionId, STopic) || X <- MsgIds],
    {Undelivered1, Undelivered2} = split(Undelivered),
    Abandoned = abandoned_session_msg(SessionId),
    Content = Delivered1 ++ Undelivered1 ++ Undelivered2 ++ [Abandoned],
    ets:insert(Store, [{X, <<>>} || X <- Content]),
    Expected = lists:usort(Delivered1 ++ Undelivered1 ++ Undelivered2),
    GCMessages = get_gc_delete_messages(),
    ?assertEqual(Expected, GCMessages),
    ok.

t_gc_abandoned_only_called_on_empty_session(Config) ->
    Store = ?config(store, Config),
    STopic = ?config(stopic, Config),
    SessionId = emqx_guid:gen(),
    MsgIds = [msg_id() || _ <- lists:seq(1, 10)],
    Delivered = [delivered_msg(X, SessionId, STopic) || X <- MsgIds],
    Undelivered = [undelivered_msg(X, SessionId, STopic) || X <- MsgIds],
    Abandoned = abandoned_session_msg(SessionId),
    Content = Delivered ++ Undelivered ++ [Abandoned],
    ets:insert(Store, [{X, <<>>} || X <- Content]),
    GCMessages = get_gc_callbacks(),

    %% Since we had messages to delete, we don't expect to get the
    %% callback on the abandoned session
    ?assertEqual([], [ X || {X, abandoned} <- GCMessages]),

    %% But if we have only the abandoned session marker for this
    %% session, it should be called.
    ets:delete_all_objects(Store),
    UndeliveredOtherSession = undelivered_msg(msg_id(), emqx_guid:gen(), <<"topic">>),
    ets:insert(Store, [{X, <<>>} || X <- [Abandoned, UndeliveredOtherSession]]),
    GCMessages2 = get_gc_callbacks(),
    ?assertEqual([Abandoned], [ X || {X, abandoned} <- GCMessages2]),
    ok.

t_session_gc_worker(init, Config) ->
    meck:new(emqx_persistent_session, [passthrough, no_link]),
    Config;
t_session_gc_worker('end',_Config) ->
    meck:unload(emqx_persistent_session),
    ok.

t_session_gc_worker(Config) ->
    STopic = ?config(stopic, Config),
    SessionID = emqx_guid:gen(),
    MsgDeleted = delivered_msg(msg_id(), SessionID, STopic),
    MarkerNotDeleted = marker_msg(msg_id(), SessionID),
    MarkerDeleted = marker_msg(guid(120 * 1000 * 1000), SessionID),
    AbandonedNotDeleted = abandoned_session_msg(SessionID),
    AbandonedDeleted = abandoned_session_msg(SessionID, 500 * 1000 * 1000),
    meck:expect(emqx_persistent_session, delete_session_message, fun(_Key) -> ok end),
    emqx_persistent_session_gc:session_gc_worker(delete, MsgDeleted),
    emqx_persistent_session_gc:session_gc_worker(marker, MarkerNotDeleted),
    emqx_persistent_session_gc:session_gc_worker(marker, MarkerDeleted),
    emqx_persistent_session_gc:session_gc_worker(abandoned, AbandonedDeleted),
    emqx_persistent_session_gc:session_gc_worker(abandoned, AbandonedNotDeleted),
    History = meck:history(emqx_persistent_session, self()),
    DeleteCalls = [ Key || {_Pid, {_, delete_session_message, [Key]}, _Result}
                               <- History],
    ?assertEqual(lists:sort([MsgDeleted, AbandonedDeleted, MarkerDeleted]),
                 lists:sort(DeleteCalls)),


    ok.


split(List) ->
    split(List, [], []).

split([], L1, L2) ->
    {L1, L2};
split([H], L1, L2) ->
    {[H|L1], L2};
split([H1, H2|Left], L1, L2) ->
    split(Left, [H1|L1], [H2|L2]).

