%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_gateway_app).

-behaviour(application).

-include_lib("emqx/include/logger.hrl").

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_gateway_sup:start_link(),
    emqx_gateway_cli:load(),
    load_default_gateway_applications(),
    load_gateway_by_default(),
    emqx_gateway_conf:load(),
    {ok, Sup}.

stop(_State) ->
    emqx_gateway_conf:unload(),
    emqx_gateway_cli:unload(),
    ok.

%%--------------------------------------------------------------------
%% Internal funcs

load_default_gateway_applications() ->
    Apps = gateway_type_searching(),
    ?LOG(info, "Starting the default gateway types: ~p", [Apps]),
    lists:foreach(fun reg/1, Apps).

gateway_type_searching() ->
    %% FIXME: Hardcoded apps
    [emqx_stomp_impl, emqx_sn_impl, emqx_exproto_impl,
     emqx_coap_impl, emqx_lwm2m_impl].

reg(Mod) ->
    try
        Mod:reg(),
        ?LOG(info, "Register ~s gateway application successfully!", [Mod])
    catch
        Class : Reason : Stk ->
            ?LOG(error, "Failed to register ~s gateway application: {~p, ~p}\n"
                        "Stacktrace: ~0p",
                        [Mod, Class, Reason, Stk])
    end.

load_gateway_by_default() ->
    load_gateway_by_default(confs()).

load_gateway_by_default([]) ->
    ok;
load_gateway_by_default([{Type, Confs}|More]) ->
    case emqx_gateway_registry:lookup(Type) of
        undefined ->
            ?LOG(error, "Skip to load ~s gateway, because it is not registered",
                        [Type]);
        _ ->
            case emqx_gateway:load(Type, Confs) of
                {ok, _} ->
                    ?LOG(debug, "Load ~s gateway successfully!", [Type]);
                {error, Reason} ->
                    ?LOG(error, "Failed to load ~s gateway: ~0p", [Type, Reason])
            end
    end,
    load_gateway_by_default(More).

confs() ->
    maps:to_list(emqx:get_config([gateway], #{})).
