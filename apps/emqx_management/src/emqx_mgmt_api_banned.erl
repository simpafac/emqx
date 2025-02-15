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

-module(emqx_mgmt_api_banned).

-include_lib("emqx/include/emqx.hrl").

-include("emqx_mgmt.hrl").

-behaviour(minirest_api).

-export([api_spec/0]).

-export([ banned/2
        , delete_banned/2
        ]).

-import(emqx_mgmt_util, [ page_params/0
                        , schema/1
                        , object_schema/1
                        , page_object_schema/1
                        , properties/1
                        , error_schema/1
                        ]).

-export([format/1]).

-define(TAB, emqx_banned).

api_spec() ->
    {[banned_api(), delete_banned_api()], []}.

-define(BANNED_TYPES, [clientid, username, peerhost]).

properties() ->
    properties([
        {as, string, <<"Banned type clientid, username, peerhost">>, [clientid, username, peerhost]},
        {who, string, <<"Client info as banned type">>},
        {by, integer, <<"Commander">>},
        {reason, string, <<"Banned reason">>},
        {at, integer, <<"Create banned time. Nullable, rfc3339, default is now">>},
        {until, string, <<"Cancel banned time. Nullable, rfc3339, default is now + 5 minute">>}
    ]).

banned_api() ->
    Path = "/banned",
    MetaData = #{
        get => #{
            description => <<"List banned">>,
            parameters => page_params(),
            responses => #{
                <<"200">> =>
                    page_object_schema(properties())}},
        post => #{
            description => <<"Create banned">>,
            'requestBody' => object_schema(properties()),
            responses => #{
                <<"200">> => schema(<<"Create success">>)}}},
    {Path, MetaData, banned}.

delete_banned_api() ->
    Path = "/banned/:as/:who",
    MetaData = #{
        delete => #{
            description => <<"Delete banned">>,
            parameters => [
                #{
                    name => as,
                    in => path,
                    required => true,
                    description => <<"Banned type">>,
                    schema => #{type => string, enum => ?BANNED_TYPES}
                },
                #{
                    name => who,
                    in => path,
                    required => true,
                    description => <<"Client info as banned type">>,
                    schema => #{type => string}
                }
            ],
            responses => #{
                <<"200">> => schema(<<"Delete banned success">>),
                <<"404">> => error_schema(<<"Banned not found">>)}}},
    {Path, MetaData, delete_banned}.

banned(get, #{query_string := Params}) ->
    Response = emqx_mgmt_api:paginate(?TAB, Params, fun format/1),
    {200, Response};
banned(post, #{body := Body}) ->
    _ = emqx_banned:create(emqx_banned:parse(Body)),
    {200}.

delete_banned(delete, #{bindings := Params}) ->
    case emqx_banned:look_up(Params) of
        [] ->
            As0 = maps:get(as, Params),
            Who0 = maps:get(who, Params),
            Message = list_to_binary(io_lib:format("~p: ~p not found", [As0, Who0])),
            {404, #{code => 'RESOURCE_NOT_FOUND', message => Message}};
        _ ->
            ok = emqx_banned:delete(Params),
            {200}
    end.

format(Banned) ->
    emqx_banned:format(Banned).
