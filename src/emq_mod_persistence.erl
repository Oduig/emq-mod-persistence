%%--------------------------------------------------------------------
%% Copyright (c) 2012-2017 Feng Lee <feng@emqtt.io>.
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

-module(emq_mod_persistence).

-author("Feng Lee <feng@emqtt.io>").

-include_lib("emqttd/include/emqttd.hrl").

-include_lib("emqttd/include/emqttd_protocol.hrl").

-export([load/1, on_client_subscribe/4, on_client_unsubscribe/4, unload/0]).

-define(TAB, ?MODULE).

%%--------------------------------------------------------------------
%% Load/Unload Hook
%%--------------------------------------------------------------------

load(Env) ->
    lager:warning("*** PLUGIN **** called load()"),
    emqttd:hook('client.subscribe', fun ?MODULE:on_client_subscribe/3, [Env]),
    emqttd:hook('client.unsubscribe', fun ?MODULE:on_client_subscribe/3, [Env]).

on_client_subscribe(ClientId, Username, TopicTable, _Env) ->
    lager:warning("*** PLUGIN **** called on_client_subscribe()"),
    {ok, TopicTable}.

on_client_unsubscribe(ClientId, Username, TopicTable, _Env) ->
    lager:warning("*** PLUGIN **** called on_client_unsubscribe()"),
    {ok, TopicTable}.

unload() ->
    lager:warning("*** PLUGIN **** called unload()"),
    emqttd:unhook('client.subscribe', fun ?MODULE:on_client_subscribe/3),
    emqttd:unhook('client.unsubscribe', fun ?MODULE:on_client_subscribe/3).

