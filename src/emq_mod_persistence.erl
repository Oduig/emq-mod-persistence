-module(emq_mod_persistence).

-include_lib("emqttd/include/emqttd.hrl").

-include_lib("emqttd/include/emqttd_protocol.hrl").

-include_lib("emqttd/include/emqttd_internal.hrl").

-export([load/1, on_client_subscribe/4, on_client_unsubscribe/4, unload/0]).

-define(TAB, ?MODULE).

%%--------------------------------------------------------------------
%% Load/Unload Hook
%%--------------------------------------------------------------------

load(Env) ->
    io:format("*** PLUGIN **** called load()~n", []),
    emqttd:hook('client.subscribe', fun ?MODULE:on_client_subscribe/4, [Env]),
    emqttd:hook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/4, [Env]).

on_client_subscribe(ClientId, Username, TopicTable, _Env) ->
    io:format("*** PLUGIN **** called on_client_subscribe()~n", []),
    {ok, TopicTable}.

on_client_unsubscribe(ClientId, Username, TopicTable, _Env) ->
    io:format("*** PLUGIN **** called on_client_unsubscribe()~n", []),
    {ok, TopicTable}.

unload() ->
    io:format("*** PLUGIN **** called unload()~n", []),
    emqttd:unhook('client.subscribe', fun ?MODULE:on_client_subscribe/4),
    emqttd:unhook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/4).

