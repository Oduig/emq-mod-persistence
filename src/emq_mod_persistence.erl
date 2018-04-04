-module(emq_mod_persistence).

-include_lib("emqttd/include/emqttd.hrl").

-include_lib("emqttd/include/emqttd_protocol.hrl").

-include_lib("emqttd/include/emqttd_internal.hrl").

-export([load/1, on_client_subscribe/4, on_client_unsubscribe/4, unload/0]).

-define(TAB, ?MODULE).

-record(mqtt_persisted, {clientId, topicTable}).

%%--------------------------------------------------------------------
%% Load/Unload Hook
%%--------------------------------------------------------------------

load(Env) ->
  io:format("*** PLUGIN *** called load()~n", []),
  initMnesia(),
  %%noinspection ErlangUnresolvedFunction
  emqttd:hook('client.subscribe', fun ?MODULE:on_client_subscribe/4, [Env]),
  %%noinspection ErlangUnresolvedFunction
  emqttd:hook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/4, [Env]),
  ClientTopicTables = loadPersistedSubscriptions(),
  lists:foreach(fun subscribeClientToTopics/1, ClientTopicTables).

on_client_subscribe(ClientId, Username, TopicTable, _) ->
  io:format("*** PLUGIN *** called on_client_subscribe() for user ~s~n", [Username]),
  case TopicTable of
    [{Topic, [{qos, 1}]}] -> persistSubscription(ClientId, Topic);
    [{Topic, [{qos, 0}]}] -> io:format("*** PLUGIN *** Ignored subscribe on ~s due to QoS 0.~n", [Topic]);
    _                     -> io:format("*** PLUGIN *** Ignored subscribe on ~p due to unexpected format.~n", [TopicTable])
  end,
  {ok, TopicTable}.

on_client_unsubscribe(ClientId, Username, TopicTable, _) ->
  io:format("*** PLUGIN *** called on_client_unsubscribe() for user ~s~n", [Username]),
  case TopicTable of
    [{Topic, _}] -> forgetSubscription(ClientId, Topic);
    _            -> io:format("*** PLUGIN *** Ignored unsubscribe on ~p due to unexpected format.~n", [TopicTable])
  end,
  {ok, TopicTable}.

unload() ->
  io:format("*** PLUGIN *** called unload()~n", []),
  %%noinspection ErlangUnresolvedFunction
  emqttd:unhook('client.subscribe', fun ?MODULE:on_client_subscribe/4),
  %%noinspection ErlangUnresolvedFunction
  emqttd:unhook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/4).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

initMnesia() ->
  ok.
%% TODO
%%  Copies = disc_copies,
%%  ok = ekka_mnesia:create_table(mqtt_persisted, [
%%    {type, set},
%%    {Copies, [node()]},
%%    {record_name, mqtt_persisted},
%%    {attributes, record_info(fields, mqtt_persisted)},
%%    {storage_properties, [{ets, [compressed]},
%%      {dets, [{auto_save, 1000}]}]}]),
%%  ok = ekka_mnesia:copy_table(mqtt_persisted),
%%  case mnesia:table_info(mqtt_persisted, storage_type) of
%%    Copies -> ok;
%%    _      -> {atomic, ok} = mnesia:change_table_copy_type(mqtt_persisted, node(), Copies)
%%  end.

loadPersistedSubscriptions() ->
  io:format("*** PLUGIN *** loading persisted subscriptions...~n", []),
%%  TODO
%%  PersistedSubscriptions = mnesia:dirty_read(mqtt_persisted),
  PersistedSubscriptions = [],
  io:format("*** PLUGIN *** done loading persisted subscriptions.~n", []),
  PersistedSubscriptions.

persistSubscription(ClientId, Topic) ->
  io:format("*** PLUGIN *** persisting subscription of ~s to topic ~s...~n", [ClientId, Topic]),
%%  TODO
%%  mnesia:dirty_write(#mqtt_persisted{clientId = ClientId, topic = Topic})
  io:format("*** PLUGIN *** done.~n", []).

forgetSubscription(ClientId, Topic) ->
  io:format("*** PLUGIN *** forgetting subscription of ~s to topic ~s...~n", [ClientId, Topic]),
%%  TODO
%%  mnesia:dirty_delete(mqtt_persisted, #mqtt_persisted{ClientId, Topic}),
  io:format("*** PLUGIN *** done.~n", []).

%% Subscribe a client to a list of topics
%% TopicTable is a list of {Topic, Qos}
subscribeClientToTopics(ClientTopic) ->
  {ClientId, Topic} = ClientTopic,
  io:format("*** PLUGIN *** subscribing ~s to topic ~s...~n", [ClientId, Topic]),
  %%noinspection ErlangUnresolvedFunction
  emqttd_client:subscribe(ClientId, [{Topic, [{qos, 1}]}]),
  io:format("*** PLUGIN *** done.~n", []).
