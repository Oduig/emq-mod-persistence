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
  initMnesia(Env),
  %%noinspection ErlangUnresolvedFunction
  emqttd:hook('client.subscribe', fun ?MODULE:on_client_subscribe/4, [Env]),
  %%noinspection ErlangUnresolvedFunction
  emqttd:hook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/4, [Env]),
  ClientTopicTables = loadPersistedSubscriptions(),
  lists:foreach(fun subscribeClientToTopics/1, ClientTopicTables).

on_client_subscribe(ClientId, Username, TopicTable, Env) ->
  io:format("*** PLUGIN **** called on_client_subscribe()~n", []),
  persistSubscription(ClientId, TopicTable),
  {ok, TopicTable}.

on_client_unsubscribe(ClientId, Username, TopicTable, Env) ->
  io:format("*** PLUGIN **** called on_client_unsubscribe()~n", []),
  forgetSubscription(ClientId, TopicTable),
  {ok, TopicTable}.

unload() ->
  io:format("*** PLUGIN **** called unload()~n", []),
  %%noinspection ErlangUnresolvedFunction
  emqttd:unhook('client.subscribe', fun ?MODULE:on_client_subscribe/4),
  %%noinspection ErlangUnresolvedFunction
  emqttd:unhook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/4).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

initMnesia(Env) ->
  ok.
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
  PersistedSubscriptions = [],
  io:format("*** PLUGIN *** done loading persisted subscriptions.~n", []),
  PersistedSubscriptions.

persistSubscription(ClientId, TopicTable) ->
  io:format("*** PLUGIN *** persisting subscription of ~s to topics ~p...~n", [ClientId, TopicTable]),
%%  TODO
%%  mnesia:dirty_write(#mqtt_persisted{topic = Topic, msg = Msg, ts = emqttd_time:now_ms(Ts)})
  io:format("*** PLUGIN *** done.~n", []).

forgetSubscription(ClientId, TopicTable) ->
  io:format("*** PLUGIN *** forgetting subscription of ~s to topics ~p...~n", [ClientId, TopicTable]),
%%  TODO
%%  mnesia:dirty_delete(mqtt_persisted, {ClientId, TopicTable}),
  io:format("*** PLUGIN *** done.~n", []).

%% Subscribe a client to a list of topics
%% TopicTable is a list of {Topic, Qos}
subscribeClientToTopics(ClientTopicTable) ->
  {ClientId, TopicTable} = ClientTopicTable,
  io:format("*** PLUGIN *** subscribing ~s to topics ~p...~n", [ClientId, TopicTable]),
%%  TODO
%%  emqttd_client:subscribe(ClientId, TopicTable),
  io:format("*** PLUGIN *** done.~n", []).
