-module(emq_mod_persistence).

-include_lib("emqttd/include/emqttd.hrl").

-include_lib("emqttd/include/emqttd_protocol.hrl").

-include_lib("emqttd/include/emqttd_internal.hrl").

-export([load/1, on_client_subscribe/4, on_client_unsubscribe/4, on_session_subscribed/4, on_message_publish/2, unload/0]).

-define(TAB, ?MODULE).

-record(mqtt_persisted_subscription, {clientId, topic}).
-record(mqtt_persisted_message, {clientId, message}).

%%--------------------------------------------------------------------
%% Load/Unload Hook
%%--------------------------------------------------------------------

load(Env) ->
  io:format("*** PLUGIN *** called load()~n", []),
  initMnesia(),
  PersistedSubscriptions = loadPersistedSubscriptions(),
  %%noinspection ErlangUnresolvedFunction
  emqttd:hook('client.subscribe', fun ?MODULE:on_client_subscribe/4, [Env]),
  %%noinspection ErlangUnresolvedFunction
  emqttd:hook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/4, [Env]),
  %%noinspection ErlangUnresolvedFunction
  emqttd:hook('session.subscribed', fun ?MODULE:on_session_subscribed/4, [Env]),
  %%noinspection ErlangUnresolvedFunction
  emqttd:hook('message.publish', fun ?MODULE:on_message_publish/2, [PersistedSubscriptions]).

on_client_subscribe(ClientId, Username, TopicTable, _) ->
  io:format("*** PLUGIN *** called on_client_subscribe() for user ~s~n", [Username]),
  case TopicTable of
    [{Topic, [{qos, 1}]}] -> persistSubscription(ClientId, Topic);
    [{Topic, [{qos, 0}]}] -> io:format("*** PLUGIN *** Ignored subscribe on ~s due to QoS 0.~n", [Topic]);
    _ -> io:format("*** PLUGIN *** Ignored subscribe on ~p due to unexpected format.~n", [TopicTable])
  end,
  {ok, TopicTable}.

on_client_unsubscribe(ClientId, Username, TopicTable, _) ->
  io:format("*** PLUGIN *** called on_client_unsubscribe() for user ~s~n", [Username]),
  case TopicTable of
    [{Topic, _}] -> forgetSubscription(ClientId, Topic);
    _ -> io:format("*** PLUGIN *** Ignored unsubscribe on ~p due to unexpected format.~n", [TopicTable])
  end,
  {ok, TopicTable}.

on_session_subscribed(ClientId, Username, {Topic, _Opts}, _) ->
  io:format("*** PLUGIN *** called on_session_subscribed() for user ~s~n", [Username]),
  io:format("*** PLUGIN *** Session subscribed with options: ~p~n", [_Opts]),
  SessionPid = self(),
  Messages = recoverTopicMessages(ClientId, Topic),
  lists:foreach(fun(Msg) -> SessionPid ! {dispatch, Topic, Msg} end, sortPersisted(Messages)),
  ok.

on_message_publish(Msg, PersistedSubscriptions) ->
  case Msg of
    #mqtt_message{topic = Topic, qos = 1} ->
      MatchingSubscriptions = lists:filter(fun({_, PersistedTopic}) -> PersistedTopic =:= Topic end, PersistedSubscriptions),
      lists:foreach(fun({ClientId, _}) -> persistMessage(ClientId, Msg) end, MatchingSubscriptions);
    _ -> ok
  end,
  {ok, Msg}.

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
  Copies = disc_only_copies,
  %%noinspection ErlangUnresolvedFunction
  ok = ekka_mnesia:create_table(mqtt_persisted_subscription, [
    {type, set},
    {Copies, [node()]},
    {record_name, mqtt_persisted_subscription},
    {attributes, record_info(fields, mqtt_persisted_subscription)},
    {storage_properties, [{ets, [compressed]},
      {dets, [{auto_save, 1000}]}]}]),
  copyMnesiaTable(mqtt_persisted_subscription, Copies),
  %%noinspection ErlangUnresolvedFunction
  ok = ekka_mnesia:create_table(mqtt_persisted_message, [
    {type, set},
    {Copies, [node()]},
    {record_name, mqtt_persisted_message},
    {attributes, record_info(fields, mqtt_persisted_message)},
    {storage_properties, [{ets, [compressed]},
      {dets, [{auto_save, 1000}]}]}]),
  copyMnesiaTable(mqtt_persisted_message, Copies).

copyMnesiaTable(Table, Copies) ->
  %%noinspection ErlangUnresolvedFunction
  ok = ekka_mnesia:copy_table(Table),
  case mnesia:table_info(Table, storage_type) of
    Copies -> ok;
    _ -> {atomic, ok} = mnesia:change_table_copy_type(Table, node(), Copies)
  end.

loadPersistedSubscriptions() ->
  io:format("*** PLUGIN *** loading persisted subscriptions...~n", []),
  Query = fun() -> mnesia:select(mqtt_persisted_subscription, [{'_', [], ['$_']}]) end,
  PersistedSubscriptions = mnesia:activity(transaction, Query),
  io:format("*** PLUGIN *** done loading ~p persisted subscriptions.~n", [length(PersistedSubscriptions)]),
  PersistedSubscriptions.

persistSubscription(ClientId, Topic) ->
  io:format("*** PLUGIN *** persisting subscription of ~s to topic ~s...~n", [ClientId, Topic]),
  mnesia:dirty_write(#mqtt_persisted_subscription{clientId = ClientId, topic = Topic}),
  io:format("*** PLUGIN *** done persisting subscription.~n", []).

forgetSubscription(ClientId, Topic) ->
  io:format("*** PLUGIN *** forgetting subscription of ~s to topic ~s...~n", [ClientId, Topic]),
  mnesia:dirty_delete_object(#mqtt_persisted_subscription{clientId = ClientId, topic = Topic}),
  io:format("*** PLUGIN *** done forgetting subscription.~n", []).

persistMessage(ClientId, Msg) ->
  mnesia:dirty_write(#mqtt_persisted_message{clientId = ClientId, message = Msg}),
  ok.

recoverTopicMessages(ClientId, Topic) ->
  io:format("*** PLUGIN *** recovering messages for client ~s on topic ~s...~n", [ClientId, Topic]),
  Query = fun() -> mnesia:select(mqtt_persisted_message, [{#mqtt_persisted_message{clientId=ClientId, _='_'}, [], ['$_']}]) end,
  PersistedMessages = mnesia:activity(transaction, Query),
  TopicMatcher = fun (#mqtt_persisted_message{message = #mqtt_message{topic = MsgTopic}}) -> MsgTopic =:= Topic end,
  RecoveredMessages = lists:filter(TopicMatcher, PersistedMessages),
  DeleteQuery = fun() -> lists:foreach(fun(Msg) -> mnesia:delete_object(Msg) end, RecoveredMessages) end,
  mnesia:transaction(DeleteQuery),
  io:format("*** PLUGIN *** done recovering ~p messages.~n", [length(RecoveredMessages)]),
  RecoveredMessages.

sortPersisted([]) ->
  [];
sortPersisted([Msg]) ->
  [Msg];
sortPersisted(Msgs) ->
  lists:sort(fun(#mqtt_message{timestamp = Ts1}, #mqtt_message{timestamp = Ts2}) -> Ts1 =< Ts2 end, Msgs).
