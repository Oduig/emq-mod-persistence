-module(emq_mod_persistence).

-include_lib("emqttd/include/emqttd.hrl").
-include_lib("emqttd/include/emqttd_protocol.hrl").
-include_lib("emqttd/include/emqttd_internal.hrl").

% on_message_publish/3,
-export([
  load/1,
  on_client_connected/3,
  on_client_subscribe/4,
  on_client_unsubscribe/4,
  on_session_subscribed/4,
  on_message_publish/3,
  unload/0
]).

-define(TAB, ?MODULE).

-record(mqtt_persisted_subscription, {clientId, topic}).
-record(mqtt_persisted_message, {clientId, message}).

%%--------------------------------------------------------------------
%% Hooks
%%--------------------------------------------------------------------

%%noinspection ErlangUnresolvedFunction
load(Env) ->
  io:format("*** PLUGIN *** called load()~n", []),
  initMnesia(),
  PersistedSubscriptions = loadPersistedSubscriptions(),
  emqttd:hook('client.connected', fun ?MODULE:on_client_connected/3, [Env]),
  emqttd:hook('client.subscribe', fun ?MODULE:on_client_subscribe/4, [Env]),
  emqttd:hook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/4, [Env]),
  emqttd:hook('session.subscribed', fun ?MODULE:on_session_subscribed/4, [Env]),
  emqttd:hook('message.publish', fun ?MODULE:on_message_publish/3, [PersistedSubscriptions, Env]).

%%noinspection ErlangUnresolvedRecord
on_client_connected(_ConnAck, Client = #mqtt_client{client_id = ClientId, clean_sess = CleanSession}, _) ->
  io:format("*** PLUGIN *** client ~s connected, client with clean=~p~n", [ClientId, CleanSession]),
  {ok, Client}.

on_client_subscribe(ClientId, Username, TopicTable, _) ->
  io:format("*** PLUGIN *** called on_client_subscribe() for user ~s~n", [Username]),
  case TopicTable of
    [{Topic, [{qos, 2}]}] -> persistSubscription(ClientId, Topic);
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

on_session_subscribed(ClientId, Username, {Topic, Opts}, _) ->
  io:format("*** PLUGIN *** called on_session_subscribed() for user ~s~n", [Username]),
  SessionPid = self(),
  Messages = recoverTopicMessages(ClientId, Topic),
  case Opts of
    [{qos, 0}] -> io:format("*** PLUGIN *** subscribed session has QoS 0, persisted messages were discarded.~n", []);
    [{qos, 1}] -> lists:foreach(fun(Msg) -> SessionPid ! {dispatch, Topic, Msg} end, sortPersisted(Messages));
    [{qos, 2}] -> lists:foreach(fun(Msg) -> SessionPid ! {dispatch, Topic, Msg} end, sortPersisted(Messages));
    _          -> io:format("*** PLUGIN *** subscribed session had an unexpected format for _Opts.~n", [])
  end,
  ok.

on_message_publish(Message, PersistedSubscriptions, _) ->
  %%noinspection ErlangUnresolvedRecord
  #mqtt_message{topic = Topic, qos = QoS} = Message,
  case QoS of
    Value when Value > 0 ->
      MatchingSubscriptions = lists:filter(fun({_, PersistedTopic}) -> PersistedTopic =:= Topic end, PersistedSubscriptions),
      lists:foreach(fun({ClientId, _}) -> persistMessage(ClientId, Message) end, MatchingSubscriptions),
      io:format("*** PLUGIN *** message persisted for ~p subscriptions.~n", [length(MatchingSubscriptions)]);
    _ -> io:format("*** PLUGIN *** message not persisted due to QoS ~p.~n", [QoS])
  end,
  {ok, Message}.

%%noinspection ErlangUnresolvedFunction
unload() ->
  io:format("*** PLUGIN *** called unload()~n", []),
  emqttd:unhook('client.connected', fun ?MODULE:on_client_connected/3),
  emqttd:unhook('client.subscribe', fun ?MODULE:on_client_subscribe/4),
  emqttd:unhook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/4),
  emqttd:unhook('session.subscribed', fun ?MODULE:on_session_subscribed/4),
  emqttd:unhook('message.publish', fun ?MODULE:on_message_publish/2).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

initMnesia() ->
  %% Only persist to disk
  Copies = disc_only_copies,
  %% Unlike table type 'set', 'bag' allows multiple records with the same key.
  %% The key is always the first property of a record.
  %% Mnesia automatically drops duplicate records, even for type 'bag'.
  %%noinspection ErlangUnresolvedFunction
  ok = ekka_mnesia:create_table(mqtt_persisted_subscription, [
    {type, bag},
    {Copies, [node()]},
    {record_name, mqtt_persisted_subscription},
    {attributes, record_info(fields, mqtt_persisted_subscription)},
    {storage_properties, [{ets, [compressed]},
      {dets, [{auto_save, 1000}]}]}]),
  copyMnesiaTable(mqtt_persisted_subscription, Copies),
  %%noinspection ErlangUnresolvedFunction
  ok = ekka_mnesia:create_table(mqtt_persisted_message, [
    {type, bag},
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
  %%noinspection ErlangUnresolvedRecord
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
  %%noinspection ErlangUnresolvedRecord
  lists:sort(fun(#mqtt_message{timestamp = Ts1}, #mqtt_message{timestamp = Ts2}) -> Ts1 =< Ts2 end, Msgs).
