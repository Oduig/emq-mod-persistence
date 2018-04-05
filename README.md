emq_mod_persistence
====================

Persistence Management Module automatically persists subscriptions from clients so that they can be restored in the event of a broker restart.
 All messages that were sent to the topic for a persisted subscription are stored while the client is away. Upon reconnect, 
 the client can resubscribe and receive the missed messages.

How it works
------------
By default in EMQTT, subscriptions that disconnect and come back online automatically receive their missed messages. 
That means that if a client reconnects in the same server lifetime, we do not need to resend the messages.
Only when a subscription disconnects, and subsequently, the broker goes down, do we need to persist and resend messages.

It is only necessary to write messages to disk when the following circumstances apply.

- Message has QoS level 1.
- A subscription with a matching topic had a dirty disconnect (no unsubscribe).
- The matching subscription had QoS level 1.
- The matching subscription has not come back online since.

It is only necessary to resend messages from disk when the following circumstances apply.

- Subscription matches a dirty disconnect that occurred before the last broker start.
- Subscription has QoS level 1.
- Subscription has `clean=false`.

Due to the default behavior, we must keep an in-memory record of dirty disconnects when the broker starts, 
so that we know which disconnects occurred in the previous broker run (eligible for resending messages) and which 
disconnects occurred in this broker run (not eligible for resending messages).


Todo list
----
This EMQTT plugin is a work in progress. The following still need to be done.

1. Persist messages in addition to subscriptions.
2. Check if persistence also works for multiple subscribed topics. Does each subscription get its own `on_session_subscribed` callback?
3. Do not send messages to new sessions started with `clean=true`.
4. Only persist messages if the subscription is disconnected. (How to do this, can we keep track of the client PID?)
5. Dump persisted messages if a subscription reconnects.
6. Handle wildcard topics.
7. Finalize solution, introduce time/storage limit for persisted messages similar to `emq-retainer`.

Configure Persistence Module
-----------------------------

etc/plugins/emq_mod_persistence.conf

Load Persistence Module
------------------------

Note: This module will be loaded by default.

```
./bin/emqttd_ctl plugins load emq_mod_persistence
```

License
-------

Apache License Version 2.0

Author
------

Guido Josquin

Based on `emq_mod_subscriptions` by Feng Lee.
