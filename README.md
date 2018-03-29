emq_mod_persistence
====================

Persistence Management Module automatically persists subscriptions from clients connecting with the flag `clean=false`.
 All messages that were sent to the topic for a persisted subscription are stored while the client is away. Upon reconnect, 
 the client is automatically subscribed and receives the missed messages.
 
This plugin allows restarting the broker without losing messages. 
Note that `QoS level 1` has to be active for both the subscription as well as all sent messages.

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

Feng Lee <feng at emqtt.io>

