PROJECT = emq_mod_persistence
PROJECT_DESCRIPTION = Subscription Persistence Module
PROJECT_VERSION = 2.0.7

BUILD_DEPS = emqttd cuttlefish
dep_emqttd = git https://github.com/emqtt/emqttd master
dep_cuttlefish = git https://github.com/emqtt/cuttlefish

ERLC_OPTS += +'{parse_transform, lager_transform}'

include erlang.mk

app.config::
	./deps/cuttlefish/cuttlefish -l info -e etc/ -c etc/emq_mod_persistence.conf -i priv/emq_mod_persistence.schema -d data
