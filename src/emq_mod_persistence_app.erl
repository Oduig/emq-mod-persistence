-module(emq_mod_persistence_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
  {ok, Sup} = emq_mod_persistence_sup:start_link(),
  emq_mod_persistence:load(application:get_all_env()),
  {ok, Sup}.

stop(_State) ->
  emq_mod_persistence:unload().
