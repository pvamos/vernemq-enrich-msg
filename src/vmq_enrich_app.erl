%% src/vmq_enrich_app.erl
%% OTP application for vmq_enrich – logs config on boot and
%% keeps a tiny process alive so the application stays running.

-module(vmq_enrich_app).
-behaviour(application).

-export([start/2, stop/1]).
-export([noop/0]).

%%% =====================================================================
%%% application behaviour
%%% =====================================================================

start(_StartType, _StartArgs) ->
    %% Log effective config on boot
    log_config_snapshot(),
    vmq_enrich_log:log(
      info,
      "vmq_enrich_app: starting (hooks configured via vmq_plugin_hooks env)",
      []
    ),
    %% Keep a linked process so the app stays alive
    Pid = proc_lib:spawn_link(?MODULE, noop, []),
    {ok, Pid}.

stop(Reason) ->
    %% No manual unregistering; hooks are managed by VerneMQ
    vmq_enrich_log:log(
      info,
      "vmq_enrich_app: stopping (~p)",
      [Reason]
    ),
    ok.

noop() ->
    receive
        stop -> ok
    after infinity ->
        ok
    end.

%%% =====================================================================
%%% config snapshot (for debugging)
%%% =====================================================================

log_config_snapshot() ->
    Lvl      = vmq_enrich_config:log_level(),
    Qos      = vmq_enrich_config:qos(),
    Retain   = vmq_enrich_config:retain(),

    MaxOut   = vmq_enrich_config:max_output_size(),

    %% Optional fields (core fields always included by encoder)
    IncTopic    = vmq_enrich_config:include_topic(),
    IncUser     = vmq_enrich_config:include_user(),
    IncClientId = vmq_enrich_config:include_clientid(),
    IncBroker   = vmq_enrich_config:include_broker(),

    Accept   = vmq_enrich_config:accept(),
    Rules    = vmq_enrich_config:topic_rules(),
    DefTgt   = vmq_enrich_config:default_target(),

    Sample   = vmq_enrich_config:log_payload_sample(),

    vmq_enrich_log:log(
      info,
      "vmq_enrich boot: log_level=~p qos=~p retain=~p max_output=~p "
      "include={topic=~p,user=~p,clientid=~p,broker=~p} "
      "accept=~p rules=~p default=~s payload_sample=~p",
      [Lvl, Qos, Retain, MaxOut,
       IncTopic, IncUser, IncClientId, IncBroker,
       Accept, Rules, val_to_text(DefTgt), Sample]
    ).

val_to_text(undefined) -> <<"undefined">>;
val_to_text(B) when is_binary(B) -> B;
val_to_text(Other) -> list_to_binary(io_lib:format("~p", [Other])).
