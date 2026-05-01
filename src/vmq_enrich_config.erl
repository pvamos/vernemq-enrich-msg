%% vmq_enrich_config.erl
%% Read configuration from environment variables.
%%
%% Protobuf-only redesign:
%% - Output is always flattened protobuf (EnrichedReading)
%% - Topic mapping rules have only: in/out (no per-rule "mode")
%%
%% Still uses JSON (jsx) only to parse VMQ_ENRICH_TOPIC_MAP.

-module(vmq_enrich_config).

-export([
    qos/0, retain/0, accept/0, topic_rules/0, default_target/0,

    %% size guardrail
    max_output_size/0,

    %% optional output fields (core fields always included by encoder)
    include_topic/0, include_user/0, include_clientid/0, include_broker/0,

    %% legacy/compat (kept for older configs/scripts)
    max_json_size/0,

    %% logging
    log_level/0, log_payload_sample/0
]).

%% ---------------------------------------------------------------------
%% QoS (0|1|2), default 0
%% ---------------------------------------------------------------------
qos() ->
    case os:getenv("VMQ_ENRICH_QOS") of
        false ->
            0;
        Val ->
            case catch list_to_integer(Val) of
                I when I =:= 0; I =:= 1; I =:= 2 -> I;
                _ -> 0
            end
    end.

%% ---------------------------------------------------------------------
%% Retain flag, default false
%% ---------------------------------------------------------------------
retain() ->
    case os:getenv("VMQ_ENRICH_RETAIN", "false") of
        "true"  -> true;
        "1"     -> true;
        "yes"   -> true;
        "on"    -> true;
        "false" -> false;
        "0"     -> false;
        "no"    -> false;
        "off"   -> false;
        _       -> false
    end.

%% ---------------------------------------------------------------------
%% Comma separated accept patterns. Empty/unset => accept all.
%% ---------------------------------------------------------------------
accept() ->
    case os:getenv("VMQ_ENRICH_ACCEPT") of
        false -> [];
        Str   -> split_csv(Str)
    end.

%% ---------------------------------------------------------------------
%% Topic mapping rules: JSON array of {"in":"<pattern>","out":"<template>"}.
%%
%% Example:
%% VMQ_ENRICH_TOPIC_MAP='[
%%   {"in":"sensors/+","out":"enriched/{1}"},
%%   {"in":"sensors/#","out":"enriched/all/{1}"}
%% ]'
%% ---------------------------------------------------------------------
topic_rules() ->
    case os:getenv("VMQ_ENRICH_TOPIC_MAP") of
        false -> [];
        Json  ->
            try
                B = to_bin(Json),
                Dec = jsx:decode(B, [return_maps]),
                normalize_rules(Dec)
            catch _:_ ->
                []
            end
    end.

%% Optional fallback output topic template if accepted but no rule matches.
%% Supports {topic}.
default_target() ->
    case os:getenv("VMQ_ENRICH_DEFAULT_TARGET") of
        false -> undefined;
        Str   -> to_bin(Str)
    end.

%% ---------------------------------------------------------------------
%% Size guardrail: VMQ_ENRICH_MAX_OUTPUT_SIZE
%% Backward compat: VMQ_ENRICH_MAX_JSON_SIZE
%% Default: 1048576 (1 MiB)
%% ---------------------------------------------------------------------
max_output_size() ->
    Str =
        case os:getenv("VMQ_ENRICH_MAX_OUTPUT_SIZE") of
            false -> os:getenv("VMQ_ENRICH_MAX_JSON_SIZE", "1048576");
            V     -> V
        end,
    case catch list_to_integer(Str) of
        I when is_integer(I), I > 0 -> I;
        _ -> 1048576
    end.

%% Backward compat alias
max_json_size() ->
    max_output_size().

%% ---------------------------------------------------------------------
%% Optional metadata fields (default: true)
%% Core fields always included by protobuf encoder:
%%   time, (ipv4 or ipv6), mac, and whichever sensor fields are present
%% ---------------------------------------------------------------------
include_topic() ->
    bool_env("VMQ_ENRICH_INCLUDE_TOPIC", true).

include_user() ->
    bool_env("VMQ_ENRICH_INCLUDE_USER", true).

include_clientid() ->
    bool_env("VMQ_ENRICH_INCLUDE_CLIENTID", true).

include_broker() ->
    bool_env("VMQ_ENRICH_INCLUDE_BROKER", true).

%% ---------------------------------------------------------------------
%% Logging config
%% ---------------------------------------------------------------------
log_level() ->
    Val = os:getenv("VMQ_ENRICH_LOG_LEVEL", "info"),
    to_level(Val).

to_level(Lv) when is_list(Lv) ->
    to_level(list_to_binary(string:lowercase(Lv)));
to_level(<<"debug">>)    -> debug;
to_level(<<"info">>)     -> info;
to_level(<<"notice">>)   -> notice;
to_level(<<"warn">>)     -> warning;
to_level(<<"warning">>)  -> warning;
to_level(<<"error">>)    -> error;
to_level(_)              -> info.

log_payload_sample() ->
    bool_env("VMQ_ENRICH_LOG_PAYLOAD_SAMPLE", false).

%%% ---------------------------------------------------------------------
%%% helpers
%%% ---------------------------------------------------------------------

bool_env(Name, Default) ->
    DefStr = case Default of true -> "true"; false -> "false" end,
    case os:getenv(Name, DefStr) of
        "true"  -> true;
        "1"     -> true;
        "yes"   -> true;
        "on"    -> true;
        "false" -> false;
        "0"     -> false;
        "no"    -> false;
        "off"   -> false;
        Other   ->
            Lower = string:lowercase(Other),
            case Lower of
                "true"  -> true;
                "1"     -> true;
                "yes"   -> true;
                "on"    -> true;
                "false" -> false;
                "0"     -> false;
                "no"    -> false;
                "off"   -> false;
                _       -> Default
            end
    end.

split_csv(Str) ->
    [ strip_ws(to_bin(S)) || S <- string:split(Str, ",", all), S =/= "" ].

strip_ws(Bin) when is_binary(Bin) ->
    list_to_binary(string:trim(binary_to_list(Bin))).

to_bin(B) when is_binary(B) -> B;
to_bin(L) when is_list(L)   -> list_to_binary(L).

normalize_rules(List) when is_list(List) ->
    [ #{in  => ensure_bin(In0),
       out => ensure_bin(Out0)}
      || M <- List,
         is_map(M),
         In0  <- [get_any([in, <<"in">>], M)],
         Out0 <- [get_any([out, <<"out">>], M)],
         In0  =/= undefined,
         Out0 =/= undefined ];
normalize_rules(_Other) ->
    [].

get_any(Keys, M) ->
    case [V || K <- Keys, {ok, V} <- [maps:find(K, M)]] of
        [V | _] -> V;
        []      -> undefined
    end.

ensure_bin(V) when is_binary(V) -> V;
ensure_bin(V) when is_list(V)   -> list_to_binary(V);
ensure_bin(V) when is_atom(V)   -> list_to_binary(atom_to_list(V));
ensure_bin(Other)               -> list_to_binary(io_lib:format("~p", [Other])).
