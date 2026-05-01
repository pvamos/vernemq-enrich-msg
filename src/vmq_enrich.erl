%% vmq_enrich.erl
%%
%% VerneMQ plugin: enrich incoming MQTT publishes and republish to mapped topics.
%%
%% Protobuf-only pipeline:
%% - INPUT  payload MUST be envsensor.Reading protobuf (new schema: mac, rssi, batt, esp32_t, bme280, sht4x)
%% - OUTPUT payload is ALWAYS envsensor.vernemq.EnrichedReading protobuf (flattened)
%%
%% Guardrail:
%%   VMQ_ENRICH_MAX_OUTPUT_SIZE (bytes).
%%
%% Topic selection:
%% - Filter by accept list (VMQ_ENRICH_ACCEPT)
%% - Map to output topic by first matching rule in VMQ_ENRICH_TOPIC_MAP
%% - Optional fallback: VMQ_ENRICH_DEFAULT_TARGET (supports {topic})
%%
%% Mapping placeholders:
%% - + captures one segment => {1}, {2}, ...
%% - # captures remainder   => {N} (single capture; may include "/")
%% - {topic} substitutes the full incoming topic

-module(vmq_enrich).

%% Pull in vmq_msg record and MQTT types
-include_lib("vmq_commons/include/vmq_types.hrl").

%% Reuse the same internal client id as vmq_server (see vmq_server.hrl)
-define(INTERNAL_CLIENT_ID, '$vmq_internal_client_id').

%% VerneMQ hooks
-export([on_publish/6, on_publish_m5/7,
         auth_on_register/5, auth_on_register_m5/6]).

%% Internal API
-export([handle/7, norm_clientid/1]).

%%% =====================================================================
%%% Hook entry points
%%% =====================================================================

%% MQTT v3/v3.1.1 auth
auth_on_register(Peer, SubscriberId, _Username, _Password, _CleanSession) ->
    %% Peer = {IpAddr, Port}; with PROXY v2 enabled, IpAddr is the real sensor IP.
    vmq_enrich_state:track_peer(Peer, SubscriberId),
    next.

%% MQTT v5 auth
auth_on_register_m5(Peer, SubscriberId, _Username, _Password, _CleanStart, _Properties) ->
    vmq_enrich_state:track_peer(Peer, SubscriberId),
    next.

%% MQTT v3/v3.1.1 publish
on_publish(Username, ClientId, QoS, Topic, Payload, IsRetain) ->
    %% Properties are not available on v3; pass [].
    handle(Username, ClientId, QoS, Topic, Payload, IsRetain, []).

%% MQTT v5 publish
on_publish_m5(Username, ClientId, QoS, Topic, Payload, IsRetain, Properties) ->
    handle(Username, ClientId, QoS, Topic, Payload, IsRetain, Properties).

%%% =====================================================================
%%% Core
%%% =====================================================================

handle(Username, ClientId, InQoS, Topic0, Payload, _IsRetain, Properties) ->
    %% Normalize VerneMQ topic arg (may be [<<"a">>,<<"b">>] or <<"a/b">>) to <<"a/b">>.
    Topic = normalize_topic(Topic0),
    try
        case accepted(Topic) of
            false ->
                vmq_enrich_log:log(debug, "vmq_enrich: topic not accepted: ~p", [Topic]),
                ok;

            true ->
                case resolve_out_topic(Topic) of
                    nomatch ->
                        vmq_enrich_log:log(debug, "vmq_enrich: accepted but no mapping for ~p", [Topic]),
                        ok;

                    {ok, OutTopic} ->
                        %% Decode input as envsensor.Reading (new schema)
                        case vmq_enrich_reading_pb:decode_reading(Payload) of
                            {ok, Reading} ->
                                OutBin =
                                    build_enriched_reading_protobuf(
                                      Username, ClientId, Topic, Reading, Properties),

                                Limit = vmq_enrich_config:max_output_size(),
                                case byte_size(OutBin) =< Limit of
                                    true ->
                                        maybe_log_publish(Topic, OutTopic, InQoS, protobuf, OutBin, Payload),
                                        publish_enriched(
                                          OutTopic,
                                          OutBin,
                                          vmq_enrich_config:qos(),
                                          vmq_enrich_config:retain()
                                        );
                                    false ->
                                        vmq_enrich_log:log(
                                          warning,
                                          "vmq_enrich: dropping (protobuf ~p bytes exceeds limit ~p) topic=~p",
                                          [byte_size(OutBin), Limit, Topic]),
                                        ok
                                end;

                            {error, DecodeReason} ->
                                vmq_enrich_log:log(
                                  warning,
                                  "vmq_enrich: drop non-Reading payload on topic=~p reason=~p",
                                  [Topic, DecodeReason]),
                                ok
                        end
                end
        end
    catch Class:CatchReason:Stacktrace ->
        vmq_enrich_log:log(
          error,
          "vmq_enrich: handle failed (topic=~p) ~p:~p stack=~p",
          [Topic, Class, CatchReason, Stacktrace]),
        ok
    end.

%%% =====================================================================
%%% Build flattened EnrichedReading protobuf (new schema)
%%% =====================================================================

build_enriched_reading_protobuf(Username, ClientId, InTopic, Reading, Properties) ->
    TimeNs      = erlang:system_time(nanosecond),
    ClientIdBin = norm_clientid(ClientId),
    {ClientIP, IsIPv6} = extract_peer(ClientIdBin, Properties),

    BrokerBin = list_to_binary(atom_to_list(node())),
    UserBin   = norm_bin(Username),

    Mac = maps:get(mac, Reading, 0),

    %% Base (required-ish) fields
    M0 = #{
        time => TimeNs,
        mac  => Mac
    },

    %% Optional metadata
    M1 = case vmq_enrich_config:include_topic() of
            true  -> M0#{topic => InTopic};
            false -> M0
         end,

    M2 = case IsIPv6 of
            true  -> M1#{ipv6 => ClientIP};
            false -> M1#{ipv4 => ClientIP}
         end,

    M3 = case vmq_enrich_config:include_user() of
            true  -> M2#{user => UserBin};
            false -> M2
         end,

    M4 = case vmq_enrich_config:include_clientid() of
            true  -> M3#{clientid => ClientIdBin};
            false -> M3
         end,

    M5 = case vmq_enrich_config:include_broker() of
            true  -> M4#{broker => BrokerBin};
            false -> M4
         end,

    %% Flatten new Reading fields + sensors
    M6 = flatten_reading_and_sensors(Reading, M5),

    vmq_enrich_reading_pb:encode_enriched_reading(M6).

flatten_reading_and_sensors(Reading, M0) ->
    %% Reading scalars
    M1 = maybe_put(rssi,   maps:get(rssi, Reading, undefined), M0),
    M2 = maybe_put(batt,   maps:get(batt, Reading, undefined), M1),
    M3 = maybe_put(esp32_t,maps:get(esp32_t, Reading, undefined), M2),

    %% bme280 (optional)
    M4 = case maps:get(bme280, Reading, undefined) of
            #{t := T} -> M3#{bme280_t => T};
            _         -> M3
         end,
    M5 = case maps:get(bme280, Reading, undefined) of
            #{p := P} -> M4#{bme280_p => P};
            _         -> M4
         end,
    M6 = case maps:get(bme280, Reading, undefined) of
            #{h := H} -> M5#{bme280_h => H};
            _         -> M5
         end,

    %% sht4x (optional)
    M7 = case maps:get(sht4x, Reading, undefined) of
            #{t := ST} -> M6#{sht4x_t => ST};
            _          -> M6
         end,
    M8 = case maps:get(sht4x, Reading, undefined) of
            #{h := SH} -> M7#{sht4x_h => SH};
            _          -> M7
         end,

    M8.

maybe_put(_Key, undefined, M) -> M;
maybe_put(_Key, null, M)      -> M;
maybe_put(Key, Val, M)        -> M#{Key => Val}.

norm_bin(undefined) -> <<>>;
norm_bin(<<>> = B)  -> B;
norm_bin(B) when is_binary(B) -> B;
norm_bin(L) when is_list(L)   -> list_to_binary(L);
norm_bin(A) when is_atom(A)   -> list_to_binary(atom_to_list(A));
norm_bin(Other)               -> list_to_binary(io_lib:format("~p", [Other])).

%% ClientId can be a bare binary or {Mountpoint, ClientId}
norm_clientid(undefined) ->
    <<>>;
norm_clientid({_, ClientId}) ->
    norm_bin(ClientId);
norm_clientid(ClientId) ->
    norm_bin(ClientId).

%%% =====================================================================
%%% Publish helper via vmq_reg:publish/4
%%% =====================================================================

publish_enriched(OutTopicBin, OutBin, QoS, Retain)
  when is_binary(OutTopicBin), is_binary(OutBin) ->
    %% CAPPublish=true => cluster-aware publish, like vmq_systree
    CAPPublish = true,
    RegView    = vmq_config:get_env(default_reg_view, vmq_reg_trie),
    ClientId   = ?INTERNAL_CLIENT_ID,
    Mountpoint = "",
    SgPolicy   = vmq_config:get_env(shared_subscription_policy, prefer_local),
    %% routing_key is a vmq_topic:topic() (list of binaries), not <<"a/b">>
    RoutingKey = vmq_topic:word(binary_to_list(OutTopicBin)),

    Msg = #vmq_msg{
            mountpoint  = Mountpoint,
            qos         = QoS,
            retain      = Retain,
            sg_policy   = SgPolicy,
            routing_key = RoutingKey,
            payload     = OutBin,
            msg_ref     = vmq_mqtt_fsm_util:msg_ref()
          },
    try
        vmq_reg:publish(CAPPublish, RegView, ClientId, Msg),
        vmq_enrich_log:log(
          debug,
          "vmq_enrich: vmq_reg:publish/4 ~p bytes to ~p qos=~p retain=~p",
          [byte_size(OutBin), OutTopicBin, QoS, Retain]
        ),
        ok
    catch C1:R1:Stack ->
        vmq_enrich_log:log(
          error,
          "vmq_enrich: vmq_reg:publish/4 failed to ~p (~p:~p) stack=~p",
          [OutTopicBin, C1, R1, Stack]
        ),
        ok
    end.

%%% =====================================================================
%%% Accept + Mapping
%%% =====================================================================

accepted(Topic) ->
    AcceptPats = vmq_enrich_config:accept(),
    case AcceptPats of
        [] ->
            %% Empty accept list => accept all
            true;
        _ ->
            lists:any(fun(Pat) -> topic_match(Pat, Topic) end, AcceptPats)
    end.

resolve_out_topic(Topic) ->
    Rules = vmq_enrich_config:topic_rules(),
    case pick_first_match(Topic, Rules) of
        nomatch ->
            case vmq_enrich_config:default_target() of
                undefined ->
                    nomatch;
                Def when is_binary(Def) ->
                    {ok, binary:replace(Def, <<"{topic}">>, Topic, [global])};
                Other ->
                    Def2 = list_to_binary(io_lib:format("~p", [Other])),
                    {ok, binary:replace(Def2, <<"{topic}">>, Topic, [global])}
            end;
        {ok, Out} ->
            {ok, Out}
    end.

pick_first_match(_Topic, []) ->
    nomatch;
pick_first_match(Topic, [#{in := InPat, out := OutPat} | Rest]) ->
    case topic_match_capture(InPat, Topic) of
        {true, Caps} ->
            {ok, apply_out(OutPat, Topic, Caps)};
        false ->
            pick_first_match(Topic, Rest)
    end;
pick_first_match(Topic, [_Weird | Rest]) ->
    %% Defensive: skip invalid rule entries
    pick_first_match(Topic, Rest).

apply_out(OutPat, Topic, Captures) when is_binary(OutPat) ->
    %% First replace {topic}, then numbered captures
    Acc0 = binary:replace(OutPat, <<"{topic}">>, Topic, [global]),
    lists:foldl(
      fun({Idx, Val}, Acc) ->
              Placeholder = iolist_to_binary(["{", integer_to_binary(Idx), "}"]),
              binary:replace(Acc, Placeholder, Val, [global])
      end,
      Acc0,
      index_binaries(Captures, 1)).

index_binaries([], _N) -> [];
index_binaries([H|T], N) -> [{N, H} | index_binaries(T, N+1)].

%%% =====================================================================
%%% Topic normalization & matching helpers (+ and #)
%%% =====================================================================

%% Normalize VerneMQ hook topic arg to <<"a/b/c">>
normalize_topic(Topic) when is_binary(Topic) ->
    Topic;
normalize_topic({_, T}) ->
    normalize_topic(T);
normalize_topic(T) when is_list(T) ->
    case T of
        [<<_/binary>> | _] -> join(T);      %% [<<"envsensor">>,<<"x">>] -> <<"envsensor/x">>
        _ -> list_to_binary(T)              %% charlist or mixed -> binary
    end;
normalize_topic(A) when is_atom(A) ->
    list_to_binary(atom_to_list(A));
normalize_topic(_) ->
    <<>>.

topic_match(Pat, Topic) ->
    case topic_match_capture(Pat, Topic) of
        {true, _Caps} -> true;
        false -> false
    end.

topic_match_capture(Pat, Topic) when is_binary(Pat), is_binary(Topic) ->
    P = split(Pat),
    T = split(Topic),
    match_parts(P, T, []).

split(Bin) ->
    case Bin of
        <<>> -> [];
        _    -> binary:split(Bin, <<"/">>, [global])
    end.

%% match_parts(PatternParts, TopicParts, AccCaptures)
match_parts([<<"#">>], Rest, Caps) ->
    %% capture remainder (may be empty)
    {true, lists:reverse([join(Rest) | Caps])};
match_parts([<<"+">> | PR], [Seg | TR], Caps) ->
    match_parts(PR, TR, [Seg | Caps]);
match_parts([P | PR], [P | TR], Caps) ->
    match_parts(PR, TR, Caps);
match_parts([], [], Caps) ->
    {true, lists:reverse(Caps)};
match_parts(_, _, _Caps) ->
    false.

join(Segs) -> iolist_to_binary(intersperse(Segs, <<"/">>)).

intersperse([], _Sep) -> [];
intersperse([X], _Sep) -> [X];
intersperse([X | Xs], Sep) -> [X, Sep | intersperse(Xs, Sep)].

%%% =====================================================================
%%% Peer info extraction
%%% =====================================================================

%% Resolve client address for enriched output.
%% 1) Prefer the value captured at auth_on_register/auth_on_register_m5 (via ETS).
%% 2) If none is present and MQTT 5 properties exist, fall back to peer/peername.
extract_peer(ClientIdBin, Props) ->
    case vmq_enrich_state:lookup(ClientIdBin) of
        {<<>>, false} ->
            extract_peer_from_props(Props);
        {IpBin, IPv6} ->
            {IpBin, IPv6}
    end.

%% Fallback: look at MQTT 5 connection properties, if available.
extract_peer_from_props(Props) when is_list(Props) ->
    case lists:keyfind(peer, 1, Props) of
        {peer, {Addr, _Port}} ->
            to_pair(Addr);
        _ ->
            case lists:keyfind(peername, 1, Props) of
                {peername, {Addr2, _Port2}} ->
                    to_pair(Addr2);
                _ ->
                    {<<>>, false}
            end
    end;
extract_peer_from_props(_) ->
    {<<>>, false}.

%% Normalize different Addr forms into {IpBin, IPv6?}

%% IPv4-mapped IPv6 tuple: ::ffff:x.x.x.x -> plain IPv4 + ipv6=false
to_pair({0,0,0,0,0,16#ffff,_,_} = Mapped) ->
    IPv4 = ipv4_from_mapped(Mapped),
    {ip_to_bin(IPv4), false};

to_pair({A,B,C,D}) ->
    {ip_to_bin({A,B,C,D}), false};

to_pair({_,_,_,_,_,_,_,_} = V6) ->
    {ip_to_bin(V6), true};

to_pair(Bin) when is_binary(Bin) ->
    normalize_ip_text(Bin);

to_pair(List) when is_list(List) ->
    normalize_ip_text(list_to_binary(List));

to_pair(_) ->
    {<<>>, false}.

%% Textual normalization – handle ::ffff:84.0.24.7 -> 84.0.24.7
normalize_ip_text(Bin) ->
    case ipv4_mapped_from_binary(Bin) of
        {ok, V4Bin} -> {V4Bin, false};
        error       -> {Bin, is_ipv6_text(Bin)}
    end.

ipv4_mapped_from_binary(Bin0) ->
    %% Make matching case-insensitive
    LB = list_to_binary(string:lowercase(binary_to_list(Bin0))),
    case LB of
        << "::ffff:", Rest/binary >> ->
            case inet:parse_address(binary_to_list(Rest)) of
                {ok, {_,_,_,_}} ->
                    %% Rest is plain dotted IPv4
                    {ok, Rest};
                _ ->
                    error
            end;
        _ ->
            error
    end.

is_ipv6_text(Bin) ->
    case binary:match(Bin, <<":">>) of
        nomatch -> false;
        _       -> true
    end.

ip_to_bin({_,_,_,_} = IPv4) ->
    list_to_binary(inet:ntoa(IPv4));
ip_to_bin({_,_,_,_,_,_,_,_} = IPv6) ->
    list_to_binary(inet:ntoa(IPv6)).

%% Same tuple → IPv4 helper as in vmq_enrich_state
ipv4_from_mapped({0,0,0,0,0,16#ffff, A, B}) ->
    <<A1:8, A2:8>> = <<A:16>>,
    <<B1:8, B2:8>> = <<B:16>>,
    {A1, A2, B1, B2}.

%%% =====================================================================
%%% Logging helpers
%%% =====================================================================

maybe_log_publish(InTopic, OutTopic, InQoS, Format, OutBin, RawPayload) ->
    %% always one summary at debug; optional tiny payload sample
    vmq_enrich_log:log(
      debug,
      "vmq_enrich: republish ~p -> ~p (in_qos=~p mode=~p out_bytes=~p)",
      [InTopic, OutTopic, InQoS, Format, byte_size(OutBin)]),
    case vmq_enrich_config:log_payload_sample() of
        true ->
            SampleBytes = 64,
            S = payload_sample(RawPayload, SampleBytes),
            vmq_enrich_log:log(
              debug,
              "vmq_enrich: payload_sample(~p): ~p",
              [SampleBytes, S]);
        false ->
            ok
    end.

payload_sample(Bin, N) when is_binary(Bin), is_integer(N), N > 0 ->
    Sz = byte_size(Bin),
    case Sz =< N of
        true  -> Bin;
        false -> <<(binary:part(Bin, 0, N))/binary>>
    end.
