%% test/vmq_enrich_tests.erl – only used in eunit
%%
%% Lightweight smoke tests for the current protobuf-only pipeline.
%% This module stubs vmq_reg:publish/4 so vmq_enrich can be exercised
%% without a running VerneMQ node.
%%
%% The tests deliberately avoid duplicating the production protobuf decoder.
%% A minimal valid envsensor.Reading payload containing only field 1 (mac,
%% fixed64) is enough to verify routing, publish behavior, invalid-input
%% handling and the output-size guardrail.

-module(vmq_reg).

-include_lib("eunit/include/eunit.hrl").
-include_lib("vmq_commons/include/vmq_types.hrl").

-export([publish/4]).

publish(_CAPPublish, _RegView, _ClientId, Msg = #vmq_msg{}) ->
    %% Convert routing_key (list of binaries) back to <<"a/b">> for assertions.
    Topic =
        case Msg#vmq_msg.routing_key of
            [] -> <<>>;
            Parts ->
                iolist_to_binary(lists:join(<<"/">>, Parts))
        end,
    Payload = Msg#vmq_msg.payload,
    QoS     = Msg#vmq_msg.qos,
    Retain  = Msg#vmq_msg.retain,
    self() ! {published, Topic, Payload, QoS, Retain},
    ok.

%% ===== helpers =====

setenv(K, V) ->
    _ = os:putenv(K, V),
    ok.

unsetenv(K) ->
    _ = os:unsetenv(K),
    ok.

reset_env() ->
    lists:foreach(
      fun unsetenv/1,
      ["VMQ_ENRICH_ACCEPT",
       "VMQ_ENRICH_TOPIC_MAP",
       "VMQ_ENRICH_DEFAULT_TARGET",
       "VMQ_ENRICH_QOS",
       "VMQ_ENRICH_RETAIN",
       "VMQ_ENRICH_MAX_OUTPUT_SIZE",
       "VMQ_ENRICH_MAX_JSON_SIZE",
       "VMQ_ENRICH_INCLUDE_TOPIC",
       "VMQ_ENRICH_INCLUDE_USER",
       "VMQ_ENRICH_INCLUDE_CLIENTID",
       "VMQ_ENRICH_INCLUDE_BROKER"]),
    flush_mailbox().

flush_mailbox() ->
    receive _ -> flush_mailbox()
    after 0 -> ok
    end.

recv_publish(TimeoutMs) ->
    receive
        {published, Topic, Payload, QoS, Retain} ->
            {Topic, Payload, QoS, Retain}
    after TimeoutMs ->
        none
    end.

%% envsensor.Reading field 1 is fixed64 mac.
%% Protobuf key = (1 << 3) | wire_type_64 = 9.
valid_reading_payload() ->
    Mac = 16#0011223344556677,
    <<9, Mac:64/little-unsigned>>.

%% ===== tests =====

rule_mapping_publish_test() ->
    reset_env(),
    setenv("VMQ_ENRICH_ACCEPT", "sensors/#"),
    setenv("VMQ_ENRICH_TOPIC_MAP",
           "[{\"in\":\"sensors/+\",\"out\":\"enriched/sensors/{1}\"}]"),
    setenv("VMQ_ENRICH_DEFAULT_TARGET", "enriched/{topic}"),
    setenv("VMQ_ENRICH_QOS", "1"),
    setenv("VMQ_ENRICH_RETAIN", "false"),

    ok = vmq_enrich:handle(<<"user">>, <<"sub">>, 0,
                           <<"sensors/dev-001">>, valid_reading_payload(), false, []),

    case recv_publish(200) of
        {Topic, Payload, QoS, Retain} ->
            ?assertEqual(<<"enriched/sensors/dev-001">>, Topic),
            ?assertEqual(1, QoS),
            ?assertEqual(false, Retain),
            ?assert(is_binary(Payload)),
            ?assert(byte_size(Payload) > 0);
        none ->
            ?assert(false)
    end.

default_target_publish_test() ->
    reset_env(),
    setenv("VMQ_ENRICH_ACCEPT", "sensors/#"),
    setenv("VMQ_ENRICH_TOPIC_MAP", "[]"),
    setenv("VMQ_ENRICH_DEFAULT_TARGET", "enriched/{topic}"),
    setenv("VMQ_ENRICH_QOS", "1"),
    setenv("VMQ_ENRICH_RETAIN", "false"),

    ok = vmq_enrich:handle(<<"user">>, <<"sub">>, 0,
                           <<"sensors/dev-002">>, valid_reading_payload(), false, []),

    case recv_publish(200) of
        {Topic, Payload, _QoS, _Retain} ->
            ?assertEqual(<<"enriched/sensors/dev-002">>, Topic),
            ?assert(is_binary(Payload)),
            ?assert(byte_size(Payload) > 0);
        none ->
            ?assert(false)
    end.

not_accepted_no_publish_test() ->
    reset_env(),
    setenv("VMQ_ENRICH_ACCEPT", "zigbee/#"),
    setenv("VMQ_ENRICH_TOPIC_MAP",
           "[{\"in\":\"zigbee/+/rx\",\"out\":\"enriched/zigbee/{1}\"}]"),
    setenv("VMQ_ENRICH_DEFAULT_TARGET", "enriched/{topic}"),

    ok = vmq_enrich:handle(<<"user">>, <<"sub">>, 0,
                           <<"sensors/ignored">>, valid_reading_payload(), false, []),

    ?assertEqual(none, recv_publish(100)).

invalid_protobuf_dropped_test() ->
    reset_env(),
    setenv("VMQ_ENRICH_ACCEPT", "sensors/#"),
    setenv("VMQ_ENRICH_TOPIC_MAP",
           "[{\"in\":\"sensors/+\",\"out\":\"enriched/sensors/{1}\"}]"),

    %% 0x80 starts a protobuf varint but is deliberately truncated.
    ok = vmq_enrich:handle(<<"user">>, <<"sub">>, 0,
                           <<"sensors/dev-invalid">>, <<16#80>>, false, []),

    ?assertEqual(none, recv_publish(100)).

oversize_protobuf_dropped_test() ->
    reset_env(),
    setenv("VMQ_ENRICH_ACCEPT", "sensors/#"),
    setenv("VMQ_ENRICH_TOPIC_MAP",
           "[{\"in\":\"sensors/+\",\"out\":\"enriched/{1}\"}]"),
    setenv("VMQ_ENRICH_QOS", "0"),
    setenv("VMQ_ENRICH_RETAIN", "false"),
    setenv("VMQ_ENRICH_MAX_OUTPUT_SIZE", "1"),

    ok = vmq_enrich:handle(<<"user">>, <<"sub">>, 0,
                           <<"sensors/dev-oversize">>, valid_reading_payload(), false, []),

    ?assertEqual(none, recv_publish(100)).

ipv4_mapped_peer_publish_smoke_test() ->
    reset_env(),
    setenv("VMQ_ENRICH_ACCEPT", "sensors/#"),
    setenv("VMQ_ENRICH_TOPIC_MAP",
           "[{\"in\":\"sensors/+\",\"out\":\"enriched/sensors/{1}\"}]"),

    {ok, Mapped} = inet:parse_address("::ffff:192.0.2.7"),
    vmq_enrich_state:track_peer({Mapped, 1883}, {<<>>, <<"cid-mapped">>}),

    ok = vmq_enrich:handle(<<"user">>, {<<>>, <<"cid-mapped">>}, 0,
                           <<"sensors/dev-ipv4mapped">>, valid_reading_payload(), false, []),

    case recv_publish(200) of
        {Topic, Payload, _QoS, _Retain} ->
            ?assertEqual(<<"enriched/sensors/dev-ipv4mapped">>, Topic),
            ?assert(is_binary(Payload)),
            ?assert(byte_size(Payload) > 0);
        none ->
            ?assert(false)
    end.
