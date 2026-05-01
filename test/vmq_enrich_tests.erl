%% test/vmq_enrich_tests.erl – only used in eunit
%% This module stubs vmq_reg:publish/4 so vmq_enrich can be tested
%% without a running VerneMQ node, and contains EUnit tests.

-module(vmq_reg).

-include_lib("eunit/include/eunit.hrl").
-include_lib("vmq_commons/include/vmq_types.hrl").

-export([publish/4]).

publish(_CAPPublish, _RegView, _ClientId, Msg = #vmq_msg{}) ->
    %% Convert routing_key (list of binaries) back to <<"a/b">> for assertions
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

setenv(K, V) -> ok = os:putenv(K, V).
unsetenv(K)  -> ok = os:unsetenv(K).

flush_mailbox() ->
  receive _ -> flush_mailbox()
  after 0 -> ok end.

recv_publish(TimeoutMs) ->
  receive
    {published, Topic, Payload, QoS, Retain} ->
      {Topic, Payload, QoS, Retain}
  after TimeoutMs ->
      none
  end.

decode_json(Bin) ->
  jsx:decode(Bin, [return_maps, {labels, binary}]).

%% ===== tests =====

rule_mapping_publish_test() ->
  %% sensors/+ -> enriched/sensors/{1}
  setenv("VMQ_ENRICH_ACCEPT", "sensors/#"),
  setenv("VMQ_ENRICH_TOPIC_MAP",
         "[{\"in\":\"sensors/+\",\"out\":\"enriched/sensors/{1}\"}]"),
  setenv("VMQ_ENRICH_DEFAULT_TARGET", "enriched/{topic}"),
  setenv("VMQ_ENRICH_QOS", "1"),
  setenv("VMQ_ENRICH_RETAIN", "false"),
  flush_mailbox(),

  ok = vmq_enrich:handle(<<"user">>, <<"sub">>, 0,
                         <<"sensors/dev-001">>, <<"hello">>, false, []),

  case recv_publish(200) of
    {Topic, Payload, QoS, Retain} ->
      ?assertEqual(<<"enriched/sensors/dev-001">>, Topic),
      ?assertEqual(1, QoS),
      ?assertEqual(false, Retain),
      Map = decode_json(Payload),
      ?assertEqual(<<"sensors/dev-001">>, maps:get(<<"topic">>, Map)),
      ?assertEqual(false, maps:get(<<"b64">>, Map)),
      ?assertEqual(<<"hello">>, maps:get(<<"payload">>, Map));
    none ->
      ?assert(false)
  end.

default_target_publish_test() ->
  %% No rule matches, use default: enriched/{topic}
  setenv("VMQ_ENRICH_ACCEPT", "sensors/#"),
  setenv("VMQ_ENRICH_TOPIC_MAP", "[]"),
  setenv("VMQ_ENRICH_DEFAULT_TARGET", "enriched/{topic}"),
  setenv("VMQ_ENRICH_QOS", "1"),
  setenv("VMQ_ENRICH_RETAIN", "false"),
  flush_mailbox(),

  ok = vmq_enrich:handle(<<"user">>, <<"sub">>, 0,
                         <<"sensors/dev-002">>, <<"hi">>, false, []),

  case recv_publish(200) of
    {Topic, Payload, _QoS, _Retain} ->
      ?assertEqual(<<"enriched/sensors/dev-002">>, Topic),
      Map = decode_json(Payload),
      ?assertEqual(<<"sensors/dev-002">>, maps:get(<<"topic">>, Map)),
      ?assertEqual(false, maps:get(<<"b64">>, Map)),
      ?assertEqual(<<"hi">>, maps:get(<<"payload">>, Map));
    none ->
      ?assert(false)
  end.

not_accepted_no_publish_test() ->
  %% Accept only zigbee/#; publishing on sensors/... should be ignored.
  setenv("VMQ_ENRICH_ACCEPT", "zigbee/#"),
  setenv("VMQ_ENRICH_TOPIC_MAP",
         "[{\"in\":\"zigbee/+/rx\",\"out\":\"enriched/zigbee/{1}\"}]"),
  setenv("VMQ_ENRICH_DEFAULT_TARGET", "enriched/{topic}"),
  flush_mailbox(),

  ok = vmq_enrich:handle(<<"user">>, <<"sub">>, 0,
                         <<"sensors/ignored">>, <<"x">>, false, []),

  ?assertEqual(none, recv_publish(100)).

payload_utf8_kept_test() ->
  %% Ensure UTF-8 payloads remain strings (not base64).
  setenv("VMQ_ENRICH_ACCEPT", "sensors/#"),
  setenv("VMQ_ENRICH_TOPIC_MAP",
         "[{\"in\":\"sensors/+\",\"out\":\"enriched/sensors/{1}\"}]"),
  flush_mailbox(),

  ok = vmq_enrich:handle(<<"u">>, <<"s">>, 0,
                         <<"sensors/dev-003">>, <<"plain-text">>, false, []),

  {_, Payload, _, _} = case recv_publish(200) of none -> ?assert(false); V -> V end,
  Map = decode_json(Payload),
  ?assertEqual(false, maps:get(<<"b64">>, Map)),
  ?assertEqual(<<"plain-text">>, maps:get(<<"payload">>, Map)).

payload_binary_b64_test() ->
  %% Non-UTF8 bytes should be base64 encoded with b64=true.
  setenv("VMQ_ENRICH_ACCEPT", "sensors/#"),
  setenv("VMQ_ENRICH_TOPIC_MAP",
         "[{\"in\":\"sensors/+\",\"out\":\"enriched/sensors/{1}\"}]"),
  flush_mailbox(),
  Bin = <<0,255,1,2,3,4>>,
  ok = vmq_enrich:handle(<<"u">>, <<"s">>, 0,
                         <<"sensors/dev-004">>, Bin, false, []),
  {_, Payload, _, _} = case recv_publish(200) of none -> ?assert(false); V -> V end,
  Map = decode_json(Payload),
  ?assertEqual(true, maps:get(<<"b64">>, Map)),
  ?assertEqual(base64:encode(Bin), maps:get(<<"payload">>, Map)).

oversize_json_dropped_test() ->
  setenv("VMQ_ENRICH_ACCEPT", "sensors/#"),
  setenv("VMQ_ENRICH_TOPIC_MAP", "[{\"in\":\"sensors/+\",\"out\":\"enriched/{1}\"}]"),
  setenv("VMQ_ENRICH_QOS", "0"),
  setenv("VMQ_ENRICH_RETAIN", "false"),
  setenv("VMQ_ENRICH_MAX_JSON_SIZE", "64"),
  flush_mailbox(),
  %% Make a payload that will exceed 64B once wrapped in JSON
  Big = <<0:2048>>,
  ok = vmq_enrich:handle(<<"u">>, <<"s">>, 0, <<"sensors/x">>, Big, false, []),
  ?assertEqual(none, recv_publish(200)).

ipv6_flag_toggle_test() ->
  setenv("VMQ_ENRICH_ACCEPT", "sensors/#"),
  setenv("VMQ_ENRICH_TOPIC_MAP", "[{\"in\":\"sensors/+\",\"out\":\"enriched/{1}\"}]"),
  setenv("VMQ_ENRICH_INCLUDE_IPV6", "false"),
  flush_mailbox(),
  ok = vmq_enrich:handle(<<"u">>, <<"s">>, 0, <<"sensors/dev">>, <<"ok">>, false, []),
  {_, Payload, _, _} = case recv_publish(200) of none -> ?assert(false); V -> V end,
  Map = decode_json(Payload),
  ?assertError(badkey, maps:get(<<"ipv6">>, Map)).

ipv4_mapped_v6_normalized_test() ->
  %% basic env
  setenv("VMQ_ENRICH_ACCEPT", "sensors/#"),
  setenv("VMQ_ENRICH_TOPIC_MAP",
         "[{\"in\":\"sensors/+\",\"out\":\"enriched/sensors/{1}\"}]"),
  flush_mailbox(),

  %% ::ffff:84.0.24.7 as tuple
  {ok, Mapped} = inet:parse_address("::ffff:84.0.24.7"),

  %% track peer as VerneMQ would on auth
  vmq_enrich_state:track_peer({Mapped, 1883}, {<<>>, <<"cid-mapped">>}),

  %% trigger a publish
  ok = vmq_enrich:handle(<<"u">>, {<<>>, <<"cid-mapped">>}, 0,
                         <<"sensors/dev-ipv4mapped">>, <<"x">>, false, []),

  {_, Payload, _, _} =
    case recv_publish(200) of none -> ?assert(false); V -> V end,
  Map = decode_json(Payload),

  ?assertEqual(<<"84.0.24.7">>, maps:get(<<"client">>, Map)),
  ?assertEqual(false, maps:get(<<"ipv6">>, Map)).
