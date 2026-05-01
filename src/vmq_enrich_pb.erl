%% src/vmq_enrich_pb.erl
%%
%% Tiny Protobuf encoder (no deps) for:
%%
%% syntax = "proto3";
%% package envsensor.vernemq;
%%
%% message EnrichedPublish {
%%   uint64 time = 1;
%%   string topic = 2;
%%   bytes  ipv4 = 3;  // 4 bytes (network order)
%%   bytes  ipv6 = 4;  // 16 bytes (network order)
%%   string user = 5;
%%   string clientid = 6;
%%   string broker = 7;
%%   bool   isutf8 = 8;
%%   oneof payload {
%%     string payload_text = 9;
%%     bytes  payload_bytes = 10;
%%   }
%% }
%%
%% Notes:
%% - This module only encodes; it does not depend on gpb/protobuffs libraries.
%% - Input is a map with (some) of these keys:
%%     time, topic, ipv4, ipv6, user, clientid, broker, isutf8, payload
%%   where payload is a binary (raw bytes). isutf8 selects field 9 vs 10.
%% - ipv4/ipv6 may be tuples, binaries (text), or lists (text).
%%
%% Exported API:
%%   encode_enriched_publish/1 -> binary()

-module(vmq_enrich_pb).

-export([encode_enriched_publish/1]).

-define(WT_VARINT, 0).
-define(WT_LEN, 2).

%%% =====================================================================
%%% Public API
%%% =====================================================================

encode_enriched_publish(M) when is_map(M) ->
    %% Required/core fields are typically guaranteed by vmq_enrich.erl,
    %% but we encode only what we can.
    Time = get_uint64(time, M, 0),
    IsUtf8 = get_bool(isutf8, M, false),
    Payload = get_bin(payload, M, <<>>),

    %% Prefer ipv4 if present; else ipv6 (as required: one of them)
    IPv4b = ip4_bytes(maps:get(ipv4, M, undefined)),
    IPv6b = ip6_bytes(maps:get(ipv6, M, undefined)),

    Topic    = get_bin(topic, M, undefined),
    User     = get_bin(user, M, undefined),
    ClientId = get_bin(clientid, M, undefined),
    Broker   = get_bin(broker, M, undefined),

    Iolist =
        [
          %% uint64 time = 1;
          enc_uint64(1, Time),

          %% string topic = 2;
          enc_string_opt(2, Topic),

          %% bytes ipv4 = 3; bytes ipv6 = 4;
          enc_ip(IPv4b, IPv6b),

          %% string user = 5;
          enc_string_opt(5, User),

          %% string clientid = 6;
          enc_string_opt(6, ClientId),

          %% string broker = 7;
          enc_string_opt(7, Broker),

          %% bool isutf8 = 8;
          enc_bool(8, IsUtf8),

          %% oneof payload { string payload_text = 9; bytes payload_bytes = 10; }
          enc_payload(IsUtf8, Payload)
        ],
    iolist_to_binary(Iolist).

%%% =====================================================================
%%% Field encoders
%%% =====================================================================

enc_payload(true, Bin) when is_binary(Bin) ->
    %% string payload_text = 9;
    enc_string(9, Bin);
enc_payload(false, Bin) when is_binary(Bin) ->
    %% bytes payload_bytes = 10;
    enc_bytes(10, Bin).

enc_ip(IPv4b, _IPv6b) when is_binary(IPv4b), byte_size(IPv4b) =:= 4 ->
    enc_bytes(3, IPv4b);
enc_ip(_IPv4b, IPv6b) when is_binary(IPv6b), byte_size(IPv6b) =:= 16 ->
    enc_bytes(4, IPv6b);
enc_ip(_IPv4b, _IPv6b) ->
    %% No usable IP; encode nothing (caller should avoid this situation).
    [].

enc_uint64(FieldNo, Int) when is_integer(Int), Int >= 0 ->
    [ key(FieldNo, ?WT_VARINT), varint(Int) ].

enc_bool(FieldNo, true)  -> [ key(FieldNo, ?WT_VARINT), varint(1) ];
enc_bool(FieldNo, false) -> [ key(FieldNo, ?WT_VARINT), varint(0) ].

enc_bytes(FieldNo, Bin) when is_binary(Bin) ->
    [ key(FieldNo, ?WT_LEN), varint(byte_size(Bin)), Bin ].

enc_string(FieldNo, Bin) when is_binary(Bin) ->
    %% In protobuf, "string" must be valid UTF-8. Caller is responsible.
    enc_bytes(FieldNo, Bin).

enc_string_opt(_FieldNo, undefined) -> [];
enc_string_opt(_FieldNo, <<>>)      -> [];
enc_string_opt(FieldNo, Bin) when is_binary(Bin) ->
    enc_string(FieldNo, Bin).

%%% =====================================================================
%%% Varint + key encoding
%%% =====================================================================

key(FieldNo, WireType) ->
    varint((FieldNo bsl 3) bor WireType).

varint(N) when is_integer(N), N >= 0 ->
    varint_loop(N, []).

varint_loop(N, Acc) when N < 128 ->
    lists:reverse([N | Acc]);
varint_loop(N, Acc) ->
    Byte = (N band 127) bor 128,
    varint_loop(N bsr 7, [Byte | Acc]).

%%% =====================================================================
%%% Input normalization helpers
%%% =====================================================================

get_uint64(Key, M, Default) ->
    case maps:get(Key, M, Default) of
        I when is_integer(I), I >= 0 ->
            I;
        B when is_binary(B) ->
            %% allow "1733675..." style binaries
            try binary_to_integer(B) catch _:_ -> Default end;
        L when is_list(L) ->
            try list_to_integer(L) catch _:_ -> Default end;
        _ ->
            Default
    end.

get_bool(Key, M, Default) ->
    case maps:get(Key, M, Default) of
        true  -> true;
        false -> false;
        <<"true">>  -> true;
        <<"false">> -> false;
        "true"  -> true;
        "false" -> false;
        1 -> true;
        0 -> false;
        _ -> Default
    end.

get_bin(Key, M, Default) ->
    case maps:get(Key, M, Default) of
        undefined -> Default;
        B when is_binary(B) -> B;
        L when is_list(L)   -> list_to_binary(L);
        A when is_atom(A)   -> list_to_binary(atom_to_list(A));
        Other               -> list_to_binary(io_lib:format("~p", [Other]))
    end.

%%% =====================================================================
%%% IP helpers (encode as 4 / 16 bytes)
%%% =====================================================================

ip4_bytes(undefined) -> undefined;
ip4_bytes(<<>>)      -> undefined;
ip4_bytes(Bin) when is_binary(Bin) ->
    ip_bytes_from_text(Bin, ipv4);
ip4_bytes(List) when is_list(List) ->
    ip4_bytes(list_to_binary(List));
ip4_bytes({0,0,0,0,0,16#ffff,_,_} = Mapped) ->
    {A,B,C,D} = ipv4_from_mapped(Mapped),
    <<A:8, B:8, C:8, D:8>>;
ip4_bytes({A,B,C,D}) ->
    <<A:8, B:8, C:8, D:8>>;
ip4_bytes(_) ->
    undefined.

ip6_bytes(undefined) -> undefined;
ip6_bytes(<<>>)      -> undefined;
ip6_bytes(Bin) when is_binary(Bin) ->
    %% Treat "::ffff:x.x.x.x" textual as IPv4; not IPv6.
    case ipv4_mapped_from_text(Bin) of
        {ok, _V4Bin} -> undefined;
        error        -> ip_bytes_from_text(Bin, ipv6)
    end;
ip6_bytes(List) when is_list(List) ->
    ip6_bytes(list_to_binary(List));
ip6_bytes({0,0,0,0,0,16#ffff,_,_}) ->
    %% IPv4-mapped IPv6 -> treat as IPv4 (not IPv6)
    undefined;
ip6_bytes({A,B,C,D,E,F,G,H}) ->
    <<A:16, B:16, C:16, D:16, E:16, F:16, G:16, H:16>>;
ip6_bytes(_) ->
    undefined.

ip_bytes_from_text(Bin0, Want) ->
    %% inet:parse_address wants a charlist
    try inet:parse_address(binary_to_list(Bin0)) of
        {ok, {A,B,C,D}} when Want =:= ipv4 ->
            <<A:8, B:8, C:8, D:8>>;
        {ok, {A,B,C,D,E,F,G,H}} when Want =:= ipv6 ->
            <<A:16, B:16, C:16, D:16, E:16, F:16, G:16, H:16>>;
        _ ->
            undefined
    catch
        _:_ -> undefined
    end.

%% Textual normalization – handle ::ffff:84.0.24.7 -> 84.0.24.7
ipv4_mapped_from_text(Bin0) ->
    LB = list_to_binary(string:lowercase(binary_to_list(Bin0))),
    case LB of
        << "::ffff:", Rest/binary >> ->
            case inet:parse_address(binary_to_list(Rest)) of
                {ok, {_,_,_,_}} -> {ok, Rest};
                _               -> error
            end;
        _ ->
            error
    end.

ipv4_from_mapped({0,0,0,0,0,16#ffff, A, B}) ->
    <<A1:8, A2:8>> = <<A:16>>,
    <<B1:8, B2:8>> = <<B:16>>,
    {A1, A2, B1, B2}.
