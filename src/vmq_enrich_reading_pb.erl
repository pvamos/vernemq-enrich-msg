%% src/vmq_enrich_reading_pb.erl
%%
%% Minimal protobuf:
%% - decode envsensor.Reading (new schema: rssi/batt/esp32_t + bme280 + sht4x)
%% - encode envsensor.vernemq.EnrichedReading (flattened)
%%
%% No external protobuf libs.

-module(vmq_enrich_reading_pb).
-export([decode_reading/1, encode_enriched_reading/1]).

-define(WT_VARINT, 0).
-define(WT_64,     1).
-define(WT_LEN,    2).
-define(WT_32,     5).

%%% ---------------------------------------------------------------------
%%% Public API
%%% ---------------------------------------------------------------------

decode_reading(Bin) when is_binary(Bin) ->
    decode_msg(Bin, reading, #{}).

encode_enriched_reading(M) when is_map(M) ->
    Time    = get_uint64(time, M, 0),
    Topic   = get_bin(topic, M, undefined),
    User    = get_bin(user, M, undefined),
    Client  = get_bin(clientid, M, undefined),
    Broker  = get_bin(broker, M, undefined),

    IPv4b   = ip4_bytes(maps:get(ipv4, M, undefined)),
    IPv6b   = ip6_bytes(maps:get(ipv6, M, undefined)),

    Mac     = maps:get(mac, M, 0),

    Rssi    = maps:get(rssi, M, 0),
    Batt    = maps:get(batt, M, 0),
    EspT    = maps:get(esp32_t, M, 0.0),

    Iolist = [
        enc_uint64(1, Time),
        enc_string_opt(2, Topic),
        enc_ip(IPv4b, IPv6b),
        enc_string_opt(5, User),
        enc_string_opt(6, Client),
        enc_string_opt(7, Broker),

        enc_fixed64(8, Mac),

        enc_sint32(9, Rssi),
        enc_uint32(10, Batt),
        enc_float(11, EspT),

        enc_float_opt(12, maps:get(bme280_t, M, undefined)),
        enc_float_opt(13, maps:get(bme280_p, M, undefined)),
        enc_float_opt(14, maps:get(bme280_h, M, undefined)),

        enc_float_opt(15, maps:get(sht4x_t, M, undefined)),
        enc_float_opt(16, maps:get(sht4x_h, M, undefined))
    ],
    iolist_to_binary(Iolist).

%%% ---------------------------------------------------------------------
%%% Decoder (minimal)
%%% ---------------------------------------------------------------------

decode_msg(<<>>, _Type, Acc) ->
    {ok, Acc};
decode_msg(Bin, Type, Acc0) ->
    case read_key(Bin) of
        {ok, FieldNo, WT, Rest} ->
            case decode_field(Type, FieldNo, WT, Rest, Acc0) of
                {ok, Rest2, Acc1}   -> decode_msg(Rest2, Type, Acc1);
                {skip, Rest2, Acc1} -> decode_msg(Rest2, Type, Acc1);
                {error, _}=E        -> E
            end;
        {error, _}=E ->
            E
    end.

%% envsensor.Reading:
decode_field(reading, 1, ?WT_64, Rest, Acc) ->
    case Rest of
        <<Mac:64/little-unsigned, Rest2/binary>> ->
            {ok, Rest2, Acc#{mac => Mac}};
        _ ->
            {error, truncated_fixed64_mac}
    end;

%% sint32 rssi = 2;  (zigzag varint)
decode_field(reading, 2, ?WT_VARINT, Rest, Acc) ->
    case read_varint(Rest) of
        {ok, V, Rest2} ->
            {ok, Rest2, Acc#{rssi => zigzag_decode32(V)}};
        E -> E
    end;

%% uint32 batt = 3;  (varint)
decode_field(reading, 3, ?WT_VARINT, Rest, Acc) ->
    case read_varint(Rest) of
        {ok, V, Rest2} ->
            {ok, Rest2, Acc#{batt => V}};
        E -> E
    end;

%% float esp32_t = 4; (fixed32)
decode_field(reading, 4, ?WT_32, Rest, Acc) ->
    case Rest of
        <<F:32/float-little, Rest2/binary>> ->
            {ok, Rest2, Acc#{esp32_t => F}};
        _ ->
            {error, truncated_fixed32_float}
    end;

%% optional BME280 bme280 = 5;
decode_field(reading, 5, ?WT_LEN, Rest, Acc) ->
    case read_len_delim(Rest) of
        {ok, Sub, Rest2} ->
            case decode_msg(Sub, bme280, #{}) of
                {ok, B} -> {ok, Rest2, Acc#{bme280 => B}};
                E       -> E
            end;
        E -> E
    end;

%% optional SHT4X sht4x = 6;
decode_field(reading, 6, ?WT_LEN, Rest, Acc) ->
    case read_len_delim(Rest) of
        {ok, Sub, Rest2} ->
            case decode_msg(Sub, sht4x, #{}) of
                {ok, S} -> {ok, Rest2, Acc#{sht4x => S}};
                E       -> E
            end;
        E -> E
    end;

%% Sensor message fields (float = fixed32, WT=5)
decode_field(bme280, 1, ?WT_32, Rest, Acc) -> decode_float_field(Rest, Acc, t);
decode_field(bme280, 2, ?WT_32, Rest, Acc) -> decode_float_field(Rest, Acc, p);
decode_field(bme280, 3, ?WT_32, Rest, Acc) -> decode_float_field(Rest, Acc, h);

decode_field(sht4x,  1, ?WT_32, Rest, Acc) -> decode_float_field(Rest, Acc, t);
decode_field(sht4x,  2, ?WT_32, Rest, Acc) -> decode_float_field(Rest, Acc, h);

%% Unknown field => skip
decode_field(_Type, _FieldNo, WT, Rest, Acc) ->
    case skip_field(WT, Rest) of
        {ok, Rest2} -> {skip, Rest2, Acc};
        E           -> E
    end.

decode_float_field(Rest, Acc, Key) ->
    case Rest of
        <<F:32/float-little, Rest2/binary>> ->
            {ok, Rest2, Acc#{Key => F}};
        _ ->
            {error, truncated_fixed32_float}
    end.

read_key(Bin) ->
    case read_varint(Bin) of
        {ok, Key, Rest} ->
            {ok, Key bsr 3, Key band 7, Rest};
        E ->
            E
    end.

read_len_delim(Bin) ->
    case read_varint(Bin) of
        {ok, Len, Rest} when Len >= 0 ->
            case Rest of
                <<Sub:Len/binary, Rest2/binary>> -> {ok, Sub, Rest2};
                _                                -> {error, truncated_len_delim}
            end;
        E ->
            E
    end.

read_varint(Bin) ->
    read_varint_loop(Bin, 0, 0).

read_varint_loop(<<>>, _Acc, _Shift) ->
    {error, truncated_varint};
read_varint_loop(<<Byte:8, Rest/binary>>, Acc, Shift) ->
    Val = Acc bor ((Byte band 127) bsl Shift),
    case (Byte band 128) of
        0 -> {ok, Val, Rest};
        _ -> read_varint_loop(Rest, Val, Shift + 7)
    end.

skip_field(?WT_VARINT, Bin) ->
    case read_varint(Bin) of
        {ok, _V, Rest} -> {ok, Rest};
        E              -> E
    end;
skip_field(?WT_64, Bin) ->
    case Bin of
        <<_:64, Rest/binary>> -> {ok, Rest};
        _                     -> {error, truncated_skip64}
    end;
skip_field(?WT_LEN, Bin) ->
    case read_len_delim(Bin) of
        {ok, _Sub, Rest} -> {ok, Rest};
        E                -> E
    end;
skip_field(?WT_32, Bin) ->
    case Bin of
        <<_:32, Rest/binary>> -> {ok, Rest};
        _                     -> {error, truncated_skip32}
    end;
skip_field(_Other, _Bin) ->
    {error, unsupported_wire_type}.

%%% ---------------------------------------------------------------------
%%% Encoder helpers
%%% ---------------------------------------------------------------------

enc_ip(IPv4b, _IPv6b) when is_binary(IPv4b), byte_size(IPv4b) =:= 4 ->
    enc_bytes(3, IPv4b);
enc_ip(_IPv4b, IPv6b) when is_binary(IPv6b), byte_size(IPv6b) =:= 16 ->
    enc_bytes(4, IPv6b);
enc_ip(_, _) ->
    [].

enc_uint64(FieldNo, Int) when is_integer(Int), Int >= 0 ->
    [ key(FieldNo, ?WT_VARINT), varint(Int) ].

enc_uint32(FieldNo, Int) when is_integer(Int), Int >= 0 ->
    [ key(FieldNo, ?WT_VARINT), varint(Int) ].

enc_sint32(FieldNo, Int) when is_integer(Int) ->
    Z = zigzag_encode32(Int),
    [ key(FieldNo, ?WT_VARINT), varint(Z) ].

enc_fixed64(FieldNo, Int) when is_integer(Int), Int >= 0 ->
    [ key(FieldNo, ?WT_64), <<Int:64/little-unsigned>> ].

enc_float(FieldNo, F) when is_integer(F) ->
    enc_float(FieldNo, float(F));
enc_float(FieldNo, F) when is_float(F) ->
    [ key(FieldNo, ?WT_32), <<F:32/float-little>> ].

enc_float_opt(_FieldNo, undefined) -> [];
enc_float_opt(_FieldNo, null)      -> [];
enc_float_opt(FieldNo, I) when is_integer(I) -> enc_float_opt(FieldNo, float(I));
enc_float_opt(FieldNo, F) when is_float(F) ->
    [ key(FieldNo, ?WT_32), <<F:32/float-little>> ].

enc_bytes(FieldNo, Bin) when is_binary(Bin) ->
    [ key(FieldNo, ?WT_LEN), varint(byte_size(Bin)), Bin ].

enc_string_opt(_FieldNo, undefined) -> [];
enc_string_opt(_FieldNo, <<>>)      -> [];
enc_string_opt(FieldNo, Bin) when is_binary(Bin) ->
    enc_bytes(FieldNo, Bin).

key(FieldNo, WireType) ->
    varint((FieldNo bsl 3) bor WireType).

varint(N) when is_integer(N), N >= 0 ->
    varint_loop_enc(N, []).

varint_loop_enc(N, Acc) when N < 128 ->
    lists:reverse([N | Acc]);
varint_loop_enc(N, Acc) ->
    Byte = (N band 127) bor 128,
    varint_loop_enc(N bsr 7, [Byte | Acc]).

%% zigzag for sint32
zigzag_encode32(I) ->
    %% mask to 32-bit unsigned
    (((I bsl 1) bxor (I bsr 31)) band 16#FFFFFFFF).

zigzag_decode32(V) when is_integer(V), V >= 0 ->
    (V bsr 1) bxor -(V band 1).

%%% ---------------------------------------------------------------------
%%% Map helpers
%%% ---------------------------------------------------------------------

get_uint64(Key, M, Default) ->
    case maps:get(Key, M, Default) of
        I when is_integer(I), I >= 0 -> I;
        B when is_binary(B) ->
            try binary_to_integer(B) catch _:_ -> Default end;
        L when is_list(L) ->
            try list_to_integer(L) catch _:_ -> Default end;
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

%%% ---------------------------------------------------------------------
%%% IP helpers (encode as 4/16 bytes from tuple or text)
%%% ---------------------------------------------------------------------

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
    case ipv4_mapped_from_text(Bin) of
        {ok, _V4Bin} -> undefined;
        error        -> ip_bytes_from_text(Bin, ipv6)
    end;
ip6_bytes(List) when is_list(List) ->
    ip6_bytes(list_to_binary(List));
ip6_bytes({0,0,0,0,0,16#ffff,_,_}) ->
    undefined;
ip6_bytes({A,B,C,D,E,F,G,H}) ->
    <<A:16, B:16, C:16, D:16, E:16, F:16, G:16, H:16>>;
ip6_bytes(_) ->
    undefined.

ip_bytes_from_text(Bin0, Want) ->
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
