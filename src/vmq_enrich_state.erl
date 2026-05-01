%% src/vmq_enrich_state.erl
%% Tiny ETS-based store mapping clientid -> {ip, ipv6?}.

-module(vmq_enrich_state).

-export([track_peer/2, lookup/1]).

-define(TABLE, vmq_enrich_clients).

ensure_table() ->
    case ets:info(?TABLE) of
        undefined ->
            ets:new(?TABLE, [named_table, set, public, {read_concurrency, true}]),
            ok;
        _ ->
            ok
    end.

%% Peer: usually {IpAddr, Port}, SubscriberId: {Mountpoint, ClientId} or ClientId
track_peer(Peer, SubscriberId) ->
    ensure_table(),
    {IpBin, IPv6} = peer_to_entry(Peer),
    ClientIdBin   = vmq_enrich:norm_clientid(SubscriberId),
    ets:insert(?TABLE, {ClientIdBin, IpBin, IPv6}),
    ok.

%% Returns {IP, IPv6?}. If nothing stored, returns {<<>>, false}.
lookup(ClientIdBin) when is_binary(ClientIdBin) ->
    ensure_table(),
    case ets:lookup(?TABLE, ClientIdBin) of
        [{_, IpBin, IPv6}] ->
            {IpBin, IPv6};
        [] ->
            {<<>>, false}
    end;
lookup(_Other) ->
    {<<>>, false}.

peer_to_entry({Addr, _Port}) ->
    IpBin = ip_to_bin(Addr),
    IPv6  = ipv6_flag(Addr),
    {IpBin, IPv6};
peer_to_entry(Addr) ->
    IpBin = ip_to_bin(Addr),
    IPv6  = ipv6_flag(Addr),
    {IpBin, IPv6}.

%% ---- IP helpers -----------------------------------------------------

%% Convert an IPv4-mapped IPv6 tuple to a plain IPv4 tuple.
ipv4_from_mapped({0,0,0,0,0,16#ffff, A, B}) ->
    <<A1:8, A2:8>> = <<A:16>>,
    <<B1:8, B2:8>> = <<B:16>>,
    {A1, A2, B1, B2}.

ip_to_bin({0,0,0,0,0,16#ffff,_,_} = Mapped) ->
    %% IPv4-mapped IPv6 -> print as plain IPv4
    IPv4 = ipv4_from_mapped(Mapped),
    list_to_binary(inet:ntoa(IPv4));
ip_to_bin({_,_,_,_} = IPv4) ->
    list_to_binary(inet:ntoa(IPv4));
ip_to_bin({_,_,_,_,_,_,_,_} = IPv6) ->
    list_to_binary(inet:ntoa(IPv6));
ip_to_bin(Bin) when is_binary(Bin) ->
    Bin;
ip_to_bin(List) when is_list(List) ->
    list_to_binary(List);
ip_to_bin(_) ->
    <<>>.

%% For IPv4-mapped IPv6, treat as IPv4 -> ipv6=false
ipv6_flag({0,0,0,0,0,16#ffff,_,_}) ->
    false;
ipv6_flag({_,_,_,_,_,_,_,_}) ->
    true;
ipv6_flag({_,_,_,_}) ->
    false;
ipv6_flag(Bin) when is_binary(Bin) ->
    case binary:match(Bin, <<":">>) of
        nomatch -> false;
        _       -> true
    end;
ipv6_flag(List) when is_list(List) ->
    ipv6_flag(list_to_binary(List));
ipv6_flag(_) ->
    false.
