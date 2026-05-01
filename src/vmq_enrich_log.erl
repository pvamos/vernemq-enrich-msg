%% vmq_enrich_log.erl
%% Tiny wrapper over OTP logger with a runtime threshold from env.

-module(vmq_enrich_log).
-export([log/3, should/1]).

%% Map levels to an order; lower = more severe
order(emergency) -> 0;
order(alert)     -> 1;
order(critical)  -> 2;
order(error)     -> 3;
order(warning)   -> 4;
order(notice)    -> 5;
order(info)      -> 6;
order(debug)     -> 7;
order(_)         -> 6.

should(Level) ->
    Conf = vmq_enrich_config:log_level(),
    order(Level) =< order(Conf).

log(Level, Fmt, Args) ->
    case should(Level) of
        true  -> logger:log(Level, Fmt, Args);
        false -> ok
    end.
