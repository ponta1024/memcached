-module(memcached_cli).
-export([start_client/0, launch_supervisor/3, launch_incr_supervisor/5]).
-define(NUM_WORKERS, 100).
-define(WR_PER_WORKER, 10000).

start_client() ->
    case gen_tcp:connect("localhost", 11211, [{active, false}, binary, {packet, line}, {reuseaddr, true}]) of
        {ok, Socket} ->
            ok = check_basics(Socket),
            ok = concurrent_incr_test(Socket),
            gen_tcp:close(Socket),
            ok = start_benchmark();
        {error, Reason } ->
            % io:format("connect: ~p~n", [Reason] ),
            {error, Reason}
    end.

check_basics(Socket) ->
    % flush_all first
    % ok = mc_flush_all(Socket),
    % read what you write...
    ok = mc_set(Socket, "foo", "bar"),
    {ok, <<"bar">>} = mc_get(Socket, "foo"),
    % read what you don't know...
    {ok, undefined} = mc_get(Socket, "ponta"),
    % delete and confirm
    ok = mc_delete(Socket, "foo"),
    {ok, undefined} = mc_get(Socket, "foo"),
    % incr and decr
    ok = mc_set(Socket, "foo", "100"),
    {ok, 105} = mc_incr(Socket, "foo", 5),
    {ok, 98} = mc_decr(Socket, "foo", 7),
    ok.

%% concurrent incr test
concurrent_incr_test(Socket) ->
    ok = mc_set(Socket, "foo", "0"),
    SuperPid = spawn(?MODULE, launch_incr_supervisor,[?NUM_WORKERS, 0, Socket, now(), self()]),
    concurrent_incr(?NUM_WORKERS, SuperPid),
    receive        
        ok -> ok
    end.

launch_incr_supervisor(N, N, Socket, S1, SPid) ->
    S2 = now(),
    io:format("~p usec elapsed, which is ~p incr /sec~n", [timer:now_diff(S2, S1), (?NUM_WORKERS)/(timer:now_diff(S2, S1)/(1000*1000))] ),
    {ok, R} = mc_get(Socket, "foo"),
    io:format("foo is ~p which should be ~p~n", [binary_to_list(R), 0+5*?NUM_WORKERS]),
    SPid ! ok;
launch_incr_supervisor(N, C, Socket, S1, SPid) ->
    process_flag(trap_exit, true),
    receive 
        _Error -> ok
        % io:format("worker(~p) ended (~p)~n", [C+1, _Error])
    end,
    launch_incr_supervisor(N, C+1, Socket, S1, SPid).

concurrent_incr(0, _SuperPid) ->
    ok;
concurrent_incr(N, SuperPid) ->
    spawn(fun() -> single_incr(SuperPid) end),
    concurrent_incr(N-1, SuperPid).

single_incr(SuperPid) ->
    link(SuperPid),
    case gen_tcp:connect("localhost", 11211, [{active, false}, binary, {packet, line}, {reuseaddr, true}]) of
        {ok, Socket} ->
            mc_incr(Socket, "foo", 5),
            gen_tcp:close(Socket);
        {error, Reason} ->
            io:format("connect: ~p~n", [Reason] ),
            {error, Reason}
    end.

%% benchmark
start_benchmark() ->
    crypto:start(),
    SuperPid = spawn(?MODULE, launch_supervisor,[?NUM_WORKERS, 0, now()]),
    concurrent_read_and_write(?NUM_WORKERS, SuperPid).

launch_supervisor(N, N, S1) ->
    S2 = now(),
    io:format("~p usec elapsed, which is ~p w/r /sec~n", [timer:now_diff(S2, S1), (?WR_PER_WORKER*?NUM_WORKERS)/(timer:now_diff(S2, S1)/(1000*1000))] );
launch_supervisor(N, C, S1) ->
    process_flag(trap_exit, true),
    receive 
        _Error -> ok
        % io:format("worker(~p) ended (~p)~n", [C+1, _Error])
    end,
    launch_supervisor(N, C+1, S1).

concurrent_read_and_write(0, _SuperPid) ->
    ok;
concurrent_read_and_write(N, SuperPid) ->
    spawn(fun() -> read_and_write(SuperPid) end),
    concurrent_read_and_write(N-1, SuperPid).

read_and_write(SuperPid) ->
    link(SuperPid),
    case gen_tcp:connect("localhost", 11211, [{active, false}, binary, {packet, line}, {reuseaddr, true}]) of
        {ok, Socket} ->
            repeat_read_write(Socket, ?WR_PER_WORKER),
            gen_tcp:close(Socket);
        {error, Reason} ->
            io:format("connect: ~p~n", [Reason] ),
            {error, Reason}
    end.

repeat_read_write(_Socket, 0) ->
    ok;
repeat_read_write(Socket, N) ->
    repeat_read_write(Socket, N-1),
    <<M1:160/integer>> = crypto:hmac(sha, <<"eigo">>, <<N>>),
    K = lists:flatten(io_lib:format("~40.16.0b", [M1])),
    <<M2:160/integer>> = crypto:hmac(sha, <<"mori">>, <<N>>),
    V = lists:flatten(io_lib:format("~40.16.0b", [M2])),
    case mc_set(Socket, K, V) of
        ok ->
            {ok, _R} = mc_get(Socket, K),
            % io:format("~w R&W success: ~p~n", [N, _R] ),
            ok;
        {error, E} ->
            io:format("~w ~p: R&W FAILED~n", [N, E] ),
            ok
    end.

mc_set(Socket, Key, Value) -> 
    case gen_tcp:send(Socket, io_lib:format("set ~s 0 0 ~w\r\n", [Key, size(list_to_binary(Value))])) of
        ok -> 
            case gen_tcp:send(Socket, io_lib:format("~s\r\n", [Value])) of
                ok ->
                    case gen_tcp:recv(Socket, 0) of
                        {ok, _} -> 
                            ok;
                        {error, Reason} ->
                            io:format("recv(A0): ~s~n",  [Reason]),
                            gen_tcp:close(Socket),
                            {error, Reason}
                    end;
                {error, Reason} ->
                    io:format("send(A1): ~s~n",  [Reason]),
                    gen_tcp:close(Socket),
                    {error, Reason}
            end;
        {error, Reason} ->
            % io:format("send(A2): ~s~n",  [Reason]),
            gen_tcp:close(Socket),
            {error, Reason}
    end.


mc_get(Socket, Key) ->
    ok = gen_tcp:send(Socket, io_lib:format("get ~s\r\n", [Key])),
    case gen_tcp:recv(Socket, 0) of
        {ok, <<"END\r\n">>} ->
            {ok, undefined};
        {ok, B} ->
            ["VALUE", _, _, Size] = string:tokens(binary_to_list(B), " \r\n"),
            inet:setopts(Socket,[{packet, raw}]),
            case gen_tcp:recv(Socket, list_to_integer(Size)) of
                {ok, Value} ->
                    % \r\nEND\r\n
                    case gen_tcp:recv(Socket, 2+3+2) of
                        {ok, _} ->
                            inet:setopts(Socket,[{packet, line}]),
                            {ok, Value};
                        {error, Reason} ->
                            io:format("recv(C): ~s~n",  [Reason]),
                            gen_tcp:close(Socket),
                            {error, Reason}
                    end;
                {error, Reason} ->
                    io:format("recv(D): ~s~n",  [Reason]),
                    gen_tcp:close(Socket),
                    {error, Reason}
            end;
        {error, Reason } ->
            % io:format("recv(E): ~s~n", [Reason]),
            gen_tcp:close(Socket),
            {error, Reason}
    end.

mc_delete(Socket, Key) ->
    ok = gen_tcp:send(Socket, io_lib:format("delete ~s\r\n", [Key])),
    case gen_tcp:recv(Socket, 0) of
        {ok, _} ->
            ok;
        {error, Reason } ->
            % io:format("recv: ~s~n",  [Reason]),
            gen_tcp:close(Socket),
            {error, Reason}
    end.

mc_incr(Socket, Key, Param) ->
    ok = gen_tcp:send(Socket, io_lib:format("incr ~s ~w\r\n", [Key, Param])),
    case gen_tcp:recv(Socket, 0) of
        {ok, V} ->
            [Value] = string:tokens(binary_to_list(V),"\r\n"),
            {ok, list_to_integer(Value)};
        {error, Reason } ->
            % io:format("recv: ~s~n", [Reason]),
            gen_tcp:close(Socket),
            {error, Reason}
    end.    

mc_decr(Socket, Key, Param) ->
    ok = gen_tcp:send(Socket, io_lib:format("decr ~s ~w\r\n", [Key, Param])),
    case gen_tcp:recv(Socket, 0) of
        {ok, V} ->
            [Value] = string:tokens(binary_to_list(V),"\r\n"),
            {ok, list_to_integer(Value)};
        {error, Reason } ->
            % io:format("recv: ~s~n", [Reason]),
            gen_tcp:close(Socket),
            {error, Reason}
    end.

mc_flush_all(Socket) ->
    ok = gen_tcp:send(Socket, <<"flush_all\r\n">>),
    case gen_tcp:recv(Socket, 0) of
        {ok, <<"OK\r\n">>} ->
            ok;
        {error, Reason } ->
            % io:format("recv: ~s~n", [Reason]),
            gen_tcp:close(Socket),
            {error, Reason}
    end.    

