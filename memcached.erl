-module(memcached).
-export([start_server/0]).
-define(PORT, 11211).
-define(NUM_WORKERS, 128).

start_server() ->
    ets:new(kvs, [public, named_table, {write_concurrency, true}, {read_concurrency, true}]),
    {ok, ListenSocket} = gen_tcp:listen(?PORT, [{active, false}, binary, {packet, line}, {reuseaddr, true}, {backlog, 512}, {nodelay, true}]),
    launch_workers(?NUM_WORKERS, ListenSocket).

launch_workers(0, _) ->
    ok;
launch_workers(N, ListenSocket) ->
    Pid = spawn(fun() -> pre_worker(ListenSocket) end),
    gen_tcp:controlling_process(ListenSocket, Pid),
    Pid ! ok,
    launch_workers(N-1, ListenSocket).

pre_worker(ListenSocket)  ->
    %% note: error if you issue gen_tcp:accept before gen_tcp:controlling_process returns (?)
    receive ok -> ok end,    
    accept(ListenSocket).

accept(ListenSocket) ->
    case gen_tcp:accept(ListenSocket) of
        {ok, Socket} ->
            recv_loop(Socket),
            accept(ListenSocket);
        {error, Reason} ->
            io:format("listen: ~s~n", [Reason]),
            accept(ListenSocket)
    end.

recv_loop(Socket) ->
    case gen_tcp:recv(Socket, 0) of
        {ok, B} ->
            Query = binary:split(B, [<<" ">>, <<"\r\n">>], [global, trim]),
            case Query of
                [<<"set">>, Key, _Flag, _Expire, Size] ->
                    set_command(Socket, Key, Size);
                [<<"get">>, Key] ->
                    get_command(Socket, Key);
                [<<"delete">>, Key] ->
                    delete_command(Socket, Key);
                [<<"incr">>, Key, Param] ->
                    incr_command(Socket, Key, Param, 1);
                [<<"decr">>, Key, Param] ->
                    incr_command(Socket, Key, Param, -1);
                [<<"flush_all">>] ->
                    flush_all_command(Socket);
                _Other ->
                    ok = gen_tcp:send(Socket, <<"ERROR\r\n">>)
            end,
            recv_loop(Socket);
        {error, closed} ->
            gen_tcp:close(Socket)
    end.

set_command(Socket, Key, Size) ->
    inet:setopts(Socket,[{packet, raw}]),
    {ok, Value} = gen_tcp:recv(Socket, binary_to_integer(Size)),
    ets:insert(kvs, {Key, Value}),
    ok = gen_tcp:send(Socket, <<"STORED\r\n">>),
    {ok, _V} = gen_tcp:recv(Socket, 2),
    inet:setopts(Socket,[{packet, line}]).

get_command(Socket, Key) ->
    case ets:lookup(kvs, Key) of
        [{Key, Value}] ->
            ok = gen_tcp:send(Socket, io_lib:format("VALUE ~s 0 ~w\r\n~s\r\nEND\r\n", [Key, size(Value), Value]));
        [] ->
            ok = gen_tcp:send(Socket, <<"END\r\n">>)
    end.

delete_command(Socket, Key) ->
    case ets:lookup(kvs, Key) of
        [{Key, _V}] ->
            ets:delete(kvs, Key),
            ok = gen_tcp:send(Socket, <<"DELETED\r\n">>);
        [] ->
            ok = gen_tcp:send(Socket, <<"NOT_FOUND\r\n">>)
    end.

incr_command(Socket, Key, Param, Polarity) ->
    case ets:lookup(kvs, Key) of
        [{Key, Value}] ->
            try binary_to_integer(Value) of
                CurrentValue ->
                    try binary_to_integer(Param) of
                        Parameter ->
                            NewValue = CurrentValue + Parameter * Polarity,
                            ok = gen_tcp:send(Socket, io_lib:format("~w\r\n", [NewValue])),
                            ets:insert(kvs, [{Key, integer_to_binary(NewValue)}])
                    catch
                        error:badarg ->
                            ok = gen_tcp:send(Socket, <<"CLIENT_ERROR invalid numeric delta argument\r\n">>)
                    end
            catch
                error:badarg ->
                    ok = gen_tcp:send(Socket, <<"CLIENT_ERROR cannot increment or decrement non-numeric value\r\n">>)
            end;
        [] ->
            ok = gen_tcp:send(Socket, <<"NOT_FOUND\r\n">>)
    end.

flush_all_command(Socket) ->
    ets:delete_all_objects(kvs),
    ok = gen_tcp:send(Socket, <<"OK\r\n">>).

