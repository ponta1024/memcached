-module(memcached).
-export([start_server/0, launch_ets/0, launch_listen/1, launch_supervisor/1, acceptor/2]).
-define(PORT, 11211).
-define(NUM_WORKERS, 128).


start_server() ->
    spawn(?MODULE, launch_ets, []),
    spawn(?MODULE, launch_listen, [self()]),
    receive ListenSocket -> ok end,
    SuperPid = spawn(?MODULE, launch_supervisor,[ListenSocket]),
    launch_workers(?NUM_WORKERS, SuperPid,  ListenSocket).

launch_ets() ->
    ets:new(kvs, [public, named_table, {write_concurrency, true}, {read_concurrency, true}]),
    receive Message -> io:format( "ets exiting...~p~n",[Message]) end.

launch_listen(Pid) ->
    {ok, ListenSocket} = gen_tcp:listen(?PORT, 
                                        [{active, false}, 
                                         binary, 
                                         {packet, line}, 
                                         {reuseaddr, true}, 
                                         {backlog, 512}, 
                                         {nodelay, true}]),
    Pid ! ListenSocket,
    receive Message -> io:format( "listen exiting...~p~n",[Message]) end.

launch_supervisor(ListenSocket) ->
    process_flag(trap_exit, true),
    receive 
        Error -> 
            io:format("acceptor died (~p) and respawned~n", [Error]),
            spawn(?MODULE, acceptor, [ListenSocket, self()])
    end,
    launch_supervisor(ListenSocket).

launch_workers(0, _, _) ->
    ok;
launch_workers(N, SuperPid, ListenSocket) ->
    spawn(?MODULE, acceptor, [ListenSocket, SuperPid]),
    launch_workers(N-1, SuperPid, ListenSocket).

acceptor(ListenSocket, SuperPid) ->
    link(SuperPid),
    case gen_tcp:accept(ListenSocket) of
        {ok, WorkerSocket} ->
            recv_loop(WorkerSocket),
            acceptor(ListenSocket, SuperPid);
        {error, Reason} ->
            io:format("listen: ~s~n", [Reason]),
            acceptor(ListenSocket, SuperPid)
    end.

recv_loop(Socket) ->
    case gen_tcp:recv(Socket, 0) of
        {ok, Data} ->
            case  binary:split(Data, [<<" ">>, <<"\r\n">>], [global, trim]) of
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
    true = ets:insert(kvs, {Key, Value}),
    {ok, _V} = gen_tcp:recv(Socket, 2),
    ok = gen_tcp:send(Socket, <<"STORED\r\n">>),
    inet:setopts(Socket,[{packet, line}]).

get_command(Socket, Key) ->
    case ets:lookup(kvs, Key) of
        [{Key, Value}] ->
            ok = gen_tcp:send(Socket, ["VALUE ", Key, " 0 ", integer_to_list(size(Value)), "\r\n", Value, "\r\nEND\r\n"]);
        [] ->
            ok = gen_tcp:send(Socket, <<"END\r\n">>)
    end.

delete_command(Socket, Key) ->
    case ets:member(kvs, Key) of
        true ->
            true = ets:delete(kvs, Key),
            ok = gen_tcp:send(Socket, <<"DELETED\r\n">>);
        false ->
            ok = gen_tcp:send(Socket, <<"NOT_FOUND\r\n">>)
    end.

incr_command(Socket, Key, Param, Polarity) ->
    case ets:lookup(kvs, Key) of
        [{Key, Value}] ->
            try binary_to_integer(Value) of
                CurrentValue ->
                    try 
                        Parameter = binary_to_integer(Param),
                        NewValue = CurrentValue + Parameter * Polarity,
                        true = ets:insert(kvs, [{Key, integer_to_binary(NewValue)}]),
                        ok = gen_tcp:send(Socket, io_lib:format("~w\r\n", [NewValue]))
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
    true = ets:delete_all_objects(kvs),
    ok = gen_tcp:send(Socket, <<"OK\r\n">>).

