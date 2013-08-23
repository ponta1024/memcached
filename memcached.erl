-module(memcached).
-export([start_server/0]).
-define(PORT, 11211).

start_server() ->
    ets:new(kvs, [public, named_table, {write_concurrency, true}, {read_concurrency, true}]),
    case gen_tcp:listen(?PORT, [{active, false}, binary, {packet, line}, {reuseaddr, true}, {backlog, 512}, {nodelay, true}]) of
	{ok, ListenSocket} ->
	    accept(ListenSocket);
	{error, Reason} ->
	    io:format("Reason: cannot launch memcaced server: ~p~n", [Reason])
    end.

accept(ListenSocket) ->
    case gen_tcp:accept(ListenSocket) of
	{ok, Socket} ->
	    Pid = spawn(fun() -> recv_loop(Socket) end),
	    gen_tcp:controlling_process(Socket, Pid),
	    accept(ListenSocket);
	{error, Reason} ->
	    io:format("listen: ~s~n", [Reason]),
	    accept(ListenSocket)
    end.

recv_loop(Socket) ->
    case gen_tcp:recv(Socket, 0) of
	{ok, B} ->
	    Query = string:tokens(binary_to_list(B), " \r\n"),
	    case Query of
		["set", Key, _Flag, _Expire, Size] ->
		    inet:setopts(Socket,[{packet, raw}]),
		    case gen_tcp:recv(Socket, list_to_integer(Size)) of
			{ok, Value} ->
			    ets:insert(kvs, {Key, Value}),
			    ok = gen_tcp:send(Socket, <<"STORED\r\n">>),
			case gen_tcp:recv(Socket, 2) of
			    {ok, _} ->
				inet:setopts(Socket,[{packet, line}]),
				ok;
			    {error, closed} ->
				io:format("recv: ~n" ),
				gen_tcp:close(Socket),
				exit(close)
			end;
			{error, closed} ->
			    io:format("recv: closed~n" ),
			    gen_tcp:close(Socket),
			    exit(close)
		    end;
		["get", Key] ->
		    case ets:lookup(kvs, Key) of
			[{Key, Value}] ->
			    ok = gen_tcp:send(Socket, io_lib:format("VALUE ~s 0 ~w\r\n~s\r\nEND\r\n", 
							       [Key, size(Value), Value]));
			[] ->
			    ok = gen_tcp:send(Socket, <<"END\r\n">>)
		    end;
		["delete", Key] ->
		    case ets:lookup(kvs, Key) of
			[{Key, _}] ->
			    ets:delete(kvs, Key),
			    ok = gen_tcp:send(Socket, <<"DELETED\r\n">>);
			[] ->
			    ok = gen_tcp:send(Socket, <<"NOT_FOUND\r\n">>)
		    end;
		["incr", Key, Param] ->
		    case ets:lookup(kvs, Key) of
			[{Key, Value}] ->
			    try list_to_integer(binary_to_list(Value)) of
				CurrentValue ->
				    try list_to_integer(Param) of
					Parameter ->
					    NewValue = CurrentValue + Parameter,
					    ok = gen_tcp:send(Socket, io_lib:format("~w\r\n", [NewValue])),
					    ets:insert(kvs, [{Key, list_to_binary(integer_to_list(NewValue))}])
				    catch
					_:_ ->
					    ok = gen_tcp:send(Socket, <<"CLIENT_ERROR invalid numeric delta argument\r\n">>)
				    end
			    catch
				_:_ ->
				    ok = gen_tcp:send(Socket, <<"CLIENT_ERROR cannot increment or decrement non-numeric value\r\n">>)
			    end;
			[] ->
			    ok = gen_tcp:send(Socket, <<"NOT_FOUND\r\n">>)
		    end;
		["decr", Key, Param] ->
		    case ets:lookup(kvs, Key) of
			[{Key, Value}] ->
			    try list_to_integer(binary_to_list(Value)) of
				CurrentValue ->
				    try list_to_integer(Param) of
					Parameter ->
					    NewValue = CurrentValue - Parameter,
					    ok = gen_tcp:send(Socket, io_lib:format("~w\r\n", [NewValue])),
					    ets:insert(kvs, [{Key, list_to_binary(integer_to_list(NewValue))}])
				    catch
					_:_ ->
					    ok = gen_tcp:send(Socket, <<"CLIENT_ERROR invalid numeric delta argument\r\n">>)
				    end
			    catch
				_:_ ->
				    ok = gen_tcp:send(Socket, <<"CLIENT_ERROR cannot increment or decrement non-numeric value\r\n">>)
			    end;
			[] ->
			    ok = gen_tcp:send(Socket, <<"NOT_FOUND\r\n">>)
		    end;
		["flush_all"] ->
		    ets:delete_all_objects(kvs),
		    ok = gen_tcp:send(Socket, <<"OK\r\n">>);
		_Other ->
		    ok = gen_tcp:send(Socket, <<"ERROR\r\n">>)
	    end,
	    recv_loop(Socket);
	{error, closed} ->
	    gen_tcp:close(Socket),
	    ok
    end.
