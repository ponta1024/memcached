memcached / memcached-cli
=========================

memcached
---------
memchached (server)  
Note: the program requires Erlang R16B01 or newer.

$ erl  
Erlang R16B01 (erts-5.10.2) [source] [64-bit] [smp:4:4] [async-threads:10] [kernel-poll:false]  
Eshell V5.10.2  (abort with ^G)  
1> c(memcached).  
{ok,memcached}  
2> memcached:start_server().  

memcached-cli
-------------
a tool to check   
- basic functions of memcached (set, get, incr, decr)  
- concurrent incr  
- concurrent read/write performance  

$ erl  
Erlang R16B01 (erts-5.10.2) [source] [smp:4:4] [async-threads:10] [hipe] [kernel-poll:false]  
Eshell V5.10.2  (abort with ^G)  
1> c(memcached_cli).  
memcached_cli.erl:218: Warning: function mc_flush_all/1 is unused  
2> memcached_cli:start_client().  
14813 usec elapsed, which is 6750.826976304597 incr /sec  
foo is "500" which should be 500  
ok  
96012741 usec elapsed, which is 10415.284363145094 w/r /sec  

