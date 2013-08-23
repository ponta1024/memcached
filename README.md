memcached
=========

Note: the program requires Erlang R16B01 or newer.

$ erl  
Erlang R16B01 (erts-5.10.2) [source] [64-bit] [smp:4:4] [async-threads:10] [kernel-poll:false]  
Eshell V5.10.2  (abort with ^G)  
1> c(memcached).  
{ok,memcached}  
2> memcached:start_server().  

