import DataStructures.OrderedSet

const EXEC = ["exec"]

baremodule Aggregate
    const NotSet = ""
    const Sum = "sum"
    const Min = "min"
    const Max = "max"
end

# Key commands
@redisfunction "del" Integer key...
@redisfunction "dump" String key
@redisfunction "exists" Bool key
@redisfunction "expire" Bool key seconds
@redisfunction "expireat" Bool key timestamp
@redisfunction "keys" Set{String} pattern
@redisfunction "migrate" Bool host port key destinationdb timeout
@redisfunction "move" Bool key db
@redisfunction "persist" Bool key
@redisfunction "pexpire" Bool key milliseconds
@redisfunction "pexpireat" Bool key millisecondstimestamp
@redisfunction "pttl" Integer key
@redisfunction "randomkey" Nullable{String}
@redisfunction "rename" String key newkey
@redisfunction "renamenx" Bool key newkey
@redisfunction "restore" Bool key ttl serializedvalue
@redisfunction "scan" Array{String, 1} cursor::Integer options...
@redisfunction "sort" Array{String, 1} key options...
@redisfunction "ttl" Integer key
function keytype(conn::RedisConnection, key)
    response = execute_command(conn, flatten_command("type", key))
    convert_response(String, response)
end
function keytype(conn::TransactionConnection, key)
    execute_command(conn, flatten_command("type", key))
end

# String commands
@redisfunction "append" Integer key value
@redisfunction "bitcount" Integer key options...
@redisfunction "bitop" Integer operation destkey key keys...
@redisfunction "bitpos" Integer key bit options...
@redisfunction "decr" Integer key
@redisfunction "decrby" Integer key decrement
@redisfunction "get" Nullable{String} key
@redisfunction "getbit" Integer key offset
@redisfunction "getrange" String key start finish
@redisfunction "getset" String key value
@redisfunction "incr" Integer key
@redisfunction "incrby" Integer key increment::Integer

# Bulk string reply: the value of key after the increment,
# as per http://redis.io/commands/incrbyfloat
@redisfunction "incrbyfloat" String key increment::Float64
@redisfunction "mget" Array{Nullable{String}, 1} key keys...
@redisfunction "mset" Bool keyvalues
@redisfunction "msetnx" Bool keyvalues
@redisfunction "psetex" String key milliseconds value
@redisfunction "set" Bool key value options...
@redisfunction "setbit" Integer key offset value
@redisfunction "setex" String key seconds value
@redisfunction "setnx" Bool key value
@redisfunction "setrange" Integer key offset value
@redisfunction "strlen" Integer key

# Hash commands
@redisfunction "hdel" Integer key field fields...
@redisfunction "hexists" Bool key field
@redisfunction "hget" Nullable{String} key field
@redisfunction "hgetall" Dict{String, String} key
@redisfunction "hincrby" Integer key field increment::Integer

# Bulk string reply: the value of key after the increment,
# as per http://redis.io/commands/hincrbyfloat
@redisfunction "hincrbyfloat" String key field increment::Float64

@redisfunction "hkeys" Array{String, 1} key
@redisfunction "hlen" Integer key
@redisfunction "hmget" Array{Nullable{String}, 1} key field fields...
@redisfunction "hmset" Bool key value
@redisfunction "hset" Bool key field value
@redisfunction "hsetnx" Bool key field value
@redisfunction "hvals" Array{String, 1} key
@redisfunction "hscan" Array key cursor::Integer options...

# List commands
@redisfunction "blpop" Array{String, 1} keys timeout
@redisfunction "brpop" Array{String, 1} keys timeout
@redisfunction "brpoplpush" String source destination timeout
@redisfunction "lindex" Nullable{String} key index
@redisfunction "linsert" Integer key place pivot value
@redisfunction "llen" Integer key
@redisfunction "lpop" Nullable{String} key
@redisfunction "lpush" Integer key value values...
@redisfunction "lpushx" Integer key value
@redisfunction "lrange" Array{String, 1} key start finish
@redisfunction "lrem" Integer key count value
@redisfunction "lset" String key index value
@redisfunction "ltrim" String key start finish
@redisfunction "rpop" Nullable{String} key
@redisfunction "rpoplpush" Nullable{String} source destination
@redisfunction "rpush" Integer key value values...
@redisfunction "rpushx" Integer key value

# Set commands
@redisfunction "sadd" Integer key member members...
@redisfunction "scard" Integer key
@redisfunction "sdiff" Set{String} key keys...
@redisfunction "sdiffstore" Integer destination key keys...
@redisfunction "sinter" Set{String} key keys...
@redisfunction "sinterstore" Integer destination key keys...
@redisfunction "sismember" Bool key member
@redisfunction "smembers" Set{String} key
@redisfunction "smove" Bool source destination member
@redisfunction "spop" Nullable{String} key
@redisfunction "srandmember" Nullable{String} key
@redisfunction "srandmember" Set{String} key count
@redisfunction "srem" Integer key member members...
@redisfunction "sunion" Set{String} key keys...
@redisfunction "sunionstore" Integer destination key keys...
@redisfunction "sscan" Set{String} key cursor::Integer options...

# Sorted set commands
#=
merl-dev: a number of methods were added to take String for score value
to enable score ranges like '(1 2,' or "-inf", "+inf",
as per docs http://redis.io/commands/zrangebyscore
=#

@redisfunction "zadd" Integer key score::Number member::String

# NOTE:  using ZADD with Dicts could introduce bugs if some scores are identical
@redisfunction "zadd" Integer key scorememberdict

#=
This following version of ZADD enables adding new members using `Tuple{Int64, String}` or
`Tuple{Float64, String}` for single or multiple additions to the sorted set without
resorting to the use of `Dict`, which cannot be used in the case where all entries have the same score.
=#
@redisfunction "zadd" Integer key scoremembertup scorememberstup...

@redisfunction "zcard" Integer key
@redisfunction "zcount" Integer key min max

# Bulk string reply: the new score of member (a double precision floating point number),
# represented as string, as per http://redis.io/commands/zincrby
@redisfunction "zincrby" String key increment member

@redisfunction "zlexcount" Integer key min max
@redisfunction "zrange" OrderedSet{String} key start finish options...
@redisfunction "zrangebylex" OrderedSet{String} key min max options...
@redisfunction "zrangebyscore" OrderedSet{String} key min max options...
@redisfunction "zrank" Nullable{Integer} key member
@redisfunction "zrem" Integer key member members...
@redisfunction "zremrangebylex" Integer key min max
@redisfunction "zremrangebyrank" Integer key start finish
@redisfunction "zremrangebyscore" Integer key start finish
@redisfunction "zrevrange" OrderedSet{String} key start finish options...
@redisfunction "zrevrangebyscore" OrderedSet{String} key start finish options...
@redisfunction "zrevrank" Nullable{Integer} key member
# ZCORE returns a Bulk string reply: the score of member (a double precision floating point
# number), represented as string.
@redisfunction "zscore" Nullable{String} key member
@redisfunction "zscan" Set{String} key cursor::Integer options...

function _build_store_internal(destination, numkeys, keys, weights, aggregate, command)
    length(keys) > 0 || throw(ClientException("Must supply at least one key"))
    suffix = []
    if length(weights) > 0
        suffix = map(string, weights)
        unshift!(suffix, "weights")
    end
    if aggregate != Aggregate.NotSet
        push!(suffix, "aggregate")
        push!(suffix, aggregate)
    end
    vcat([command, destination, numkeys], keys, suffix)
end

# TODO: PipelineConnection and TransactionConnection
function zinterstore(conn::RedisConnectionBase, destination, numkeys,
    keys::Array, weights=[]; aggregate=Aggregate.NotSet)
    command = _build_store_internal(destination, numkeys, keys, weights, aggregate, "zinterstore")
    execute_command(conn, command)
end

function zunionstore(conn::RedisConnectionBase, destination, numkeys::Integer,
    keys::Array, weights=[]; aggregate=Aggregate.NotSet)
    command = _build_store_internal(destination, numkeys, keys, weights, aggregate, "zunionstore")
    execute_command(conn, command)
end

# HyperLogLog commands
@redisfunction "pfadd" Bool key element elements...
@redisfunction "pfcount" Integer key keys...
@redisfunction "pfmerge" Bool destkey sourcekey sourcekeys...

# Connection commands
@redisfunction "auth" String password
@redisfunction "echo" String message
@redisfunction "ping" String
@redisfunction "quit" Bool
@redisfunction "select" String index

# Transaction commands
@redisfunction "discard" Bool
@redisfunction "exec" Array{Bool} # only one element ever in this array?
@redisfunction "multi" Bool
@redisfunction "unwatch" Bool
@redisfunction "watch" Bool key keys...

# Scripting commands
# TODO: PipelineConnection and TransactionConnection
function evalscript(conn::RedisConnection, script, numkeys::Integer, args)
    response = execute_command(conn, flatten_command("eval", script, numkeys, args))
    convert_eval_response(Any, response)
end

#################################################################
# TODO: NEED TO TEST BEYOND THIS POINT
@redisfunction "evalsha" Any sha1 numkeys keys args
@redisfunction "script_exists" Array script scripts...
@redisfunction "script_flush" String
@redisfunction "script_kill" String
@redisfunction "script_load" String script

# Server commands
@redisfunction "bgrewriteaof" Bool
@redisfunction "bgsave" String
@redisfunction "client_getname" String
@redisfunction "client_list" String
@redisfunction "client_pause" Bool timeout
@redisfunction "client_setname" Bool name
@redisfunction "cluster_slots" Array
@redisfunction "command" Array
@redisfunction "command_count" Integer
@redisfunction "command_info" Array command commands...
@redisfunction "config_get" Array parameter
@redisfunction "config_resetstat" Bool
@redisfunction "config_rewrite" Bool
@redisfunction "config_set" Bool parameter value
@redisfunction "dbsize" Integer
@redisfunction "debug_object" String key
@redisfunction "debug_segfault" Any
@redisfunction "flushall" String
@redisfunction "flushdb" String Integer
@redisfunction "info" String
@redisfunction "info" String section
@redisfunction "lastsave" Integer
@redisfunction "role" Array
@redisfunction "save" Bool
@redisfunction "shutdown" String
@redisfunction "shutdown" String option
@redisfunction "slaveof" String host port
@redisfunction "_time" Array{String, 1}

# Sentinel commands
@sentinelfunction "master" Dict{String, String} mastername
@sentinelfunction "reset" Integer pattern
@sentinelfunction "failover" Any mastername
@sentinelfunction "monitor" Bool name ip port quorum
@sentinelfunction "remove" Bool name
@sentinelfunction "set" Bool name option value

function sentinel_masters(conn::SentinelConnection)
    response = execute_command(conn, flatten_command("sentinel", "masters"))
    [convert_response(Dict, master) for master in response]
end

function sentinel_slaves(conn::SentinelConnection, mastername)
    response = execute_command(conn, flatten_command("sentinel", "slaves", mastername))
    [convert_response(Dict, slave) for slave in response]
end

function sentinel_getmasteraddrbyname(conn::SentinelConnection, mastername)
    execute_command(conn, flatten_command("sentinel", "get-master-addr-by-name", mastername))
end

# Custom commands (PubSub/Transaction)
@redisfunction "publish" Integer channel message

function _subscribe(conn::SubscriptionConnection, channels::Array)
    execute_command_without_reply(conn, unshift!(channels, "subscribe"))
end

function subscribe(conn::SubscriptionConnection, channel::String, callback::Function)
    conn.callbacks[channel] = callback
    _subscribe(conn, [channel])
end

function subscribe(conn::SubscriptionConnection, subs::Dict{String, Function})
    for (channel, callback) in subs
        conn.callbacks[channel] = callback
    end
    _subscribe(conn, collect(keys(subs)))
end

function unsubscribe(conn::SubscriptionConnection, channel::String)
    delete!(conn.callbacks, channel)
    execute_command_without_reply(conn, unshift!([channel], "unsubscribe"))
end

# function unsubscribe(conn::SubscriptionConnection, channels...)
#     for channel in channels
#         delete!(conn.callbacks, channel)
#     end
#     execute_command(conn, unshift!(channels, "unsubscribe"))
# end

function _psubscribe(conn::SubscriptionConnection, patterns::Array)
    execute_command_without_reply(conn, unshift!(patterns, "psubscribe"))
end

function psubscribe(conn::SubscriptionConnection, pattern::String, callback::Function)
    conn.callbacks[pattern] = callback
    _psubscribe(conn, [pattern])
end

function psubscribe(conn::SubscriptionConnection, subs::Dict{String, Function})
    for (pattern, callback) in subs
        conn.callbacks[pattern] = callback
    end
    _psubscribe(conn, collect(values(subs)))
end

function punsubscribe(conn::SubscriptionConnection, patterns...)
    for pattern in patterns
        delete!(conn.pcallbacks, pattern)
    end
    execute_command(conn, unshift!(patterns, "punsubscribe"))
end

#Need a specialized version of execute to keep the connection in the transaction state
function exec(conn::TransactionConnection)
    response = execute_command(conn, EXEC)
    multi(conn)
    response
end

###############################################################
# The following Redis commands can be typecast to Julia structs
###############################################################

function time(c::RedisConnection)
    t = _time(c)
    s = parse(Int,t[1])
    ms = parse(Float64, t[2])
    s += (ms / 1e6)
    return unix2datetime(s)
end
