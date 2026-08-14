-- 业务作用：目标节点持久确认已接收 Fanout shard，建立 start 截止索引后向根执行器发回执信号。
-- KEYS[1] Fanout 根 HASH；KEYS[2] shard HASH；KEYS[3] receipt deadline ZSET；KEYS[4] ready ZSET；KEYS[5] 根执行器回执 channel。
-- ARGV[1] fanoutId；ARGV[2] seq；ARGV[3] targetNodeIdentity；ARGV[4] executorId；ARGV[5] assignmentEpoch；
-- ARGV[6] start 重新可见延迟毫秒；ARGV[7] PUBLISH 或 SPUBLISH。
-- 返回：NOT_COMMITTED、ADOPTED、STATE_MISMATCH、STALE_ASSIGNMENT，或 {OK, redisNow, startVisibleAt}。
-- 安全不变量：目标身份与 assignmentEpoch 必须同时匹配；先持久 RECEIVED 并删除 receipt deadline，再发 Pub/Sub 信号。

local rootState = redis.call('HGET', KEYS[1], 'state')
if rootState ~= 'COMMITTED' and rootState ~= 'WAITING_CHILDREN' then return {'NOT_COMMITTED'} end
local state = redis.call('HGET', KEYS[2], 'state')
if state == 'RECEIVED' and redis.call('HGET', KEYS[2], 'assignmentEpoch') == ARGV[5] then return {'ADOPTED'} end
if state ~= 'AWAITING_RECEIPT' then return {'STATE_MISMATCH'} end
if redis.call('HGET', KEYS[2], 'targetNodeIdentity') ~= ARGV[3]
        or redis.call('HGET', KEYS[2], 'assignmentEpoch') ~= ARGV[5] then return {'STALE_ASSIGNMENT'} end
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local startVisibleAt = now + tonumber(ARGV[6])
redis.call('HSET', KEYS[2], 'state', 'RECEIVED', 'receivedByExecutorId', ARGV[4],
        'receivedAt', now, 'startVisibleAt', startVisibleAt)
redis.call('ZREM', KEYS[3], ARGV[1] .. ':' .. ARGV[2])
redis.call('ZADD', KEYS[4], startVisibleAt, ARGV[1] .. ':' .. ARGV[2])
-- Pub/Sub 只加速根节点观察，即使信号丢失，前面的持久状态仍是权威确认。
redis.call(ARGV[7], KEYS[5], ARGV[1] .. '|' .. ARGV[2] .. '|' .. ARGV[5] .. '|RECEIVED')
return {'OK', tostring(now), tostring(startVisibleAt)}
