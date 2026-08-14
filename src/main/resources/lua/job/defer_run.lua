-- 业务作用：当本地暂无执行容量或缺少兼容 Handler 时，把已拉取的普通 Run 延后放回可见索引。
-- KEYS[1] Run HASH；KEYS[2] visible ZSET；KEYS[3] Dispatch Stream。
-- ARGV[1] runId；ARGV[2] Stream messageId；ARGV[3] 重新可见延迟毫秒；ARGV[4] 消费组。
-- 返回：STALE，或 {DEFERRED, delayMs}。
-- 原子性：只有 QUEUED Run 可延后；新可见截止点与当前消息 ACK/删除在同一 slot 内一次提交，避免 Run 失去恢复入口。

if redis.call('HGET', KEYS[1], 'state') ~= 'QUEUED' then return {'STALE'} end
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local nextVisibleAt = now + tonumber(ARGV[3])
redis.call('HSET', KEYS[1], 'nextVisibleAt', nextVisibleAt)
redis.call('ZADD', KEYS[2], nextVisibleAt, ARGV[1])
redis.call('XACK', KEYS[3], ARGV[4], ARGV[2])
redis.call('XDEL', KEYS[3], ARGV[2])
return {'DEFERRED', tostring(tonumber(ARGV[3]))}
