-- 业务作用：以单调定义修订号删除任务，并保留 tombstone 阻止旧进程重新登记。
-- KEYS[1] 任务名 SET；KEYS[2] schedule ZSET；KEYS[3] 任务定义 HASH。
-- ARGV[1] jobName；ARGV[2] 删除修订号；ARGV[3] tombstone 保留期毫秒。
-- 返回：NOT_FOUND、{STALE, currentRevision} 或 {OK, redisNow}。
-- 安全不变量：不允许较低修订号覆盖已知定义；先写 DELETED 与保留截止点，再从 schedule 移除。

local currentRevision = redis.call('HGET', KEYS[3], 'definitionRevision')
if not currentRevision then return {'NOT_FOUND'} end
if tonumber(currentRevision) > tonumber(ARGV[2]) then return {'STALE', currentRevision} end
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
redis.call('HSET', KEYS[3], 'state', 'DELETED', 'definitionRevision', ARGV[2],
        'deletedAt', now, 'tombstoneUntil', now + tonumber(ARGV[3]))
redis.call('SADD', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[1])
return {'OK', tostring(now)}
