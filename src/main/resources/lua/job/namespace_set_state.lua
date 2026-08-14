-- 业务作用：设置一个调度分片的命名空间触发门禁，供手工和自动触发脚本原子复验。
-- KEYS[1] 命名空间控制 HASH。
-- ARGV[1] ENABLED 或 PAUSED；ARGV[2] 操作来源。
-- 返回：INVALID 或 {OK, redisNow}。
-- 原子性：状态、更新时刻和操作来源一次发布；该门禁只阻止新触发，不撤销已有执行权。

if ARGV[1] ~= 'ENABLED' and ARGV[1] ~= 'PAUSED' then return {'INVALID'} end
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
redis.call('HSET', KEYS[1], 'namespaceState', ARGV[1], 'updatedAt', now, 'updatedBy', ARGV[2])
return {'OK', tostring(now)}
