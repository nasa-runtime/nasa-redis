-- 业务作用：请求取消普通 Run；已持权或等待 Fanout 的 Run 采用协作式信号，未开始 Run 立即收敛。
-- KEYS[1] Run HASH；KEYS[2] visible ZSET；KEYS[3] leases ZSET；KEYS[4] waiting ZSET；KEYS[5] running HASH；
-- KEYS[6] 任务 waitq；KEYS[7] completion Stream；KEYS[8] 任务定义 HASH；KEYS[9] schedule ZSET。
-- ARGV[1] runId；ARGV[2] jobName；ARGV[3] Run 终态保留期毫秒；ARGV[4] 调度分片键前缀。
-- 返回：NOT_FOUND、ALREADY_COMPLETED，或 {OK, optionalWakeDelayMs}。
-- 安全不变量：RUNNING/FANOUT_CREATING/WAITING_CHILDREN 不伪造已停止，只发布 cancelRequestedAt；立即取消只在释放实际串行槽后推进下一队首。

local state = redis.call('HGET', KEYS[1], 'state')
if not state then return {'NOT_FOUND'} end
if state == 'SUCCEEDED' or state == 'FAILED' or state == 'DEAD' or state == 'SKIPPED' or state == 'CANCELLED' then
    return {'ALREADY_COMPLETED'}
end
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
if state == 'RUNNING' or state == 'FANOUT_CREATING' or state == 'WAITING_CHILDREN' then
    -- 业务线程可能正在执行外部副作用，此处只设置协作式门禁，由 checkpoint/续期传递。
    redis.call('HSET', KEYS[1], 'cancelRequestedAt', now)
    return {'OK'}
end
redis.call('HSET', KEYS[1], 'state', 'CANCELLED', 'finishedAt', now, 'resultCode', 'CANCELLED')
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('ZREM', KEYS[4], ARGV[1])
redis.call('ZREM', KEYS[6], ARGV[1])
local releaseNext = false
local currentOwner = redis.call('HGET', KEYS[5], ARGV[2])
if currentOwner == ARGV[1] then
    redis.call('HDEL', KEYS[5], ARGV[2])
    releaseNext = true
elseif state == 'QUEUED' and not currentOwner then
    -- 已从 waitq 提升但尚未 start 的队首没有 running 记录，取消后必须继续开放下一队首。
    releaseNext = true
end
redis.call('XADD', KEYS[7], 'MAXLEN', '~', 100000, '*', 'runId', ARGV[1], 'state', 'CANCELLED')
redis.call('PEXPIRE', KEYS[1], ARGV[3])
if redis.call('HGET', KEYS[1], 'triggerType') == 'FIXED_DELAY'
        and redis.call('HGET', KEYS[8], 'state') == 'ENABLED' then
    local nextFireAt = now + tonumber(redis.call('HGET', KEYS[8], 'intervalMs') or '0')
    redis.call('HSET', KEYS[8], 'lastFireAt', redis.call('HGET', KEYS[1], 'logicalFireAt'),
            'nextFireAt', nextFireAt)
    redis.call('ZADD', KEYS[9], nextFireAt, ARGV[2])
end
if releaseNext then
    -- 只有占有串行槽或已成为无 running 记录的队首时，取消才负责开放下一成员。
    local prefix = ARGV[4]
    while true do
        local nextEntry = redis.call('ZPOPMIN', KEYS[6], 1)
        if not nextEntry[1] then break end
        local nextRunKey = prefix .. 'run:' .. nextEntry[1]
        if redis.call('HGET', nextRunKey, 'state') == 'BLOCKED' then
            redis.call('HSET', nextRunKey, 'state', 'QUEUED', 'nextVisibleAt', now)
            redis.call('ZADD', KEYS[2], now, nextEntry[1])
            return {'OK', '0'}
        end
    end
end
return {'OK', '-1'}
