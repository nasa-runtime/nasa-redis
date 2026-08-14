-- 业务作用：在跨 slot Fanout 桶未于截止点前提交时，将普通根 Run 收敛为失败并释放串行槽。
-- KEYS[1] 普通根 Run HASH；KEYS[2] waiting ZSET；KEYS[3] running HASH；KEYS[4] visible ZSET；KEYS[5] 任务 waitq；KEYS[6] completion Stream。
-- ARGV[1] runId；ARGV[2] jobName；ARGV[3] Run 终态保留期毫秒；ARGV[4] 可见性延迟（当前分支仅返回立即唤醒）；ARGV[5] 分片键前缀。
-- 返回：STATE_MISMATCH、{NOT_DUE, deadline}，或 {OK, FAILED, redisNow, wakeDelayMs}。
-- 安全不变量：只有 Run 内截止点与 waiting score 一致且确已到期才收敛；释放串行槽后只推进一个有效队首。

if redis.call('HGET', KEYS[1], 'state') ~= 'FANOUT_CREATING' then return {'STATE_MISMATCH'} end
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local deadline = tonumber(redis.call('HGET', KEYS[1], 'createDeadlineAt') or '0')
local score = tonumber(redis.call('ZSCORE', KEYS[2], ARGV[1]) or '0')
if deadline ~= score or deadline > now then return {'NOT_DUE', tostring(deadline)} end
redis.call('HSET', KEYS[1], 'state', 'FAILED', 'finishedAt', now,
        'resultCode', 'FAILED', 'errorType', 'FANOUT_CREATE_TIMEOUT',
        'resultSummary', 'fanout root was not committed before create deadline', 'owner', '')
redis.call('ZREM', KEYS[2], ARGV[1])
if redis.call('HGET', KEYS[3], ARGV[2]) == ARGV[1] then redis.call('HDEL', KEYS[3], ARGV[2]) end
redis.call('ZREM', KEYS[4], ARGV[1])
redis.call('ZREM', KEYS[5], ARGV[1])
redis.call('XADD', KEYS[6], 'MAXLEN', '~', 100000, '*', 'runId', ARGV[1],
        'state', 'FAILED', 'resultCode', 'FAILED', 'errorType', 'FANOUT_CREATE_TIMEOUT')
redis.call('PEXPIRE', KEYS[1], ARGV[3])
-- 跳过已被其它路径收敛的队列成员，仅为第一个 BLOCKED Run 重建可见入口。
while true do
    local nextEntry = redis.call('ZPOPMIN', KEYS[5], 1)
    if not nextEntry[1] then break end
    local nextRunKey = ARGV[5] .. 'run:' .. nextEntry[1]
    if redis.call('HGET', nextRunKey, 'state') == 'BLOCKED' then
        local visibleAt = now
        redis.call('HSET', nextRunKey, 'state', 'QUEUED', 'nextVisibleAt', visibleAt)
        redis.call('ZADD', KEYS[4], visibleAt, nextEntry[1])
        return {'OK', 'FAILED', tostring(now), '0'}
    end
end
return {'OK', 'FAILED', tostring(now), '-1'}
