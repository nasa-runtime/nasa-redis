-- 业务作用：把桶内 Fanout 的 COMMITTED 或终态幂等回填到普通根 Run，并在终态时释放串行槽。
-- KEYS[1] 普通根 Run HASH；KEYS[2] waiting ZSET；KEYS[3] running HASH；KEYS[4] visible ZSET；KEYS[5] 任务 waitq；KEYS[6] completion Stream。
-- ARGV[1] rootRunId；ARGV[2] fanoutId；ARGV[3] rootAttempt；ARGV[4] COMMITTED 或桶内终态；ARGV[5] errorType；
-- ARGV[6] 根最大等待毫秒；ARGV[7] resultSummary；ARGV[8] rootJobName；ARGV[9] Run 保留期毫秒；
-- ARGV[10] 可见性延迟（当前队首直接唤醒分支不使用）；ARGV[11] 分片键前缀。
-- 返回：STALE、{ALREADY_COMPLETED, state}、STATE_MISMATCH，或 {OK, state, redisNowOrDeadline, wakeDelayMs}。
-- 安全不变量：fanoutId 与 rootAttempt 必须匹配；COMMITTED 只转入 WAITING_CHILDREN；根终态持久后才释放槽并唤醒下一队首。

if redis.call('HGET', KEYS[1], 'fanoutId') ~= ARGV[2]
        or redis.call('HGET', KEYS[1], 'rootAttempt') ~= ARGV[3] then return {'STALE'} end
local current = redis.call('HGET', KEYS[1], 'state')
if current == 'SUCCEEDED' or current == 'FAILED' or current == 'CANCELLED' then return {'ALREADY_COMPLETED', current} end
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
if ARGV[4] == 'COMMITTED' then
    if current == 'WAITING_CHILDREN' then return {'ADOPTED'} end
    if current ~= 'FANOUT_CREATING' then return {'STATE_MISMATCH'} end
    local deadline = now + tonumber(ARGV[6])
    redis.call('HSET', KEYS[1], 'state', 'WAITING_CHILDREN', 'owner', '', 'waitDeadlineAt', deadline)
    redis.call('ZADD', KEYS[2], deadline, ARGV[1])
    return {'OK', 'WAITING_CHILDREN', tostring(deadline), '-1'}
end
if current ~= 'WAITING_CHILDREN' and current ~= 'FANOUT_CREATING' then return {'STATE_MISMATCH'} end
local rootState = ARGV[4] == 'SUCCEEDED' and 'SUCCEEDED'
        or (ARGV[4] == 'CANCELLED' and ARGV[5] ~= 'WAIT_TIMEOUT' and 'CANCELLED' or 'FAILED')
redis.call('HSET', KEYS[1], 'state', rootState, 'finishedAt', now,
        'resultCode', ARGV[4], 'errorType', ARGV[5], 'resultSummary', ARGV[7])
redis.call('ZREM', KEYS[2], ARGV[1])
if redis.call('HGET', KEYS[3], ARGV[8]) == ARGV[1] then redis.call('HDEL', KEYS[3], ARGV[8]) end
redis.call('ZREM', KEYS[4], ARGV[1])
redis.call('ZREM', KEYS[5], ARGV[1])
redis.call('XADD', KEYS[6], 'MAXLEN', '~', 100000, '*', 'runId', ARGV[1], 'state', rootState, 'resultCode', ARGV[4])
redis.call('PEXPIRE', KEYS[1], ARGV[9])
-- 串行槽已释放，仅将第一个仍为 BLOCKED 的成员送回统一 visible 门禁。
while true do
    local nextEntry = redis.call('ZPOPMIN', KEYS[5], 1)
    if not nextEntry[1] then break end
    local nextRunKey = ARGV[11] .. 'run:' .. nextEntry[1]
    if redis.call('HGET', nextRunKey, 'state') == 'BLOCKED' then
        local visibleAt = now
        redis.call('HSET', nextRunKey, 'state', 'QUEUED', 'nextVisibleAt', visibleAt)
        redis.call('ZADD', KEYS[4], visibleAt, nextEntry[1])
        return {'OK', rootState, tostring(now), '0'}
    end
end
return {'OK', rootState, tostring(now), '-1'}
