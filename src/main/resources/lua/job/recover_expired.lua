-- 业务作用：在服务端确认租约已到期后撤销旧 owner 的调度状态权威，并将普通 Run 或 Fanout shard 推进到重试、重分配或终态。
-- KEYS（NORMAL）：Run HASH、leases ZSET、visible ZSET、running HASH、任务 waitq、waiting ZSET、completion Stream、任务 HASH、schedule ZSET、Dispatch Stream。
-- KEYS（FANOUT）：shard HASH、leases ZSET、ready ZSET、Fanout 根 HASH、receipts ZSET、roots/watch ZSET、completion Stream、GC ZSET。
-- ARGV[1] 索引成员/runId；ARGV[2] jobName/workerName；ARGV[3] maxAttempts；ARGV[4] retryDelayMs；ARGV[5] retentionMs；ARGV[6] NORMAL/FANOUT；
-- FANOUT ARGV[7..9] fanoutId、seq、failurePolicy；NORMAL ARGV[10] 分片键前缀。
-- 返回：STALE、{NOT_DUE, leaseUntil}，或 {OK, nextState, redisNow, optionalWakeDelayMs}。
-- 安全不变量：Run/shard 内 leaseUntil 必须与 leases score 一致且已到期；旧 owner 被清空后才建立新恢复入口。

if redis.call('HGET', KEYS[1], 'state') ~= 'RUNNING' then
    redis.call('ZREM', KEYS[2], ARGV[1])
    return {'STALE'}
end
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local wakeDelayMs = -1
local leaseUntil = tonumber(redis.call('HGET', KEYS[1], 'leaseUntil') or '0')
local score = tonumber(redis.call('ZSCORE', KEYS[2], ARGV[1]) or '0')
-- 只以 Redis TIME 与双份一致的租约截止点判定失权，避免迟到扫描撤销新租约。
if leaseUntil ~= score or leaseUntil > now then return {'NOT_DUE', tostring(leaseUntil)} end

if ARGV[6] == 'FANOUT' then
    redis.call('ZREM', KEYS[2], ARGV[1])
    redis.call('ZREM', KEYS[3], ARGV[1])
    redis.call('ZREM', KEYS[5], ARGV[1])
    local attempt = tonumber(redis.call('HGET', KEYS[1], 'attempt') or '0')
    local rootState = redis.call('HGET', KEYS[4], 'state')
    if rootState == 'CANCELLING' then
        -- 根已取消时不再为过期 shard 创建新 attempt，直接计入取消聚合。
        redis.call('HSET', KEYS[1], 'state', 'CANCELLED', 'owner', '', 'leaseUntil', '',
                'finishedAt', now, 'resultCode', 'CANCELLED', 'errorType', 'ROOT_CANCELLING')
        redis.call('HINCRBY', KEYS[4], 'cancelledCount', 1)
    elseif attempt < tonumber(ARGV[3]) and ARGV[9] == 'REASSIGN_ON_FAILURE' then
        redis.call('HSET', KEYS[1], 'state', 'AWAITING_CAPABILITY', 'owner', '', 'leaseUntil', '',
                'errorType', 'LEASE_EXPIRED')
        redis.call('ZADD', KEYS[6], now, ARGV[7])
        return {'OK', 'AWAITING_CAPABILITY', tostring(now)}
    elseif attempt < tonumber(ARGV[3]) then
        local startVisibleAt = now + tonumber(ARGV[4])
        redis.call('HSET', KEYS[1], 'state', 'RECEIVED', 'owner', '', 'leaseUntil', '',
                'errorType', 'LEASE_EXPIRED', 'startVisibleAt', startVisibleAt)
        redis.call('ZADD', KEYS[3], startVisibleAt, ARGV[1])
        return {'OK', 'RECEIVED', tostring(now)}
    else
        redis.call('HSET', KEYS[1], 'state', 'DEAD', 'owner', '', 'leaseUntil', '',
                'finishedAt', now, 'resultCode', 'LEASE_EXPIRED', 'errorType', 'LEASE_EXPIRED')
        redis.call('HINCRBY', KEYS[4], 'deadCount', 1)
    end
    local terminal = tonumber(redis.call('HGET', KEYS[4], 'successCount') or '0')
            + tonumber(redis.call('HGET', KEYS[4], 'failedCount') or '0')
            + tonumber(redis.call('HGET', KEYS[4], 'deadCount') or '0')
            + tonumber(redis.call('HGET', KEYS[4], 'skippedCount') or '0')
            + tonumber(redis.call('HGET', KEYS[4], 'cancelledCount') or '0')
    local total = tonumber(redis.call('HGET', KEYS[4], 'shardTotal') or '0')
    if terminal == total then
        local failed = tonumber(redis.call('HGET', KEYS[4], 'failedCount') or '0')
                + tonumber(redis.call('HGET', KEYS[4], 'deadCount') or '0')
        local skipped = tonumber(redis.call('HGET', KEYS[4], 'skippedCount') or '0')
        local cancelled = tonumber(redis.call('HGET', KEYS[4], 'cancelledCount') or '0')
        local cancelling = redis.call('HGET', KEYS[4], 'state') == 'CANCELLING'
        local completedState = cancelling and 'CANCELLED' or (failed > 0 and 'FAILED'
                or (skipped > 0 and 'PARTIAL_FAILED'
                or (cancelled == total and 'CANCELLED' or 'SUCCEEDED')))
        local expireAt = now + tonumber(ARGV[5])
        redis.call('HSET', KEYS[4], 'state', completedState, 'finishedAt', now, 'expireAt', expireAt)
        redis.call('ZADD', KEYS[8], expireAt, ARGV[7])
        redis.call('XADD', KEYS[7], 'MAXLEN', '~', 100000, '*', 'event', 'FANOUT_COMPLETED',
                'fanoutId', ARGV[7], 'rootRunId', redis.call('HGET', KEYS[4], 'rootRunId'),
                'state', completedState)
    end
    return {'OK', redis.call('HGET', KEYS[1], 'state'), tostring(now)}
end

redis.call('ZREM', KEYS[2], ARGV[1])
local releasedSlot = false
if redis.call('HGET', KEYS[4], ARGV[2]) == ARGV[1] then
    redis.call('HDEL', KEYS[4], ARGV[2])
    releasedSlot = true
end
local attempt = tonumber(redis.call('HGET', KEYS[1], 'attempt') or '0')
local nextState = attempt < tonumber(ARGV[3]) and 'RETRY_WAIT' or 'DEAD'
redis.call('HSET', KEYS[1], 'state', nextState, 'owner', '', 'leaseUntil', '', 'errorType', 'LEASE_EXPIRED')
if nextState == 'RETRY_WAIT' then
    local nextVisibleAt = now + tonumber(ARGV[4])
    redis.call('HSET', KEYS[1], 'nextVisibleAt', nextVisibleAt)
    redis.call('ZADD', KEYS[3], nextVisibleAt, ARGV[1])
    wakeDelayMs = tonumber(ARGV[4])
else
    redis.call('ZREM', KEYS[3], ARGV[1])
    redis.call('ZREM', KEYS[5], ARGV[1])
    redis.call('ZREM', KEYS[6], ARGV[1])
    redis.call('XADD', KEYS[7], 'MAXLEN', '~', 100000, '*', 'runId', ARGV[1], 'state', nextState, 'resultCode', 'LEASE_EXPIRED')
    redis.call('PEXPIRE', KEYS[1], ARGV[5])
    if redis.call('HGET', KEYS[1], 'triggerType') == 'FIXED_DELAY'
            and redis.call('HGET', KEYS[8], 'state') == 'ENABLED' then
        local nextFireAt = now + tonumber(redis.call('HGET', KEYS[8], 'intervalMs') or '0')
        redis.call('HSET', KEYS[8], 'lastFireAt', redis.call('HGET', KEYS[1], 'logicalFireAt'),
                'nextFireAt', nextFireAt)
        redis.call('ZADD', KEYS[9], nextFireAt, ARGV[2])
    end
end
if releasedSlot then
    -- 仅有实际释放当前串行槽的恢复者可推进下一队首。
    local prefix = ARGV[10]
    while true do
        local nextEntry = redis.call('ZPOPMIN', KEYS[5], 1)
        if not nextEntry[1] then break end
        local nextRunKey = prefix .. 'run:' .. nextEntry[1]
        if redis.call('HGET', nextRunKey, 'state') == 'BLOCKED' then
            -- 租约恢复与正常完成使用同一可见性门禁，暂停时不为尚未 start 的队首创建派发消息。
            local visibleAt = now
            redis.call('HSET', nextRunKey, 'state', 'QUEUED', 'nextVisibleAt', visibleAt)
            redis.call('ZADD', KEYS[3], visibleAt, nextEntry[1])
            wakeDelayMs = 0
            break
        end
    end
end
return {'OK', nextState, tostring(now), tostring(wakeDelayMs)}
