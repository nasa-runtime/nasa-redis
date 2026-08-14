-- 业务作用：提交普通 Run 或 Fanout shard 的当前 attempt 结果，根据重试上限进入下一可执行状态或终态。
-- KEYS（NORMAL）：Run HASH、leases ZSET、visible ZSET、waiting ZSET、running HASH、任务 waitq、completion Stream、Dispatch Stream、任务 HASH、schedule ZSET。
-- KEYS（FANOUT）：shard HASH、leases ZSET、ready ZSET、receipts ZSET、Fanout 根 HASH、roots/watch ZSET、completion Stream、GC ZSET。
-- ARGV[1..10] runId/shardRunId、owner、attemptToken、jobName/workerName、resultCode、resultSummary、maxAttempts、retryDelayMs、retentionMs、NORMAL/FANOUT；
-- FANOUT ARGV[11..13] fanoutId、seq、assignmentEpoch；NORMAL ARGV[14] 分片键前缀。
-- 返回：STATE_MISMATCH、STALE_OWNER、STALE_ASSIGNMENT，或 {OK, nextState, redisNow, optionalWakeDelayMs}。
-- 安全不变量：owner 与 attemptToken 必须同时匹配；Fanout 还必须匹配 assignmentEpoch；释放执行权、写结果与推进队首在同一 slot 内提交。

if ARGV[10] == 'FANOUT' then
    if redis.call('HGET', KEYS[1], 'state') ~= 'RUNNING' then return {'STATE_MISMATCH'} end
    if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[2]
            or redis.call('HGET', KEYS[1], 'attemptToken') ~= ARGV[3] then return {'STALE_OWNER'} end
    if redis.call('HGET', KEYS[1], 'assignmentEpoch') ~= ARGV[13] then return {'STALE_ASSIGNMENT'} end
    local time = redis.call('TIME')
    local now = time[1] * 1000 + math.floor(time[2] / 1000)
    local member = ARGV[11] .. ':' .. ARGV[12]
    redis.call('ZREM', KEYS[2], member)
    redis.call('ZREM', KEYS[3], member)
    redis.call('ZREM', KEYS[4], member)
    local attempt = tonumber(redis.call('HGET', KEYS[1], 'attempt') or '0')
    local submittedResult = ARGV[5]
    -- 根已进入 CANCELLING 时不再接受 shard 的新重试，当前完成统一按取消收敛。
    if redis.call('HGET', KEYS[5], 'state') == 'CANCELLING' then submittedResult = 'CANCELLED' end
    if submittedResult == 'RETRY' and attempt < tonumber(ARGV[7]) then
        local startVisibleAt = now + tonumber(ARGV[8])
        redis.call('HSET', KEYS[1], 'state', 'RECEIVED', 'owner', '', 'leaseUntil', '',
                'resultCode', submittedResult, 'resultSummary', ARGV[6], 'startVisibleAt', startVisibleAt)
        redis.call('ZADD', KEYS[3], startVisibleAt, member)
        return {'OK', 'RECEIVED', tostring(now)}
    end
    local nextState = submittedResult == 'SUCCESS' and 'SUCCEEDED'
            or (submittedResult == 'CANCELLED' and 'CANCELLED'
            or (submittedResult == 'RETRY' and 'DEAD' or 'FAILED'))
    redis.call('HSET', KEYS[1], 'state', nextState, 'owner', '', 'leaseUntil', '',
            'finishedAt', now, 'resultCode', submittedResult, 'resultSummary', ARGV[6])
    local counter = nextState == 'SUCCEEDED' and 'successCount'
            or (nextState == 'FAILED' and 'failedCount'
            or (nextState == 'DEAD' and 'deadCount' or 'cancelledCount'))
    redis.call('HINCRBY', KEYS[5], counter, 1)
    local terminal = tonumber(redis.call('HGET', KEYS[5], 'successCount') or '0')
            + tonumber(redis.call('HGET', KEYS[5], 'failedCount') or '0')
            + tonumber(redis.call('HGET', KEYS[5], 'deadCount') or '0')
            + tonumber(redis.call('HGET', KEYS[5], 'skippedCount') or '0')
            + tonumber(redis.call('HGET', KEYS[5], 'cancelledCount') or '0')
    local total = tonumber(redis.call('HGET', KEYS[5], 'shardTotal') or '0')
    if terminal == total then
        -- 最后一个 shard 原子决定根终态，不依赖完成顺序。
        local failed = tonumber(redis.call('HGET', KEYS[5], 'failedCount') or '0')
                + tonumber(redis.call('HGET', KEYS[5], 'deadCount') or '0')
        local skipped = tonumber(redis.call('HGET', KEYS[5], 'skippedCount') or '0')
        local cancelled = tonumber(redis.call('HGET', KEYS[5], 'cancelledCount') or '0')
        local cancelling = redis.call('HGET', KEYS[5], 'state') == 'CANCELLING'
        local rootState = cancelling and 'CANCELLED' or (failed > 0 and 'FAILED'
                or (skipped > 0 and 'PARTIAL_FAILED'
                or (cancelled == total and 'CANCELLED' or 'SUCCEEDED')))
        local expireAt = now + tonumber(ARGV[9])
        redis.call('HSET', KEYS[5], 'state', rootState, 'finishedAt', now, 'expireAt', expireAt)
        redis.call('ZADD', KEYS[8], expireAt, ARGV[11])
        redis.call('XADD', KEYS[7], 'MAXLEN', '~', 100000, '*', 'event', 'FANOUT_COMPLETED',
                'fanoutId', ARGV[11], 'rootRunId', redis.call('HGET', KEYS[5], 'rootRunId'), 'state', rootState)
    end
    return {'OK', nextState, tostring(now)}
end

if redis.call('HGET', KEYS[1], 'state') ~= 'RUNNING' then return {'STATE_MISMATCH'} end
if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[2] or redis.call('HGET', KEYS[1], 'attemptToken') ~= ARGV[3] then
    return {'STALE_OWNER'}
end
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local wakeDelayMs = -1
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('ZREM', KEYS[4], ARGV[1])
redis.call('ZREM', KEYS[6], ARGV[1])
local releasedSlot = false
if redis.call('HGET', KEYS[5], ARGV[4]) == ARGV[1] then
    redis.call('HDEL', KEYS[5], ARGV[4])
    releasedSlot = true
end

local result = ARGV[5]
local attempt = tonumber(redis.call('HGET', KEYS[1], 'attempt') or '0')
local nextState
if result == 'SUCCESS' then nextState = 'SUCCEEDED'
elseif result == 'CANCELLED' then nextState = 'CANCELLED'
elseif result == 'RETRY' and attempt < tonumber(ARGV[7]) then nextState = 'RETRY_WAIT'
elseif result == 'RETRY' then nextState = 'DEAD'
else nextState = 'FAILED' end

redis.call('HSET', KEYS[1], 'state', nextState, 'resultCode', result,
        'resultSummary', ARGV[6], 'finishedAt', now, 'owner', '', 'leaseUntil', '')
if nextState == 'RETRY_WAIT' then
    local nextVisibleAt = now + tonumber(ARGV[8])
    redis.call('HSET', KEYS[1], 'nextVisibleAt', nextVisibleAt)
    redis.call('ZADD', KEYS[3], nextVisibleAt, ARGV[1])
    wakeDelayMs = tonumber(ARGV[8])
else
    redis.call('XADD', KEYS[7], 'MAXLEN', '~', 100000, '*', 'runId', ARGV[1], 'state', nextState, 'resultCode', result)
    redis.call('PEXPIRE', KEYS[1], ARGV[9])
    if redis.call('HGET', KEYS[1], 'triggerType') == 'FIXED_DELAY'
            and redis.call('HGET', KEYS[9], 'state') == 'ENABLED' then
        local nextFireAt = now + tonumber(redis.call('HGET', KEYS[9], 'intervalMs') or '0')
        redis.call('HSET', KEYS[9], 'lastFireAt', redis.call('HGET', KEYS[1], 'logicalFireAt'),
                'nextFireAt', nextFireAt)
        redis.call('ZADD', KEYS[10], nextFireAt, ARGV[4])
    end
end
if releasedSlot then
    -- 只在确认当前 Run 仍占有串行槽时开放下一队首，避免迟到完成重复推进。
    local prefix = ARGV[14]
    while true do
        local nextEntry = redis.call('ZPOPMIN', KEYS[6], 1)
        if not nextEntry[1] then break end
        local nextRunKey = prefix .. 'run:' .. nextEntry[1]
        if redis.call('HGET', nextRunKey, 'state') == 'BLOCKED' then
            -- 队首先回到统一可见索引，任务暂停门禁和正常派发都由 promoter 复验，避免两类积压语义分叉。
            local visibleAt = now
            redis.call('HSET', nextRunKey, 'state', 'QUEUED', 'nextVisibleAt', visibleAt)
            redis.call('ZADD', KEYS[3], visibleAt, nextEntry[1])
            wakeDelayMs = 0
            break
        end
    end
end
return {'OK', nextState, tostring(now), tostring(wakeDelayMs)}
