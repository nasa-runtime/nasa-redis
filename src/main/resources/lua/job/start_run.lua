-- 业务作用：为普通 Run 或 Fanout shard 原子分配 attempt、单调 fencing token 和租约，同时确认当前持久消息。
-- KEYS（NORMAL）：Run HASH、leases ZSET、visible ZSET、running HASH、任务 waitq、fences HASH、Dispatch Stream、completion Stream、任务定义 HASH。
-- KEYS（FANOUT）：shard HASH、leases ZSET、ready ZSET、Fanout 根 HASH、receipts ZSET、fence 所在根 HASH、inbox Stream、completion Stream、Fanout 根 HASH。
-- ARGV[1..11] jobName/workerName、runId/shardRunId、executorId、messageId、leaseMs、concurrency、maxSerialBacklog、deferDelayMs、consumerGroup、retentionMs、NORMAL/FANOUT；
-- NORMAL ARGV[12..18] serialOverflowPolicy、protocolVersion、definitionRevision、contractRevision、schemaId、wireCodecs、分片键前缀；
-- FANOUT ARGV[12..15] targetNodeIdentity、assignmentEpoch、fanoutId、seq。
-- 返回：NOT_COMMITTED、STALE_ASSIGNMENT、NOT_FOUND、DEFERRED、PROTOCOL_UNSUPPORTED、CONFLICT、STALE、CANCELLED、SKIPPED、BLOCKED、FENCING_REGRESSION，
-- 或 {ADOPTED/STARTED, attempt, attemptToken, redisNow, leaseUntil}。
-- 安全不变量：执行前重新复验根状态、目标 assignment、任务门禁、协议与合同；新 token 必须严格大于记录中历史 token；状态、租约和消息 ACK 一次提交。

local state = redis.call('HGET', KEYS[1], 'state')
if ARGV[11] == 'FANOUT' then
    local rootState = redis.call('HGET', KEYS[4], 'state')
    -- 只有已提交且未进入取消/终态的根可以开放新 shard 执行权。
    if rootState ~= 'COMMITTED' and rootState ~= 'WAITING_CHILDREN' then return {'NOT_COMMITTED'} end
    if redis.call('HGET', KEYS[1], 'targetNodeIdentity') ~= ARGV[12]
            or redis.call('HGET', KEYS[1], 'assignmentEpoch') ~= ARGV[13] then return {'STALE_ASSIGNMENT'} end
    local time = redis.call('TIME')
    local now = time[1] * 1000 + math.floor(time[2] / 1000)
    if state == 'RUNNING' and redis.call('HGET', KEYS[1], 'owner') == ARGV[3] then
        local leaseUntil = now + tonumber(ARGV[5])
        redis.call('HSET', KEYS[1], 'leaseUntil', leaseUntil)
        redis.call('ZADD', KEYS[2], leaseUntil, ARGV[14] .. ':' .. ARGV[15])
        redis.call('XACK', KEYS[7], ARGV[9], ARGV[4])
        redis.call('XDEL', KEYS[7], ARGV[4])
        return {'ADOPTED', redis.call('HGET', KEYS[1], 'attempt'),
                redis.call('HGET', KEYS[1], 'attemptToken'), tostring(now), tostring(leaseUntil)}
    end
    if state ~= 'RECEIVED' then return {'STATE_MISMATCH'} end
    local previousToken = tonumber(redis.call('HGET', KEYS[1], 'attemptToken')) or 0
    local token = redis.call('HINCRBY', KEYS[6], 'fence:' .. ARGV[15], 1)
    -- 持久 fencing 计数必须超过 shard 记录中的历史 token，否则封闭执行入口。
    if token <= previousToken then return {'FENCING_REGRESSION'} end
    local attempt = redis.call('HINCRBY', KEYS[1], 'attempt', 1)
    local leaseUntil = now + tonumber(ARGV[5])
    redis.call('HSET', KEYS[1], 'state', 'RUNNING', 'owner', ARGV[3],
            'attemptToken', token, 'startedAt', now, 'leaseUntil', leaseUntil)
    redis.call('ZADD', KEYS[2], leaseUntil, ARGV[14] .. ':' .. ARGV[15])
    redis.call('ZREM', KEYS[3], ARGV[14] .. ':' .. ARGV[15])
    redis.call('XACK', KEYS[7], ARGV[9], ARGV[4])
    redis.call('XDEL', KEYS[7], ARGV[4])
    return {'STARTED', tostring(attempt), tostring(token), tostring(now), tostring(leaseUntil)}
end

if not state then
    redis.call('XACK', KEYS[7], ARGV[9], ARGV[4])
    redis.call('XDEL', KEYS[7], ARGV[4])
    return {'NOT_FOUND'}
end
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)

if redis.call('HGET', KEYS[9], 'state') ~= 'ENABLED' then
    -- 暂停任务只延后未开始 Run，当前消息确认后仍由 visible 索引保证恢复。
    local nextVisibleAt = now + tonumber(ARGV[8])
    redis.call('HSET', KEYS[1], 'nextVisibleAt', nextVisibleAt)
    redis.call('ZADD', KEYS[3], nextVisibleAt, ARGV[2])
    redis.call('XACK', KEYS[7], ARGV[9], ARGV[4])
    redis.call('XDEL', KEYS[7], ARGV[4])
    return {'DEFERRED'}
end

if tonumber(redis.call('HGET', KEYS[1], 'protocolVersion') or '0') > tonumber(ARGV[13]) then
    -- 当前执行器不理解更高协议时不得启动 Handler，保留 Run 等待兼容节点。
    local nextVisibleAt = now + tonumber(ARGV[8])
    redis.call('HSET', KEYS[1], 'errorType', 'PROTOCOL_UNSUPPORTED', 'nextVisibleAt', nextVisibleAt)
    redis.call('ZADD', KEYS[3], nextVisibleAt, ARGV[2])
    redis.call('XACK', KEYS[7], ARGV[9], ARGV[4])
    return {'PROTOCOL_UNSUPPORTED'}
end
local storedCodec = redis.call('HGET', KEYS[1], 'wireCodec') or ''
if redis.call('HGET', KEYS[1], 'jobName') ~= ARGV[1]
        or redis.call('HGET', KEYS[1], 'definitionRevision') ~= ARGV[14]
        or redis.call('HGET', KEYS[1], 'contractRevision') ~= ARGV[15]
        or redis.call('HGET', KEYS[1], 'schemaId') ~= ARGV[16]
        or not string.find(',' .. ARGV[17] .. ',', ',' .. storedCodec .. ',', 1, true) then
    -- 定义、合同、Schema 或 codec 任一不匹配都拒绝解码和执行，避免滚动发布期间使用错误语义。
    local nextVisibleAt = now + tonumber(ARGV[8])
    redis.call('HSET', KEYS[1], 'errorType', 'CONTRACT_MISMATCH', 'nextVisibleAt', nextVisibleAt)
    redis.call('ZADD', KEYS[3], nextVisibleAt, ARGV[2])
    redis.call('XACK', KEYS[7], ARGV[9], ARGV[4])
    redis.call('XDEL', KEYS[7], ARGV[4])
    return {'CONFLICT'}
end

if state == 'RUNNING' and redis.call('HGET', KEYS[1], 'owner') == ARGV[3] then
    local leaseUntil = now + tonumber(ARGV[5])
    redis.call('HSET', KEYS[1], 'leaseUntil', leaseUntil)
    redis.call('ZADD', KEYS[2], leaseUntil, ARGV[2])
    redis.call('XACK', KEYS[7], ARGV[9], ARGV[4])
    redis.call('XDEL', KEYS[7], ARGV[4])
    return {'ADOPTED', redis.call('HGET', KEYS[1], 'attempt'), redis.call('HGET', KEYS[1], 'attemptToken'), tostring(now), tostring(leaseUntil)}
end

if state ~= 'QUEUED' then
    redis.call('XACK', KEYS[7], ARGV[9], ARGV[4])
    if state == 'SUCCEEDED' or state == 'FAILED' or state == 'DEAD' or state == 'SKIPPED' or state == 'CANCELLED' then
        redis.call('XDEL', KEYS[7], ARGV[4])
    end
    return {'STALE'}
end

if redis.call('HGET', KEYS[1], 'cancelRequestedAt') then
    redis.call('HSET', KEYS[1], 'state', 'CANCELLED', 'finishedAt', now, 'resultCode', 'CANCELLED')
    redis.call('ZREM', KEYS[2], ARGV[2])
    redis.call('ZREM', KEYS[3], ARGV[2])
    redis.call('ZREM', KEYS[5], ARGV[2])
    redis.call('XACK', KEYS[7], ARGV[9], ARGV[4])
    redis.call('XDEL', KEYS[7], ARGV[4])
    redis.call('XADD', KEYS[8], 'MAXLEN', '~', 100000, '*', 'runId', ARGV[2], 'state', 'CANCELLED')
    redis.call('PEXPIRE', KEYS[1], ARGV[10])
    return {'CANCELLED'}
end

if ARGV[6] ~= 'PARALLEL' then
    -- 串行槽的占用、溢出处置和 waitq 入队必须与当前 Run 状态一次提交。
    local occupied = redis.call('HGET', KEYS[4], ARGV[1])
    if occupied and occupied ~= ARGV[2] then
        if ARGV[6] == 'DISCARD_IF_RUNNING' then
            redis.call('HSET', KEYS[1], 'state', 'SKIPPED', 'finishedAt', now, 'resultCode', 'RUNNING_EXISTS')
            redis.call('ZREM', KEYS[3], ARGV[2])
            redis.call('XACK', KEYS[7], ARGV[9], ARGV[4])
            redis.call('XDEL', KEYS[7], ARGV[4])
            redis.call('XADD', KEYS[8], 'MAXLEN', '~', 100000, '*', 'runId', ARGV[2], 'state', 'SKIPPED')
            redis.call('PEXPIRE', KEYS[1], ARGV[10])
            return {'SKIPPED'}
        end
        if ARGV[6] == 'SERIAL_QUEUE' and redis.call('ZCARD', KEYS[5]) >= tonumber(ARGV[7]) then
            if ARGV[12] == 'SKIP_NEWEST' then
                redis.call('HSET', KEYS[1], 'state', 'SKIPPED', 'finishedAt', now,
                        'resultCode', 'SERIAL_BACKLOG_OVERFLOW')
                redis.call('ZREM', KEYS[3], ARGV[2])
                redis.call('XACK', KEYS[7], ARGV[9], ARGV[4])
                redis.call('XDEL', KEYS[7], ARGV[4])
                redis.call('XADD', KEYS[8], 'MAXLEN', '~', 100000, '*', 'runId', ARGV[2],
                        'state', 'SKIPPED', 'resultCode', 'SERIAL_BACKLOG_OVERFLOW')
                redis.call('PEXPIRE', KEYS[1], ARGV[10])
                return {'SKIPPED'}
            end
            local oldest = redis.call('ZPOPMIN', KEYS[5], 1)
            if oldest[1] then
                local oldestKey = ARGV[18] .. 'run:' .. oldest[1]
                if redis.call('HGET', oldestKey, 'state') == 'BLOCKED' then
                    redis.call('HSET', oldestKey, 'state', 'SKIPPED', 'finishedAt', now,
                            'resultCode', 'SERIAL_BACKLOG_OVERFLOW')
                    redis.call('XADD', KEYS[8], 'MAXLEN', '~', 100000, '*', 'runId', oldest[1],
                            'state', 'SKIPPED', 'resultCode', 'SERIAL_BACKLOG_OVERFLOW')
                    redis.call('PEXPIRE', oldestKey, ARGV[10])
                end
            end
        end
        redis.call('HSET', KEYS[1], 'state', 'BLOCKED')
        redis.call('ZREM', KEYS[3], ARGV[2])
        redis.call('ZADD', KEYS[5], redis.call('HGET', KEYS[1], 'logicalFireAt'), ARGV[2])
        redis.call('XACK', KEYS[7], ARGV[9], ARGV[4])
        redis.call('XDEL', KEYS[7], ARGV[4])
        return {'BLOCKED'}
    end
end

local previousToken = tonumber(redis.call('HGET', KEYS[1], 'attemptToken')) or 0
local attemptToken = redis.call('HINCRBY', KEYS[6], ARGV[1], 1)
-- 新 token 不大于 Run 历史值意味着 fencing 权威发生回退，必须拒绝执行而不是继续写外部资源。
if attemptToken <= previousToken then
    local nextVisibleAt = now + tonumber(ARGV[8])
    redis.call('HSET', KEYS[1], 'errorType', 'FENCING_REGRESSION', 'nextVisibleAt', nextVisibleAt)
    redis.call('ZADD', KEYS[3], nextVisibleAt, ARGV[2])
    redis.call('XACK', KEYS[7], ARGV[9], ARGV[4])
    redis.call('XDEL', KEYS[7], ARGV[4])
    return {'FENCING_REGRESSION'}
end
local attempt = redis.call('HINCRBY', KEYS[1], 'attempt', 1)
local leaseUntil = now + tonumber(ARGV[5])
redis.call('HSET', KEYS[1], 'state', 'RUNNING', 'owner', ARGV[3],
        'attemptToken', attemptToken, 'startedAt', now, 'leaseUntil', leaseUntil)
if ARGV[6] ~= 'PARALLEL' then redis.call('HSET', KEYS[4], ARGV[1], ARGV[2]) end
redis.call('ZADD', KEYS[2], leaseUntil, ARGV[2])
redis.call('ZREM', KEYS[3], ARGV[2])
redis.call('ZREM', KEYS[5], ARGV[2])
redis.call('XACK', KEYS[7], ARGV[9], ARGV[4])
redis.call('XDEL', KEYS[7], ARGV[4])
return {'STARTED', tostring(attempt), tostring(attemptToken), tostring(now), tostring(leaseUntil)}
