-- 业务作用：扫描到期 visible 成员，为仍可执行的普通 Run 重建可能丢失的 Dispatch 消息。
-- KEYS[1] visible ZSET。
-- ARGV[1] 单轮上限；ARGV[2] visibilityTimeoutMs；ARGV[3] 最大消息重建次数；ARGV[4] 超限或暂停时的再扫描延迟毫秒；
-- ARGV[5] 调度分片键前缀。
-- 返回：{promotedCount, nextScore, redisNow}。
-- 安全不变量：Run 内 nextVisibleAt 必须与 ZSET score 一致；任务暂停时只延后，不创建新消息；超过重建上限后保留持久诊断与后续扫描。

local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local runIds = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, tonumber(ARGV[1]))
local prefix = ARGV[5]
local promoted = 0
for _, runId in ipairs(runIds) do
    local runKey = prefix .. 'run:' .. runId
    local state = redis.call('HGET', runKey, 'state')
    local storedScore = tonumber(redis.call('HGET', runKey, 'nextVisibleAt') or '0')
    local indexScore = tonumber(redis.call('ZSCORE', KEYS[1], runId) or '0')
    -- HASH 与 ZSET 的截止点同时匹配才能提升，迟到扫描不得重放旧截止点。
    if (state == 'QUEUED' or state == 'RETRY_WAIT') and storedScore == indexScore then
        local jobName = redis.call('HGET', runKey, 'jobName')
        if redis.call('HGET', prefix .. 'job:' .. jobName, 'state') ~= 'ENABLED' then
            -- 暂停门禁在恢复路径再次复验，避免积压消息绕过 pause。
            local nextVisibleAt = now + tonumber(ARGV[4])
            redis.call('HSET', runKey, 'nextVisibleAt', nextVisibleAt)
            redis.call('ZADD', KEYS[1], nextVisibleAt, runId)
        else
            local attempts = redis.call('HINCRBY', runKey, 'dispatchAttempts', 1)
            local nextVisibleAt = now + (attempts > tonumber(ARGV[3]) and tonumber(ARGV[4]) or tonumber(ARGV[2]))
            local workerName = redis.call('HGET', runKey, 'workerName')
            local workerKey = redis.call('HGET', runKey, 'workerKey')
            redis.call('HSET', runKey, 'state', 'QUEUED', 'nextVisibleAt', nextVisibleAt)
            if attempts <= tonumber(ARGV[3]) then
                redis.call('XADD', prefix .. 'dispatch:' .. workerKey, '*',
                        'runId', runId, 'jobName', jobName,
                        'workerName', workerName, 'protocolVersion', redis.call('HGET', runKey, 'protocolVersion'),
                        'definitionRevision', redis.call('HGET', runKey, 'definitionRevision'))
            else
                redis.call('HSET', runKey, 'errorType', 'NO_CAPABLE_EXECUTOR')
            end
            redis.call('ZADD', KEYS[1], nextVisibleAt, runId)
            promoted = promoted + 1
        end
    else
        redis.call('ZREM', KEYS[1], runId)
    end
end
local nextEntry = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
return {tostring(promoted), nextEntry[2] or '', tostring(now)}
