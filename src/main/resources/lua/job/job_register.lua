-- 业务作用：以单调修订号和规范摘要登记任务定义，并原子建立任务枚举与首个调度时刻。
-- KEYS[1] 任务名 SET；KEYS[2] schedule ZSET；KEYS[3] 任务定义 HASH。
-- ARGV[1..8] jobName、definitionRevision、definitionDigest、state、scheduleShard、workerName、workerKey、trigger；
-- ARGV[9..17] scheduleType、cron、zone、intervalMs、concurrency、misfire、timeoutMs、maxAttempts、retryDelayMs；
-- ARGV[18..24] contractRevision、schemaId、wireCodecs、fanoutReceiptTimeoutMs、fanoutReceiptMaxRetries、fanoutFailurePolicy、nextFireAt。
-- 返回：{DELETED, revision}、{STALE, revision}、{CONFLICT, revision}、{ADOPTED, revision} 或 {OK, revision}。
-- 安全不变量：低修订号不能回滚定义；同修订号但摘要不同时进入 CONFLICT 并停止触发，不按节点启动顺序裁决。

local currentRevision = redis.call('HGET', KEYS[3], 'definitionRevision')
if currentRevision then
    local requestedRevision = tonumber(ARGV[2])
    if redis.call('HGET', KEYS[3], 'state') == 'DELETED' and requestedRevision <= tonumber(currentRevision) then
        return {'DELETED', currentRevision}
    end
    if tonumber(currentRevision) > requestedRevision then
        return {'STALE', currentRevision}
    end
    if tonumber(currentRevision) == requestedRevision then
        local currentDigest = redis.call('HGET', KEYS[3], 'definitionDigest')
        if currentDigest ~= ARGV[3] then
            -- 相同修订号出现两份语义不同的定义时封闭调度，必须经显式管理动作选择。
            redis.call('HSET', KEYS[3], 'state', 'CONFLICT')
            redis.call('ZREM', KEYS[2], ARGV[1])
            return {'CONFLICT', currentRevision}
        end
        return {'ADOPTED', currentRevision}
    end
end

redis.call('SADD', KEYS[1], ARGV[1])
redis.call('HSET', KEYS[3],
        'jobName', ARGV[1],
        'definitionRevision', ARGV[2],
        'definitionDigest', ARGV[3],
        'state', ARGV[4],
        'scheduleShard', ARGV[5],
        'workerName', ARGV[6],
        'workerKey', ARGV[7],
        'trigger', ARGV[8],
        'scheduleType', ARGV[9],
        'cron', ARGV[10],
        'zone', ARGV[11],
        'intervalMs', ARGV[12],
        'concurrency', ARGV[13],
        'misfire', ARGV[14],
        'timeoutMs', ARGV[15],
        'maxAttempts', ARGV[16],
        'retryDelayMs', ARGV[17],
        'contractRevision', ARGV[18],
        'schemaId', ARGV[19],
        'wireCodecs', ARGV[20],
        'fanoutReceiptTimeoutMs', ARGV[21],
        'fanoutReceiptMaxRetries', ARGV[22],
        'fanoutFailurePolicy', ARGV[23],
        'nextFireAt', ARGV[24])
redis.call('HDEL', KEYS[3], 'deletedAt', 'tombstoneUntil')

if ARGV[4] == 'ENABLED' and tonumber(ARGV[24]) > 0 then
    redis.call('ZADD', KEYS[2], ARGV[24], ARGV[1])
else
    redis.call('ZREM', KEYS[2], ARGV[1])
end
return {'OK', ARGV[2]}
