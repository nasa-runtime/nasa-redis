-- 业务作用：撤销旧 Fanout assignment 的 inbox 与期限索引，然后在保持 seq/executionKey 不变的前提下切换目标节点。
-- KEYS[1] shard HASH；KEYS[2] receipts ZSET；KEYS[3] ready ZSET；KEYS[4] leases ZSET；KEYS[5] 旧目标 inbox Stream。
-- ARGV[1] fanoutId；ARGV[2] seq；ARGV[3] 旧 targetNodeIdentity；ARGV[4] 预期 assignmentEpoch；
-- ARGV[5..7] 新 targetNodeIdentity、startupId、heartbeatRevision；ARGV[8] inbox 消费组；ARGV[9] assignment 上限。
-- 返回：BUSY、STALE_ASSIGNMENT、NO_CAPABLE_EXECUTOR，或 {OK, nextEpoch}。
-- 安全不变量：RUNNING shard 不允许直接换目标；旧消息与所有截止索引先在同一原子操作中撤销，才增加 assignmentEpoch。

local state = redis.call('HGET', KEYS[1], 'state')
if state == 'RUNNING' then return {'BUSY'} end
if redis.call('HGET', KEYS[1], 'targetNodeIdentity') ~= ARGV[3]
        or redis.call('HGET', KEYS[1], 'assignmentEpoch') ~= ARGV[4] then return {'STALE_ASSIGNMENT'} end
local assignmentCount = tonumber(redis.call('HGET', KEYS[1], 'assignmentCount') or '1')
local oldMessageId = redis.call('HGET', KEYS[1], 'inboxMessageId')
-- 旧 inbox 必须与 assignment 切换同时撤销，否则旧节点可能在新代次已发布后再次接收。
if oldMessageId and oldMessageId ~= '' then
    redis.pcall('XACK', KEYS[5], ARGV[8], oldMessageId)
    redis.call('XDEL', KEYS[5], oldMessageId)
end
if assignmentCount >= tonumber(ARGV[9]) or ARGV[5] == '' then
    redis.call('HSET', KEYS[1], 'state', 'AWAITING_CAPABILITY', 'errorType', 'NO_CAPABLE_EXECUTOR',
            'owner', '', 'attemptToken', '', 'leaseUntil', '', 'inboxMessageId', '',
            'receivedByExecutorId', '', 'receivedAt', '', 'receiptDeadlineAt', '', 'startVisibleAt', '')
    redis.call('ZREM', KEYS[2], ARGV[1] .. ':' .. ARGV[2])
    redis.call('ZREM', KEYS[3], ARGV[1] .. ':' .. ARGV[2])
    redis.call('ZREM', KEYS[4], ARGV[1] .. ':' .. ARGV[2])
    return {'NO_CAPABLE_EXECUTOR'}
end
local nextEpoch = tonumber(ARGV[4]) + 1
redis.call('HSET', KEYS[1], 'state', 'AWAITING_RECEIPT',
        'targetNodeIdentity', ARGV[5], 'targetStartupId', ARGV[6],
        'targetHeartbeatRevision', ARGV[7],
        'assignmentEpoch', nextEpoch, 'assignmentCount', assignmentCount + 1,
        'receiptRetryCount', 0, 'readyWakeupCount', 0, 'owner', '',
        'attemptToken', '', 'leaseUntil', '', 'inboxMessageId', '',
        'receivedByExecutorId', '', 'receivedAt', '', 'receiptDeadlineAt', '', 'startVisibleAt', '')
redis.call('ZREM', KEYS[2], ARGV[1] .. ':' .. ARGV[2])
redis.call('ZREM', KEYS[3], ARGV[1] .. ':' .. ARGV[2])
redis.call('ZREM', KEYS[4], ARGV[1] .. ':' .. ARGV[2])
return {'OK', tostring(nextEpoch)}
