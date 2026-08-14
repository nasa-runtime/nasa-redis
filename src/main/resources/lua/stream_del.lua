-- 业务作用：删除 Stream 中早于指定毫秒时间戳的全部消息，供调用方按保留期回收数据。
-- KEYS[1] Stream 键；ARGV[1] 毫秒时间戳，不包含序列号部分。
-- 返回：实际删除的消息数量；没有匹配消息时返回 0。

-- 流名
local stream = KEYS[1];
-- 指定时间戳
local timestamp = ARGV[1];

-- 查出时间戳之前所有消息id
local messages = redis.call("xrange", stream, "-", "(" .. timestamp .. "-0");
-- 在Lua中，空的table没有键值对，所以如果调用next()函数遍历table，如果返回值为nil，表示table为空。
-- next(array) == nil
-- 当table中没有任何元素时，使用#操作符获取table的长度为0，可以作为判断是否为空的依据
if #messages == 0 then
    return 0;
end
-- 处理每条消息
for i, message in ipairs(messages) do
    -- 数组第一位是messageId
    -- 执行删除操作
    redis.call("xdel", stream, message[1]);
end
return #messages;
