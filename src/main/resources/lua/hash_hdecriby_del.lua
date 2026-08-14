-- 业务作用：原子扣减 Hash 字段，并在结果不大于 0 时删除该字段，避免零值或负值长期占用空间。
-- KEYS[1] Hash 键；ARGV[1] Hash field；ARGV[2] 扣减数量，缺省为 1。
-- 返回：扣减后的整数；字段被删除时仍返回删除前的计算结果。

-- Hash 键。
local k = KEYS[1];
-- Hash field。
local hk = ARGV[1];
-- 自减数量，默认1
local delta = tonumber(ARGV[2]) or 1;
-- 自减，拿自减后的值
local v = redis.call("hincrby", k, hk, -delta);
-- <= 0 时删除hk
if v <= 0 then
    redis.call("hdel", k, hk);
end
-- 必须 return 整数: 脚本若返回顶层 nil/bulk, Lettuce ObjectOutput (ScriptOutputType.OBJECT)
-- 会对构造器初始化的不可变 Collections.emptyList() 调 add(), 抛 UnsupportedOperationException.
-- 整数走 ObjectOutput.set(long) 会先建可变 ArrayList, 不崩; 调用方忽略此返回值.
return v;
