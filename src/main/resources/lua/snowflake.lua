-- 业务作用：以 Redis TIME 为时钟生成雪花 ID，并用保留序列处理短时回拨，保证同一 workerId 的结果不重复。
-- ARGV[1] 生成数量；空或 "1" 返回单个整数，大于 1 时返回对应数量的整数数组。
-- 返回：单个雪花 ID 或雪花 ID 数组；workerId 与位宽由脚本载入前的框架配置确定。

-- 机器码 必须由外部设定，最大值 2^WorkerIdBitLength-1
local workerId = 1;
-- 基础时间（ms单位）  不能超过当前系统时间
local baseTime = 1704038400000;
-- 机器码位长 默认值6，取值范围 [1, 15]（要求：序列数位长+机器码位长不超过22）
local workerIdBitLength = 6;
-- 序列数位长 默认值6，取值范围 [3, 21]（要求：序列数位长+机器码位长不超过22）
local seqBitLength = 5;
-- 序列数位长 设置范围 [MinSeqNumber, 2^SeqBitLength-1]，默认值0，表示最大序列数取最大值（2^SeqBitLength-1]）
local maxSeqNumber = (2 ^ seqBitLength) - 1;
-- 最小序列数（含） 默认值5，取值范围 [5, MaxSeqNumber]，每毫秒的前5个序列数对应编号是0-4是保留位，其中1-4是时间回拨相应预留位，0是手工新值预留位
local minSeqNumber = 5
-- 最大漂移次数（含） 默认2000，推荐范围500-10000（与计算能力有关）
local topOverCostCount = 2000;

local _TimestampShift = workerIdBitLength + seqBitLength;
local _CurrentSeqNumber = minSeqNumber;
local _LastTimeTick = 0;
local _TurnBackTimeTick = 0;
local _TurnBackIndex = 0;
local _IsOverCost = false;
local _OverCostCountInOneTerm = 0;
local _GenCountInOneTerm = 0;

local function GetCurrentTimeTick()
    local now = redis.call('TIME');
    -- now[1] 是redis返回的秒数
    -- now[2] 是redis返回的微妙数
    return now[1]*1000 + now[2]/1000 - baseTime;
end

local function CalcTurnBackId(useTimeTick)
    local result = (useTimeTick * 2 ^ _TimestampShift) + (workerId * 2 ^ seqBitLength) + _TurnBackIndex;
    _TurnBackTimeTick = _TurnBackTimeTick - 1;
    return result;
end

local function CalcId(useTimeTick)
    local result = (useTimeTick * 2 ^ _TimestampShift) + (workerId * 2 ^ seqBitLength) + _CurrentSeqNumber;
    _CurrentSeqNumber = _CurrentSeqNumber + 1;
    return result;
end

local function NextNormalId()
    -- 获取当前毫秒时间
    local currentTimeTick = GetCurrentTimeTick();
    if currentTimeTick < _LastTimeTick then
        if _TurnBackTimeTick < 1 then
            _TurnBackTimeTick = _LastTimeTick - 1;
            _TurnBackIndex = _TurnBackIndex + 1;
            -- 每毫秒序列数的前5位是预留位，0用于手工新值，1-4是时间回拨次序
            -- 支持4次回拨次序（避免回拨重叠导致ID重复），可无限次回拨（次序循环使用）。
            if _TurnBackIndex > 4 then
                _TurnBackIndex = 1;
            end
        end
        return CalcTurnBackId(_TurnBackTimeTick);
    end
    -- 时间追平时，_TurnBackTimeTick清零
    if _TurnBackTimeTick > 0 then
        _TurnBackTimeTick = 0;
    end
    if currentTimeTick > _LastTimeTick then
        _LastTimeTick = currentTimeTick;
        _CurrentSeqNumber = minSeqNumber;
        return CalcId(_LastTimeTick);
    end
    if _CurrentSeqNumber > maxSeqNumber then
        _LastTimeTick = _LastTimeTick + 1;
        _CurrentSeqNumber = minSeqNumber;
        _IsOverCost = true;
        _OverCostCountInOneTerm = 1;
        _GenCountInOneTerm = 1;
        return CalcId(_LastTimeTick);
    end
    return CalcId(_LastTimeTick);
end
-- 生成 ID 的数量。
local c = ARGV[1];
if c == nil or c == "1" then
    return NextNormalId();
else
    local array = {};
    for i = 1, c do
        array[i] = NextNormalId();
    end
    return array;
end
