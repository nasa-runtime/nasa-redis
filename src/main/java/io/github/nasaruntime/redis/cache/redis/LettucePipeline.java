package io.github.nasaruntime.redis.cache.redis;

import io.github.nasaruntime.core.base.ObjectPool;
import io.github.nasaruntime.core.base.RecycleLinkedList;
import io.github.nasaruntime.core.base.RecycleLinkedMap;
import io.github.nasaruntime.redis.cache.redis.search.EntityWriteOp;
import io.github.nasaruntime.redis.cache.redis.search.JsonArrayLuaScripts;
import io.github.nasaruntime.redis.cache.redis.search.RediSearch;
import io.github.nasaruntime.redis.cache.redis.search.executor.StringCommandType;
import io.github.nasaruntime.core.evt.PooledEvtData;
import io.github.nasaruntime.core.exception.CacheException;
import io.github.nasaruntime.core.function.Action;
import io.github.nasaruntime.core.function.BiConsumerRecycler;
import io.github.nasaruntime.core.function.Consumer3;
import io.github.nasaruntime.core.utils.ColUtils;
import io.github.nasaruntime.core.utils.ContextUtils;
import io.github.nasaruntime.core.utils.MapUtils;
import io.github.nasaruntime.core.utils.StringUtils;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.output.IntegerOutput;
import io.lettuce.core.output.NestedMultiOutput;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.output.ValueOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * 零分配 Pipeline 执行器 — 用平坦命令缓冲区 (flat command buffer) 替代 lambda 捕获.
 *
 * <h2>1. 解决什么问题</h2>
 * {@link LettucePipeline} 的 Actuator 对每条写命令都创建一个 lambda (Consumer / Function) 对象,
 * lambda 捕获 key/hashKey/value 三个 byte[] 引用, 存入 {@code RecycleLinkedList} 链表节点.
 * <b>每条命令 = 1 个 lambda + 1 个链表节点 = 2 个短命对象</b>.
 * <p>
 * 在撮合热路径 (CoinEngine.trading) 中, 一批 100 笔成交 × 每笔 10 条写命令 = 2000 个 Young Gen 垃圾对象.
 * 虽然 Young GC 很快, 但高 TPS 下累积仍会增加 GC 频率和延迟毛刺.
 *
 * <h2>2. 核心设计: 并行数组</h2>
 * 用 5 条并行数组替代 lambda + 链表:
 * <pre>
 *   byte[]   ops      — 操作码 (OP_HSET=10, OP_HDEL=11, OP_XADD=50 ...)
 *   byte[][] arg1     — 第 1 个参数 (永远是 byte[] key)
 *   Object[] arg2     — 第 2 个参数 (byte[] hashKey/field, 或 String — XDEL/XTRIM_MINID)
 *   Object[] arg3     — 第 3 个参数 (byte[] value, 或 String — XACK id)
 *   long[]   longArg  — 数值参数 (delta / millis / score)
 * </pre>
 * 每条命令入队 = 5 条数组下标赋值, <b>零对象分配</b>.
 * <p>
 * pipeline() 执行时遍历数组, switch(ops[i]) 分发到 lettuce async commands, 一次 flushCommands.
 * <p>
 * 数组预分配 pipelineLength 槽位, 不够时 doubling 扩容 (Arrays.copyOf).
 * restore() 清引用 (避免内存泄漏) + count=0, 数组本身保留, 下次复用.
 *
 * <h2>3. 两条入队路径</h2>
 * <pre>
 *   ① 平坦数组 (CmdBuffer)   — 所有写命令都走这里, 零分配 / O(1) 入队
 *                              简单命令: arg1/arg2/arg3 + longArg/longArg2 五槽搞定
 *                              复杂参数: 多值命令 (multiSet / hSet(Map) / zAdd varargs) 拆成 N 条单值命令;
 *                                       Map/byte[][] (xAdd 多 field / evalAsync) 用 extras 槽接住
 *   ② add(Function) → LF    — 读命令, 需要 RedisFuture 拿结果, 走 lambda + awaitAll
 *                              eval/scriptFlush/scriptKill 同步版也走这里 (要等命令完成)
 * </pre>
 * 两条队列在 {@link Actuator#doFlush} 内合并, 同一 connection、同一次 flushCommands 一起发.
 *
 * <h2>4. 不支持变长参数 (varargs)</h2>
 * 平坦数组每个 slot 固定 arg1/arg2/arg3 三个槽位, 不支持 {@code sadd(key, v1, v2, v3)} 这种多值形式.
 * <p>
 * 处理方式: 多值拆成多条单值命令入队. 例如 {@code sadd(key, v1, v2, v3)} → 3 条 OP_SADD.
 * Redis pipeline 下一次 flush, 网络开销与单条多值完全相同.
 *
 * <h2>5. 线程模型</h2>
 * <pre>
 *   Actuator 实例: 每个 RedisProxy 一个 CACHE 单例 (全局共享长存); 另有 openIsolated 的 throwaway 临时实例. 自身无可变实例状态
 *        ↓
 *   ThreadLocal&lt;CmdBuffer&gt; CMD: 每线程独占一个 CmdBuffer (并行数组, 所有写命令)  ← 实例级 ThreadLocal
 *   ThreadLocal&lt;RecycleLinkedList&lt;Function&gt;&gt; LF: 每线程独占一个读命令列表
 *   ThreadLocal&lt;Boolean&gt; open / ThreadLocal&lt;Object&gt; execTag: pipeline 控制状态
 * </pre>
 * 同一 Actuator 可被任意线程并发使用, 各线程操作自己的 ThreadLocal 状态, 零锁零竞争.
 * ThreadLocal 是<b>实例级</b>字段: 不同 Actuator 实例 (单例 vs throwaway) 状态互不干扰.
 *
 * <h2>6. execTag 嵌套机制</h2>
 * 与 {@link LettucePipeline} 完全一致:
 * <ul>
 *   <li>{@code open(execTag)} — 设置执行标识, 只有匹配的 {@code pipeline(execTag)} 才触发 flush</li>
 *   <li>嵌套时内层 open 不覆盖外层 tag, 内层 pipeline(innerTag) 不会 flush, 命令自动合并到外层</li>
 *   <li>{@code pipeline()} (无参) — 等价于 pipeline(null), tag==null 时总是 flush</li>
 * </ul>
 *
 * <h2>7. 生命周期</h2>
 * <pre>
 *   pipeline() → pipelineForce() → doFlush()
 *        ↓
 *   finally { clear() }
 *        ↓
 *   CmdBuffer: 一律 recycle 回池 + CMD.remove() (池化资源, 不保留悬挂引用), 不分线程类型
 *   LF / Actions:
 *     平台线程 + CACHE 单例:  仅 clear 容器, 保留 ThreadLocal 引用复用 (单例长存, 容器是轻量链表壳子)
 *     虚拟线程 或 throwaway 实例: recycle() + remove() (防 ThreadLocal 泄漏 / 残条目)
 * </pre>
 *
 * <h2>使用示例</h2>
 * <pre>
 * Actuator act = LettucePipeline.open(redisProxy, null);
 * act.hSetAsync(key, hashKey, value);    // → enqueue: ops[0]=OP_HSET, arg1[0]=key, arg2[0]=hk, arg3[0]=v
 * act.hDelAsync(key, hashKey);           // → enqueue: ops[1]=OP_HDEL, arg1[1]=key, arg2[1]=hk
 * act.publishAsync(stream, event, msg);  // → enqueue: ops[2]=OP_XADD, arg1[2]=stream, arg2[2]=event, arg3[2]=msg
 * act.pipeline();                        // → dispatch: switch(10)→hset, switch(11)→hdel, switch(50)→xadd → flush
 * </pre>
 */
@SuppressWarnings("all")
@Slf4j
public abstract class LettucePipeline {

    /**
     * Lua eval 空 byte[][] 占位
     */
    public static final byte[][] EMPTY_BYTE2 = new byte[0][0];

    // ==================== 操作码 (short, 按数据类型分块连续编号) ====================
    //
    // 排布约定:
    //   1-19     通用 / key 级 (DEL / EXPIRE / TTL / EXISTS / TYPE / DUMP / KEYS ...)
    //   20-49    Hash (HSET / HGET / HMGET ... HVALS / HRANDFIELD / HINCRBYFLOAT)
    //   50-79    Set (SADD / SCARD / SMEMBERS / SPOP / SMOVE ...)
    //   80-119   List (LPUSH / RPUSH / LRANGE / LPOP / LINSERT / LPOS ...)
    //   120-159  String (SET / GET / MGET / MSET / INCRBYFLOAT ...)
    //   160-199  Stream (XADD / XTRIM / XACK / XRANGE / XGROUP ...)
    //   200-209  Pub/Sub
    //   210-259  ZSet (ZADD / ZRANGE / ZRANGEBYSCORE / ZPOPMIN / ZMSCORE ...)
    //   260-269  Script (EVAL / EVALSHA / SCRIPT_LOAD ...)
    //   270-279  RedisJSON
    //   280-289  RediSearch FT.*

    // ---- 1-19: 通用 / key 级 ----
    static final short OP_DEL = 1;
    // pexpire(ms)
    static final short OP_EXPIRE = 2;
    // pexpireat(unix-ms)
    static final short OP_EXPIRE_AT = 3;
    static final short OP_PERSIST = 4;
    static final short OP_PTTL = 5;
    // ttl 秒级
    static final short OP_TTL = 6;
    // exists(k)
    static final short OP_EXISTS = 7;
    // exists(k1, k2, ...), extras = byte[][] keys
    static final short OP_EXISTS_MULTI = 8;
    // del(byte[]... ks), extras = byte[][]
    static final short OP_DEL_MULTI = 9;
    // dump(k)
    static final short OP_DUMP = 10;
    // keys(pattern)
    static final short OP_KEYS = 11;
    // type(k) → "string"/"list"/"hash"/"set"/"zset"/"stream"/"none"
    static final short OP_TYPE = 12;

    // ---- 20-49: Hash ----
    static final short OP_HSET = 20;
    static final short OP_HDEL = 21;
    // hdel(k, (byte[][]) extras)
    static final short OP_HDEL_MULTI = 22;
    static final short OP_HSET_NX = 23;
    static final short OP_HINCRBY = 24;
    static final short OP_HINCRBYFLOAT = 25;
    // 业务专用: hincrBy(k, hk, -delta) + 余 0 时 del hk
    static final short OP_HDECR_BY_AND_DEL = 26;
    // hexpire (a1=key, a2=hashKey, longArg=millis)
    static final short OP_HEXPIRE = 27;
    // hexpire(k, Duration.ofMillis(longArg), (byte[][]) extras)
    static final short OP_HEXPIRE_MULTI = 28;
    // hexists(k, (byte[]) a2)
    static final short OP_HEXISTS = 29;
    // hget(k, (byte[]) a2)
    static final short OP_HGET = 30;
    static final short OP_HGETALL = 31;
    // hmget(k, (byte[][]) extras)
    static final short OP_HMGET = 32;
    static final short OP_HKEYS = 33;
    static final short OP_HVALS = 34;
    static final short OP_HLEN = 35;
    // hmset(k, (Map<byte[], byte[]>) extras)
    static final short OP_HMSET = 36;
    static final short OP_HRANDFIELD = 37;
    static final short OP_HRANDFIELD_COUNT = 38;

    // ---- 50-79: Set ----
    static final short OP_SADD = 50;
    static final short OP_SREM = 51;
    // sadd(k, (byte[][]) extras)
    static final short OP_SADD_MULTI = 52;
    // srem(k, (byte[][]) extras)
    static final short OP_SREM_MULTI = 53;
    static final short OP_SCARD = 54;
    // sismember(k, (byte[]) a3)
    static final short OP_SISMEMBER = 55;
    // smismember(k, (byte[][]) extras)
    static final short OP_SMISMEMBER = 56;
    static final short OP_SMEMBERS = 57;
    static final short OP_SPOP = 58;
    // spop(k, longArg)
    static final short OP_SPOP_COUNT = 59;
    static final short OP_SRANDMEMBER = 60;
    // srandmember(k, longArg)
    static final short OP_SRANDMEMBER_COUNT = 61;
    // sdiff((byte[][]) extras)
    static final short OP_SDIFF = 62;
    // sinter((byte[][]) extras)
    static final short OP_SINTER = 63;
    // sunion((byte[][]) extras)
    static final short OP_SUNION = 64;
    // smove(src=a1, dst=a2, member=a3)
    static final short OP_SMOVE = 65;

    // ---- 80-119: List ----
    static final short OP_LPUSH = 80;
    // lpushx (key 存在时才推)
    static final short OP_LPUSH_X = 81;
    // lpush(k, (byte[][]) extras)
    static final short OP_LPUSH_MULTI = 82;
    static final short OP_RPUSH = 83;
    // rpushx
    static final short OP_RPUSH_X = 84;
    // rpush(k, (byte[][]) extras)
    static final short OP_RPUSH_MULTI = 85;
    // lrem (longArg = count)
    static final short OP_LREM = 86;
    // lset (longArg = index)
    static final short OP_LSET = 87;
    // ltrim (longArg = start, longArg2 = end)
    static final short OP_LTRIM = 88;
    static final short OP_LPOP = 89;
    // lpop(k, (int) longArg)
    static final short OP_LPOP_COUNT = 90;
    static final short OP_RPOP = 91;
    // rpop(k, (int) longArg)
    static final short OP_RPOP_COUNT = 92;
    // lrange(k, longArg, longArg2)
    static final short OP_LRANGE = 93;
    // lindex(k, longArg)
    static final short OP_LINDEX = 94;
    static final short OP_LLEN = 95;
    // linsert(k, before=true, pivot=a2, value=a3)
    static final short OP_LINSERT_BEFORE = 96;
    // linsert(k, before=false, pivot=a2, value=a3)
    static final short OP_LINSERT_AFTER = 97;
    // lpos(k, value=a3)
    static final short OP_LPOS = 98;
    // lpos(k, value=a3, count=longArg)
    static final short OP_LPOS_COUNT = 99;

    // ---- 120-159: String ----
    static final short OP_SET = 120;
    // psetex (longArg = millis)
    static final short OP_SET_EX = 121;
    // setnx
    static final short OP_SET_NX = 122;
    // setrange (longArg = offset)
    static final short OP_SETRANGE = 123;
    static final short OP_APPEND = 124;
    static final short OP_INCR = 125;
    static final short OP_DECR = 126;
    static final short OP_INCRBY = 127;
    static final short OP_DECRBY = 128;
    // incrbyfloat(k, double delta); delta 存 longArg 的 doubleToRawLongBits
    static final short OP_INCRBYFLOAT = 129;
    static final short OP_GET = 130;
    // getrange(k, longArg, longArg2)
    static final short OP_GETRANGE = 131;
    // getset(k, (byte[]) a3)
    static final short OP_GETSET = 132;
    static final short OP_GETDEL = 133;
    // mget((byte[][]) extras)
    static final short OP_MGET = 134;
    // mset((Map<byte[], byte[]>) extras)
    static final short OP_MSET = 135;
    static final short OP_STRLEN = 136;

    // ---- 160-199: Stream ----
    static final short OP_XADD = 160;
    // xadd 多 (field, message), extras = Map<byte[], byte[]>
    static final short OP_XADD_MULTI = 161;
    // xtrim MAXLEN (longArg = maxlen)
    static final short OP_XTRIM_MAXLEN = 162;
    // xtrim MINID (a2 = String minId, 直传)
    static final short OP_XTRIM_MINID = 163;
    // xtrim(s, (XTrimArgs) extras)
    static final short OP_XTRIM_ARGS = 164;
    // xdel (a2 = String messageId, 直传)
    static final short OP_XDEL = 165;
    // xdel(s, (String[]) extras messageIds)
    static final short OP_XDEL_MULTI = 166;
    // xack (a1=stream, a2=group byte[], a3=String id; 多 id 拆多条)
    static final short OP_XACK = 167;
    // xack(s, (byte[]) a3 group, (String[]) extras ids)
    static final short OP_XACK_MULTI = 168;
    static final short OP_XLEN = 169;
    // xrange(s, (Range<String>) extras)
    static final short OP_XRANGE = 170;
    // xrange(s, (Range<String>) extras, Limit.from((int) longArg))
    static final short OP_XRANGE_LIMIT = 171;
    static final short OP_XREVRANGE = 172;
    static final short OP_XREVRANGE_LIMIT = 173;
    static final short OP_XINFO_GROUPS = 174;
    // xinfoConsumers(s, (byte[]) a3 group)
    static final short OP_XINFO_CONSUMERS = 175;
    // xgroupCreate((XReadArgs.StreamOffset<byte[]>) extras offset, (byte[]) a3 group, XGroupCreateArgs.Builder.mkstream())
    static final short OP_XGROUP_CREATE = 176;
    // xgroupDestroy(s, (byte[]) a3 group)
    static final short OP_XGROUP_DESTROY = 177;
    // xautoclaim(s, (XAutoClaimArgs<byte[]>) extras)
    static final short OP_XAUTOCLAIM = 178;

    // ---- 200-209: Pub/Sub ----
    // publish channel
    static final short OP_PUB = 200;

    // ---- 210-259: ZSet ----
    // longArg = Double.doubleToRawLongBits(score)
    static final short OP_ZADD = 210;
    // zadd(k, (Object[]) extras scoreAndVals)
    static final short OP_ZADD_MULTI = 211;
    static final short OP_ZREM = 212;
    // zrem(k, (byte[][]) extras)
    static final short OP_ZREM_MULTI = 213;
    // longArg = score bits
    static final short OP_ZINCRBY = 214;
    // zremrangebyrank (longArg = start, longArg2 = end)
    static final short OP_ZREMRANGE = 215;
    // zremrangebylex (a2 = min, a3 = max)
    static final short OP_ZREMRANGE_BY_LEX = 216;
    // zremrangebylex(k, (Range<byte[]>) extras)
    static final short OP_ZREMRANGE_BY_LEX_RANGE = 217;
    // zremrangebyscore (longArg = min bits, longArg2 = max bits)
    static final short OP_ZREMRANGE_BY_SCORE = 218;
    // zremrangebyscore(k, (Range<? extends Number>) extras)
    static final short OP_ZREMRANGE_BY_SCORE_RANGE = 219;
    static final short OP_ZCARD = 220;
    // zcount(k, (Range<byte[]>) extras)
    static final short OP_ZCOUNT_LEX = 221;
    // zcount(k, (Range<? extends Number>) extras)
    static final short OP_ZCOUNT_SCORE = 222;
    static final short OP_ZRANK = 223;
    static final short OP_ZREVRANK = 224;
    static final short OP_ZSCORE = 225;
    // zmscore(k, (byte[][]) extras members)
    static final short OP_ZMSCORE = 226;
    // zrange(k, longArg, longArg2)
    static final short OP_ZRANGE = 227;
    // zrangebylex(k, (Range<byte[]>) extras)
    static final short OP_ZRANGEBYLEX = 228;
    // zrangebylex(k, (Range<byte[]>) extras, (Limit) a2)
    static final short OP_ZRANGEBYLEX_LIMIT = 229;
    // zrangebyscore(k, (Range<? extends Number>) extras)
    static final short OP_ZRANGEBYSCORE = 230;
    // zrangebyscore(k, (Range<? extends Number>) extras, (Limit) a2)
    static final short OP_ZRANGEBYSCORE_LIMIT = 231;
    // zrevrange(k, longArg, longArg2)
    static final short OP_ZREVRANGE = 232;
    static final short OP_ZREVRANGEBYSCORE = 233;
    static final short OP_ZREVRANGEBYSCORE_LIMIT = 234;
    static final short OP_ZRANGE_WITHSCORES = 235;
    static final short OP_ZREVRANGE_WITHSCORES = 236;
    static final short OP_ZRANGEBYSCORE_WITHSCORES = 237;
    static final short OP_ZRANGEBYSCORE_WITHSCORES_LIMIT = 238;
    static final short OP_ZREVRANGEBYSCORE_WITHSCORES = 239;
    static final short OP_ZREVRANGEBYSCORE_WITHSCORES_LIMIT = 240;
    // zpopmin(k) / zpopmin(k, longArg=count) / zpopmax 同
    static final short OP_ZPOPMIN = 241;
    static final short OP_ZPOPMIN_COUNT = 242;
    static final short OP_ZPOPMAX = 243;
    static final short OP_ZPOPMAX_COUNT = 244;
    // zrandmember(k) / zrandmember(k, longArg=count)
    static final short OP_ZRANDMEMBER = 245;
    static final short OP_ZRANDMEMBER_COUNT = 246;

    // ---- 260-269: Script ----
    // eval((byte[]) a1 script, (ScriptOutputType) a3 type, (Object[]) extras={byte[][] keys, byte[][] args})
    static final short OP_EVAL = 260;
    // evalAsync, extras = Object[]{byte[] script, byte[][] keys, byte[][] args}
    static final short OP_EVAL_ASYNC = 261;
    // evalsha((String) a2 sha1, (ScriptOutputType) a3 type, (Object[]) extras={byte[][] keys, byte[][] args})
    static final short OP_EVALSHA = 262;
    // scriptLoad((byte[]) a1)
    static final short OP_SCRIPT_LOAD = 263;
    // scriptExists((String[]) extras)
    static final short OP_SCRIPT_EXISTS = 264;
    static final short OP_SCRIPT_FLUSH = 265;
    static final short OP_SCRIPT_KILL = 266;

    // ---- 270-279: RedisJSON ----
    // JSON.SET k $ v: a1=key, a2=path bytes ("$"), a3=value bytes (json)
    static final short OP_JSON_SET = 270;
    // JSON.SET k $.subpath v: 同 OP_JSON_SET, 独立 OP 留给后续监控 / 路径优化用
    static final short OP_JSON_SET_FIELD = 271;
    // JSON.DEL k [path]: a1=key, a2=path bytes (可选)
    static final short OP_JSON_DEL = 272;
    // JSON.GET k [path]: a1=key, a2=path bytes (可选)
    static final short OP_JSON_GET = 273;
    // JSON.NUMINCRBY k path delta: a1=key, a2=path bytes (必填), a3=delta 数字字符串 bytes
    // RedisJSON 原生原子加减命令: 服务端单线程执行, 并发安全; delta 负数即 decr (RedisJSON 无独立 DECRBY 命令).
    // 返回新值字符串 (legacy path → "3", jsonpath → "[3]"); 业务侧 fire-and-forget 时丢弃即可.
    static final short OP_JSON_NUMINCRBY = 274;

    // ---- 280-289: RediSearch FT.* ----
    // FT.SEARCH idx ...: a1=index bytes, extras=(String[]) 剩余 args (query / SORTBY / LIMIT / DIALECT)
    static final short OP_FT_SEARCH = 280;
    // FT.AGGREGATE idx ...
    static final short OP_FT_AGGREGATE = 281;
    // FT.CREATE idx ON ... SCHEMA ...
    static final short OP_FT_CREATE = 282;
    // FT.INFO idx
    static final short OP_FT_INFO = 283;
    // FT.DROPINDEX idx [DD]
    static final short OP_FT_DROPINDEX = 284;

    // ---- RediSearch / RedisJSON 命令字 (lettuce 标准库没有这些, 走 c.dispatch 自定义命令) ----

    /**
     * RedisJSON / RediSearch 命令名 → ProtocolKeyword 单例 (byte[] name 预序列化, 每条命令零 GC).
     * 走 {@link StringCommandType#of(String)} 静态缓存, 跟 frameworkDataRsCommandExecutor 全局复用同一实例.
     */
    private static final ProtocolKeyword CMD_JSON_SET = StringCommandType.of("JSON.SET");
    private static final ProtocolKeyword CMD_JSON_DEL = StringCommandType.of("JSON.DEL");
    private static final ProtocolKeyword CMD_JSON_GET = StringCommandType.of("JSON.GET");
    private static final ProtocolKeyword CMD_JSON_NUMINCRBY = StringCommandType.of("JSON.NUMINCRBY");
    private static final ProtocolKeyword CMD_FT_SEARCH = StringCommandType.of("FT.SEARCH");
    private static final ProtocolKeyword CMD_FT_AGGREGATE = StringCommandType.of("FT.AGGREGATE");
    private static final ProtocolKeyword CMD_FT_CREATE = StringCommandType.of("FT.CREATE");
    private static final ProtocolKeyword CMD_FT_INFO = StringCommandType.of("FT.INFO");
    private static final ProtocolKeyword CMD_FT_DROPINDEX = StringCommandType.of("FT.DROPINDEX");

    /**
     * 命令缓冲区初始容量, 等于 pipelineLength.
     * 命令累积数 (CmdBuffer.count + LF.size()) 达到 pipelineLength 时触发 autoFlush, 正常不会超过此值, 预分配刚好够用.
     * <p>
     * 同时作为 {@link RedisProxy#before} tick 的 drain 单 tick 上限, 防业务持续高速 offer 时信号线程长占 pipelineLock.
     */
    public static int PIPELINE_LENGTH = 1000;

    /**
     * 全局缓存: RedisProxy → Actuator, 与 LettucePipeline.CACHE 同构
     */
    static final Map<RedisProxy, Actuator> CACHE = new ConcurrentHashMap<>();

    /**
     * 业务作用：为一个命令代理登记批次执行器，使该实例上的批量能力可用。
     *
     * @param redisProxy 命令代理
     * 返回: 无返回值。
     */
    public static void initialize(RedisProxy redisProxy) {
        CACHE.put(redisProxy, new Actuator(redisProxy));
    }

    /**
     * 业务作用：撤销命令代理对应的批次执行器登记，避免应用上下文重建后旧代理仍被静态缓存持有。
     *
     * @param redisProxy 生命周期已经结束的命令代理
     * 返回: 无返回值；后续按该代理加载执行器将返回 null。
     */
    public static void destroy(RedisProxy redisProxy) {
        Actuator removed = CACHE.remove(redisProxy);
        if (removed == null) return;
        defaultLock.lock();
        try {
            if (DEFAULT == removed) DEFAULT = null;
        } finally {
            defaultLock.unlock();
        }
    }

    static Actuator DEFAULT;
    private static final ReentrantLock defaultLock = new ReentrantLock();

    /**
     * 业务作用：获取默认 Actuator (绑定名为 "redisTemplate" 的 Spring Bean)
     *
     * @return 见上述说明。
     */
    static Actuator load() {
        if (Objects.nonNull(DEFAULT)) return DEFAULT;
        defaultLock.lock();
        try {
            if (Objects.nonNull(DEFAULT)) return DEFAULT;
            return DEFAULT = load(ContextUtils.getBean("redisTemplate", RedisTemplate.class));
        } finally {
            defaultLock.unlock();
        }
    }

    /**
     * 业务作用：按模板取批次执行器。
     *
     * @param redisTemplate 操作模板
     * @return 该模板对应的批次执行器。
     */
    public static Actuator load(RedisTemplate<String, Object> redisTemplate) {
        return load(RedisProxy.load(redisTemplate));
    }

    /**
     * 业务作用：按命令代理取批次执行器。
     *
     * @param redisProxy 命令代理
     * @return 该代理的批次执行器；未登记时为 null。
     */
    public static Actuator load(RedisProxy redisProxy) {
        return CACHE.get(redisProxy);
    }

    // ==================== 静态入口 (mirror LettucePipeline 完整 API) ====================

    /**
     * 业务作用：在默认执行器上开启一个批次，此后命令进入缓冲而不立即发出。
     * 必须配对调用收尾方法，否则缓冲状态会残留在当前线程上污染后续调用。
     *
     * <p>参数说明: 无。
     *
     * @return 默认执行器。
     */
    public static Actuator open() {
        return load().open();
    }

    /**
     * 业务作用：在默认执行器上开启带标识的批次。
     * 只有以相同标识收尾才会真正发出；嵌套时内层标识<b>不覆盖</b>外层，内层收尾不生效，
     * 命令自动并入外层批次一起发出。
     *
     * @param execTag 批次标识
     * @return 默认执行器。
     */
    public static Actuator open(Object execTag) {
        return load().open(execTag);
    }

    /**
     * 业务作用：在指定命令代理上开启带标识的批次。
     *
     * @param redisProxy 命令代理
     * @param execTag    批次标识
     * @return 该代理的批次执行器。
     */
    public static Actuator open(RedisProxy redisProxy, Object execTag) {
        return load(redisProxy).open(execTag);
    }

    /**
     * 业务作用：在指定模板对应的执行器上开启带标识的批次。
     *
     * @param redisTemplate 操作模板
     * @param execTag       批次标识
     * @return 对应的批次执行器。
     */
    public static Actuator open(RedisTemplate<String, Object> redisTemplate, Object execTag) {
        return load(redisTemplate).open(execTag);
    }

    /**
     * 业务作用：获取一个用完即弃的临时执行器, 使用完毕交由 GC 回收。
     * <p>
     * <b>设计目标</b>: 在当前线程已经 {@code open()} 一个 session 的路径上, 需要再开一个<b>独立</b>执行器,
     * 不受外层 open 上下文控制、也不污染外层 session。
     * <p>
     * <b>为何独立</b>: {@link Actuator} 的 open/execTag/CMD/LF 都是<b>实例级</b> ThreadLocal,
     * 这里 {@code new Actuator} 拥有完全独立的 ThreadLocal 存储, 与 {@link #CACHE} 单例互不干扰,
     * 因此绝不会读写到外层 session 的缓冲命令。throwaway 标记让其 {@link Actuator#clear} 走全 remove,
     * 不在线程 ThreadLocalMap 留残条目。
     * <p>
     * <b>调用方约束 (重要)</b>:
     * <ul>
     *   <li>本执行器是用完即弃的, 不像 CACHE 单例那样有后续 session 兜底清理。<b>必须</b>用 try/finally
     *       保证最终调用 {@link Actuator#pipelineForce()} / {@link Actuator#pipeline()} / {@link Actuator#clearSession()}。
     *       openIsolated 本身只 new 对象 + 设 open ThreadLocal, 不借连接; 忘记收尾会让其 ThreadLocal 状态残留到当前线程
     *       (直到线程结束 / ThreadLocalMap 后续清理)。若已进入 flush / autoFlush 路径再异常, 才涉及连接归还、permit 释放、CmdBuffer 回收依赖异常分支。</li>
     *   <li>每次调用都分配一个新 Actuator + 5 个 ThreadLocal, 有 GC 成本。<b>只用于低频场景, 不要放热路径。</b></li>
     *   <li>它在另一条独立连接上立即 flush, 会先于外层仍在缓冲的 session 到达 Redis — 这是"独立执行"的预期语义,
     *       调用方需自行确保两者之间没有顺序依赖。</li>
     *   <li>异常路径想丢弃已入队命令: 调 {@link Actuator#clearSession()} (abort, 只清不发)。</li>
     * </ul>
     * <p>
     * 不设 execTag (恒为 null): 临时执行器无嵌套需求, 用无参 {@link Actuator#pipeline()} 或 {@link Actuator#pipelineForce()} 都能正常 flush。
     *
     * @param redisProxy 命令代理，决定连接与序列化方式
     * @return 见上述说明。
     */
    public static Actuator openIsolated(RedisProxy redisProxy) {
        return new Actuator(redisProxy, true).open(null);
    }

    /**
     * 业务作用：按需取执行器: 当前线程在该 redisProxy 的 {@link #CACHE} 单例上<b>已 open</b> session 时, 返回一个 throwaway 独立执行器
     * ({@link #openIsolated}), 避免内部立即 flush 的批次破坏外层 session; <b>否则复用 CACHE 单例</b> (零分配) 并 open。
     * <p>
     * 用于 {@code hMGetToMap} / {@code lPopAll} 这类"内部独立批次": 嵌套场景要隔离, 非嵌套常态不想付出 new Actuator 的 GC。
     * <p>
     * 返回的执行器都需调用方在 try/finally 里收尾 ({@link Actuator#pipelineForce()} / {@link Actuator#pipeline()} / {@link Actuator#clearSession()})。
     *
     * @param redisProxy 命令代理，决定连接与序列化方式
     */
    public static Actuator openNested(RedisProxy redisProxy) {
        Actuator cached = load(redisProxy);
        return cached.isSessionOpen() ? openIsolated(redisProxy) : cached.open(null);
    }

    /**
     * 业务作用：按键的序列化方式把字符串键转成字节，供需要直接拼装命令的调用方使用。
     *
     * @param key 缓存键
     * @return 序列化后的字节。
     */
    public static byte[] serialize(String key) {
        return load().serializeKey(key);
    }

    /**
     * 业务作用：按哈希字段的序列化方式把字段名转成字节。
     * 键与哈希字段可配置不同的序列化方式，混用会让写入与读取对不上。
     *
     * @param hashKey 哈希字段名
     * @return 序列化后的字节。
     */
    public static byte[] hashSerialize(String hashKey) {
        return load().serializeHKey(hashKey);
    }

    /**
     * 业务作用：把一条需要取回结果的命令排入批次。
     * <p>
     * 需要结果的命令走函数队列而非平坦缓冲：调用方要持有结果句柄做后续处理，
     * 用固定槽位的数组表达不了。读命令在热路径上占比很低，这条路径的分配开销可以接受。
     *
     * @param func 产出结果句柄的命令函数
     * 返回: 无返回值。
     */
    public static void add(Function<RedisClusterAsyncCommands<byte[], byte[]>, RedisFuture<?>> func) {
        load().add(func);
    }

    /**
     * 业务作用：在默认执行器上收尾并发出当前批次。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值。
     */
    public static void pipeline() {
        load().pipeline();
    }

    /**
     * 业务作用：以给定标识收尾批次；标识不匹配时不发出，命令并入外层批次。
     *
     * @param execTag 批次标识
     * 返回: 无返回值。
     */
    public static void pipeline(Object execTag) {
        load().pipeline(execTag);
    }

    /**
     * 业务作用：删除键。
     * 键不存在时不报错，因此可安全用于幂等清理。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void del(Object key) {
        load().del(key);
    }

    /**
     * 业务作用：删除键。
     * 键不存在时不报错，因此可安全用于幂等清理。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void delAsync(Object key) {
        load().delAsync(key);
    }

    /**
     * 业务作用：设置键的存活时长。
     * 已存在的存活时长会被覆盖；键不存在时设置不生效。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param timeout 超时时长
     * @param unit 时长单位
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void expire(Object key, long timeout, TimeUnit unit) {
        load().expire(key, timeout, unit);
    }

    /**
     * 业务作用：设置键的存活时长。
     * 已存在的存活时长会被覆盖；键不存在时设置不生效。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param timeout 超时时长
     * @param unit 时长单位
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void expireAsync(Object key, long timeout, TimeUnit unit) {
        load().expireAsync(key, timeout, unit);
    }

    /**
     * 业务作用：设置键的存活时长。
     * 已存在的存活时长会被覆盖；键不存在时设置不生效。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param millis 毫秒数
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void expire(Object key, long millis) {
        load().expire(key, millis);
    }

    /**
     * 业务作用：设置键的存活时长。
     * 已存在的存活时长会被覆盖；键不存在时设置不生效。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param millis 毫秒数
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void expireAsync(Object key, long millis) {
        load().expireAsync(key, millis);
    }

    /**
     * 业务作用：把键的过期时刻设为绝对时间。
     * 依赖服务端时钟，与客户端时钟不一致时过期时刻会偏移。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param millis 毫秒数
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void expireAt(Object key, long millis) {
        load().expireAt(key, millis);
    }

    /**
     * 业务作用：把键的过期时刻设为绝对时间。
     * 依赖服务端时钟，与客户端时钟不一致时过期时刻会偏移。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param millis 毫秒数
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void expireAtAsync(Object key, long millis) {
        load().expireAtAsync(key, millis);
    }

    /**
     * 业务作用：移除键的存活时长使其长期保留。
     * 此后该键不再自动回收，需由业务显式删除，否则会持续占用内存。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void persist(Object key) {
        load().persist(key);
    }

    /**
     * 业务作用：移除键的存活时长使其长期保留。
     * 此后该键不再自动回收，需由业务显式删除，否则会持续占用内存。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void persistAsync(Object key) {
        load().persistAsync(key);
    }

    /**
     * 业务作用：为哈希中的<b>单个字段</b>设置存活时长，到期后只删该字段，其余字段与键本身不受影响。
     * 该能力要求 Redis 7.4 及以上；低版本服务端会拒绝该命令。已存在的字段存活时长会被覆盖。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param millis 毫秒数
     * @param hashKey 哈希字段名
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void expire(Object key, long millis, Object hashKey) {
        load().expire(key, millis, hashKey);
    }

    /**
     * 业务作用：为哈希中的<b>单个字段</b>设置存活时长，到期后只删该字段，其余字段与键本身不受影响。
     * 该能力要求 Redis 7.4 及以上；低版本服务端会拒绝该命令。已存在的字段存活时长会被覆盖。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param millis 毫秒数
     * @param hashKey 哈希字段名
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void expireAsync(Object key, long millis, Object hashKey) {
        load().expireAsync(key, millis, hashKey);
    }

    /**
     * 业务作用：为哈希中的<b>单个字段</b>设置存活时长，到期后只删该字段，其余字段与键本身不受影响。
     * 该能力要求 Redis 7.4 及以上；低版本服务端会拒绝该命令。已存在的字段存活时长会被覆盖。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param timeout 超时时长
     * @param unit 时长单位
     * @param hashKey 哈希字段名
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void expire(Object key, long timeout, TimeUnit unit, Object hashKey) {
        load().expire(key, timeout, unit, hashKey);
    }

    /**
     * 业务作用：为哈希中的<b>单个字段</b>设置存活时长，到期后只删该字段，其余字段与键本身不受影响。
     * 该能力要求 Redis 7.4 及以上；低版本服务端会拒绝该命令。已存在的字段存活时长会被覆盖。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param timeout 超时时长
     * @param unit 时长单位
     * @param hashKey 哈希字段名
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void expireAsync(Object key, long timeout, TimeUnit unit, Object hashKey) {
        load().expireAsync(key, timeout, unit, hashKey);
    }

    /**
     * 业务作用：为哈希中的<b>单个字段</b>设置存活时长，到期后只删该字段，其余字段与键本身不受影响。
     * 该能力要求 Redis 7.4 及以上；低版本服务端会拒绝该命令。已存在的字段存活时长会被覆盖。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param timeout 超时时长
     * @param hashKey 哈希字段名
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void expire(Object key, Duration timeout, Object hashKey) {
        load().expire(key, timeout, hashKey);
    }

    /**
     * 业务作用：为哈希中的<b>单个字段</b>设置存活时长，到期后只删该字段，其余字段与键本身不受影响。
     * 该能力要求 Redis 7.4 及以上；低版本服务端会拒绝该命令。已存在的字段存活时长会被覆盖。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param timeout 超时时长
     * @param hashKey 哈希字段名
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void expireAsync(Object key, Duration timeout, Object hashKey) {
        load().expireAsync(key, timeout, hashKey);
    }

    /**
     * 业务作用：写入字符串值。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param value 待写入的值
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static <T> void set(Object key, T value) {
        load().set(key, value);
    }

    /**
     * 业务作用：写入字符串值。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param value 待写入的值
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static <T> void setAsync(Object key, T value) {
        load().setAsync(key, value);
    }

    /**
     * 业务作用：写入字符串值。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * @param timeout 超时时长
     * @param unit 时长单位
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void set(Object key, Object val, int timeout, TimeUnit unit) {
        load().set(key, val, timeout, unit);
    }

    /**
     * 业务作用：写入字符串值。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * @param timeout 超时时长
     * @param unit 时长单位
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void setAsync(Object key, Object val, int timeout, TimeUnit unit) {
        load().setAsync(key, val, timeout, unit);
    }

    /**
     * 业务作用：写入字符串值。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * @param millis 毫秒数
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void set(Object key, Object val, int millis) {
        load().set(key, val, millis);
    }

    /**
     * 业务作用：写入字符串值。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * @param millis 毫秒数
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void setAsync(Object key, Object val, int millis) {
        load().setAsync(key, val, millis);
    }

    /**
     * 业务作用：仅在键不存在时写入。
     * 写入与判存在服务端原子完成，常用于抢占型的互斥标记。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void setIfAbsent(Object key, Object val) {
        load().setIfAbsent(key, val);
    }

    /**
     * 业务作用：仅在键不存在时写入。
     * 写入与判存在服务端原子完成，常用于抢占型的互斥标记。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void setIfAbsentAsync(Object key, Object val) {
        load().setIfAbsentAsync(key, val);
    }

    /**
     * 业务作用：从指定偏移开始覆写字符串的一段。
     * 偏移超出原长度时中间以零字节填充，可能一次分配大量内存。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * @param offset 偏移量
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void setRange(Object key, Object val, long offset) {
        load().setRange(key, val, offset);
    }

    /**
     * 业务作用：从指定偏移开始覆写字符串的一段。
     * 偏移超出原长度时中间以零字节填充，可能一次分配大量内存。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * @param offset 偏移量
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void setRangeAsync(Object key, Object val, long offset) {
        load().setRangeAsync(key, val, offset);
    }

    /**
     * 业务作用：批量写入多个键值。
     * 本实现拆成多条单值命令入队，网络开销与原生批量命令相同。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param kvMap 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void multiSet(Map kvMap) {
        load().multiSet(kvMap);
    }

    /**
     * 业务作用：批量写入多个键值。
     * 本实现拆成多条单值命令入队，网络开销与原生批量命令相同。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param kvMap 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void multiSetAsync(Map kvMap) {
        load().multiSetAsync(kvMap);
    }

    /**
     * 业务作用：按增量原子自增。
     * 自增在服务端完成，多个客户端并发调用不会丢更新。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void increment(Object key) {
        load().increment(key);
    }

    /**
     * 业务作用：按增量原子自增。
     * 自增在服务端完成，多个客户端并发调用不会丢更新。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void incrementAsync(Object key) {
        load().incrementAsync(key);
    }

    /**
     * 业务作用：按增量原子自增。
     * 自增在服务端完成，多个客户端并发调用不会丢更新。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param delta 增减量
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void increment(Object key, long delta) {
        load().increment(key, delta);
    }

    /**
     * 业务作用：按增量原子自增。
     * 自增在服务端完成，多个客户端并发调用不会丢更新。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param delta 增减量
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void incrementAsync(Object key, long delta) {
        load().incrementAsync(key, delta);
    }

    /**
     * 业务作用：按减量原子自减。
     * 结果可为负数，需要下界约束的场景应改用脚本。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void decrement(Object key) {
        load().decrement(key);
    }

    /**
     * 业务作用：按减量原子自减。
     * 结果可为负数，需要下界约束的场景应改用脚本。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void decrementAsync(Object key) {
        load().decrementAsync(key);
    }

    /**
     * 业务作用：按减量原子自减。
     * 结果可为负数，需要下界约束的场景应改用脚本。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param delta 增减量
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void decrement(Object key, long delta) {
        load().decrement(key, delta);
    }

    /**
     * 业务作用：按减量原子自减。
     * 结果可为负数，需要下界约束的场景应改用脚本。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param delta 增减量
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void decrementAsync(Object key, long delta) {
        load().decrementAsync(key, delta);
    }

    /**
     * 业务作用：在字符串末尾追加内容。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void append(Object key, Object val) {
        load().append(key, val);
    }

    /**
     * 业务作用：在字符串末尾追加内容。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void appendAsync(Object key, Object val) {
        load().appendAsync(key, val);
    }

    /**
     * 业务作用：删除哈希字段。
     * 字段不存在时不报错。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param hashKey 哈希字段名
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void hDel(Object key, Object hashKey) {
        load().hDel(key, hashKey);
    }

    /**
     * 业务作用：删除哈希字段。
     * 字段不存在时不报错。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param hashKey 哈希字段名
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void hDelAsync(Object key, Object hashKey) {
        load().hDelAsync(key, hashKey);
    }

    /**
     * 业务作用：写入哈希字段。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param hashKey 哈希字段名
     * @param value 待写入的值
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static <T> void hSet(Object key, Object hashKey, T value) {
        load().hSet(key, hashKey, value);
    }

    /**
     * 业务作用：写入哈希字段。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param hashKey 哈希字段名
     * @param value 待写入的值
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static <T> void hSetAsync(Object key, Object hashKey, T value) {
        load().hSetAsync(key, hashKey, value);
    }

    /**
     * 业务作用：写入哈希字段。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param khkvMap 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void hSet(Map<String, Map> khkvMap) {
        load().hSet(khkvMap);
    }

    /**
     * 业务作用：写入哈希字段。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param khkvMap 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void hSetAsync(Map<String, Map> khkvMap) {
        load().hSetAsync(khkvMap);
    }

    /**
     * 业务作用：写入哈希字段。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param hkvMap 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static <T> void hSet(Object key, Map<String, T> hkvMap) {
        load().hSet(key, hkvMap);
    }

    /**
     * 业务作用：写入哈希字段。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param hkvMap 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static <T> void hSetAsync(Object key, Map<String, T> hkvMap) {
        load().hSetAsync(key, hkvMap);
    }

    /**
     * 业务作用：仅在哈希字段不存在时写入。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param hashKey 哈希字段名
     * @param val 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void hSetNx(Object key, Object hashKey, Object val) {
        load().hSetNx(key, hashKey, val);
    }

    /**
     * 业务作用：仅在哈希字段不存在时写入。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param hashKey 哈希字段名
     * @param val 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void hSetNxAsync(Object key, Object hashKey, Object val) {
        load().hSetNxAsync(key, hashKey, val);
    }

    /**
     * 业务作用：按增量原子自增哈希字段。
     * 自增在服务端完成，并发调用不会丢更新。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param hashKey 哈希字段名
     * @param delta 增减量
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void hIncrBy(Object key, Object hashKey, long delta) {
        load().hIncrBy(key, hashKey, delta);
    }

    /**
     * 业务作用：按增量原子自增哈希字段。
     * 自增在服务端完成，并发调用不会丢更新。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param hashKey 哈希字段名
     * @param delta 增减量
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void hIncrByAsync(Object key, Object hashKey, long delta) {
        load().hIncrByAsync(key, hashKey, delta);
    }

    /**
     * 业务作用：按增量原子自增哈希字段。
     * 自增在服务端完成，并发调用不会丢更新。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param hashKey 哈希字段名
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void hIncrBy(Object key, Object hashKey) {
        load().hIncrBy(key, hashKey);
    }

    /**
     * 业务作用：按增量原子自增哈希字段。
     * 自增在服务端完成，并发调用不会丢更新。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param hashKey 哈希字段名
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void hIncrByAsync(Object key, Object hashKey) {
        load().hIncrByAsync(key, hashKey);
    }

    /**
     * 业务作用：按减量原子自减哈希字段。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param hashKey 哈希字段名
     * @param delta 增减量
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void hDecrBy(Object key, Object hashKey, long delta) {
        load().hDecrBy(key, hashKey, delta);
    }

    /**
     * 业务作用：按减量原子自减哈希字段。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param hashKey 哈希字段名
     * @param delta 增减量
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void hDecrByAsync(Object key, Object hashKey, long delta) {
        load().hDecrByAsync(key, hashKey, delta);
    }

    /**
     * 业务作用：按减量原子自减哈希字段。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param hashKey 哈希字段名
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void hDecrBy(Object key, Object hashKey) {
        load().hDecrBy(key, hashKey);
    }

    /**
     * 业务作用：按减量原子自减哈希字段。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param hashKey 哈希字段名
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void hDecrByAsync(Object key, Object hashKey) {
        load().hDecrByAsync(key, hashKey);
    }

    /**
     * 业务作用：按减量自减哈希字段并在减到零时删除该字段。
     * 自减与删除由 Lua 脚本在服务端原子完成——分两步会在两者之间留出窗口，使字段可能被并发写回而残留一个零值。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param hashKey 哈希字段名
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void hDecrByAndDel(Object key, Object hashKey) {
        load().hDecrByAndDel(key, hashKey);
    }

    /**
     * 业务作用：按减量自减哈希字段并在减到零时删除该字段。
     * 自减与删除由 Lua 脚本在服务端原子完成——分两步会在两者之间留出窗口，使字段可能被并发写回而残留一个零值。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param hashKey 哈希字段名
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void hDecrByAndDelAsync(Object key, Object hashKey) {
        load().hDecrByAndDelAsync(key, hashKey);
    }

    /**
     * 业务作用：按减量自减哈希字段并在减到零时删除该字段。
     * 自减与删除由 Lua 脚本在服务端原子完成——分两步会在两者之间留出窗口，使字段可能被并发写回而残留一个零值。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param hashKey 哈希字段名
     * @param delta 增减量
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void hDecrByAndDel(Object key, Object hashKey, long delta) {
        load().hDecrByAndDel(key, hashKey, delta);
    }

    /**
     * 业务作用：按减量自减哈希字段并在减到零时删除该字段。
     * 自减与删除由 Lua 脚本在服务端原子完成——分两步会在两者之间留出窗口，使字段可能被并发写回而残留一个零值。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param hashKey 哈希字段名
     * @param delta 增减量
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void hDecrByAndDelAsync(Object key, Object hashKey, long delta) {
        load().hDecrByAndDelAsync(key, hashKey, delta);
    }

    /**
     * 业务作用：从列表头部插入元素。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void lPush(Object key, Object val) {
        load().lPush(key, val);
    }

    /**
     * 业务作用：从列表头部插入元素。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void lPushAsync(Object key, Object val) {
        load().lPushAsync(key, val);
    }

    /**
     * 业务作用：仅在列表已存在时从头部插入。
     * 列表不存在时不创建，用于避免凭空建出空列表。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void lPushIfAbsent(Object key, Object val) {
        load().lPushIfAbsent(key, val);
    }

    /**
     * 业务作用：仅在列表已存在时从头部插入。
     * 列表不存在时不创建，用于避免凭空建出空列表。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void lPushIfAbsentAsync(Object key, Object val) {
        load().lPushIfAbsentAsync(key, val);
    }

    /**
     * 业务作用：按值删除列表中的元素。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param count 数量上限
     * @param val 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void lRem(Object key, long count, Object val) {
        load().lRem(key, count, val);
    }

    /**
     * 业务作用：按值删除列表中的元素。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param count 数量上限
     * @param val 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void lRemAsync(Object key, long count, Object val) {
        load().lRemAsync(key, count, val);
    }

    /**
     * 业务作用：按下标覆盖列表元素。
     * 下标越界时报错。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param index 下标
     * @param val 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void lSet(Object key, long index, Object val) {
        load().lSet(key, index, val);
    }

    /**
     * 业务作用：按下标覆盖列表元素。
     * 下标越界时报错。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param index 下标
     * @param val 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void lSetAsync(Object key, long index, Object val) {
        load().lSetAsync(key, index, val);
    }

    /**
     * 业务作用：把列表裁剪到指定下标区间。
     * 区间之外的元素被永久删除，不可撤销。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param start 起始下标
     * @param end 结束下标
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void lTrim(Object key, long start, long end) {
        load().lTrim(key, start, end);
    }

    /**
     * 业务作用：把列表裁剪到指定下标区间。
     * 区间之外的元素被永久删除，不可撤销。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param start 起始下标
     * @param end 结束下标
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void lTrimAsync(Object key, long start, long end) {
        load().lTrimAsync(key, start, end);
    }

    /**
     * 业务作用：从列表尾部插入元素。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void rPush(Object key, Object val) {
        load().rPush(key, val);
    }

    /**
     * 业务作用：从列表尾部插入元素。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void rPushAsync(Object key, Object val) {
        load().rPushAsync(key, val);
    }

    /**
     * 业务作用：仅在列表已存在时从尾部插入。
     * 列表不存在时不创建。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void rPushIfAbsent(Object key, Object val) {
        load().rPushIfAbsent(key, val);
    }

    /**
     * 业务作用：仅在列表已存在时从尾部插入。
     * 列表不存在时不创建。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void rPushIfAbsentAsync(Object key, Object val) {
        load().rPushIfAbsentAsync(key, val);
    }

    /**
     * 业务作用：向集合添加成员。
     * 已存在的成员不会重复加入。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void sAdd(Object key, Object val) {
        load().sAdd(key, val);
    }

    /**
     * 业务作用：向集合添加成员。
     * 已存在的成员不会重复加入。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void sAddAsync(Object key, Object val) {
        load().sAddAsync(key, val);
    }

    /**
     * 业务作用：从集合移除成员。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void sRem(Object key, Object val) {
        load().sRem(key, val);
    }

    /**
     * 业务作用：从集合移除成员。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void sRemAsync(Object key, Object val) {
        load().sRemAsync(key, val);
    }

    /**
     * 业务作用：向有序集合添加成员并设定分值。
     * 成员已存在时更新其分值。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param score 有序集合分值
     * @param val 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void zAdd(Object key, double score, Object val) {
        load().zAdd(key, score, val);
    }

    /**
     * 业务作用：向有序集合添加成员并设定分值。
     * 成员已存在时更新其分值。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param score 有序集合分值
     * @param val 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void zAddAsync(Object key, double score, Object val) {
        load().zAddAsync(key, score, val);
    }

    /**
     * 业务作用：向有序集合添加成员并设定分值。
     * 成员已存在时更新其分值。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param scoreAndVals 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void zAdd(Object key, Object... scoreAndVals) {
        load().zAdd(key, scoreAndVals);
    }

    /**
     * 业务作用：向有序集合添加成员并设定分值。
     * 成员已存在时更新其分值。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param scoreAndVals 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void zAddAsync(Object key, Object... scoreAndVals) {
        load().zAddAsync(key, scoreAndVals);
    }

    /**
     * 业务作用：从有序集合移除成员。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void zRem(Object key, Object val) {
        load().zRem(key, val);
    }

    /**
     * 业务作用：从有序集合移除成员。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param val 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void zRemAsync(Object key, Object val) {
        load().zRemAsync(key, val);
    }

    /**
     * 业务作用：按字典序区间批量移除成员。
     * 仅当集合内所有成员分值相同时结果才有意义。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param min 区间下界
     * @param max 区间上界
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void zRemRangeByLex(Object key, String min, String max) {
        load().zRemRangeByLex(key, min, max);
    }

    /**
     * 业务作用：按字典序区间批量移除成员。
     * 仅当集合内所有成员分值相同时结果才有意义。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param min 区间下界
     * @param max 区间上界
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void zRemRangeByLexAsync(Object key, String min, String max) {
        load().zRemRangeByLexAsync(key, min, max);
    }

    /**
     * 业务作用：按排名区间批量移除成员。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param start 起始下标
     * @param end 结束下标
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void zRemRange(Object key, long start, long end) {
        load().zRemRange(key, start, end);
    }

    /**
     * 业务作用：按排名区间批量移除成员。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param start 起始下标
     * @param end 结束下标
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void zRemRangeAsync(Object key, long start, long end) {
        load().zRemRangeAsync(key, start, end);
    }

    /**
     * 业务作用：按分值区间批量移除成员。
     * 常用于按时间戳分值裁剪过期数据。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param min 区间下界
     * @param max 区间上界
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void zRemRangeByScore(Object key, double min, double max) {
        load().zRemRangeByScore(key, min, max);
    }

    /**
     * 业务作用：按分值区间批量移除成员。
     * 常用于按时间戳分值裁剪过期数据。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param min 区间下界
     * @param max 区间上界
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void zRemRangeByScoreAsync(Object key, double min, double max) {
        load().zRemRangeByScoreAsync(key, min, max);
    }

    /**
     * 业务作用：按分值区间批量移除成员。
     * 常用于按时间戳分值裁剪过期数据。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param rang 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void zRemRangeByScore(Object key, Range<Double> rang) {
        load().zRemRangeByScore(key, rang);
    }

    /**
     * 业务作用：按分值区间批量移除成员。
     * 常用于按时间戳分值裁剪过期数据。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param rang 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void zRemRangeByScoreAsync(Object key, Range<Double> rang) {
        load().zRemRangeByScoreAsync(key, rang);
    }

    /**
     * 业务作用：按增量原子调整成员分值。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param score 有序集合分值
     * @param value 待写入的值
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static <V> void zIncrBy(Object key, double score, V value) {
        load().zIncrBy(key, score, value);
    }

    /**
     * 业务作用：按增量原子调整成员分值。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param key 缓存键
     * @param score 有序集合分值
     * @param value 待写入的值
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static <V> void zIncrByAsync(Object key, double score, V value) {
        load().zIncrByAsync(key, score, value);
    }

    /**
     * 业务作用：清空服务端的 Lua 脚本缓存。
     * 此后所有按摘要执行都会失败，直至脚本重新载入。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void scriptFlush() {
        load().scriptFlush();
    }

    /**
     * 业务作用：清空服务端的 Lua 脚本缓存。
     * 此后所有按摘要执行都会失败，直至脚本重新载入。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void scriptFlushAsync() {
        load().scriptFlushAsync();
    }

    /**
     * 业务作用：终止正在执行的 Lua 脚本。
     * 已产生写入的脚本无法被终止，只能等待其完成或重启实例。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void scriptKill() {
        load().scriptKill();
    }

    /**
     * 业务作用：终止正在执行的 Lua 脚本。
     * 已产生写入的脚本无法被终止，只能等待其完成或重启实例。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void scriptKillAsync() {
        load().scriptKillAsync();
    }

    /**
     * 业务作用：执行 Lua 脚本。
     * 脚本在服务端原子执行，期间不会有其它命令插入，是实现复合原子操作的手段。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param script Lua 脚本
     * @param keys 缓存键集合
     * @param args 脚本参数
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void eval(String script, String[] keys, Object... args) {
        load().eval(script, keys, args);
    }

    /**
     * 业务作用：执行 Lua 脚本。
     * 脚本在服务端原子执行，期间不会有其它命令插入，是实现复合原子操作的手段。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param script Lua 脚本
     * @param keys 缓存键集合
     * @param args 脚本参数
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void evalAsync(String script, String[] keys, Object... args) {
        load().evalAsync(script, keys, args);
    }

    /**
     * 业务作用：向 Stream 发布一条业务事件。
     * 事件由订阅方按消费组读取；Stream 需配合长度裁剪，否则无限增长。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param channel 频道名
     * @param message 消息体
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void pub(String channel, Object message) {
        load().pub(channel, message);
    }

    /**
     * 业务作用：向 Stream 发布一条业务事件。
     * 事件由订阅方按消费组读取；Stream 需配合长度裁剪，否则无限增长。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param channel 频道名
     * @param message 消息体
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void pubAsync(String channel, Object message) {
        load().pubAsync(channel, message);
    }

    /**
     * 业务作用：向频道发布消息。
     * 订阅是即时的，发布时没有订阅者则消息直接丢弃，不做任何保留。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param stream Stream 键
     * @param message 消息体
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void publish(Object stream, Object message) {
        load().publish(stream, message);
    }

    /**
     * 业务作用：向频道发布消息。
     * 订阅是即时的，发布时没有订阅者则消息直接丢弃，不做任何保留。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param stream Stream 键
     * @param message 消息体
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void publishAsync(Object stream, Object message) {
        load().publishAsync(stream, message);
    }

    /**
     * 业务作用：向频道发布消息。
     * 订阅是即时的，发布时没有订阅者则消息直接丢弃，不做任何保留。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param stream Stream 键
     * @param event 事件名
     * @param message 消息体
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void publish(Object stream, Object event, Object message) {
        load().publish(stream, event, message);
    }

    /**
     * 业务作用：向频道发布消息。
     * 订阅是即时的，发布时没有订阅者则消息直接丢弃，不做任何保留。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param stream Stream 键
     * @param event 事件名
     * @param message 消息体
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void publishAsync(Object stream, Object event, Object message) {
        load().publishAsync(stream, event, message);
    }

    // ---- 分区发布 (默认 Actuator) ----

    /**
     * 业务作用：按分区键把事件发布到对应的分区 Stream。
     * 同一分区键恒落到同一分区，从而保证该键的事件被同一个消费者按序处理。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param topic 主题名
     * @param partitionKey 分区键，决定落到哪个分区
     * @param data 业务数据
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void partition(String topic, long partitionKey, Object data) {
        load().partition(topic, partitionKey, data);
    }

    /**
     * 业务作用：按分区键把事件发布到对应的分区 Stream。
     * 同一分区键恒落到同一分区，从而保证该键的事件被同一个消费者按序处理。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param topic 主题名
     * @param partitionKey 分区键，决定落到哪个分区
     * @param data 业务数据
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void partition(String topic, String partitionKey, Object data) {
        load().partition(topic, partitionKey, data);
    }

    /**
     * 业务作用：按分区键把事件发布到对应的分区 Stream。
     * 同一分区键恒落到同一分区，从而保证该键的事件被同一个消费者按序处理。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param topic 主题名
     * @param event 事件名
     * @param partitionKey 分区键，决定落到哪个分区
     * @param data 业务数据
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void partition(String topic, String event, long partitionKey, Object data) {
        load().partition(topic, event, partitionKey, data);
    }

    /**
     * 业务作用：按分区键把事件发布到对应的分区 Stream。
     * 同一分区键恒落到同一分区，从而保证该键的事件被同一个消费者按序处理。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param topic 主题名
     * @param event 事件名
     * @param partitionKey 分区键，决定落到哪个分区
     * @param data 业务数据
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void partition(String topic, String event, String partitionKey, Object data) {
        load().partition(topic, event, partitionKey, data);
    }

    /**
     * 业务作用：按分区键把事件发布到对应的分区 Stream。
     * 同一分区键恒落到同一分区，从而保证该键的事件被同一个消费者按序处理。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param topic 主题名
     * @param partitionKey 分区键，决定落到哪个分区
     * @param data 业务数据
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void partitionAsync(String topic, long partitionKey, Object data) {
        load().partitionAsync(topic, partitionKey, data);
    }

    /**
     * 业务作用：按分区键把事件发布到对应的分区 Stream。
     * 同一分区键恒落到同一分区，从而保证该键的事件被同一个消费者按序处理。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param topic 主题名
     * @param partitionKey 分区键，决定落到哪个分区
     * @param data 业务数据
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void partitionAsync(String topic, String partitionKey, Object data) {
        load().partitionAsync(topic, partitionKey, data);
    }

    /**
     * 业务作用：按分区键把事件发布到对应的分区 Stream。
     * 同一分区键恒落到同一分区，从而保证该键的事件被同一个消费者按序处理。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param topic 主题名
     * @param event 事件名
     * @param partitionKey 分区键，决定落到哪个分区
     * @param data 业务数据
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void partitionAsync(String topic, String event, long partitionKey, Object data) {
        load().partitionAsync(topic, event, partitionKey, data);
    }

    /**
     * 业务作用：按分区键把事件发布到对应的分区 Stream。
     * 同一分区键恒落到同一分区，从而保证该键的事件被同一个消费者按序处理。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param topic 主题名
     * @param event 事件名
     * @param partitionKey 分区键，决定落到哪个分区
     * @param data 业务数据
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void partitionAsync(String topic, String event, String partitionKey, Object data) {
        load().partitionAsync(topic, event, partitionKey, data);
    }

    /**
     * 业务作用：向 Stream 追加一条消息。
     * 追加后由消费组按各自位点读取；Stream 需配合长度裁剪，否则会无限增长。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param stream Stream 键
     * @param message 消息体
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void xAdd(Object stream, Object message) {
        load().xAdd(stream, message);
    }

    /**
     * 业务作用：向 Stream 追加一条消息。
     * 追加后由消费组按各自位点读取；Stream 需配合长度裁剪，否则会无限增长。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param stream Stream 键
     * @param message 消息体
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void xAddAsync(Object stream, Object message) {
        load().xAddAsync(stream, message);
    }

    /**
     * 业务作用：向 Stream 追加一条消息。
     * 追加后由消费组按各自位点读取；Stream 需配合长度裁剪，否则会无限增长。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param stream Stream 键
     * @param field 哈希字段名
     * @param message 消息体
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void xAdd(Object stream, Object field, Object message) {
        load().xAdd(stream, field, message);
    }

    /**
     * 业务作用：向 Stream 追加一条消息。
     * 追加后由消费组按各自位点读取；Stream 需配合长度裁剪，否则会无限增长。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param stream Stream 键
     * @param field 哈希字段名
     * @param message 消息体
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void xAddAsync(Object stream, Object field, Object message) {
        load().xAddAsync(stream, field, message);
    }

    /**
     * 业务作用：向 Stream 追加一条消息。
     * 追加后由消费组按各自位点读取；Stream 需配合长度裁剪，否则会无限增长。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param stream Stream 键
     * @param fieldAndMessages 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void xAdd(Object stream, Object... fieldAndMessages) {
        load().xAdd(stream, fieldAndMessages);
    }

    /**
     * 业务作用：向 Stream 追加一条消息。
     * 追加后由消费组按各自位点读取；Stream 需配合长度裁剪，否则会无限增长。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param stream Stream 键
     * @param fieldAndMessages 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void xAddAsync(Object stream, Object... fieldAndMessages) {
        load().xAddAsync(stream, fieldAndMessages);
    }

    /**
     * 业务作用：按最大条数裁剪 Stream。
     * 被裁掉的消息永久丢失，包括尚未被消费的。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param stream Stream 键
     * @param maxlen 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void xTrimMaxlen(Object stream, long maxlen) {
        load().xTrimMaxlen(stream, maxlen);
    }

    /**
     * 业务作用：按最大条数裁剪 Stream。
     * 被裁掉的消息永久丢失，包括尚未被消费的。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param stream Stream 键
     * @param maxlen 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void xTrimMaxlenAsync(Object stream, long maxlen) {
        load().xTrimMaxlenAsync(stream, maxlen);
    }

    /**
     * 业务作用：按最小条目标识裁剪 Stream。
     * 早于该标识的消息永久丢失。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param stream Stream 键
     * @param millis 毫秒数
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void xTrimMinId(Object stream, long millis) {
        load().xTrimMinId(stream, millis);
    }

    /**
     * 业务作用：按最小条目标识裁剪 Stream。
     * 早于该标识的消息永久丢失。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param stream Stream 键
     * @param millis 毫秒数
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void xTrimMinIdAsync(Object stream, long millis) {
        load().xTrimMinIdAsync(stream, millis);
    }

    /**
     * 业务作用：按最小条目标识裁剪 Stream。
     * 早于该标识的消息永久丢失。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param stream Stream 键
     * @param minId 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void xTrimMinId(Object stream, String minId) {
        load().xTrimMinId(stream, minId);
    }

    /**
     * 业务作用：按最小条目标识裁剪 Stream。
     * 早于该标识的消息永久丢失。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param stream Stream 键
     * @param minId 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void xTrimMinIdAsync(Object stream, String minId) {
        load().xTrimMinIdAsync(stream, minId);
    }

    /**
     * 业务作用：按条目标识删除 Stream 消息。
     * 已被消费组读取但未确认的条目删除后不会再投递。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param stream Stream 键
     * @param messageId 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void xDel(Object stream, String messageId) {
        load().xDel(stream, messageId);
    }

    /**
     * 业务作用：按条目标识删除 Stream 消息。
     * 已被消费组读取但未确认的条目删除后不会再投递。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param stream Stream 键
     * @param messageId 见方法语义
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void xDelAsync(Object stream, String messageId) {
        load().xDelAsync(stream, messageId);
    }

    /**
     * 业务作用：确认消费组已处理某条消息。
     * 不确认的消息会留在待处理列表中，被空闲接管机制重新投递。
     * <p>
     * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
     * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param stream Stream 键
     * @param group 消费组名
     * @param ids 条目标识集合
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void ack(Object stream, Object group, String... ids) {
        load().ack(stream, group, ids);
    }

    /**
     * 业务作用：确认消费组已处理某条消息。
     * 不确认的消息会留在待处理列表中，被空闲接管机制重新投递。
     * <p>
     * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
     * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
     * <p>
     * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
     *
     * @param stream Stream 键
     * @param group 消费组名
     * @param ids 条目标识集合
     * 返回: 无返回值；命令的执行结果不回传给调用方。
     */
    public static void ackAsync(Object stream, Object group, String... ids) {
        load().ackAsync(stream, group, ids);
    }

    /**
     * 业务作用：pipeline 批量执行 — 接收 funcs (读命令 Function 列表), 一次 flushCommands + awaitAll, 返回 future 列表.
     * 与 LettucePipeline 的 pipeline(funcs, consumers) 对应, 写命令 Consumer 路径已被 CmdBuffer 替代,
     * 业务方有自定义读取需求时仍走 funcs.
     *
     * @param funcs 见上述说明
     */
    public static RecycleLinkedMap<Function<RedisClusterAsyncCommands<byte[], byte[]>, RedisFuture<?>>, RedisFuture<?>> pipeline(
            List<Function<RedisClusterAsyncCommands<byte[], byte[]>, RedisFuture<?>>> funcs) {
        return load().pipeline(funcs);
    }

    /**
     * 业务作用：pipeline 批量 get — funcs 拿 future, 然后用 valueConverter 反序列化, 返回结果列表
     *
     * @param map 见上述说明
     */
    public static void pipeline(LinkedHashMap<Function<RedisClusterAsyncCommands<byte[], byte[]>, RedisFuture<?>>, Consumer<Object>> map) {
        load().pipeline(map);
    }

    // ==================== CmdBuffer: 写命令的平坦缓冲区 ====================

    /**
     * 池化的写命令缓冲区 — 跨线程共享, 借/还语义.
     * <p>
     * 与 {@link RecycleLinkedList} / {@link RecycleLinkedMap} 同族:
     * <ul>
     *   <li>{@link #of()} 从 {@link #POOL} 借出 (空池则 new)</li>
     *   <li>{@link #recycle()} 还池 (内部先调 {@link #restore()} 清状态)</li>
     *   <li>容量 {@code nasa.object-pool.cmd-buffer-capacity} 默认 64 (单个 CmdBuffer 32KB+, 64 个 ≈ 2MB pool 上限)</li>
     * </ul>
     * <p>
     * 与 ThreadLocal 配合: 一次 pipeline session 内 ThreadLocal 缓存 borrowed buf, session 结束 recycle + remove.
     * 平台线程 / 虚拟线程行为统一, 不再有"虚拟线程每次 new 32KB"的 GC 压力.
     */
    static final class CmdBuffer implements ObjectPool.Recycler<CmdBuffer> {

        static final ObjectPool<CmdBuffer> POOL = new ObjectPool<>(
                ContextUtils.getPropertyInt("nasa.object-pool.cmd-buffer-capacity", 200)) {
            /**
             * 业务作用：池空时新建一个命令缓冲，由对象池在借不到空闲实例时调用。
             *
             * <p>参数说明: 无。
             *
             * @return 新建的命令缓冲。
             */
            @Override
            public CmdBuffer newObject() {
                // 容量取当前 pipelineLength. 若 pipelineLength 改大, 池里旧 buf 会通过 ensureCapacity 自动扩容,
                // 池稳态会逐渐替换为大 buf
                return new CmdBuffer(PIPELINE_LENGTH);
            }
        };

        private final ObjectPool.PooledHandle<CmdBuffer> handle = new ObjectPool.PooledHandle<>(POOL);

        /**
         * 业务作用：从池借出 (空池则 new). 用完必须调 {@link #recycle} 归还
         *
         * @return 见上述说明。
         */
        public static CmdBuffer of() {
            return POOL.get();
        }

        /**
         * 业务作用：暴露本缓冲的池化句柄，供对象池完成借出与归还的状态跟踪。
         *
         * <p>参数说明: 无。
         *
         * @return 池化句柄。
         */
        @Override
        public ObjectPool.PooledHandle<CmdBuffer> handle() {
            return this.handle;
        }

        /**
         * 业务作用：还池前的状态清理 (Recycler 协议要求): 清 arg1/arg2/arg3/extras 引用 (释放 byte[]/String
         * 和 Map/byte[][] 等大对象, 避免持有 → 内存泄漏); count=0; primitive 数组 (longArg/longArg2/ops)
         * 不必清, 下次 enqueue 会覆盖.
         * <p>
         * extras 中若是 {@link RecycleLinkedMap} (OP_XADD_MULTI 的 field-message map), 一并 recycle 还池;
         * dispatch 后 lettuce 已把 byte[] 引用复制到 CommandArgs, map 不再被引用, 可安全回收.
         */
        @Override
        public void restore() {
            for (int i = 0; i < count; i++) {
                arg1[i] = null;
                arg2[i] = null;
                arg3[i] = null;
                Object e = extras[i];
                if (e instanceof RecycleLinkedMap rm) rm.recycle();
                extras[i] = null;
                // lf 在 dispatch 阶段已被 complete (业务线程在 getFinally 中等待递归展开), 这里只清引用避免持有
                lfs[i] = null;
            }
            count = 0;
        }

        /**
         * 操作码, 对应 OP_HSET / OP_HDEL / OP_XADD 等常量.
         * <p>
         * 用 short 是因为 OP 码超过 127 (byte 上限) — RedisProxy 读命令 + 多值变体合计 ~150 个 op,
         * 不能用 byte 装. short 高 2 字节空着, 用 {@link #SYNC_BIT} = 0x4000 (位 14) 标记 sync, 低 14 位 = 真 OP (0-16383).
         */
        short[] ops;
        /**
         * 第 1 个参数: 永远是 byte[] redis key. 保持 byte[][] 类型安全, dispatch 无需 cast.
         */
        byte[][] arg1;
        /**
         * 第 2 个参数: byte[] (hash field / stream event 等) 或 String (XDEL messageId / XTRIM minId).
         * <p>
         * 用 Object[] 是因为 OP_XDEL/OP_XTRIM_MINID 这种 lettuce API 直接收 String 的命令,
         * 可避免 byte[]↔String 来回转换. dispatch 时按 op 强转 (byte[]) 或 (String).
         */
        Object[] arg2;
        /**
         * 第 3 个参数: byte[] (value) 或 String (XACK id). 见 {@link #arg2} 注释.
         */
        Object[] arg3;
        /**
         * 第一数值参数：delta（HINCRBY）/ millis（EXPIRE）/ score 的 long bits（ZADD）/ start（LTRIM）
         */
        long[] longArg;
        /**
         * 第二数值参数：供 LTRIM/ZREMRANGE/ZREMRANGEBYSCORE 等（start, end）双数值命令使用
         */
        long[] longArg2;
        /**
         * 罕用复杂参数 (Map / byte[][] / Object[]): 大部分命令此槽为 null, 不分配.
         * <p>
         * 用法:
         * <ul>
         *   <li>OP_XADD_MULTI: 存 {@code Map<byte[], byte[]>} 多 (field, message)</li>
         *   <li>OP_EVAL_ASYNC: 存 {@code Object[]{byte[] script, byte[][] keys, byte[][] args}}</li>
         * </ul>
         * 这两条命令都是冷路径 (eval / 多 field xadd 极少出现在撮合热路径), 用 extras 槽接住,
         * 反正它们的语义本身就需要变长 byte[][] 参数, 不能避免一次中间对象分配.
         */
        Object[] extras;
        /**
         * 返回值占位 future: 读命令通过此槽把 dispatchOne 返回的 RedisFuture 回填给业务线程的 LettuceFuture.
         * <p>
         * 写命令 (fire-and-forget) 此槽为 null, sync 写命令走 SYNC_BIT + syncFutures 列表, 互斥关系.
         */
        LettuceFuture<?>[] lfs;
        /**
         * 当前已入队的命令数
         */
        int count;

        /**
         * 业务作用：按给定容量预分配各条并行数组。
         * 预分配到批次长度上限，使稳态下入队不触发扩容——扩容要复制全部数组，发生在热路径上代价明显。
         *
         * @param capacity 初始槽位数
         */
        CmdBuffer(int capacity) {
            this.ops = new short[capacity];
            this.arg1 = new byte[capacity][];
            this.arg2 = new Object[capacity];
            this.arg3 = new Object[capacity];
            this.longArg = new long[capacity];
            this.longArg2 = new long[capacity];
            this.extras = new Object[capacity];
            this.lfs = new LettuceFuture[capacity];
        }

        /**
         * 业务作用：容量不足时 doubling 扩容, Arrays.copyOf 保留已有数据
         */
        void ensureCapacity() {
            if (count < ops.length) return;
            int newCap = ops.length << 1;
            ops = Arrays.copyOf(ops, newCap);
            arg1 = Arrays.copyOf(arg1, newCap);
            arg2 = Arrays.copyOf(arg2, newCap);
            arg3 = Arrays.copyOf(arg3, newCap);
            longArg = Arrays.copyOf(longArg, newCap);
            longArg2 = Arrays.copyOf(longArg2, newCap);
            extras = Arrays.copyOf(extras, newCap);
            lfs = Arrays.copyOf(lfs, newCap);
        }
    }

    // ==================== Actuator: pipeline 执行器 ====================

    /**
     * 执行器. 自身无可变实例状态 (序列化器 + throwaway 标记都是 final), 所有 session 可变状态通过实例级 ThreadLocal 隔离,
     * 各线程独立操作, 零锁零竞争.
     * <p>
     * 两种来源: ① {@link #CACHE} 单例 — 每个 RedisProxy 一个, 全局共享长存 ({@link #load}); ② throwaway 临时实例
     * ({@link #openIsolated}) — 用完即弃. 因 ThreadLocal 是<b>实例级</b>字段, 两者状态互不干扰 (这正是 openIsolated 能隔离外层 session 的基础).
     */
    public static class Actuator {

        // ---- pipeline 控制状态 (ThreadLocal, 语义与 LettucePipeline.Actuator 完全一致) ----

        /**
         * pipeline 是否已开启
         */
        private final ThreadLocal<Boolean> open = new ThreadLocal<>();
        /**
         * 执行标识: 嵌套场景下只有匹配的 tag 才触发 flush
         */
        private final ThreadLocal<Object> execTag = new ThreadLocal<>();
        /**
         * pipeline 执行成功后的回调
         */
        private final ThreadLocal<RecycleLinkedList<Action>> pipelineSuccessActions = new ThreadLocal<>();

        /**
         * 写命令缓冲区: 平坦数组, 每条命令 = 数组下标赋值, 零对象分配
         */
        private final ThreadLocal<CmdBuffer> CMD = new ThreadLocal<>();

        /**
         * 读命令: 需要等 future 拿结果, 保留 lambda (读操作在热路径中极少)
         */
        private final ThreadLocal<RecycleLinkedList<Function<RedisClusterAsyncCommands<byte[], byte[]>, RedisFuture<?>>>> LF = new ThreadLocal<>();

        // ---- redis 序列化器 (构造时初始化, 不可变) ----
        private final RedisProxy redisProxy;
        private final RedisSerializer<String> keySerializer;
        private final RedisSerializer<Object> valueSerializer;
        private final RedisSerializer<String> hashKeySerializer;
        private final RedisSerializer<Object> hashValueSerializer;

        /**
         * 用完即弃 (由 {@link LettucePipeline#openIsolated} 创建): 非 CACHE 单例, 一次 session 后丢弃待 GC.
         * <p>
         * 影响 {@link #clear}: throwaway 实例即使在平台线程也走全 {@code remove()} 分支 —
         * 单例 Actuator 才需要 "平台线程保留 LF/actions 容器复用" 的优化; throwaway 不会被复用,
         * 保留容器没意义, 反而在线程 ThreadLocalMap 留下以已弃 ThreadLocal 实例为弱键的残条目 (待 GC 才清).
         */
        private final boolean throwaway;

        /**
         * 业务作用：建出与某个命令代理绑定的批次执行器，作为长期复用的单例。
         *
         * @param redisProxy 命令代理
         */
        Actuator(RedisProxy redisProxy) {
            this(redisProxy, false);
        }

        /**
         * 业务作用：建出批次执行器，并标明它是否为用完即弃的临时实例。
         * 临时实例在收尾时走全量清理，不在线程本地表里留残条目——它不会被复用，留着只是负担。
         *
         * @param redisProxy 命令代理
         * @param throwaway  true 表示用完即弃的临时实例
         */
        Actuator(RedisProxy redisProxy, boolean throwaway) {
            this.redisProxy = redisProxy;
            this.keySerializer = (RedisSerializer<String>) redisProxy.getKeySerializer();
            this.valueSerializer = (RedisSerializer<Object>) redisProxy.getValueSerializer();
            this.hashKeySerializer = (RedisSerializer<String>) redisProxy.getHashKeySerializer();
            this.hashValueSerializer = (RedisSerializer<Object>) redisProxy.getHashValueSerializer();
            this.throwaway = throwaway;
        }

        // ---- 序列化工具: 外部可调用, 提前序列化 key 供后续 byte[] 直传 ----

        /**
         * 业务作用：按配置的序列化方式把键转成字节。
         *
         * @param key 待序列化的键
         * @return 序列化后的字节。
         */
        public byte[] serializeKey(String key) {
            return this.keySerializer.serialize(key);
        }

        /**
         * 业务作用：按配置的序列化方式把值转成字节。
         *
         * @param value 待序列化的值
         * @return 序列化后的字节。
         */
        public byte[] serializeValue(Object value) {
            return this.valueSerializer.serialize(value);
        }

        /**
         * 业务作用：按配置的序列化方式把哈希字段名转成字节。
         *
         * @param hashKey 待序列化的哈希字段名
         * @return 序列化后的字节。
         */
        public byte[] serializeHKey(String hashKey) {
            return this.hashKeySerializer.serialize(hashKey);
        }

        /**
         * 业务作用：按配置的序列化方式把哈希字段值转成字节。
         *
         * @param value 待序列化的哈希字段值
         * @return 序列化后的字节。
         */
        public byte[] serializeHValue(Object value) {
            return this.hashValueSerializer.serialize(value);
        }

        // ==================== 内部序列化: Object 兼容 byte[] 直传 ====================

        /**
         * 业务作用：key 序列化: 如果已是 byte[] 直接返回 (预序列化场景), 否则走 keySerializer.
         * 所有写命令入队前统一走这 4 个方法, 保证入 CmdBuffer 的都是 byte[].
         *
         * @param key 缓存键
         */
        private byte[] serKey(Object key) {
            return key instanceof byte[] bs ? bs : this.serializeKey((String) key);
        }

        /**
         * 业务作用：序列化值的内部快捷入口，供各命令封装复用。
         *
         * @param val 待序列化的值
         * @return 序列化后的字节。
         */
        private byte[] serVal(Object val) {
            return val instanceof byte[] bs ? bs : this.serializeValue(val);
        }

        /**
         * 业务作用：序列化哈希字段名的内部快捷入口，供各命令封装复用。
         *
         * @param key 待序列化的哈希字段名
         * @return 序列化后的字节。
         */
        private byte[] serHKey(Object key) {
            return key instanceof byte[] bs ? bs : this.serializeHKey((String) key);
        }

        /**
         * 业务作用：序列化哈希字段值的内部快捷入口，供各命令封装复用。
         *
         * @param val 待序列化的哈希字段值
         * @return 序列化后的字节。
         */
        private byte[] serHVal(Object val) {
            return val instanceof byte[] bs ? bs : this.serializeHValue(val);
        }

        // ==================== pipeline 控制 ====================

        /**
         * 业务作用：判断当前线程是否<b>未</b>开启批次，据此决定命令是缓冲还是立即发出。
         *
         * <p>参数说明: 无。
         *
         * @return 未开启批次返回 true。
         */
        private boolean nonOpen() {
            Boolean start = open.get();
            return start == null || !start;
        }

        /**
         * 业务作用：拿当前 session 的 CmdBuffer. ThreadLocal 缓存 borrowed buf, session 结束 ({@link #clear})
         * 一并 recycle 回池. 同 session 内反复调用返回同一个 buf, 跨 session 可能拿到同一个或不同 buf
         * (取决于池的回收/借出顺序).
         */
        private CmdBuffer getCmdBuffer() {
            CmdBuffer buf = CMD.get();
            if (buf == null) {
                buf = CmdBuffer.of();   // 从池借出
                CMD.set(buf);
            }
            return buf;
        }

        /**
         * 业务作用：当前已缓冲命令总数 = 写命令 (CmdBuffer.count) + 读命令 (LF.size()).
         * 替代原 RingInteger 计数器, 入队后判断是否触发 {@link #pipelineAutoFlush}.
         *
         * @return 见上述说明。
         */
        private int totalCount() {
            CmdBuffer buf = CMD.get();
            int n = buf == null ? 0 : buf.count;
            var lf = LF.get();
            if (lf != null) n += lf.size();
            return n;
        }

        /**
         * 业务作用：不带参数 open: 等价 open(null), 任何地方 pipeline() 都能 flush
         *
         * @return 见上述说明。
         */
        public Actuator open() {
            return open(null);
        }

        /**
         * 业务作用：开启 pipeline 并设置执行标识.
         * <p>
         * execTag=null: 任何地方都可以调 pipeline() 触发 flush.
         * execTag!=null: 只有 pipeline(sameTag) 才触发 flush, 嵌套时内层 tag 不覆盖外层.
         *
         * @param execTag 批次执行标识，只有匹配的收尾调用才会真正发出批次
         * @return 见上述说明。
         */
        public Actuator open(Object execTag) {
            this.open.set(true);
            if (execTag == null) return this;
            if (this.execTag.get() == null) {
                this.execTag.set(execTag);
            }
            return this;
        }

        // ==================== 读 / 复杂写命令入队 (lambda 路径) ====================

        /**
         * 业务作用：读命令入队 (hGet / get / zRange 等需要拿结果的操作).
         * <p>
         * 仍用 Function lambda, 因为 caller 需要持有 RedisFuture 做后续处理 (反序列化等),
         * 无法用平坦数组表达. 读操作在热路径中极少, 不影响性能目标.
         *
         * @param func 取回结果的命令函数
         */
        public void add(Function<RedisClusterAsyncCommands<byte[], byte[]>, RedisFuture<?>> func) {
            if (func == null) return;
            if (nonOpen()) {
                // 未开启 pipeline 直接执行, 单条 lambda 立即 flush 然后 await
                RecycleLinkedList<Function<RedisClusterAsyncCommands<byte[], byte[]>, RedisFuture<?>>> funcs = RecycleLinkedList.of();
                funcs.add(func);
                // pipeline(funcs) 返回池化 futures map, 本路径不需要结果, 必须 recycle 归池 (否则破坏零 GC)
                RecycleLinkedMap<Function<RedisClusterAsyncCommands<byte[], byte[]>, RedisFuture<?>>, RedisFuture<?>> futures = null;
                try {
                    futures = this.pipeline(funcs);
                } finally {
                    if (futures != null) futures.recycle();
                    funcs.recycle();
                }
                return;
            }
            RecycleLinkedList<Function<RedisClusterAsyncCommands<byte[], byte[]>, RedisFuture<?>>> list = LF.get();
            if (list == null) {
                list = RecycleLinkedList.of();
                LF.set(list);
            }
            list.add(func);
            if (totalCount() >= PIPELINE_LENGTH) this.pipelineAutoFlush();
        }

        /**
         * 业务作用：登记一个在批次<b>成功发出之后</b>执行的动作。
         * 用于「命令确实写进 Redis 之后才该做」的后续处理（如发通知、改本地状态）——
         * 在发出前做会让批次失败时留下已经生效的副作用。
         *
         * @param action 批次成功后执行的动作
         * 返回: 无返回值。
         */
        public void afterPipelineSuccess(Action action) {
            RecycleLinkedList<Action> actions = this.pipelineSuccessActions.get();
            if (actions == null) {
                this.pipelineSuccessActions.set(actions = RecycleLinkedList.of());
            }
            actions.add(action);
        }

        // ==================== 写命令入队: 统一走 enqueue, 纯数组赋值 ====================

        /**
         * sync/async 标记位: ops short 的位 14 (0x4000). 低 14 位 = 真 OP (0-16383, 足够).
         * 用 short 是因为 OP 码超过 127 (byte 上限) — 升级到 short 后位高 2 字节留 sync + 扩展位.
         * dispatch 时 mask 出真 OP, sync 标记 = (rawOp &amp; SYNC_BIT) != 0.
         * 这样 sync 也走 CmdBuffer 零 lambda 分配, 只是 dispatch 时把 RedisFuture 收进 sync 列表 awaitAll.
         */
        private static final short SYNC_BIT = (short) 0x4000;

        /**
         * 业务作用：async 入队 (5 参便捷): fire-and-forget, dispatch 后丢弃 future.
         *
         * @param op 见上述说明
         * @param a1 见上述说明
         * @param a2 见上述说明
         * @param a3 见上述说明
         * @param l  见上述说明
         */
        private void enqueueAsync(short op, byte[] a1, Object a2, Object a3, long l) {
            this.enqueueImpl(false, op, a1, a2, a3, l, 0L, null);
        }

        /**
         * 业务作用：async 入队 (7 参完整): 支持 longArg2 + extras.
         *
         * @param op    见上述说明
         * @param a1    见上述说明
         * @param a2    见上述说明
         * @param a3    见上述说明
         * @param l     见上述说明
         * @param l2    见上述说明
         * @param extra 见上述说明
         */
        private void enqueueAsync(short op, byte[] a1, Object a2, Object a3, long l, long l2, Object extra) {
            this.enqueueImpl(false, op, a1, a2, a3, l, l2, extra);
        }

        /**
         * 业务作用：sync 入队 (5 参便捷): 命令完成后 awaitAll, 但不返回结果.
         * <p>
         * 走 CmdBuffer (零 lambda 分配), ops 高位置位标记 sync, dispatch 时收集 future, doFlush 末尾 awaitAll.
         *
         * @param op 见上述说明
         * @param a1 见上述说明
         * @param a2 见上述说明
         * @param a3 见上述说明
         * @param l  见上述说明
         */
        private void enqueueSync(short op, byte[] a1, Object a2, Object a3, long l) {
            this.enqueueImpl(true, op, a1, a2, a3, l, 0L, null);
        }

        /**
         * 业务作用：sync 入队 (7 参完整): 支持 longArg2 + extras.
         *
         * @param op    见上述说明
         * @param a1    见上述说明
         * @param a2    见上述说明
         * @param a3    见上述说明
         * @param l     见上述说明
         * @param l2    见上述说明
         * @param extra 见上述说明
         */
        private void enqueueSync(short op, byte[] a1, Object a2, Object a3, long l, long l2, Object extra) {
            this.enqueueImpl(true, op, a1, a2, a3, l, l2, extra);
        }

        /**
         * 业务作用：入队核心: nonOpen 直接 doFlushSingle (单条立即执行), 已 open 写 CmdBuffer 槽位.
         * sync 标记编入 ops 高位 (零内存开销).
         *
         * @param sync  是否等待执行结果
         * @param op    见上述说明
         * @param a1    见上述说明
         * @param a2    见上述说明
         * @param a3    见上述说明
         * @param l     见上述说明
         * @param l2    见上述说明
         * @param extra 见上述说明
         */
        private void enqueueImpl(boolean sync, short op, byte[] a1, Object a2, Object a3, long l, long l2, Object extra) {
            // ① 未 open: 不缓冲, 单条 dispatch 立即发送 (nonOpen 走 doFlushSingle, 内部按 sync 决定是否 await)
            if (nonOpen()) {
                this.doFlushSingle(sync, op, a1, a2, a3, l, l2, extra);
                return;
            }
            // ② 取/借 CmdBuffer (ThreadLocal 缓存当前 session, 末尾 clear 时归还池)
            CmdBuffer buf = getCmdBuffer();
            // ③ 容量不足时 doubling 扩容 (Arrays.copyOf), 保持已有数据
            buf.ensureCapacity();
            int i = buf.count++;
            // ④ ops 位 14 = sync 标记: dispatch 时 mask 出真 OP + 收集 sync 槽 future
            buf.ops[i] = sync ? (short) (op | SYNC_BIT) : op;
            // ⑤ 5 个数据槽位赋值, 引用直传, 零中间对象
            buf.arg1[i] = a1;
            buf.arg2[i] = a2;
            buf.arg3[i] = a3;
            buf.longArg[i] = l;
            buf.longArg2[i] = l2;
            buf.extras[i] = extra;
            // ⑥ 命令累积达 pipelineLength 触发 autoFlush (中间 flush, 不关闭 open, 继续缓冲)
            if (totalCount() >= PIPELINE_LENGTH) this.pipelineAutoFlush();
        }

        /**
         * 业务作用：入队带返回值占位 (lf): 读命令通过此入口把 RedisFuture 回填给业务线程的 LettuceFuture.
         * <p>
         * nonOpen 路径: 立即 dispatch 后 lf.complete(rf), 业务线程在 getFinally 内递归展开 RedisFuture.
         * open 路径: 写 CmdBuffer 槽位 (含 lfs[i]=lf), dispatch 阶段统一回填.
         * <p>
         * fire-and-forget 场景传 lf=null 即可, 等价于 {@link #enqueueImpl} async 路径.
         *
         * @param op    见上述说明
         * @param a1    见上述说明
         * @param a2    见上述说明
         * @param a3    见上述说明
         * @param l     见上述说明
         * @param l2    见上述说明
         * @param extra 见上述说明
         * @param lf    见上述说明
         */
        private void enqueueWithFuture(short op, byte[] a1, Object a2, Object a3, long l, long l2, Object extra, LettuceFuture<?> lf) {
            if (this.nonOpen()) {
                this.doFlushSingleWithFuture(op, a1, a2, a3, l, l2, extra, lf);
                return;
            }
            CmdBuffer buf = this.getCmdBuffer();
            buf.ensureCapacity();
            int i = buf.count++;
            buf.ops[i] = op;
            buf.arg1[i] = a1;
            buf.arg2[i] = a2;
            buf.arg3[i] = a3;
            buf.longArg[i] = l;
            buf.longArg2[i] = l2;
            buf.extras[i] = extra;
            buf.lfs[i] = lf;
            if (this.totalCount() >= PIPELINE_LENGTH) this.pipelineAutoFlush();
        }

        /**
         * 业务作用：nonOpen 兜底: 立即单条 dispatch 后回填 lf.
         * <p>
         * lf == null 时退化为 fire-and-forget, 失败挂 exceptionally 打 log; lf != null 时调 lf.complete(rf),
         * 失败的 RedisFuture 会在业务线程 getFinally 递归 get() 时抛出, 由业务方处理.
         *
         * @param op    见上述说明
         * @param a1    见上述说明
         * @param a2    见上述说明
         * @param a3    见上述说明
         * @param l     见上述说明
         * @param l2    见上述说明
         * @param extra 见上述说明
         * @param lf    见上述说明
         */
        private void doFlushSingleWithFuture(short op, byte[] a1, Object a2, Object a3, long l, long l2, Object extra, LettuceFuture<?> lf) {
            PipelineConnectionPool pool = redisProxy.getPipelinePool();
            StatefulConnection<byte[], byte[]> connection = null;
            RedisClusterAsyncCommands<byte[], byte[]> c = null;
            RedisFuture<?> f = null;
            boolean flushed = false;
            try {
                // borrow 放进 try: 失败也走 catch 解阻塞 lf + finally 回收 extra
                connection = pool.borrow();
                c = PipelineConnectionPool.async(connection);
                c.setAutoFlushCommands(false);
                f = this.dispatchOne(c, op, a1, a2, a3, l, l2, extra);
                c.flushCommands();
                flushed = true;
            } catch (Throwable ex) {
                // borrow / dispatchOne / flushCommands 抛异常时, lf 不会被 completeLf 回填, 业务线程会在 getFinally 永久阻塞,
                // 这里先 completeExceptionally 解阻塞再上抛.
                if (lf != null) lf.completeExceptionally(ex);
                throw ex;
            } finally {
                // borrow 成功才还原 autoFlush + 归还连接; borrow 失败 (connection==null) 只回收 extra.
                // flush 成功且 autoFlush 还原成功才回池, 否则 invalidate 关闭 (buffer 可能残留未 flush 命令, 回池会被下个 borrower 串出).
                if (connection != null) {
                    boolean restoreOk = false;
                    try {
                        c.setAutoFlushCommands(true);
                        restoreOk = true;
                    } catch (Exception ignored) {
                    }
                    if (flushed && restoreOk) {
                        pool.release(connection);
                    } else {
                        pool.invalidate(connection);
                    }
                }
                if (extra instanceof RecycleLinkedMap rm) rm.recycle();
            }
            if (f == null) return;
            if (lf != null) {
                this.completeLf(lf, f);
                return;
            }
            attachAsyncErrorLog(f, op, a1, extra);
        }

        /**
         * 业务作用：把单个 PipelineTask 搬入 CmdBuffer.
         * <p>
         * 单消费者调用 (RedisProxy.before TimingWheel 1ms tick), 调用方负责:
         * <ul>
         *   <li>循环 {@code poll()} cqueue 拿 task (不用 iterator+clear, 避免弱一致 race 丢 task)</li>
         *   <li>每个 task 包 try/catch/finally — 异常时 {@code lf.completeExceptionally(ex)} 解阻塞业务线程,
         *       finally 内 {@code t.recycle()} 保证池化对象归还</li>
         * </ul>
         * 本方法只做"字段引用直传到 buf"一件事, 不做异常恢复 — 单条 task 的失败由调用方决定如何处理.
         *
         * @param t 见上述说明
         */
        public void drainTask(PipelineTask t) {
            this.enqueueWithFuture(t.op, t.arg1, t.arg2, t.arg3, t.longArg, t.longArg2, t.extras, t.lf);
        }

        /**
         * 业务作用：按下标区间读取列表元素。
         * 下标支持负数表示从尾部倒数。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param k 见方法语义
         * @param start 起始下标
         * @param end 结束下标
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void lrangeAsync(byte[] k, long start, long end, LettuceFuture<RedisFuture<List<byte[]>>> lf) {
            this.enqueueWithFuture(OP_LRANGE, k, null, null, start, end, null, lf);
        }

        /**
         * 业务作用：批量读取哈希字段。
         * 结果与入参顺序一一对应。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param k 见方法语义
         * @param hashKeys 哈希字段名集合
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void hmgetAsync(byte[] k, byte[][] hashKeys, LettuceFuture<RedisFuture<List<KeyValue<byte[], byte[]>>>> lf) {
            this.enqueueWithFuture(OP_HMGET, k, null, null, 0L, 0L, hashKeys, lf);
        }

        /**
         * 业务作用：把外层 LettuceFuture 包装的 RedisFuture 完成, 通用 raw 类型转换.
         * 业务线程在 getFinally 内通过 instanceof Future 递归 get() 拿真值.
         *
         * @param lf 见上述说明
         * @param rf 见上述说明
         */
        @SuppressWarnings({"unchecked", "rawtypes"})
        private void completeLf(LettuceFuture<?> lf, RedisFuture<?> rf) {
            ((LettuceFuture) lf).complete(rf);
        }

        /* NOTE ------------------- common start ------------------------------------------------------------------------ */

        /**
         * 业务作用：删除键。
         * 键不存在时不报错，因此可安全用于幂等清理。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void del(Object key) {
            if (key == null) return;
            this.enqueueSync(OP_DEL, serKey(key), null, null, 0);
        }

        /**
         * 业务作用：删除键。
         * 键不存在时不报错，因此可安全用于幂等清理。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void delAsync(Object key) {
            if (key == null) return;
            this.enqueueAsync(OP_DEL, serKey(key), null, null, 0);
        }

        /**
         * 业务作用：设置键的存活时长。
         * 已存在的存活时长会被覆盖；键不存在时设置不生效。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param timeout 超时时长
         * @param unit 时长单位
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void expire(Object key, long timeout, TimeUnit unit) {
            this.expire(key, unit.toMillis(timeout));
        }

        /**
         * 业务作用：设置键的存活时长。
         * 已存在的存活时长会被覆盖；键不存在时设置不生效。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param millis 毫秒数
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void expire(Object key, long millis) {
            if (key == null) return;
            this.enqueueSync(OP_EXPIRE, serKey(key), null, null, millis);
        }

        /**
         * 业务作用：设置键的存活时长。
         * 已存在的存活时长会被覆盖；键不存在时设置不生效。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param timeout 超时时长
         * @param unit 时长单位
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void expireAsync(Object key, long timeout, TimeUnit unit) {
            this.expireAsync(key, unit.toMillis(timeout));
        }

        /**
         * 业务作用：设置键的存活时长。
         * 已存在的存活时长会被覆盖；键不存在时设置不生效。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param millis 毫秒数
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void expireAsync(Object key, long millis) {
            if (key == null) return;
            this.enqueueAsync(OP_EXPIRE, serKey(key), null, null, millis);
        }

        /**
         * 业务作用：把键的过期时刻设为绝对时间。
         * 依赖服务端时钟，与客户端时钟不一致时过期时刻会偏移。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param millis 毫秒数
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void expireAt(Object key, long millis) {
            if (key == null) return;
            this.enqueueSync(OP_EXPIRE_AT, serKey(key), null, null, millis);
        }

        /**
         * 业务作用：把键的过期时刻设为绝对时间。
         * 依赖服务端时钟，与客户端时钟不一致时过期时刻会偏移。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param millis 毫秒数
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void expireAtAsync(Object key, long millis) {
            if (key == null) return;
            this.enqueueAsync(OP_EXPIRE_AT, serKey(key), null, null, millis);
        }

        /**
         * 业务作用：移除键的存活时长使其长期保留。
         * 此后该键不再自动回收，需由业务显式删除，否则会持续占用内存。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void persist(Object key) {
            if (key == null) return;
            this.enqueueSync(OP_PERSIST, serKey(key), null, null, 0);
        }

        /**
         * 业务作用：移除键的存活时长使其长期保留。
         * 此后该键不再自动回收，需由业务显式删除，否则会持续占用内存。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void persistAsync(Object key) {
            if (key == null) return;
            this.enqueueAsync(OP_PERSIST, serKey(key), null, null, 0);
        }

        /**
         * 业务作用：为哈希中的<b>单个字段</b>设置存活时长，到期后只删该字段，其余字段与键本身不受影响。
         * 该能力要求 Redis 7.4 及以上；低版本服务端会拒绝该命令。已存在的字段存活时长会被覆盖。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param millis 毫秒数
         * @param hashKey 哈希字段名
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void expire(Object key, long millis, Object hashKey) {
            this.expire(key, Duration.ofMillis(millis), hashKey);
        }

        /**
         * 业务作用：为哈希中的<b>单个字段</b>设置存活时长，到期后只删该字段，其余字段与键本身不受影响。
         * 该能力要求 Redis 7.4 及以上；低版本服务端会拒绝该命令。已存在的字段存活时长会被覆盖。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param millis 毫秒数
         * @param hashKey 哈希字段名
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void expireAsync(Object key, long millis, Object hashKey) {
            this.expireAsync(key, Duration.ofMillis(millis), hashKey);
        }

        /**
         * 业务作用：为哈希中的<b>单个字段</b>设置存活时长，到期后只删该字段，其余字段与键本身不受影响。
         * 该能力要求 Redis 7.4 及以上；低版本服务端会拒绝该命令。已存在的字段存活时长会被覆盖。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param timeout 超时时长
         * @param unit 时长单位
         * @param hashKey 哈希字段名
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void expire(Object key, long timeout, TimeUnit unit, Object hashKey) {
            this.expire(key, Duration.ofMillis(unit.toMillis(timeout)), hashKey);
        }

        /**
         * 业务作用：为哈希中的<b>单个字段</b>设置存活时长，到期后只删该字段，其余字段与键本身不受影响。
         * 该能力要求 Redis 7.4 及以上；低版本服务端会拒绝该命令。已存在的字段存活时长会被覆盖。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param timeout 超时时长
         * @param unit 时长单位
         * @param hashKey 哈希字段名
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void expireAsync(Object key, long timeout, TimeUnit unit, Object hashKey) {
            this.expireAsync(key, Duration.ofMillis(unit.toMillis(timeout)), hashKey);
        }

        /**
         * 业务作用：为哈希中的<b>单个字段</b>设置存活时长，到期后只删该字段，其余字段与键本身不受影响。
         * 该能力要求 Redis 7.4 及以上；低版本服务端会拒绝该命令。已存在的字段存活时长会被覆盖。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param timeout 超时时长
         * @param hashKey 哈希字段名
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void expire(Object key, Duration timeout, Object hashKey) {
            if (key == null || hashKey == null) return;
            byte[] k = serKey(key);
            byte[] hk = serHKey(hashKey);
            this.enqueueSync(OP_HEXPIRE, k, hk, null, timeout.toMillis());
        }

        /**
         * 业务作用：为哈希中的<b>单个字段</b>设置存活时长，到期后只删该字段，其余字段与键本身不受影响。
         * 该能力要求 Redis 7.4 及以上；低版本服务端会拒绝该命令。已存在的字段存活时长会被覆盖。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param timeout 超时时长
         * @param hashKey 哈希字段名
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void expireAsync(Object key, Duration timeout, Object hashKey) {
            if (key == null || hashKey == null) return;
            byte[] k = serKey(key);
            byte[] hk = serHKey(hashKey);
            this.enqueueAsync(OP_HEXPIRE, k, hk, null, timeout.toMillis());
        }

        /* NOTE ------------------- String start ------------------------------------------------------------------------ */

        /**
         * 业务作用：写入字符串值。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param value 待写入的值
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public <T> void set(Object key, T value) {
            if (value == null) return;
            this.enqueueSync(OP_SET, serKey(key), null, serVal(value), 0);
        }

        /**
         * 业务作用：写入字符串值。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param value 待写入的值
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public <T> void setAsync(Object key, T value) {
            if (value == null) return;
            this.enqueueAsync(OP_SET, serKey(key), null, serVal(value), 0);
        }

        /**
         * 业务作用：写入字符串值。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param val 见方法语义
         * @param timeout 超时时长
         * @param unit 时长单位
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void set(Object key, Object val, int timeout, TimeUnit unit) {
            this.set(key, val, (int) unit.toMillis(timeout));
        }

        /**
         * 业务作用：写入字符串值。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param val 见方法语义
         * @param timeout 超时时长
         * @param unit 时长单位
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void setAsync(Object key, Object val, int timeout, TimeUnit unit) {
            this.setAsync(key, val, (int) unit.toMillis(timeout));
        }

        /**
         * 业务作用：写入字符串值。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param val 见方法语义
         * @param millis 毫秒数
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void set(Object key, Object val, int millis) {
            if (key == null || val == null) return;
            this.enqueueSync(OP_SET_EX, serKey(key), null, serVal(val), millis);
        }

        /**
         * 业务作用：写入字符串值。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param val 见方法语义
         * @param millis 毫秒数
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void setAsync(Object key, Object val, int millis) {
            if (key == null || val == null) return;
            this.enqueueAsync(OP_SET_EX, serKey(key), null, serVal(val), millis);
        }

        /**
         * 业务作用：仅在键不存在时写入。
         * 写入与判存在服务端原子完成，常用于抢占型的互斥标记。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param val 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void setIfAbsent(Object key, Object val) {
            if (key == null || val == null) return;
            this.enqueueSync(OP_SET_NX, serKey(key), null, serVal(val), 0);
        }

        /**
         * 业务作用：仅在键不存在时写入。
         * 写入与判存在服务端原子完成，常用于抢占型的互斥标记。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param val 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void setIfAbsentAsync(Object key, Object val) {
            if (key == null || val == null) return;
            this.enqueueAsync(OP_SET_NX, serKey(key), null, serVal(val), 0);
        }

        /**
         * 业务作用：从指定偏移开始覆写字符串的一段。
         * 偏移超出原长度时中间以零字节填充，可能一次分配大量内存。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param val 见方法语义
         * @param offset 偏移量
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void setRange(Object key, Object val, long offset) {
            if (key == null) return;
            this.enqueueSync(OP_SETRANGE, serKey(key), null, serVal(val), offset);
        }

        /**
         * 业务作用：从指定偏移开始覆写字符串的一段。
         * 偏移超出原长度时中间以零字节填充，可能一次分配大量内存。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param val 见方法语义
         * @param offset 偏移量
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void setRangeAsync(Object key, Object val, long offset) {
            if (key == null) return;
            this.enqueueAsync(OP_SETRANGE, serKey(key), null, serVal(val), offset);
        }

        /**
         * 业务作用：批量写入多个键值。
         * 本实现拆成多条单值命令入队，网络开销与原生批量命令相同。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param kvMap 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void multiSet(Map kvMap) {
            if (MapUtils.isEmpty(kvMap)) return;
            kvMap.forEach((k, v) -> {
                if (v == null) return;
                this.set(k.toString(), v);
            });
        }

        /**
         * 业务作用：批量写入多个键值。
         * 本实现拆成多条单值命令入队，网络开销与原生批量命令相同。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param kvMap 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void multiSetAsync(Map kvMap) {
            if (MapUtils.isEmpty(kvMap)) return;
            kvMap.forEach((k, v) -> {
                if (v == null) return;
                this.setAsync(k.toString(), v);
            });
        }

        /**
         * 业务作用：按增量原子自增。
         * 自增在服务端完成，多个客户端并发调用不会丢更新。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void increment(Object key) {
            Objects.requireNonNull(key);
            this.enqueueSync(OP_INCR, serKey(key), null, null, 0);
        }

        /**
         * 业务作用：按增量原子自增。
         * 自增在服务端完成，多个客户端并发调用不会丢更新。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void incrementAsync(Object key) {
            Objects.requireNonNull(key);
            this.enqueueAsync(OP_INCR, serKey(key), null, null, 0);
        }

        /**
         * 业务作用：按增量原子自增。
         * 自增在服务端完成，多个客户端并发调用不会丢更新。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param delta 增减量
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void increment(Object key, long delta) {
            Objects.requireNonNull(key);
            this.enqueueSync(OP_INCRBY, serKey(key), null, null, delta);
        }

        /**
         * 业务作用：按增量原子自增。
         * 自增在服务端完成，多个客户端并发调用不会丢更新。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param delta 增减量
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void incrementAsync(Object key, long delta) {
            Objects.requireNonNull(key);
            this.enqueueAsync(OP_INCRBY, serKey(key), null, null, delta);
        }

        /**
         * 业务作用：按减量原子自减。
         * 结果可为负数，需要下界约束的场景应改用脚本。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void decrement(Object key) {
            Objects.requireNonNull(key);
            this.enqueueSync(OP_DECR, serKey(key), null, null, 0);
        }

        /**
         * 业务作用：按减量原子自减。
         * 结果可为负数，需要下界约束的场景应改用脚本。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void decrementAsync(Object key) {
            Objects.requireNonNull(key);
            this.enqueueAsync(OP_DECR, serKey(key), null, null, 0);
        }

        /**
         * 业务作用：按减量原子自减。
         * 结果可为负数，需要下界约束的场景应改用脚本。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param delta 增减量
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void decrement(Object key, long delta) {
            Objects.requireNonNull(key);
            this.enqueueSync(OP_DECRBY, serKey(key), null, null, delta);
        }

        /**
         * 业务作用：按减量原子自减。
         * 结果可为负数，需要下界约束的场景应改用脚本。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param delta 增减量
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void decrementAsync(Object key, long delta) {
            Objects.requireNonNull(key);
            this.enqueueAsync(OP_DECRBY, serKey(key), null, null, delta);
        }

        /**
         * 业务作用：在字符串末尾追加内容。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param val 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void append(Object key, Object val) {
            if (key == null || val == null) return;
            this.enqueueSync(OP_APPEND, serKey(key), null, serVal(val), 0);
        }

        /**
         * 业务作用：在字符串末尾追加内容。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param val 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void appendAsync(Object key, Object val) {
            if (key == null || val == null) return;
            this.enqueueAsync(OP_APPEND, serKey(key), null, serVal(val), 0);
        }

        /* NOTE ------------------- String / Key 扩展 (读 + 多 key 批量) -------------------------------------------------- */

        /**
         * 业务作用：读取字符串值。
         * 键不存在时返回空。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void getAsync(Object key, LettuceFuture<RedisFuture<byte[]>> lf) {
            this.enqueueWithFuture(OP_GET, serKey(key), null, null, 0L, 0L, null, lf);
        }

        /**
         * 业务作用：批量读取多个键值。
         * 结果与入参顺序一一对应，不存在的键对应位置为空。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param keys 缓存键集合
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void mGetAsync(Object[] keys, LettuceFuture<RedisFuture<List<KeyValue<byte[], byte[]>>>> lf) {
            byte[][] ks = new byte[keys.length][];
            for (int i = 0; i < keys.length; i++) ks[i] = serKey(keys[i]);
            this.enqueueWithFuture(OP_MGET, null, null, null, 0L, 0L, ks, lf);
        }

        /**
         * 业务作用：读取字符串的指定字节区间。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param start 起始下标
         * @param end 结束下标
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void getRangeAsync(Object key, long start, long end, LettuceFuture<RedisFuture<byte[]>> lf) {
            this.enqueueWithFuture(OP_GETRANGE, serKey(key), null, null, start, end, null, lf);
        }

        /**
         * 业务作用：写入新值并返回旧值。
         * 两步在服务端原子完成，可用于取走并重置一个计数或标记。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param newVal 见方法语义
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void getSetAsync(Object key, Object newVal, LettuceFuture<RedisFuture<byte[]>> lf) {
            this.enqueueWithFuture(OP_GETSET, serKey(key), null, serVal(newVal), 0L, 0L, null, lf);
        }

        /**
         * 业务作用：读取并随即删除键。
         * 两步在服务端原子完成，适合一次性取走的令牌类数据。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void getDelAsync(Object key, LettuceFuture<RedisFuture<byte[]>> lf) {
            this.enqueueWithFuture(OP_GETDEL, serKey(key), null, null, 0L, 0L, null, lf);
        }

        /**
         * 业务作用：查询字符串值的字节长度。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void strLenAsync(Object key, LettuceFuture<RedisFuture<Long>> lf) {
            this.enqueueWithFuture(OP_STRLEN, serKey(key), null, null, 0L, 0L, null, lf);
        }

        /**
         * 业务作用：判断键是否存在。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void existsAsync(Object key, LettuceFuture<RedisFuture<Long>> lf) {
            this.enqueueWithFuture(OP_EXISTS, serKey(key), null, null, 0L, 0L, null, lf);
        }

        /**
         * 业务作用：批量判断多个键是否存在。
         * 返回存在的键数量而非逐个结果。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param keys 缓存键集合
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void existsMultiAsync(Object[] keys, LettuceFuture<RedisFuture<Long>> lf) {
            byte[][] ks = new byte[keys.length][];
            for (int i = 0; i < keys.length; i++) ks[i] = serKey(keys[i]);
            this.enqueueWithFuture(OP_EXISTS_MULTI, null, null, null, 0L, 0L, ks, lf);
        }

        /**
         * 业务作用：查询键的剩余存活毫秒数。
         * 键不存在或未设存活时长时返回负值，两种情形取值不同。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void pttlAsync(Object key, LettuceFuture<RedisFuture<Long>> lf) {
            this.enqueueWithFuture(OP_PTTL, serKey(key), null, null, 0L, 0L, null, lf);
        }

        /**
         * 业务作用：查询键的剩余存活毫秒数。
         * 键不存在或未设存活时长时返回负值，两种情形取值不同，需按 Redis 约定区分。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void ttlAsync(Object key, LettuceFuture<RedisFuture<Long>> lf) {
            this.enqueueWithFuture(OP_TTL, serKey(key), null, null, 0L, 0L, null, lf);
        }

        /**
         * 业务作用：查询键的数据类型。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void typeAsync(Object key, LettuceFuture<RedisFuture<String>> lf) {
            this.enqueueWithFuture(OP_TYPE, serKey(key), null, null, 0L, 0L, null, lf);
        }

        /**
         * 业务作用：按模式匹配键名。
         * 该命令会遍历整个键空间，<b>在大实例上会阻塞服务端</b>，生产环境应改用游标扫描。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param pattern 匹配模式
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void keysAsync(Object pattern, LettuceFuture<RedisFuture<List<byte[]>>> lf) {
            this.enqueueWithFuture(OP_KEYS, serKey(pattern), null, null, 0L, 0L, null, lf);
        }

        /**
         * 业务作用：导出键的序列化内容。
         * 内容格式与 Redis 版本绑定，跨版本恢复可能失败。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void dumpAsync(Object key, LettuceFuture<RedisFuture<byte[]>> lf) {
            this.enqueueWithFuture(OP_DUMP, serKey(key), null, null, 0L, 0L, null, lf);
        }

        /**
         * 业务作用：批量删除多个键。
         * 拆成多条单键命令入队，网络开销与原生批量命令相同。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param keys 缓存键集合
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void delMulti(Object[] keys) {
            if (keys == null || keys.length == 0) return;
            byte[][] ks = new byte[keys.length][];
            for (int i = 0; i < keys.length; i++) ks[i] = serKey(keys[i]);
            this.enqueueSync(OP_DEL_MULTI, null, null, null, 0L, 0L, ks);
        }

        /**
         * 业务作用：批量删除多个键。
         * 拆成多条单键命令入队，网络开销与原生批量命令相同。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param keys 缓存键集合
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void delMultiAsync(Object[] keys) {
            if (keys == null || keys.length == 0) return;
            byte[][] ks = new byte[keys.length][];
            for (int i = 0; i < keys.length; i++) ks[i] = serKey(keys[i]);
            this.enqueueAsync(OP_DEL_MULTI, null, null, null, 0L, 0L, ks);
        }

        /**
         * 业务作用：按浮点增量原子调整值。
         * 浮点累加存在精度误差，金额一类不可用它累计。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param delta 增减量
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void incrByFloat(Object key, double delta) {
            this.enqueueSync(OP_INCRBYFLOAT, serKey(key), null, null, Double.doubleToRawLongBits(delta));
        }

        /**
         * 业务作用：按浮点增量原子调整值。
         * 浮点累加存在精度误差，金额一类不可用它累计。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param delta 增减量
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void incrByFloatAsync(Object key, double delta) {
            this.enqueueAsync(OP_INCRBYFLOAT, serKey(key), null, null, Double.doubleToRawLongBits(delta));
        }

        /* NOTE ------------------- Hash start -------------------------------------------------------------------------- */

        /**
         * 业务作用：写入哈希字段。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param hashKey 哈希字段名
         * @param value 待写入的值
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public <T> void hSet(Object key, Object hashKey, T value) {
            if (value == null) return;
            this.enqueueSync(OP_HSET, serKey(key), serHKey(hashKey), serHVal(value), 0);
        }

        /**
         * 业务作用：写入哈希字段。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param hashKey 哈希字段名
         * @param value 待写入的值
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public <T> void hSetAsync(Object key, Object hashKey, T value) {
            if (value == null) return;
            this.enqueueAsync(OP_HSET, serKey(key), serHKey(hashKey), serHVal(value), 0);
        }

        /**
         * 业务作用：写入哈希字段。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param khkvMap 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void hSet(Map<String, Map> khkvMap) {
            if (MapUtils.isEmpty(khkvMap)) return;
            khkvMap.forEach((key, hkv) -> {
                if (MapUtils.isEmpty(hkv)) return;
                hkv.forEach((hk, v) -> {
                    if (v == null) return;
                    this.hSet(key, hk.toString(), v);
                });
            });
        }

        /**
         * 业务作用：写入哈希字段。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param khkvMap 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void hSetAsync(Map<String, Map> khkvMap) {
            if (MapUtils.isEmpty(khkvMap)) return;
            khkvMap.forEach((key, hkv) -> {
                if (MapUtils.isEmpty(hkv)) return;
                hkv.forEach((hk, v) -> {
                    if (v == null) return;
                    this.hSetAsync(key, hk.toString(), v);
                });
            });
        }

        /**
         * 业务作用：写入哈希字段。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param hkvMap 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public <T> void hSet(Object key, Map<String, T> hkvMap) {
            if (MapUtils.isEmpty(hkvMap)) return;
            hkvMap.forEach((hk, v) -> {
                if (v == null) return;
                this.hSet(key, hk, v);
            });
        }

        /**
         * 业务作用：写入哈希字段。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param hkvMap 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public <T> void hSetAsync(Object key, Map<String, T> hkvMap) {
            if (MapUtils.isEmpty(hkvMap)) return;
            hkvMap.forEach((hk, v) -> {
                if (v == null) return;
                this.hSetAsync(key, hk, v);
            });
        }

        /**
         * 业务作用：删除哈希字段。
         * 字段不存在时不报错。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param hashKey 哈希字段名
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void hDel(Object key, Object hashKey) {
            if (hashKey == null) return;
            this.enqueueSync(OP_HDEL, serKey(key), serHKey(hashKey), null, 0);
        }

        /**
         * 业务作用：删除哈希字段。
         * 字段不存在时不报错。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param hashKey 哈希字段名
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void hDelAsync(Object key, Object hashKey) {
            if (hashKey == null) return;
            this.enqueueAsync(OP_HDEL, serKey(key), serHKey(hashKey), null, 0);
        }

        /**
         * 业务作用：仅在哈希字段不存在时写入。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param hashKey 哈希字段名
         * @param val 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void hSetNx(Object key, Object hashKey, Object val) {
            if (key == null || hashKey == null || val == null) return;
            this.enqueueSync(OP_HSET_NX, serKey(key), serHKey(hashKey), serHVal(val), 0);
        }

        /**
         * 业务作用：仅在哈希字段不存在时写入。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param hashKey 哈希字段名
         * @param val 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void hSetNxAsync(Object key, Object hashKey, Object val) {
            if (key == null || hashKey == null || val == null) return;
            this.enqueueAsync(OP_HSET_NX, serKey(key), serHKey(hashKey), serHVal(val), 0);
        }

        /**
         * 业务作用：按增量原子自增哈希字段。
         * 自增在服务端完成，并发调用不会丢更新。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param hashKey 哈希字段名
         * @param delta 增减量
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void hIncrBy(Object key, Object hashKey, long delta) {
            this.enqueueSync(OP_HINCRBY, serKey(key), serHKey(hashKey), null, delta);
        }

        /**
         * 业务作用：按增量原子自增哈希字段。
         * 自增在服务端完成，并发调用不会丢更新。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param hashKey 哈希字段名
         * @param delta 增减量
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void hIncrByAsync(Object key, Object hashKey, long delta) {
            this.enqueueAsync(OP_HINCRBY, serKey(key), serHKey(hashKey), null, delta);
        }

        /**
         * 业务作用：按增量原子自增哈希字段。
         * 自增在服务端完成，并发调用不会丢更新。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param hashKey 哈希字段名
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void hIncrBy(Object key, Object hashKey) {
            this.hIncrBy(key, hashKey, 1);
        }

        /**
         * 业务作用：按增量原子自增哈希字段。
         * 自增在服务端完成，并发调用不会丢更新。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param hashKey 哈希字段名
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void hIncrByAsync(Object key, Object hashKey) {
            this.hIncrByAsync(key, hashKey, 1);
        }

        /**
         * 业务作用：按减量原子自减哈希字段。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param hashKey 哈希字段名
         * @param delta 增减量
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void hDecrBy(Object key, Object hashKey, long delta) {
            this.hIncrBy(key, hashKey, -delta);
        }

        /**
         * 业务作用：按减量原子自减哈希字段。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param hashKey 哈希字段名
         * @param delta 增减量
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void hDecrByAsync(Object key, Object hashKey, long delta) {
            this.hIncrByAsync(key, hashKey, -delta);
        }

        /**
         * 业务作用：按减量原子自减哈希字段。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param hashKey 哈希字段名
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void hDecrBy(Object key, Object hashKey) {
            this.hDecrBy(key, hashKey, 1);
        }

        /**
         * 业务作用：按减量原子自减哈希字段。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param hashKey 哈希字段名
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void hDecrByAsync(Object key, Object hashKey) {
            this.hDecrByAsync(key, hashKey, 1);
        }

        /**
         * 业务作用：按减量自减哈希字段并在减到零时删除该字段。
         * 自减与删除由 Lua 脚本在服务端原子完成——分两步会在两者之间留出窗口，使字段可能被并发写回而残留一个零值。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param hashKey 哈希字段名
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void hDecrByAndDel(Object key, Object hashKey) {
            this.hDecrByAndDel(key, hashKey, 1);
        }

        /**
         * 业务作用：按减量自减哈希字段并在减到零时删除该字段。
         * 自减与删除由 Lua 脚本在服务端原子完成——分两步会在两者之间留出窗口，使字段可能被并发写回而残留一个零值。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param hashKey 哈希字段名
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void hDecrByAndDelAsync(Object key, Object hashKey) {
            this.hDecrByAndDelAsync(key, hashKey, 1);
        }

        /**
         * 业务作用：按减量自减哈希字段并在减到零时删除该字段。
         * 自减与删除由 Lua 脚本在服务端原子完成——分两步会在两者之间留出窗口，使字段可能被并发写回而残留一个零值。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param hashKey 哈希字段名
         * @param delta 增减量
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void hDecrByAndDel(Object key, Object hashKey, long delta) {
            this.enqueueSync(OP_HDECR_BY_AND_DEL, serKey(key), serHKey(hashKey), null, delta);
        }

        /**
         * 业务作用：按减量自减哈希字段并在减到零时删除该字段。
         * 自减与删除由 Lua 脚本在服务端原子完成——分两步会在两者之间留出窗口，使字段可能被并发写回而残留一个零值。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param hashKey 哈希字段名
         * @param delta 增减量
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void hDecrByAndDelAsync(Object key, Object hashKey, long delta) {
            this.enqueueAsync(OP_HDECR_BY_AND_DEL, serKey(key), serHKey(hashKey), null, delta);
        }

        /* NOTE ------------------- Hash 扩展 (读 / 多 field 批量 / 浮点 incr) ----------------------------------------------- */

        /**
         * 业务作用：读取哈希字段。
         * 字段不存在时返回空。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param hashKey 哈希字段名
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void hGetAsync(Object key, Object hashKey, LettuceFuture<RedisFuture<byte[]>> lf) {
            this.enqueueWithFuture(OP_HGET, serKey(key), serHKey(hashKey), null, 0L, 0L, null, lf);
        }

        /**
         * 业务作用：读取哈希的全部字段与值。
         * 字段极多时会一次性返回全部内容并阻塞服务端，大哈希应改用游标扫描。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void hGetAllAsync(Object key, LettuceFuture<RedisFuture<Map<byte[], byte[]>>> lf) {
            this.enqueueWithFuture(OP_HGETALL, serKey(key), null, null, 0L, 0L, null, lf);
        }

        /**
         * 业务作用：批量读取哈希字段。
         * 结果与入参顺序一一对应，不存在的字段对应位置为空。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param hashKeys 哈希字段名集合
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void hMGetAsync(Object key, Object[] hashKeys, LettuceFuture<RedisFuture<List<KeyValue<byte[], byte[]>>>> lf) {
            byte[][] hks = new byte[hashKeys.length][];
            for (int i = 0; i < hashKeys.length; i++) hks[i] = serHKey(hashKeys[i]);
            this.enqueueWithFuture(OP_HMGET, serKey(key), null, null, 0L, 0L, hks, lf);
        }

        /**
         * 业务作用：判断哈希字段是否存在。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param hashKey 哈希字段名
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void hExistsAsync(Object key, Object hashKey, LettuceFuture<RedisFuture<Boolean>> lf) {
            this.enqueueWithFuture(OP_HEXISTS, serKey(key), serHKey(hashKey), null, 0L, 0L, null, lf);
        }

        /**
         * 业务作用：读取哈希的全部字段名。
         * 字段极多时会一次性返回全部内容，应评估返回体量。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void hKeysAsync(Object key, LettuceFuture<RedisFuture<List<byte[]>>> lf) {
            this.enqueueWithFuture(OP_HKEYS, serKey(key), null, null, 0L, 0L, null, lf);
        }

        /**
         * 业务作用：读取哈希的全部字段值。
         * 字段极多时会一次性返回全部内容，应评估返回体量。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void hValsAsync(Object key, LettuceFuture<RedisFuture<List<byte[]>>> lf) {
            this.enqueueWithFuture(OP_HVALS, serKey(key), null, null, 0L, 0L, null, lf);
        }

        /**
         * 业务作用：查询哈希的字段数。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void hLenAsync(Object key, LettuceFuture<RedisFuture<Long>> lf) {
            this.enqueueWithFuture(OP_HLEN, serKey(key), null, null, 0L, 0L, null, lf);
        }

        /**
         * 业务作用：随机读取哈希字段。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void hRandFieldAsync(Object key, LettuceFuture<RedisFuture<byte[]>> lf) {
            this.enqueueWithFuture(OP_HRANDFIELD, serKey(key), null, null, 0L, 0L, null, lf);
        }

        /**
         * 业务作用：随机读取哈希字段。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param count 数量上限
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void hRandFieldAsync(Object key, long count, LettuceFuture<RedisFuture<List<byte[]>>> lf) {
            this.enqueueWithFuture(OP_HRANDFIELD_COUNT, serKey(key), null, null, count, 0L, null, lf);
        }

        /**
         * 业务作用：批量写入哈希字段。
         * 本实现拆成多条单字段命令入队。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param hashMap 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void hMSet(Object key, Map<?, ?> hashMap) {
            if (key == null || MapUtils.isEmpty(hashMap)) return;
            RecycleLinkedMap<byte[], byte[]> m = RecycleLinkedMap.of();
            for (Map.Entry<?, ?> e : hashMap.entrySet()) {
                if (e.getValue() == null) continue;
                m.put(serHKey(e.getKey()), serHVal(e.getValue()));
            }
            if (m.isEmpty()) {
                m.recycle();
                return;
            }
            this.enqueueSync(OP_HMSET, serKey(key), null, null, 0L, 0L, m);
        }

        /**
         * 业务作用：批量写入哈希字段。
         * 本实现拆成多条单字段命令入队。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param hashMap 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void hMSetAsync(Object key, Map<?, ?> hashMap) {
            if (key == null || MapUtils.isEmpty(hashMap)) return;
            RecycleLinkedMap<byte[], byte[]> m = RecycleLinkedMap.of();
            for (Map.Entry<?, ?> e : hashMap.entrySet()) {
                if (e.getValue() == null) continue;
                m.put(serHKey(e.getKey()), serHVal(e.getValue()));
            }
            if (m.isEmpty()) {
                m.recycle();
                return;
            }
            this.enqueueAsync(OP_HMSET, serKey(key), null, null, 0L, 0L, m);
        }

        /**
         * 业务作用：批量删除哈希字段。
         * 拆成多条单字段命令入队。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param hashKeys 哈希字段名集合
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void hDelMulti(Object key, Object[] hashKeys) {
            if (key == null || hashKeys == null || hashKeys.length == 0) return;
            byte[][] hks = new byte[hashKeys.length][];
            for (int i = 0; i < hashKeys.length; i++) hks[i] = serHKey(hashKeys[i]);
            this.enqueueSync(OP_HDEL_MULTI, serKey(key), null, null, 0L, 0L, hks);
        }

        /**
         * 业务作用：批量删除哈希字段。
         * 拆成多条单字段命令入队。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param hashKeys 哈希字段名集合
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void hDelMultiAsync(Object key, Object[] hashKeys) {
            if (key == null || hashKeys == null || hashKeys.length == 0) return;
            byte[][] hks = new byte[hashKeys.length][];
            for (int i = 0; i < hashKeys.length; i++) hks[i] = serHKey(hashKeys[i]);
            this.enqueueAsync(OP_HDEL_MULTI, serKey(key), null, null, 0L, 0L, hks);
        }

        /**
         * 业务作用：按浮点增量原子调整哈希字段。
         * 浮点累加存在精度误差。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param hashKey 哈希字段名
         * @param delta 增减量
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void hIncrByFloat(Object key, Object hashKey, double delta) {
            this.enqueueSync(OP_HINCRBYFLOAT, serKey(key), serHKey(hashKey), null, Double.doubleToRawLongBits(delta));
        }

        /**
         * 业务作用：按浮点增量原子调整哈希字段。
         * 浮点累加存在精度误差。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param hashKey 哈希字段名
         * @param delta 增减量
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void hIncrByFloatAsync(Object key, Object hashKey, double delta) {
            this.enqueueAsync(OP_HINCRBYFLOAT, serKey(key), serHKey(hashKey), null, Double.doubleToRawLongBits(delta));
        }

        /* NOTE ------------------- List start -------------------------------------------------------------------------- */

        /**
         * 业务作用：从列表头部插入元素。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param val 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void lPush(Object key, Object val) {
            if (key == null || val == null) return;
            this.enqueueSync(OP_LPUSH, serKey(key), null, serVal(val), 0);
        }

        /**
         * 业务作用：从列表头部插入元素。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param val 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void lPushAsync(Object key, Object val) {
            if (key == null || val == null) return;
            this.enqueueAsync(OP_LPUSH, serKey(key), null, serVal(val), 0);
        }

        /**
         * 业务作用：仅在列表已存在时从头部插入。
         * 列表不存在时不创建，用于避免凭空建出空列表。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param val 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void lPushIfAbsent(Object key, Object val) {
            if (key == null) return;
            this.enqueueSync(OP_LPUSH_X, serKey(key), null, serVal(val), 0);
        }

        /**
         * 业务作用：仅在列表已存在时从头部插入。
         * 列表不存在时不创建，用于避免凭空建出空列表。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param val 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void lPushIfAbsentAsync(Object key, Object val) {
            if (key == null) return;
            this.enqueueAsync(OP_LPUSH_X, serKey(key), null, serVal(val), 0);
        }

        /**
         * 业务作用：按值删除列表中的元素。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param count 数量上限
         * @param val 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void lRem(Object key, long count, Object val) {
            if (key == null) return;
            this.enqueueSync(OP_LREM, serKey(key), null, serVal(val), count);
        }

        /**
         * 业务作用：按值删除列表中的元素。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param count 数量上限
         * @param val 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void lRemAsync(Object key, long count, Object val) {
            if (key == null) return;
            this.enqueueAsync(OP_LREM, serKey(key), null, serVal(val), count);
        }

        /**
         * 业务作用：按下标覆盖列表元素。
         * 下标越界时报错。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param index 下标
         * @param val 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void lSet(Object key, long index, Object val) {
            if (val == null) return;
            this.enqueueSync(OP_LSET, serKey(key), null, serVal(val), index);
        }

        /**
         * 业务作用：按下标覆盖列表元素。
         * 下标越界时报错。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param index 下标
         * @param val 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void lSetAsync(Object key, long index, Object val) {
            if (val == null) return;
            this.enqueueAsync(OP_LSET, serKey(key), null, serVal(val), index);
        }

        /**
         * 业务作用：把列表裁剪到指定下标区间。
         * 区间之外的元素被永久删除，不可撤销。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param start 起始下标
         * @param end 结束下标
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void lTrim(Object key, long start, long end) {
            if (key == null) return;
            this.enqueueSync(OP_LTRIM, serKey(key), null, null, start, end, null);
        }

        /**
         * 业务作用：把列表裁剪到指定下标区间。
         * 区间之外的元素被永久删除，不可撤销。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param start 起始下标
         * @param end 结束下标
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void lTrimAsync(Object key, long start, long end) {
            if (key == null) return;
            this.enqueueAsync(OP_LTRIM, serKey(key), null, null, start, end, null);
        }

        /**
         * 业务作用：从列表尾部插入元素。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param val 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void rPush(Object key, Object val) {
            if (key == null) return;
            this.enqueueSync(OP_RPUSH, serKey(key), null, serVal(val), 0);
        }

        /**
         * 业务作用：从列表尾部插入元素。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param val 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void rPushAsync(Object key, Object val) {
            if (key == null) return;
            this.enqueueAsync(OP_RPUSH, serKey(key), null, serVal(val), 0);
        }

        /**
         * 业务作用：仅在列表已存在时从尾部插入。
         * 列表不存在时不创建。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param val 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void rPushIfAbsent(Object key, Object val) {
            if (key == null) return;
            this.enqueueSync(OP_RPUSH_X, serKey(key), null, serVal(val), 0);
        }

        /**
         * 业务作用：仅在列表已存在时从尾部插入。
         * 列表不存在时不创建。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param val 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void rPushIfAbsentAsync(Object key, Object val) {
            if (key == null) return;
            this.enqueueAsync(OP_RPUSH_X, serKey(key), null, serVal(val), 0);
        }

        /* NOTE ------------------- List 扩展 (读 / 弹出 / 多值批量 / 插入) ------------------------------------------------ */

        /**
         * 业务作用：批量从列表头部插入元素。
         * 拆成多条单值命令入队；<b>插入顺序与原生批量命令一致</b>。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param vals 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void lPushMulti(Object key, Object[] vals) {
            if (key == null || vals == null || vals.length == 0) return;
            byte[][] vs = new byte[vals.length][];
            for (int i = 0; i < vals.length; i++) vs[i] = serVal(vals[i]);
            this.enqueueSync(OP_LPUSH_MULTI, serKey(key), null, null, 0L, 0L, vs);
        }

        /**
         * 业务作用：批量从列表头部插入元素。
         * 拆成多条单值命令入队；<b>插入顺序与原生批量命令一致</b>。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param vals 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void lPushMultiAsync(Object key, Object[] vals) {
            if (key == null || vals == null || vals.length == 0) return;
            byte[][] vs = new byte[vals.length][];
            for (int i = 0; i < vals.length; i++) vs[i] = serVal(vals[i]);
            this.enqueueAsync(OP_LPUSH_MULTI, serKey(key), null, null, 0L, 0L, vs);
        }

        /**
         * 业务作用：批量从列表尾部插入元素。
         * 拆成多条单值命令入队。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param vals 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void rPushMulti(Object key, Object[] vals) {
            if (key == null || vals == null || vals.length == 0) return;
            byte[][] vs = new byte[vals.length][];
            for (int i = 0; i < vals.length; i++) vs[i] = serVal(vals[i]);
            this.enqueueSync(OP_RPUSH_MULTI, serKey(key), null, null, 0L, 0L, vs);
        }

        /**
         * 业务作用：批量从列表尾部插入元素。
         * 拆成多条单值命令入队。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param vals 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void rPushMultiAsync(Object key, Object[] vals) {
            if (key == null || vals == null || vals.length == 0) return;
            byte[][] vs = new byte[vals.length][];
            for (int i = 0; i < vals.length; i++) vs[i] = serVal(vals[i]);
            this.enqueueAsync(OP_RPUSH_MULTI, serKey(key), null, null, 0L, 0L, vs);
        }

        /**
         * 业务作用：在列表中指定基准元素之前插入。
         * 基准元素不存在时不插入。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param pivot 见方法语义
         * @param value 待写入的值
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void lInsertBefore(Object key, Object pivot, Object value) {
            if (key == null) return;
            this.enqueueSync(OP_LINSERT_BEFORE, serKey(key), serVal(pivot), serVal(value), 0);
        }

        /**
         * 业务作用：在列表中指定基准元素之前插入。
         * 基准元素不存在时不插入。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param pivot 见方法语义
         * @param value 待写入的值
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void lInsertBeforeAsync(Object key, Object pivot, Object value) {
            if (key == null) return;
            this.enqueueAsync(OP_LINSERT_BEFORE, serKey(key), serVal(pivot), serVal(value), 0);
        }

        /**
         * 业务作用：在列表中指定基准元素之后插入。
         * 基准元素不存在时不插入。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param pivot 见方法语义
         * @param value 待写入的值
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void lInsertAfter(Object key, Object pivot, Object value) {
            if (key == null) return;
            this.enqueueSync(OP_LINSERT_AFTER, serKey(key), serVal(pivot), serVal(value), 0);
        }

        /**
         * 业务作用：在列表中指定基准元素之后插入。
         * 基准元素不存在时不插入。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param pivot 见方法语义
         * @param value 待写入的值
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void lInsertAfterAsync(Object key, Object pivot, Object value) {
            if (key == null) return;
            this.enqueueAsync(OP_LINSERT_AFTER, serKey(key), serVal(pivot), serVal(value), 0);
        }

        /**
         * 业务作用：从列表头部弹出元素。
         * 列表为空时返回空。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void lPopAsync(Object key, LettuceFuture<RedisFuture<byte[]>> lf) {
            this.enqueueWithFuture(OP_LPOP, serKey(key), null, null, 0L, 0L, null, lf);
        }

        /**
         * 业务作用：从列表头部弹出元素。
         * 列表为空时返回空。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param count 数量上限
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void lPopAsync(Object key, long count, LettuceFuture<RedisFuture<List<byte[]>>> lf) {
            this.enqueueWithFuture(OP_LPOP_COUNT, serKey(key), null, null, count, 0L, null, lf);
        }

        /**
         * 业务作用：从列表尾部弹出元素。
         * 列表为空时返回空。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void rPopAsync(Object key, LettuceFuture<RedisFuture<byte[]>> lf) {
            this.enqueueWithFuture(OP_RPOP, serKey(key), null, null, 0L, 0L, null, lf);
        }

        /**
         * 业务作用：从列表尾部弹出元素。
         * 列表为空时返回空。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param count 数量上限
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void rPopAsync(Object key, long count, LettuceFuture<RedisFuture<List<byte[]>>> lf) {
            this.enqueueWithFuture(OP_RPOP_COUNT, serKey(key), null, null, count, 0L, null, lf);
        }

        /**
         * 业务作用：按下标区间读取列表元素。
         * 下标支持负数表示从尾部倒数。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param start 起始下标
         * @param end 结束下标
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void lRangeAsync(Object key, long start, long end, LettuceFuture<RedisFuture<List<byte[]>>> lf) {
            this.enqueueWithFuture(OP_LRANGE, serKey(key), null, null, start, end, null, lf);
        }

        /**
         * 业务作用：按下标读取列表元素。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param index 下标
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void lIndexAsync(Object key, long index, LettuceFuture<RedisFuture<byte[]>> lf) {
            this.enqueueWithFuture(OP_LINDEX, serKey(key), null, null, index, 0L, null, lf);
        }

        /**
         * 业务作用：查询列表长度。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void lLenAsync(Object key, LettuceFuture<RedisFuture<Long>> lf) {
            this.enqueueWithFuture(OP_LLEN, serKey(key), null, null, 0L, 0L, null, lf);
        }

        /**
         * 业务作用：查询元素在列表中的下标。
         * 元素不存在时返回空。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param element 见方法语义
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void lPosAsync(Object key, Object element, LettuceFuture<RedisFuture<Long>> lf) {
            this.enqueueWithFuture(OP_LPOS, serKey(key), null, serVal(element), 0L, 0L, null, lf);
        }

        /**
         * 业务作用：查询元素在列表中的下标。
         * 元素不存在时返回空。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param element 见方法语义
         * @param count 数量上限
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void lPosAsync(Object key, Object element, long count, LettuceFuture<RedisFuture<List<Long>>> lf) {
            this.enqueueWithFuture(OP_LPOS_COUNT, serKey(key), null, serVal(element), count, 0L, null, lf);
        }

        /* NOTE ------------------- Set start --------------------------------------------------------------------------- */

        /**
         * 业务作用：向集合添加成员。
         * 已存在的成员不会重复加入。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param val 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void sAdd(Object key, Object val) {
            if (key == null || val == null) return;
            this.enqueueSync(OP_SADD, serKey(key), null, serVal(val), 0);
        }

        /**
         * 业务作用：向集合添加成员。
         * 已存在的成员不会重复加入。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param val 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void sAddAsync(Object key, Object val) {
            if (key == null || val == null) return;
            this.enqueueAsync(OP_SADD, serKey(key), null, serVal(val), 0);
        }

        /**
         * 业务作用：从集合移除成员。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param val 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void sRem(Object key, Object val) {
            if (key == null || val == null) return;
            this.enqueueSync(OP_SREM, serKey(key), null, serVal(val), 0);
        }

        /**
         * 业务作用：从集合移除成员。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param val 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void sRemAsync(Object key, Object val) {
            if (key == null || val == null) return;
            this.enqueueAsync(OP_SREM, serKey(key), null, serVal(val), 0);
        }

        /* NOTE ------------------- Set 扩展 (读 / 多元素批量 / 集合运算 / 跨集合搬移) ------------------------------------ */

        /**
         * 业务作用：批量向集合添加成员。
         * 拆成多条单值命令入队。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param members 集合成员集合
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void sAddMulti(Object key, Object[] members) {
            if (key == null || members == null || members.length == 0) return;
            byte[][] ms = new byte[members.length][];
            for (int i = 0; i < members.length; i++) ms[i] = serVal(members[i]);
            this.enqueueSync(OP_SADD_MULTI, serKey(key), null, null, 0L, 0L, ms);
        }

        /**
         * 业务作用：批量向集合添加成员。
         * 拆成多条单值命令入队。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param members 集合成员集合
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void sAddMultiAsync(Object key, Object[] members) {
            if (key == null || members == null || members.length == 0) return;
            byte[][] ms = new byte[members.length][];
            for (int i = 0; i < members.length; i++) ms[i] = serVal(members[i]);
            this.enqueueAsync(OP_SADD_MULTI, serKey(key), null, null, 0L, 0L, ms);
        }

        /**
         * 业务作用：批量从集合移除成员。
         * 拆成多条单值命令入队。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param members 集合成员集合
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void sRemMulti(Object key, Object[] members) {
            if (key == null || members == null || members.length == 0) return;
            byte[][] ms = new byte[members.length][];
            for (int i = 0; i < members.length; i++) ms[i] = serVal(members[i]);
            this.enqueueSync(OP_SREM_MULTI, serKey(key), null, null, 0L, 0L, ms);
        }

        /**
         * 业务作用：批量从集合移除成员。
         * 拆成多条单值命令入队。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param members 集合成员集合
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void sRemMultiAsync(Object key, Object[] members) {
            if (key == null || members == null || members.length == 0) return;
            byte[][] ms = new byte[members.length][];
            for (int i = 0; i < members.length; i++) ms[i] = serVal(members[i]);
            this.enqueueAsync(OP_SREM_MULTI, serKey(key), null, null, 0L, 0L, ms);
        }

        /**
         * 业务作用：把成员从一个集合移到另一个集合。
         * 移动在服务端原子完成，不会出现两边都有或都没有的中间态。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param src 见方法语义
         * @param dst 见方法语义
         * @param member 集合成员
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void sMove(Object src, Object dst, Object member) {
            if (src == null || dst == null || member == null) return;
            this.enqueueSync(OP_SMOVE, serKey(src), serKey(dst), serVal(member), 0);
        }

        /**
         * 业务作用：把成员从一个集合移到另一个集合。
         * 移动在服务端原子完成，不会出现两边都有或都没有的中间态。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param src 见方法语义
         * @param dst 见方法语义
         * @param member 集合成员
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void sMoveAsync(Object src, Object dst, Object member) {
            if (src == null || dst == null || member == null) return;
            this.enqueueAsync(OP_SMOVE, serKey(src), serKey(dst), serVal(member), 0);
        }

        /**
         * 业务作用：查询集合的成员数。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void sCardAsync(Object key, LettuceFuture<RedisFuture<Long>> lf) {
            this.enqueueWithFuture(OP_SCARD, serKey(key), null, null, 0L, 0L, null, lf);
        }

        /**
         * 业务作用：判断成员是否在集合中。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param member 集合成员
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void sIsMemberAsync(Object key, Object member, LettuceFuture<RedisFuture<Boolean>> lf) {
            this.enqueueWithFuture(OP_SISMEMBER, serKey(key), null, serVal(member), 0L, 0L, null, lf);
        }

        /**
         * 业务作用：批量判断多个成员是否在集合中。
         * 结果与入参顺序一一对应。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param members 集合成员集合
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void sMIsMemberAsync(Object key, Object[] members, LettuceFuture<RedisFuture<List<Boolean>>> lf) {
            byte[][] ms = new byte[members.length][];
            for (int i = 0; i < members.length; i++) ms[i] = serVal(members[i]);
            this.enqueueWithFuture(OP_SMISMEMBER, serKey(key), null, null, 0L, 0L, ms, lf);
        }

        /**
         * 业务作用：读取集合全部成员。
         * 成员极多时会一次性返回全部内容，应评估返回体量。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void sMembersAsync(Object key, LettuceFuture<RedisFuture<Set<byte[]>>> lf) {
            this.enqueueWithFuture(OP_SMEMBERS, serKey(key), null, null, 0L, 0L, null, lf);
        }

        /**
         * 业务作用：随机弹出集合成员。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void sPopAsync(Object key, LettuceFuture<RedisFuture<byte[]>> lf) {
            this.enqueueWithFuture(OP_SPOP, serKey(key), null, null, 0L, 0L, null, lf);
        }

        /**
         * 业务作用：随机弹出集合成员。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param count 数量上限
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void sPopAsync(Object key, long count, LettuceFuture<RedisFuture<Set<byte[]>>> lf) {
            this.enqueueWithFuture(OP_SPOP_COUNT, serKey(key), null, null, count, 0L, null, lf);
        }

        /**
         * 业务作用：随机读取集合成员但不移除。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void sRandMemberAsync(Object key, LettuceFuture<RedisFuture<byte[]>> lf) {
            this.enqueueWithFuture(OP_SRANDMEMBER, serKey(key), null, null, 0L, 0L, null, lf);
        }

        /**
         * 业务作用：随机读取集合成员但不移除。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param count 数量上限
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void sRandMemberAsync(Object key, long count, LettuceFuture<RedisFuture<List<byte[]>>> lf) {
            this.enqueueWithFuture(OP_SRANDMEMBER_COUNT, serKey(key), null, null, count, 0L, null, lf);
        }

        /**
         * 业务作用：求集合差集。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param keys 缓存键集合
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void sDiffAsync(Object[] keys, LettuceFuture<RedisFuture<Set<byte[]>>> lf) {
            byte[][] ks = new byte[keys.length][];
            for (int i = 0; i < keys.length; i++) ks[i] = serKey(keys[i]);
            this.enqueueWithFuture(OP_SDIFF, null, null, null, 0L, 0L, ks, lf);
        }

        /**
         * 业务作用：求集合交集。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param keys 缓存键集合
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void sInterAsync(Object[] keys, LettuceFuture<RedisFuture<Set<byte[]>>> lf) {
            byte[][] ks = new byte[keys.length][];
            for (int i = 0; i < keys.length; i++) ks[i] = serKey(keys[i]);
            this.enqueueWithFuture(OP_SINTER, null, null, null, 0L, 0L, ks, lf);
        }

        /**
         * 业务作用：求集合并集。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param keys 缓存键集合
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void sUnionAsync(Object[] keys, LettuceFuture<RedisFuture<Set<byte[]>>> lf) {
            byte[][] ks = new byte[keys.length][];
            for (int i = 0; i < keys.length; i++) ks[i] = serKey(keys[i]);
            this.enqueueWithFuture(OP_SUNION, null, null, null, 0L, 0L, ks, lf);
        }

        /* NOTE ------------------- sorted set start -------------------------------------------------------------------- */

        /**
         * 业务作用：向有序集合添加成员并设定分值。
         * 成员已存在时更新其分值。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param score 有序集合分值
         * @param val 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void zAdd(Object key, double score, Object val) {
            if (key == null || val == null) return;
            this.enqueueSync(OP_ZADD, serKey(key), null, serVal(val), Double.doubleToRawLongBits(score));
        }

        /**
         * 业务作用：向有序集合添加成员并设定分值。
         * 成员已存在时更新其分值。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param score 有序集合分值
         * @param val 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void zAddAsync(Object key, double score, Object val) {
            if (key == null || val == null) return;
            this.enqueueAsync(OP_ZADD, serKey(key), null, serVal(val), Double.doubleToRawLongBits(score));
        }

        /**
         * 业务作用：向有序集合添加成员并设定分值。
         * 成员已存在时更新其分值。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param scoreAndVals 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void zAdd(Object key, Object... scoreAndVals) {
            if (key == null) return;
            int pair = scoreAndVals.length >> 1;
            if (pair << 1 != scoreAndVals.length) {
                throw new IllegalArgumentException("ScoresAndValues.length must be a multiple of 2 and contain a sequence of score1, value1, score2, value2, scoreN, valueN");
            }
            for (int i = 0; i < pair; i++) {
                int idx = i << 1;
                Object score = scoreAndVals[idx];
                Object val = scoreAndVals[idx + 1];
                if (val == null) continue;
                double s = (score instanceof Number n) ? n.doubleValue() : Double.parseDouble(score.toString());
                this.zAdd(key, s, val);
            }
        }

        /**
         * 业务作用：向有序集合添加成员并设定分值。
         * 成员已存在时更新其分值。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param scoreAndVals 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void zAddAsync(Object key, Object... scoreAndVals) {
            if (key == null) return;
            int pair = scoreAndVals.length >> 1;
            if (pair << 1 != scoreAndVals.length) {
                throw new IllegalArgumentException("ScoresAndValues.length must be a multiple of 2 and contain a sequence of score1, value1, score2, value2, scoreN, valueN");
            }
            for (int i = 0; i < pair; i++) {
                int idx = i << 1;
                Object score = scoreAndVals[idx];
                Object val = scoreAndVals[idx + 1];
                if (val == null) continue;
                double s = (score instanceof Number n) ? n.doubleValue() : Double.parseDouble(score.toString());
                this.zAddAsync(key, s, val);
            }
        }

        /**
         * 业务作用：从有序集合移除成员。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param val 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void zRem(Object key, Object val) {
            if (key == null || val == null) return;
            this.enqueueSync(OP_ZREM, serKey(key), null, serVal(val), 0);
        }

        /**
         * 业务作用：从有序集合移除成员。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param val 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void zRemAsync(Object key, Object val) {
            if (key == null || val == null) return;
            this.enqueueAsync(OP_ZREM, serKey(key), null, serVal(val), 0);
        }

        /**
         * 业务作用：按字典序区间批量移除成员。
         * 仅当集合内所有成员分值相同时结果才有意义。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param min 区间下界
         * @param max 区间上界
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void zRemRangeByLex(Object key, String min, String max) {
            if (key == null) return;
            byte[] minBytes = Objects.requireNonNull(valueSerializer.serialize(min));
            byte[] maxBytes = Objects.requireNonNull(valueSerializer.serialize(max));
            this.enqueueSync(OP_ZREMRANGE_BY_LEX, serKey(key), minBytes, maxBytes, 0);
        }

        /**
         * 业务作用：按字典序区间批量移除成员。
         * 仅当集合内所有成员分值相同时结果才有意义。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param min 区间下界
         * @param max 区间上界
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void zRemRangeByLexAsync(Object key, String min, String max) {
            if (key == null) return;
            byte[] minBytes = Objects.requireNonNull(valueSerializer.serialize(min));
            byte[] maxBytes = Objects.requireNonNull(valueSerializer.serialize(max));
            this.enqueueAsync(OP_ZREMRANGE_BY_LEX, serKey(key), minBytes, maxBytes, 0);
        }

        /**
         * 业务作用：按排名区间批量移除成员。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param start 起始下标
         * @param end 结束下标
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void zRemRange(Object key, long start, long end) {
            if (key == null) return;
            this.enqueueSync(OP_ZREMRANGE, serKey(key), null, null, start, end, null);
        }

        /**
         * 业务作用：按排名区间批量移除成员。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param start 起始下标
         * @param end 结束下标
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void zRemRangeAsync(Object key, long start, long end) {
            if (key == null) return;
            this.enqueueAsync(OP_ZREMRANGE, serKey(key), null, null, start, end, null);
        }

        /**
         * 业务作用：按分值区间批量移除成员。
         * 常用于按时间戳分值裁剪过期数据。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param min 区间下界
         * @param max 区间上界
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void zRemRangeByScore(Object key, double min, double max) {
            if (key == null) return;
            this.enqueueSync(OP_ZREMRANGE_BY_SCORE, serKey(key), null, null,
                    Double.doubleToRawLongBits(min), Double.doubleToRawLongBits(max), null);
        }

        /**
         * 业务作用：按分值区间批量移除成员。
         * 常用于按时间戳分值裁剪过期数据。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param min 区间下界
         * @param max 区间上界
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void zRemRangeByScoreAsync(Object key, double min, double max) {
            if (key == null) return;
            this.enqueueAsync(OP_ZREMRANGE_BY_SCORE, serKey(key), null, null,
                    Double.doubleToRawLongBits(min), Double.doubleToRawLongBits(max), null);
        }

        /**
         * 业务作用：按分值区间批量移除成员。
         * 常用于按时间戳分值裁剪过期数据。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param rang 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void zRemRangeByScore(Object key, Range<Double> rang) {
            if (key == null) return;
            Range<Double> rg = (rang == null) ? Range.unbounded() : rang;
            Range.Boundary<Double> lower = rg.getLower();
            Range.Boundary<Double> upper = rg.getUpper();
            double min = lower.getValue() != null ? lower.getValue() : Double.NEGATIVE_INFINITY;
            double max = upper.getValue() != null ? upper.getValue() : Double.POSITIVE_INFINITY;
            this.zRemRangeByScore(key, min, max);
        }

        /**
         * 业务作用：按分值区间批量移除成员。
         * 常用于按时间戳分值裁剪过期数据。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param rang 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void zRemRangeByScoreAsync(Object key, Range<Double> rang) {
            if (key == null) return;
            Range<Double> rg = (rang == null) ? Range.unbounded() : rang;
            Range.Boundary<Double> lower = rg.getLower();
            Range.Boundary<Double> upper = rg.getUpper();
            double min = lower.getValue() != null ? lower.getValue() : Double.NEGATIVE_INFINITY;
            double max = upper.getValue() != null ? upper.getValue() : Double.POSITIVE_INFINITY;
            this.zRemRangeByScoreAsync(key, min, max);
        }

        /**
         * 业务作用：按增量原子调整成员分值。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param score 有序集合分值
         * @param value 待写入的值
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public <V> void zIncrBy(Object key, double score, V value) {
            this.enqueueSync(OP_ZINCRBY, serKey(key), null, serVal(value), Double.doubleToRawLongBits(score));
        }

        /**
         * 业务作用：按增量原子调整成员分值。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param score 有序集合分值
         * @param value 待写入的值
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public <V> void zIncrByAsync(Object key, double score, V value) {
            this.enqueueAsync(OP_ZINCRBY, serKey(key), null, serVal(value), Double.doubleToRawLongBits(score));
        }

        /* NOTE ------------------- ZSet 读 / 元数据 (回填 LettuceFuture) ------------------------------------------------- */
        /*
         * ZSet 读命令族: 全部走 enqueueWithFuture, caller 持 LettuceFuture<RedisFuture<T>>, pipeline 内累积,
         * flush 完成后 lf.getFinally() 拿真值. 入参 Object key/member 走 serKey/serVal 自动处理 byte[] 直通,
         * 不再单独提供 byte[] 重载. 读命令不区分 sync/async — 都需要回填 future, async 语义在 pipeline 中没有意义.
         */

        /**
         * 业务作用：查询有序集合的成员数。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void zCardAsync(Object key, LettuceFuture<RedisFuture<Long>> lf) {
            this.enqueueWithFuture(OP_ZCARD, serKey(key), null, null, 0L, 0L, null, lf);
        }

        /**
         * 业务作用：读取成员的分值。
         * 成员不存在时返回空。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param member 集合成员
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void zScoreAsync(Object key, Object member, LettuceFuture<RedisFuture<Double>> lf) {
            this.enqueueWithFuture(OP_ZSCORE, serKey(key), null, serVal(member), 0L, 0L, null, lf);
        }

        /**
         * 业务作用：批量读取多个成员的分值。
         * 结果与入参顺序一一对应，不存在的成员对应位置为空。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param members 集合成员集合
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void zMScoreAsync(Object key, Object[] members, LettuceFuture<RedisFuture<List<Double>>> lf) {
            byte[][] ms = new byte[members.length][];
            for (int i = 0; i < members.length; i++) ms[i] = serVal(members[i]);
            this.enqueueWithFuture(OP_ZMSCORE, serKey(key), null, null, 0L, 0L, ms, lf);
        }

        /**
         * 业务作用：查询成员的正序排名。
         * 成员不存在时返回空。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param member 集合成员
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void zRankAsync(Object key, Object member, LettuceFuture<RedisFuture<Long>> lf) {
            this.enqueueWithFuture(OP_ZRANK, serKey(key), null, serVal(member), 0L, 0L, null, lf);
        }

        /**
         * 业务作用：查询成员的倒序排名。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param member 集合成员
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void zRevRankAsync(Object key, Object member, LettuceFuture<RedisFuture<Long>> lf) {
            this.enqueueWithFuture(OP_ZREVRANK, serKey(key), null, serVal(member), 0L, 0L, null, lf);
        }

        /**
         * 业务作用：统计分值落在给定区间内的成员数。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param min 区间下界
         * @param max 区间上界
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void zCountAsync(Object key, double min, double max, LettuceFuture<RedisFuture<Long>> lf) {
            this.enqueueWithFuture(OP_ZCOUNT_SCORE, serKey(key), null, null, 0L, 0L, Range.create(min, max), lf);
        }

        /**
         * 业务作用：统计分值落在给定区间内的成员数。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param range 见方法语义
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void zCountAsync(Object key, Range<? extends Number> range, LettuceFuture<RedisFuture<Long>> lf) {
            this.enqueueWithFuture(OP_ZCOUNT_SCORE, serKey(key), null, null, 0L, 0L, range, lf);
        }

        /**
         * 业务作用：统计字典序区间内的成员数。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param range 见方法语义
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void zLexCountAsync(Object key, Range<byte[]> range, LettuceFuture<RedisFuture<Long>> lf) {
            this.enqueueWithFuture(OP_ZCOUNT_LEX, serKey(key), null, null, 0L, 0L, range, lf);
        }

        /**
         * 业务作用：按排名区间正序读取成员。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param start 起始下标
         * @param end 结束下标
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void zRangeAsync(Object key, long start, long end, LettuceFuture<RedisFuture<List<byte[]>>> lf) {
            this.enqueueWithFuture(OP_ZRANGE, serKey(key), null, null, start, end, null, lf);
        }

        /**
         * 业务作用：按排名区间倒序读取成员。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param start 起始下标
         * @param end 结束下标
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void zRevRangeAsync(Object key, long start, long end, LettuceFuture<RedisFuture<List<byte[]>>> lf) {
            this.enqueueWithFuture(OP_ZREVRANGE, serKey(key), null, null, start, end, null, lf);
        }

        /**
         * 业务作用：按排名区间正序读取成员及其分值。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param start 起始下标
         * @param end 结束下标
         * @param lf 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void zRangeWithScoresAsync(Object key, long start, long end,
                                          LettuceFuture<RedisFuture<List<ScoredValue<byte[]>>>> lf) {
            this.enqueueWithFuture(OP_ZRANGE_WITHSCORES, serKey(key), null, null, start, end, null, lf);
        }

        /**
         * 业务作用：按排名区间倒序读取成员及其分值。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param start 起始下标
         * @param end 结束下标
         * @param lf 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void zRevRangeWithScoresAsync(Object key, long start, long end,
                                             LettuceFuture<RedisFuture<List<ScoredValue<byte[]>>>> lf) {
            this.enqueueWithFuture(OP_ZREVRANGE_WITHSCORES, serKey(key), null, null, start, end, null, lf);
        }

        /**
         * 业务作用：按分值区间正序读取成员。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param range 见方法语义
         * @param lf 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void zRangeByScoreAsync(Object key, Range<? extends Number> range,
                                       LettuceFuture<RedisFuture<List<byte[]>>> lf) {
            this.enqueueWithFuture(OP_ZRANGEBYSCORE, serKey(key), null, null, 0L, 0L, range, lf);
        }

        /**
         * 业务作用：按分值区间正序读取成员。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param range 见方法语义
         * @param limit 数量上限
         * @param lf 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void zRangeByScoreAsync(Object key, Range<? extends Number> range, Limit limit,
                                       LettuceFuture<RedisFuture<List<byte[]>>> lf) {
            this.enqueueWithFuture(OP_ZRANGEBYSCORE_LIMIT, serKey(key), limit, null, 0L, 0L, range, lf);
        }

        /**
         * 业务作用：按分值区间正序读取成员及其分值。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param range 见方法语义
         * @param lf 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void zRangeByScoreWithScoresAsync(Object key, Range<? extends Number> range,
                                                 LettuceFuture<RedisFuture<List<ScoredValue<byte[]>>>> lf) {
            this.enqueueWithFuture(OP_ZRANGEBYSCORE_WITHSCORES, serKey(key), null, null, 0L, 0L, range, lf);
        }

        /**
         * 业务作用：按分值区间正序读取成员及其分值。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param range 见方法语义
         * @param limit 数量上限
         * @param lf 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void zRangeByScoreWithScoresAsync(Object key, Range<? extends Number> range, Limit limit,
                                                 LettuceFuture<RedisFuture<List<ScoredValue<byte[]>>>> lf) {
            this.enqueueWithFuture(OP_ZRANGEBYSCORE_WITHSCORES_LIMIT, serKey(key), limit, null, 0L, 0L, range, lf);
        }

        /**
         * 业务作用：按分值区间倒序读取成员。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param range 见方法语义
         * @param lf 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void zRevRangeByScoreAsync(Object key, Range<? extends Number> range,
                                          LettuceFuture<RedisFuture<List<byte[]>>> lf) {
            this.enqueueWithFuture(OP_ZREVRANGEBYSCORE, serKey(key), null, null, 0L, 0L, range, lf);
        }

        /**
         * 业务作用：按分值区间倒序读取成员。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param range 见方法语义
         * @param limit 数量上限
         * @param lf 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void zRevRangeByScoreAsync(Object key, Range<? extends Number> range, Limit limit,
                                          LettuceFuture<RedisFuture<List<byte[]>>> lf) {
            this.enqueueWithFuture(OP_ZREVRANGEBYSCORE_LIMIT, serKey(key), limit, null, 0L, 0L, range, lf);
        }

        /**
         * 业务作用：按分值区间倒序读取成员及其分值。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param range 见方法语义
         * @param lf 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void zRevRangeByScoreWithScoresAsync(Object key, Range<? extends Number> range,
                                                    LettuceFuture<RedisFuture<List<ScoredValue<byte[]>>>> lf) {
            this.enqueueWithFuture(OP_ZREVRANGEBYSCORE_WITHSCORES, serKey(key), null, null, 0L, 0L, range, lf);
        }

        /**
         * 业务作用：按分值区间倒序读取成员及其分值。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param range 见方法语义
         * @param limit 数量上限
         * @param lf 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void zRevRangeByScoreWithScoresAsync(Object key, Range<? extends Number> range, Limit limit,
                                                    LettuceFuture<RedisFuture<List<ScoredValue<byte[]>>>> lf) {
            this.enqueueWithFuture(OP_ZREVRANGEBYSCORE_WITHSCORES_LIMIT, serKey(key), limit, null, 0L, 0L, range, lf);
        }

        /**
         * 业务作用：按字典序区间读取成员。
         * 仅当集合内所有成员分值相同时结果才有意义。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param range 见方法语义
         * @param lf 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void zRangeByLexAsync(Object key, Range<byte[]> range,
                                     LettuceFuture<RedisFuture<List<byte[]>>> lf) {
            this.enqueueWithFuture(OP_ZRANGEBYLEX, serKey(key), null, null, 0L, 0L, range, lf);
        }

        /**
         * 业务作用：按字典序区间读取成员。
         * 仅当集合内所有成员分值相同时结果才有意义。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param key 缓存键
         * @param range 见方法语义
         * @param limit 数量上限
         * @param lf 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void zRangeByLexAsync(Object key, Range<byte[]> range, Limit limit,
                                     LettuceFuture<RedisFuture<List<byte[]>>> lf) {
            this.enqueueWithFuture(OP_ZRANGEBYLEX_LIMIT, serKey(key), limit, null, 0L, 0L, range, lf);
        }

        /**
         * 业务作用：弹出分值最小的成员。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void zPopMinAsync(Object key, LettuceFuture<RedisFuture<ScoredValue<byte[]>>> lf) {
            this.enqueueWithFuture(OP_ZPOPMIN, serKey(key), null, null, 0L, 0L, null, lf);
        }

        /**
         * 业务作用：弹出分值最小的成员。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param count 数量上限
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void zPopMinAsync(Object key, long count, LettuceFuture<RedisFuture<List<ScoredValue<byte[]>>>> lf) {
            this.enqueueWithFuture(OP_ZPOPMIN_COUNT, serKey(key), null, null, count, 0L, null, lf);
        }

        /**
         * 业务作用：弹出分值最大的成员。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void zPopMaxAsync(Object key, LettuceFuture<RedisFuture<ScoredValue<byte[]>>> lf) {
            this.enqueueWithFuture(OP_ZPOPMAX, serKey(key), null, null, 0L, 0L, null, lf);
        }

        /**
         * 业务作用：弹出分值最大的成员。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param count 数量上限
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void zPopMaxAsync(Object key, long count, LettuceFuture<RedisFuture<List<ScoredValue<byte[]>>>> lf) {
            this.enqueueWithFuture(OP_ZPOPMAX_COUNT, serKey(key), null, null, count, 0L, null, lf);
        }

        /**
         * 业务作用：随机读取有序集合成员。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void zRandMemberAsync(Object key, LettuceFuture<RedisFuture<byte[]>> lf) {
            this.enqueueWithFuture(OP_ZRANDMEMBER, serKey(key), null, null, 0L, 0L, null, lf);
        }

        /**
         * 业务作用：随机读取有序集合成员。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param key 缓存键
         * @param count 数量上限
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void zRandMemberAsync(Object key, long count, LettuceFuture<RedisFuture<List<byte[]>>> lf) {
            this.enqueueWithFuture(OP_ZRANDMEMBER_COUNT, serKey(key), null, null, count, 0L, null, lf);
        }

        /* NOTE ------------------- script start ------------------------------------------------------------------------ */

        /**
         * 业务作用：清空服务端的 Lua 脚本缓存。
         * 此后所有按摘要执行都会失败，直至脚本重新载入。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * <p>参数说明: 无。
         *
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void scriptFlush() {
            this.enqueueSync(OP_SCRIPT_FLUSH, null, null, null, 0);
        }

        /**
         * 业务作用：清空服务端的 Lua 脚本缓存。
         * 此后所有按摘要执行都会失败，直至脚本重新载入。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * <p>参数说明: 无。
         *
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void scriptFlushAsync() {
            this.enqueueAsync(OP_SCRIPT_FLUSH, null, null, null, 0);
        }

        /**
         * 业务作用：终止正在执行的 Lua 脚本。
         * 已产生写入的脚本无法被终止，只能等待其完成或重启实例。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * <p>参数说明: 无。
         *
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void scriptKill() {
            this.enqueueSync(OP_SCRIPT_KILL, null, null, null, 0);
        }

        /**
         * 业务作用：终止正在执行的 Lua 脚本。
         * 已产生写入的脚本无法被终止，只能等待其完成或重启实例。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * <p>参数说明: 无。
         *
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void scriptKillAsync() {
            this.enqueueAsync(OP_SCRIPT_KILL, null, null, null, 0);
        }

        /**
         * 业务作用：执行 Lua 脚本。
         * 脚本在服务端原子执行，期间不会有其它命令插入，是实现复合原子操作的手段。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param script Lua 脚本
         * @param keys 缓存键集合
         * @param args 脚本参数
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void eval(String script, String[] keys, Object... args) {
            Objects.requireNonNull(script);
            byte[][] ks = serializeKeys(keys);
            byte[][] as = serializeArgs(args);
            byte[] s = Objects.requireNonNull(RedisSerializer.string().serialize(script));
            this.enqueueSync(OP_EVAL_ASYNC, null, null, null, 0, 0, new Object[]{s, ks, as});
        }

        /**
         * 业务作用：执行 Lua 脚本。
         * 脚本在服务端原子执行，期间不会有其它命令插入，是实现复合原子操作的手段。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param script Lua 脚本
         * @param keys 缓存键集合
         * @param args 脚本参数
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void evalAsync(String script, String[] keys, Object... args) {
            Objects.requireNonNull(script);
            byte[][] ks = serializeKeys(keys);
            byte[][] as = serializeArgs(args);
            byte[] s = Objects.requireNonNull(RedisSerializer.string().serialize(script));
            this.enqueueAsync(OP_EVAL_ASYNC, null, null, null, 0, 0, new Object[]{s, ks, as});
        }

        /**
         * 业务作用：批量序列化脚本的键参数。
         *
         * @param keys 键数组
         * @return 序列化后的字节数组。
         */
        private byte[][] serializeKeys(String[] keys) {
            if (ColUtils.isEmpty(keys)) return EMPTY_BYTE2;
            byte[][] ks = new byte[keys.length][];
            for (int i = 0; i < keys.length; i++) {
                ks[i] = keySerializer.serialize(keys[i]);
            }
            return ks;
        }

        /**
         * 业务作用：批量序列化脚本的其余参数。
         *
         * @param args 参数数组
         * @return 序列化后的字节数组。
         */
        private byte[][] serializeArgs(Object[] args) {
            if (ColUtils.isEmpty(args)) return EMPTY_BYTE2;
            byte[][] as = new byte[args.length][];
            for (int i = 0; i < args.length; i++) {
                as[i] = RedisSerializer.string().serialize(String.valueOf(args[i]));
            }
            return as;
        }

        /* NOTE ------------------- publish/subscribe start ------------------------------------------------------------- */

        /**
         * 业务作用：向 Stream 发布一条业务事件。
         * 事件由订阅方按消费组读取；Stream 需配合长度裁剪，否则无限增长。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param channel 频道名
         * @param message 消息体
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void pub(String channel, Object message) {
            if (StringUtils.isBlank(channel)) return;
            byte[] cnl = RedisSerializer.string().serialize(channel);
            byte[] msg = message instanceof byte[] bs ? bs : valueSerializer.serialize(message);
            this.enqueueSync(OP_PUB, cnl, null, msg, 0);
        }

        /**
         * 业务作用：向 Stream 发布一条业务事件。
         * 事件由订阅方按消费组读取；Stream 需配合长度裁剪，否则无限增长。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param channel 频道名
         * @param message 消息体
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void pubAsync(String channel, Object message) {
            if (StringUtils.isBlank(channel)) return;
            byte[] cnl = RedisSerializer.string().serialize(channel);
            byte[] msg = message instanceof byte[] bs ? bs : valueSerializer.serialize(message);
            this.enqueueAsync(OP_PUB, cnl, null, msg, 0);
        }

        /* NOTE ------------------- stream start ------------------------------------------------------------------------ */

        /**
         * 业务作用：向频道发布消息。
         * 订阅是即时的，发布时没有订阅者则消息直接丢弃，不做任何保留。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param stream Stream 键
         * @param message 消息体
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void publish(Object stream, Object message) {
            this.publish(stream, RedisProxy.STREAM_EVENT, message);
        }

        /**
         * 业务作用：向频道发布消息。
         * 订阅是即时的，发布时没有订阅者则消息直接丢弃，不做任何保留。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param stream Stream 键
         * @param message 消息体
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void publishAsync(Object stream, Object message) {
            this.publishAsync(stream, RedisProxy.STREAM_EVENT, message);
        }

        /**
         * 业务作用：向频道发布消息。
         * 订阅是即时的，发布时没有订阅者则消息直接丢弃，不做任何保留。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param stream Stream 键
         * @param event 事件名
         * @param message 消息体
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void publish(Object stream, Object event, Object message) {
            // 在 borrow PooledEvtData 前校验 (event 即 xAdd 的 field), 避免无意义借还
            if (this.invalidXAddArg(stream, event, message)) return;
            PooledEvtData pm = PooledEvtData.of();
            RecycleLinkedMap<String, Object> pt = RedisProxyHolder.passthrough();
            try {
                pm.setTopic(stream instanceof byte[] bs ? keySerializer.deserialize(bs) : (String) stream);
                pm.setData(message);
                pm.setPassthrough(pt);
                this.xAdd(stream, event, pm);
            } finally {
                pm.recycle();
                // PooledEvtData.restore 不再 cascade, caller 显式归还 passthrough 到池
                if (pt != null) pt.recycle();
            }
        }

        /**
         * 业务作用：向频道发布消息。
         * 订阅是即时的，发布时没有订阅者则消息直接丢弃，不做任何保留。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param stream Stream 键
         * @param event 事件名
         * @param message 消息体
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void publishAsync(Object stream, Object event, Object message) {
            if (this.invalidXAddArg(stream, event, message)) return;
            PooledEvtData pm = PooledEvtData.of();
            RecycleLinkedMap<String, Object> pt = RedisProxyHolder.passthrough();
            try {
                pm.setTopic(stream instanceof byte[] bs ? keySerializer.deserialize(bs) : (String) stream);
                pm.setData(message);
                pm.setPassthrough(pt);
                this.xAddAsync(stream, event, pm);
            } finally {
                pm.recycle();
                if (pt != null) pt.recycle();
            }
        }

        /**
         * 业务作用：向 Stream 追加一条消息。
         * 追加后由消费组按各自位点读取；Stream 需配合长度裁剪，否则会无限增长。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param stream Stream 键
         * @param message 消息体
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void xAdd(Object stream, Object message) {
            this.xAdd(stream, RedisProxy.STREAM_EVENT, message);
        }

        /**
         * 业务作用：向 Stream 追加一条消息。
         * 追加后由消费组按各自位点读取；Stream 需配合长度裁剪，否则会无限增长。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param stream Stream 键
         * @param message 消息体
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void xAddAsync(Object stream, Object message) {
            this.xAddAsync(stream, RedisProxy.STREAM_EVENT, message);
        }

        /**
         * 业务作用：向 Stream 追加一条消息。
         * 追加后由消费组按各自位点读取；Stream 需配合长度裁剪，否则会无限增长。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param stream Stream 键
         * @param field 哈希字段名
         * @param message 消息体
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void xAdd(Object stream, Object field, Object message) {
            // 与 RedisProxy.xAdd 单条对齐: stream/field blank(String) 或 null、message null 直接 return (Redis Stream 不接受空 key/field/value)
            if (this.invalidXAddArg(stream, field, message)) return;
            byte[] s = stream instanceof byte[] bs ? bs : keySerializer.serialize((String) stream);
            byte[] f = field instanceof byte[] bs ? bs : hashKeySerializer.serialize((String) field);
            byte[] v = message instanceof byte[] bs ? bs : hashValueSerializer.serialize(message);
            this.enqueueSync(OP_XADD, s, f, v, 0);
        }

        /**
         * 业务作用：向 Stream 追加一条消息。
         * 追加后由消费组按各自位点读取；Stream 需配合长度裁剪，否则会无限增长。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param stream Stream 键
         * @param field 哈希字段名
         * @param message 消息体
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void xAddAsync(Object stream, Object field, Object message) {
            if (this.invalidXAddArg(stream, field, message)) return;
            byte[] s = stream instanceof byte[] bs ? bs : keySerializer.serialize((String) stream);
            byte[] f = field instanceof byte[] bs ? bs : hashKeySerializer.serialize((String) field);
            byte[] v = message instanceof byte[] bs ? bs : hashValueSerializer.serialize(message);
            this.enqueueAsync(OP_XADD, s, f, v, 0);
        }

        /**
         * 业务作用：单条 xAdd 入参非法判定: stream/field/message 任一 null, 或 stream/field 为空白 String → 非法.
         * (stream/field/message 允许 byte[] raw 直传, 此时不判空白.)
         *
         * @param stream  Stream 键
         * @param field   哈希字段名
         * @param message 消息体
         * @return 见上述说明。
         */
        private boolean invalidXAddArg(Object stream, Object field, Object message) {
            if (stream == null || field == null || message == null) return true;
            if (stream instanceof String s && StringUtils.isBlank(s)) return true;
            return field instanceof String f && StringUtils.isBlank(f);
        }

        /**
         * 业务作用：向 Stream 追加一条消息。
         * 追加后由消费组按各自位点读取；Stream 需配合长度裁剪，否则会无限增长。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param stream Stream 键
         * @param fieldAndMessages 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void xAdd(Object stream, Object... fieldAndMessages) {
            this.xAddImpl(true, stream, fieldAndMessages);
        }

        /**
         * 业务作用：向 Stream 追加一条消息。
         * 追加后由消费组按各自位点读取；Stream 需配合长度裁剪，否则会无限增长。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param stream Stream 键
         * @param fieldAndMessages 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void xAddAsync(Object stream, Object... fieldAndMessages) {
            this.xAddImpl(false, stream, fieldAndMessages);
        }

        /**
         * 业务作用：追加 Stream 消息的内部实现，按字段个数分派到单字段或多字段两条路径。
         * 单字段用固定槽位承载，多字段用额外参数槽承载一个映射——多字段情形在业务中占比低，
         * 为它把固定槽位加宽会让所有命令都多占内存。
         *
         * @param sync             true 表示等待执行结果
         * @param stream           Stream 键
         * @param fieldAndMessages 交替排列的字段名与内容
         * 返回: 无返回值。
         */
        private void xAddImpl(boolean sync, Object stream, Object... fieldAndMessages) {
            if (stream == null) return;
            // XADD 至少要一组 field/value; 0 个 pair 会构造无效命令 (空 XADD), fail-fast 暴露调用方错误
            if (fieldAndMessages == null || fieldAndMessages.length == 0) {
                throw new IllegalArgumentException("xAdd fieldAndMessages must contain at least one field/value pair");
            }
            int pair = fieldAndMessages.length >> 1;
            if (pair << 1 != fieldAndMessages.length) {
                throw new IllegalArgumentException("fieldAndMessages.length must be a multiple of 2 and contain a sequence of field1, message1, field2, message2, fieldN, messageN");
            }
            // 分配池化 map 前先校验所有 field/message 非 null: XADD 多 field 是同一 entry, 缺字段会改变业务结构,
            // 故 fail-fast (不静默过滤); 同时避免后续构建 map 中途 NPE 导致已 of() 的池化 map 泄漏.
            for (int i = 0; i < fieldAndMessages.length; i++) {
                if (fieldAndMessages[i] == null) {
                    throw new IllegalArgumentException("xAdd field/message must not be null at index " + i);
                }
            }
            byte[] s = stream instanceof byte[] bs ? bs : keySerializer.serialize((String) stream);
            // 池化 map: 正常 dispatch 后由 doFlushSingle (nonOpen) 或 CmdBuffer.restore (open) 自动回收;
            // 构建过程 (序列化) 若异常, try/catch 就地 recycle 后再抛, 防泄漏.
            RecycleLinkedMap<byte[], byte[]> map = RecycleLinkedMap.of();
            try {
                for (int i = 0; i < pair; i++) {
                    int index = i << 1;
                    byte[] hk = hashKeySerializer.serialize(fieldAndMessages[index++].toString());
                    Object msg = fieldAndMessages[index];
                    byte[] v = msg instanceof byte[] bs ? bs : hashValueSerializer.serialize(msg);
                    map.put(hk, v);
                }
            } catch (Throwable ex) {
                map.recycle();
                throw ex;
            }
            if (sync) this.enqueueSync(OP_XADD_MULTI, s, null, null, 0, 0, map);
            else this.enqueueAsync(OP_XADD_MULTI, s, null, null, 0, 0, map);
        }

        /**
         * 业务作用：按最大条数裁剪 Stream。
         * 被裁掉的消息永久丢失，包括尚未被消费的。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param stream Stream 键
         * @param maxlen 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void xTrimMaxlen(Object stream, long maxlen) {
            if (stream == null || maxlen < 1) return;
            byte[] s = stream instanceof byte[] bs ? bs : keySerializer.serialize((String) stream);
            this.enqueueSync(OP_XTRIM_MAXLEN, s, null, null, maxlen);
        }

        /**
         * 业务作用：按最大条数裁剪 Stream。
         * 被裁掉的消息永久丢失，包括尚未被消费的。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param stream Stream 键
         * @param maxlen 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void xTrimMaxlenAsync(Object stream, long maxlen) {
            if (stream == null || maxlen < 1) return;
            byte[] s = stream instanceof byte[] bs ? bs : keySerializer.serialize((String) stream);
            this.enqueueAsync(OP_XTRIM_MAXLEN, s, null, null, maxlen);
        }

        /**
         * 业务作用：按最小条目标识裁剪 Stream。
         * 早于该标识的消息永久丢失。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param stream Stream 键
         * @param millis 毫秒数
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void xTrimMinId(Object stream, long millis) {
            this.xTrimMinId(stream, millis + "-0");
        }

        /**
         * 业务作用：按最小条目标识裁剪 Stream。
         * 早于该标识的消息永久丢失。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param stream Stream 键
         * @param millis 毫秒数
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void xTrimMinIdAsync(Object stream, long millis) {
            this.xTrimMinIdAsync(stream, millis + "-0");
        }

        /**
         * 业务作用：按最小条目标识裁剪 Stream。
         * 早于该标识的消息永久丢失。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param stream Stream 键
         * @param minId 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void xTrimMinId(Object stream, String minId) {
            if (stream == null || StringUtils.isBlank(minId)) return;
            byte[] s = stream instanceof byte[] bs ? bs : keySerializer.serialize((String) stream);
            this.enqueueSync(OP_XTRIM_MINID, s, minId, null, 0);
        }

        /**
         * 业务作用：按最小条目标识裁剪 Stream。
         * 早于该标识的消息永久丢失。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param stream Stream 键
         * @param minId 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void xTrimMinIdAsync(Object stream, String minId) {
            if (stream == null || StringUtils.isBlank(minId)) return;
            byte[] s = stream instanceof byte[] bs ? bs : keySerializer.serialize((String) stream);
            this.enqueueAsync(OP_XTRIM_MINID, s, minId, null, 0);
        }

        /**
         * 业务作用：按条目标识删除 Stream 消息。
         * 已被消费组读取但未确认的条目删除后不会再投递。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param stream Stream 键
         * @param messageId 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void xDel(Object stream, String messageId) {
            if (stream == null || StringUtils.isBlank(messageId)) return;
            byte[] s = stream instanceof byte[] bs ? bs : keySerializer.serialize((String) stream);
            this.enqueueSync(OP_XDEL, s, messageId, null, 0);
        }

        /**
         * 业务作用：按条目标识删除 Stream 消息。
         * 已被消费组读取但未确认的条目删除后不会再投递。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param stream Stream 键
         * @param messageId 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void xDelAsync(Object stream, String messageId) {
            if (stream == null || StringUtils.isBlank(messageId)) return;
            byte[] s = stream instanceof byte[] bs ? bs : keySerializer.serialize((String) stream);
            this.enqueueAsync(OP_XDEL, s, messageId, null, 0);
        }

        /**
         * 业务作用：确认消费组已处理某条消息。
         * 不确认的消息会留在待处理列表中，被空闲接管机制重新投递。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param stream Stream 键
         * @param group 消费组名
         * @param ids 条目标识集合
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void ack(Object stream, Object group, String... ids) {
            Objects.requireNonNull(stream);
            Objects.requireNonNull(group);
            if (ids == null || ids.length == 0) return;
            byte[] s = stream instanceof byte[] bs ? bs : keySerializer.serialize((String) stream);
            byte[] g = group instanceof byte[] bs ? bs : keySerializer.serialize((String) group);
            for (String id : ids) {
                if (StringUtils.isBlank(id)) continue;
                this.enqueueSync(OP_XACK, s, g, id, 0);
            }
        }

        /**
         * 业务作用：确认消费组已处理某条消息。
         * 不确认的消息会留在待处理列表中，被空闲接管机制重新投递。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param stream Stream 键
         * @param group 消费组名
         * @param ids 条目标识集合
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void ackAsync(Object stream, Object group, String... ids) {
            Objects.requireNonNull(stream);
            Objects.requireNonNull(group);
            if (ids == null || ids.length == 0) return;
            byte[] s = stream instanceof byte[] bs ? bs : keySerializer.serialize((String) stream);
            byte[] g = group instanceof byte[] bs ? bs : keySerializer.serialize((String) group);
            for (String id : ids) {
                if (StringUtils.isBlank(id)) continue;
                this.enqueueAsync(OP_XACK, s, g, id, 0);
            }
        }

        // ==================== 分区发布 (暂未接入, 等删 LettucePipeline 时由 RedisPartition 改签名接入) ====================
        //
        // 现状: RedisPartition.load(redisProxy).pipeline(LettucePipeline.Actuator, ...) 形参绑定老 Actuator,
        // ---- partition 委托 RedisPartition (topic 必须 String, 会被拼接路由) ----

        /**
         * 业务作用：按分区键把事件发布到对应的分区 Stream。
         * 同一分区键恒落到同一分区，从而保证该键的事件被同一个消费者按序处理。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param topic 主题名
         * @param partitionKey 分区键，决定落到哪个分区
         * @param data 业务数据
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void partition(String topic, long partitionKey, Object data) {
            RedisPartition.load(redisProxy).pipeline(this, topic, partitionKey, data);
        }

        /**
         * 业务作用：按分区键把事件发布到对应的分区 Stream。
         * 同一分区键恒落到同一分区，从而保证该键的事件被同一个消费者按序处理。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param topic 主题名
         * @param partitionKey 分区键，决定落到哪个分区
         * @param data 业务数据
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void partition(String topic, String partitionKey, Object data) {
            RedisPartition.load(redisProxy).pipeline(this, topic, partitionKey, data);
        }

        /**
         * 业务作用：按分区键把事件发布到对应的分区 Stream。
         * 同一分区键恒落到同一分区，从而保证该键的事件被同一个消费者按序处理。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param topic 主题名
         * @param event 事件名
         * @param partitionKey 分区键，决定落到哪个分区
         * @param data 业务数据
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void partition(String topic, String event, long partitionKey, Object data) {
            RedisPartition.load(redisProxy).pipeline(this, topic, event, partitionKey, data);
        }

        /**
         * 业务作用：按分区键把事件发布到对应的分区 Stream。
         * 同一分区键恒落到同一分区，从而保证该键的事件被同一个消费者按序处理。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param topic 主题名
         * @param event 事件名
         * @param partitionKey 分区键，决定落到哪个分区
         * @param data 业务数据
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void partition(String topic, String event, String partitionKey, Object data) {
            RedisPartition.load(redisProxy).pipeline(this, topic, event, partitionKey, data);
        }

        /**
         * 业务作用：按分区键把事件发布到对应的分区 Stream。
         * 同一分区键恒落到同一分区，从而保证该键的事件被同一个消费者按序处理。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param topic 主题名
         * @param partitionKey 分区键，决定落到哪个分区
         * @param data 业务数据
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void partitionAsync(String topic, long partitionKey, Object data) {
            RedisPartition.load(redisProxy).pipelineAsync(this, topic, partitionKey, data);
        }

        /**
         * 业务作用：按分区键把事件发布到对应的分区 Stream。
         * 同一分区键恒落到同一分区，从而保证该键的事件被同一个消费者按序处理。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param topic 主题名
         * @param partitionKey 分区键，决定落到哪个分区
         * @param data 业务数据
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void partitionAsync(String topic, String partitionKey, Object data) {
            RedisPartition.load(redisProxy).pipelineAsync(this, topic, partitionKey, data);
        }

        /**
         * 业务作用：按分区键把事件发布到对应的分区 Stream。
         * 同一分区键恒落到同一分区，从而保证该键的事件被同一个消费者按序处理。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param topic 主题名
         * @param event 事件名
         * @param partitionKey 分区键，决定落到哪个分区
         * @param data 业务数据
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void partitionAsync(String topic, String event, long partitionKey, Object data) {
            RedisPartition.load(redisProxy).pipelineAsync(this, topic, event, partitionKey, data);
        }

        /**
         * 业务作用：按分区键把事件发布到对应的分区 Stream。
         * 同一分区键恒落到同一分区，从而保证该键的事件被同一个消费者按序处理。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param topic 主题名
         * @param event 事件名
         * @param partitionKey 分区键，决定落到哪个分区
         * @param data 业务数据
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void partitionAsync(String topic, String event, String partitionKey, Object data) {
            RedisPartition.load(redisProxy).pipelineAsync(this, topic, event, partitionKey, data);
        }

        /* NOTE ------------------- RediSearch / RedisJSON start ----------------------------------------------------- */
        /*
         * 设计原则:
         *   - 业务侧 API 都是 entity / (Class, id) / String index — 不接受 byte[], 避免业务手拼 key 跟 RediSearch.save
         *     写出来的 key 不一致 (撤单/查询找不到的隐患).
         *   - byte[] 版本是 private 内部入口, 走 enqueueSync / enqueueAsync / enqueueWithFuture; key bytes
         *     一律由 redisProxy.rediSearch().keyBytes(...) / jsonBytes(...) 算出, 跟 RediSearch.save 写出的 key 一致.
         */

        // ---- byte[] private 底层入口 (entity / String 重载调) ----

        /**
         * 业务作用：以已序列化的字节直接写入 JSON 路径，不等待结果。
         * 参数在调用前已完成序列化，避免在批次热路径上重复转换。
         *
         * @param key   缓存键
         * @param path  JSON 路径
         * @param value 待写入的内容
         * 返回: 无返回值。
         */
        private void jsonSetAsyncBytes(byte[] key, byte[] path, byte[] value) {
            if (key == null) return;
            this.enqueueAsync(OP_JSON_SET, key, path, value, 0L);
        }

        /**
         * 业务作用：以已序列化的字节直接写入 JSON 路径，等待结果并可感知失败。
         * 参数在调用前已完成序列化，避免在批次热路径上重复转换。
         *
         * @param key   缓存键
         * @param path  JSON 路径
         * @param value 待写入的内容
         * 返回: 无返回值。
         */
        private void jsonSetSyncBytes(byte[] key, byte[] path, byte[] value) {
            if (key == null) return;
            this.enqueueSync(OP_JSON_SET, key, path, value, 0L);
        }

        /**
         * 业务作用：以已序列化的字节直接写入 JSON 字段，不等待结果。
         * 参数在调用前已完成序列化，避免在批次热路径上重复转换。
         *
         * @param key     缓存键
         * @param subpath JSON 子路径
         * @param value   待写入的内容
         * 返回: 无返回值。
         */
        private void jsonSetFieldAsyncBytes(byte[] key, byte[] subpath, byte[] value) {
            if (key == null) return;
            this.enqueueAsync(OP_JSON_SET_FIELD, key, subpath, value, 0L);
        }

        /**
         * 业务作用：以已序列化的字节直接写入 JSON 字段，等待结果并可感知失败。
         * 参数在调用前已完成序列化，避免在批次热路径上重复转换。
         *
         * @param key     缓存键
         * @param subpath JSON 子路径
         * @param value   待写入的内容
         * 返回: 无返回值。
         */
        private void jsonSetFieldSyncBytes(byte[] key, byte[] subpath, byte[] value) {
            if (key == null) return;
            this.enqueueSync(OP_JSON_SET_FIELD, key, subpath, value, 0L);
        }

        /**
         * 业务作用：以已序列化的字节直接调整 JSON 数值字段，不等待结果。
         * 参数在调用前已完成序列化，避免在批次热路径上重复转换。
         *
         * @param key   缓存键
         * @param path  JSON 路径
         * @param delta 增减量
         * 返回: 无返回值。
         */
        private void jsonNumIncrByAsyncBytes(byte[] key, byte[] path, byte[] delta) {
            if (key == null) return;
            this.enqueueAsync(OP_JSON_NUMINCRBY, key, path, delta, 0L);
        }

        /**
         * 业务作用：以已序列化的字节直接调整 JSON 数值字段，等待结果并可感知失败。
         * 参数在调用前已完成序列化，避免在批次热路径上重复转换。
         *
         * @param key   缓存键
         * @param path  JSON 路径
         * @param delta 增减量
         * 返回: 无返回值。
         */
        private void jsonNumIncrBySyncBytes(byte[] key, byte[] path, byte[] delta) {
            if (key == null) return;
            this.enqueueSync(OP_JSON_NUMINCRBY, key, path, delta, 0L);
        }

        /**
         * 业务作用：以已序列化的字节直接删除 JSON 文档，不等待结果。
         * 参数在调用前已完成序列化，避免在批次热路径上重复转换。
         *
         * @param key 缓存键
         * 返回: 无返回值。
         */
        private void jsonDelAsyncBytes(byte[] key) {
            if (key == null) return;
            this.enqueueAsync(OP_JSON_DEL, key, null, null, 0L);
        }

        /**
         * 业务作用：以已序列化的字节直接删除 JSON 文档，等待结果并可感知失败。
         * 参数在调用前已完成序列化，避免在批次热路径上重复转换。
         *
         * @param key 缓存键
         * 返回: 无返回值。
         */
        private void jsonDelSyncBytes(byte[] key) {
            if (key == null) return;
            this.enqueueSync(OP_JSON_DEL, key, null, null, 0L);
        }

        /**
         * 业务作用：以已序列化的字节读取 JSON 内容，结果经由给定的结果句柄回传。
         *
         * @param key  缓存键
         * @param path JSON 路径
         * @param lf   承接结果的句柄
         * 返回: 无返回值。
         */
        private void jsonGetAsyncBytes(byte[] key, byte[] path, LettuceFuture<RedisFuture<byte[]>> lf) {
            this.enqueueWithFuture(OP_JSON_GET, key, path, null, 0L, 0L, null, lf);
        }

        // ---- entity / Class+id 公开入口 (JSON / RedisJSON) ----

        /**
         * 业务作用：写入 JSON 文档或其中的某个路径。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param entity 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void jsonSetAsync(Object entity) {
            if (entity == null) return;
            RediSearch rs = redisProxy.rediSearch();
            this.jsonSetAsyncBytes(rs.keyBytes(entity), RediSearch.DOLLAR_PATH, rs.jsonBytes(entity));
        }

        /**
         * 业务作用：写入 JSON 文档或其中的某个路径。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param entity 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void jsonSet(Object entity) {
            if (entity == null) return;
            RediSearch rs = redisProxy.rediSearch();
            this.jsonSetSyncBytes(rs.keyBytes(entity), RediSearch.DOLLAR_PATH, rs.jsonBytes(entity));
        }

        /**
         * 业务作用：删除 JSON 文档中的某个路径。
         * 路径不存在时不报错。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param type     实体类型，据其键模板定位目标文档
         * @param id       实体主键
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void jsonDelAsync(Class<?> type, Object id) {
            if (type == null || id == null) return;
            this.jsonDelAsyncBytes(redisProxy.rediSearch().keyBytes(type, id));
        }

        /**
         * 业务作用：删除 JSON 文档中的某个路径。
         * 路径不存在时不报错。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param type     实体类型，据其键模板定位目标文档
         * @param id       实体主键
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void jsonDel(Class<?> type, Object id) {
            if (type == null || id == null) return;
            this.jsonDelSyncBytes(redisProxy.rediSearch().keyBytes(type, id));
        }

        /**
         * 业务作用：删除 JSON 文档中的某个路径。
         * 路径不存在时不报错。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param type     实体类型，据其键模板定位目标文档
         * @param parts    按键模板顺序排列的键片段
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void jsonDelAsync(Class<?> type, Object... parts) {
            if (type == null || parts == null || parts.length == 0) return;
            this.jsonDelAsyncBytes(redisProxy.rediSearch().keyBytes(type, parts));
        }

        /**
         * 业务作用：删除 JSON 文档中的某个路径。
         * 路径不存在时不报错。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param type     实体类型，据其键模板定位目标文档
         * @param parts    按键模板顺序排列的键片段
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void jsonDel(Class<?> type, Object... parts) {
            if (type == null || parts == null || parts.length == 0) return;
            this.jsonDelSyncBytes(redisProxy.rediSearch().keyBytes(type, parts));
        }

        /**
         * 业务作用：写入 JSON 文档中的单个字段。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param type     实体类型，据其键模板定位目标文档
         * @param id       实体主键
         * @param jsonPath JSON 路径
         * @param value 待写入的值
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void jsonSetFieldAsync(Class<?> type, Object id, String jsonPath, String value) {
            if (type == null || id == null) return;
            this.jsonSetFieldAsyncBytes(redisProxy.rediSearch().keyBytes(type, id),
                    jsonPath.getBytes(StandardCharsets.UTF_8),
                    value.getBytes(StandardCharsets.UTF_8));
        }

        /**
         * 业务作用：写入 JSON 文档中的单个字段。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param type     实体类型，据其键模板定位目标文档
         * @param id       实体主键
         * @param jsonPath JSON 路径
         * @param value 待写入的值
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void jsonSetField(Class<?> type, Object id, String jsonPath, String value) {
            if (type == null || id == null) return;
            this.jsonSetFieldSyncBytes(redisProxy.rediSearch().keyBytes(type, id),
                    jsonPath.getBytes(StandardCharsets.UTF_8),
                    value.getBytes(StandardCharsets.UTF_8));
        }

        /**
         * 业务作用：写入 JSON 文档中的单个字段。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param type     实体类型，据其键模板定位目标文档
         * @param parts    按键模板顺序排列的键片段
         * @param jsonPath JSON 路径
         * @param value 待写入的值
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void jsonSetFieldAsync(Class<?> type, Object[] parts, String jsonPath, String value) {
            if (type == null || parts == null || parts.length == 0) return;
            this.jsonSetFieldAsyncBytes(redisProxy.rediSearch().keyBytes(type, parts),
                    jsonPath.getBytes(StandardCharsets.UTF_8),
                    value.getBytes(StandardCharsets.UTF_8));
        }

        /**
         * 业务作用：写入 JSON 文档中的单个字段。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param type     实体类型，据其键模板定位目标文档
         * @param parts    按键模板顺序排列的键片段
         * @param jsonPath JSON 路径
         * @param value 待写入的值
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void jsonSetField(Class<?> type, Object[] parts, String jsonPath, String value) {
            if (type == null || parts == null || parts.length == 0) return;
            this.jsonSetFieldSyncBytes(redisProxy.rediSearch().keyBytes(type, parts),
                    jsonPath.getBytes(StandardCharsets.UTF_8),
                    value.getBytes(StandardCharsets.UTF_8));
        }

        /**
         * 业务作用：按增量原子调整 JSON 文档中的数值字段。
         * 自增在服务端完成，并发调用不会丢更新。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param type     实体类型，据其键模板定位目标文档
         * @param id       实体主键
         * @param jsonPath JSON 路径
         * @param delta 增减量
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void jsonNumIncrByAsync(Class<?> type, Object id, String jsonPath, long delta) {
            if (type == null || id == null) return;
            this.jsonNumIncrByAsyncBytes(redisProxy.rediSearch().keyBytes(type, id),
                    jsonPath.getBytes(StandardCharsets.UTF_8),
                    Long.toString(delta).getBytes(StandardCharsets.UTF_8));
        }

        /**
         * 业务作用：按增量原子调整 JSON 文档中的数值字段。
         * 自增在服务端完成，并发调用不会丢更新。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param type     实体类型，据其键模板定位目标文档
         * @param id       实体主键
         * @param jsonPath JSON 路径
         * @param delta 增减量
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void jsonNumIncrBy(Class<?> type, Object id, String jsonPath, long delta) {
            if (type == null || id == null) return;
            this.jsonNumIncrBySyncBytes(redisProxy.rediSearch().keyBytes(type, id),
                    jsonPath.getBytes(StandardCharsets.UTF_8),
                    Long.toString(delta).getBytes(StandardCharsets.UTF_8));
        }

        /**
         * 业务作用：按增量原子调整 JSON 文档中的数值字段。
         * 自增在服务端完成，并发调用不会丢更新。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param type     实体类型，据其键模板定位目标文档
         * @param parts    按键模板顺序排列的键片段
         * @param jsonPath JSON 路径
         * @param delta 增减量
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void jsonNumIncrByAsync(Class<?> type, Object[] parts, String jsonPath, long delta) {
            if (type == null || parts == null || parts.length == 0) return;
            this.jsonNumIncrByAsyncBytes(redisProxy.rediSearch().keyBytes(type, parts),
                    jsonPath.getBytes(StandardCharsets.UTF_8),
                    Long.toString(delta).getBytes(StandardCharsets.UTF_8));
        }

        /**
         * 业务作用：按增量原子调整 JSON 文档中的数值字段。
         * 自增在服务端完成，并发调用不会丢更新。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param type     实体类型，据其键模板定位目标文档
         * @param parts    按键模板顺序排列的键片段
         * @param jsonPath JSON 路径
         * @param delta 增减量
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void jsonNumIncrBy(Class<?> type, Object[] parts, String jsonPath, long delta) {
            if (type == null || parts == null || parts.length == 0) return;
            this.jsonNumIncrBySyncBytes(redisProxy.rediSearch().keyBytes(type, parts),
                    jsonPath.getBytes(StandardCharsets.UTF_8),
                    Long.toString(delta).getBytes(StandardCharsets.UTF_8));
        }

        /**
         * 业务作用：读取 JSON 文档或其中的某个路径。
         * 路径不存在时返回空。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param type     实体类型，据其键模板定位目标文档
         * @param id       实体主键
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void jsonGetAsync(Class<?> type, Object id, LettuceFuture<RedisFuture<byte[]>> lf) {
            if (type == null || id == null) return;
            this.jsonGetAsyncBytes(redisProxy.rediSearch().keyBytes(type, id), null, lf);
        }

        /**
         * 业务作用：读取 JSON 文档或其中的某个路径。
         * 路径不存在时返回空。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param type     实体类型，据其键模板定位目标文档
         * @param id       实体主键
         * @param jsonPath JSON 路径
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void jsonGetAsync(Class<?> type, Object id, String jsonPath, LettuceFuture<RedisFuture<byte[]>> lf) {
            if (type == null || id == null) return;
            byte[] pathBytes = jsonPath == null ? null : jsonPath.getBytes(StandardCharsets.UTF_8);
            this.jsonGetAsyncBytes(redisProxy.rediSearch().keyBytes(type, id), pathBytes, lf);
        }

        // ---- String index 公开入口 (RediSearch FT.*) ----

        /**
         * 业务作用：执行全文检索。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param index 下标
         * @param args 脚本参数
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void ftSearchAsync(String index, String[] args, LettuceFuture<RedisFuture<List<Object>>> lf) {
            if (index == null) return;
            this.enqueueWithFuture(OP_FT_SEARCH, index.getBytes(StandardCharsets.UTF_8), null, null, 0L, 0L, args, lf);
        }

        /**
         * 业务作用：执行聚合检索。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param index 下标
         * @param args 脚本参数
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void ftAggregateAsync(String index, String[] args, LettuceFuture<RedisFuture<List<Object>>> lf) {
            if (index == null) return;
            this.enqueueWithFuture(OP_FT_AGGREGATE, index.getBytes(StandardCharsets.UTF_8), null, null, 0L, 0L, args, lf);
        }

        /**
         * 业务作用：创建全文索引。
         * 索引已存在时报错；索引建立后新写入的文档才会被自动纳入。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param index 下标
         * @param args 脚本参数
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void ftCreate(String index, String[] args) {
            if (index == null) return;
            this.enqueueSync(OP_FT_CREATE, index.getBytes(StandardCharsets.UTF_8), null, null, 0L, 0L, args);
        }

        /**
         * 业务作用：创建全文索引。
         * 索引已存在时报错；索引建立后新写入的文档才会被自动纳入。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param index 下标
         * @param args 脚本参数
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void ftCreateAsync(String index, String[] args, LettuceFuture<RedisFuture<List<Object>>> lf) {
            if (index == null) return;
            this.enqueueWithFuture(OP_FT_CREATE, index.getBytes(StandardCharsets.UTF_8), null, null, 0L, 0L, args, lf);
        }

        /**
         * 业务作用：读取索引的结构与统计信息。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param index 下标
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void ftInfoAsync(String index, LettuceFuture<RedisFuture<List<Object>>> lf) {
            if (index == null) return;
            this.enqueueWithFuture(OP_FT_INFO, index.getBytes(StandardCharsets.UTF_8), null, null, 0L, 0L, null, lf);
        }

        /**
         * 业务作用：删除全文索引。
         * 只删索引本身，不删被索引的文档。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param index 下标
         * @param dd 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void ftDropIndex(String index, boolean dd) {
            if (index == null) return;
            String[] args = dd ? new String[]{"DD"} : null;
            this.enqueueSync(OP_FT_DROPINDEX, index.getBytes(StandardCharsets.UTF_8), null, null, 0L, 0L, args);
        }

        /**
         * 业务作用：删除全文索引。
         * 只删索引本身，不删被索引的文档。
         * <p>
         * 本方法把命令排入批次，并把结果<b>经由入参的结果句柄回传</b>：批次发出后由调用方对句柄取值，
         * 取值时若命令失败，异常在该处抛出，因此失败是可感知的。
         * <p>
         * 句柄取值会<b>阻塞</b>到批次真正发出为止，务必在收尾之后再取值——
         * 在同一线程上先取值再收尾会永久挂起。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令立即单独发出。
         *
         * @param index 下标
         * @param dd 见方法语义
         * @param lf 承接命令结果的句柄，批次发出后由调用方对其取值
         * 返回: 无返回值；命令结果经由入参的结果句柄回传。
         */
        public void ftDropIndexAsync(String index, boolean dd, LettuceFuture<RedisFuture<List<Object>>> lf) {
            if (index == null) return;
            String[] args = dd ? new String[]{"DD"} : null;
            this.enqueueWithFuture(OP_FT_DROPINDEX, index.getBytes(StandardCharsets.UTF_8), null, null, 0L, 0L, args, lf);
        }

        // ---- HASH 模式 entity 入口 (HMSET 非 null + HDEL_MULTI 已被设为 null 的字段) ----

        /**
         * 业务作用：以哈希形式整体保存一个实体。
         * 拆成多条字段写入命令入队。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param entity 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void hashSaveAsync(Object entity) {
            if (entity == null) return;
            EntityWriteOp op = redisProxy.rediSearch().buildHashOp(entity);
            if (op.hashNames().length > 0) {
                // Lettuce hmset 接 Map<K,V>, 内部仅 entrySet 遍历; 用池化 RecycleLinkedMap 复用 OP_HMSET 路径 (跟 hMSet 一致),
                // CmdBuffer.restore 自动归还节点, 稳态零 GC. byte[] 作 key 走 identity 但每次 new byte[] 独立 + 字段名唯一, 不撞.
                RecycleLinkedMap<byte[], byte[]> m = RecycleLinkedMap.of();
                for (int i = 0; i < op.hashNames().length; i++) {
                    m.put(op.hashNames()[i], op.hashValues()[i]);
                }
                this.enqueueAsync(OP_HMSET, op.keyBytes(), null, null, 0L, 0L, m);
            }
            if (op.toDeleteBytes().length > 0) {
                this.enqueueAsync(OP_HDEL_MULTI, op.keyBytes(), null, null, 0L, 0L, op.toDeleteBytes());
            }
        }

        /**
         * 业务作用：以哈希形式整体保存一个实体。
         * 拆成多条字段写入命令入队。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param entity 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void hashSave(Object entity) {
            if (entity == null) return;
            EntityWriteOp op = redisProxy.rediSearch().buildHashOp(entity);
            if (op.hashNames().length > 0) {
                RecycleLinkedMap<byte[], byte[]> m = RecycleLinkedMap.of();
                for (int i = 0; i < op.hashNames().length; i++) {
                    m.put(op.hashNames()[i], op.hashValues()[i]);
                }
                this.enqueueSync(OP_HMSET, op.keyBytes(), null, null, 0L, 0L, m);
            }
            if (op.toDeleteBytes().length > 0) {
                this.enqueueSync(OP_HDEL_MULTI, op.keyBytes(), null, null, 0L, 0L, op.toDeleteBytes());
            }
        }

        // ---- JSON_ARRAY / BUCKET 模式 entity 入口 (LUA append / replace-or-append) ----

        /**
         * 业务作用：向 JSON 数组追加元素。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param entity 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void jsonArraySaveAsync(Object entity) {
            if (entity == null) return;
            RediSearch rs = redisProxy.rediSearch();
            byte[] key = rs.arrayKeyBytes(entity);
            byte[] json = rs.jsonBytes(entity);
            this.evalAsyncBytes(JsonArrayLuaScripts.INIT_OR_APPEND_BYTES,
                    new byte[][]{key}, new byte[][]{json});
        }

        /**
         * 业务作用：向 JSON 数组追加元素。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param entity 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void jsonArraySave(Object entity) {
            if (entity == null) return;
            RediSearch rs = redisProxy.rediSearch();
            byte[] key = rs.arrayKeyBytes(entity);
            byte[] json = rs.jsonBytes(entity);
            this.evalSyncBytes(JsonArrayLuaScripts.INIT_OR_APPEND_BYTES,
                    new byte[][]{key}, new byte[][]{json});
        }

        /**
         * 业务作用：向 JSON 数组追加元素，已存在同标识元素时整体替换。
         * 追加与替换由 Lua 脚本在服务端原子完成——分两步会在两者之间留出窗口，使同一标识出现两份。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param entity 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void jsonArraySaveOrReplaceAsync(Object entity) {
            if (entity == null) return;
            RediSearch rs = redisProxy.rediSearch();
            byte[] key = rs.arrayKeyBytes(entity);
            // idDelFilter 必须按 id 真实类型渲染 literal：数字不加引号，字符串加引号；否则会与 JSON 字段类型不匹配。
            byte[] delFilter = rs.idDelFilter(entity).getBytes(StandardCharsets.UTF_8);
            byte[] json = rs.jsonBytes(entity);
            this.evalAsyncBytes(JsonArrayLuaScripts.REPLACE_OR_APPEND_BYTES, new byte[][]{key},
                    new byte[][]{delFilter, json});
        }

        /**
         * 业务作用：向 JSON 数组追加元素，已存在同标识元素时整体替换。
         * 追加与替换由 Lua 脚本在服务端原子完成——分两步会在两者之间留出窗口，使同一标识出现两份。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param entity 见方法语义
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void jsonArraySaveOrReplace(Object entity) {
            if (entity == null) return;
            RediSearch rs = redisProxy.rediSearch();
            byte[] key = rs.arrayKeyBytes(entity);
            byte[] delFilter = rs.idDelFilter(entity).getBytes(StandardCharsets.UTF_8);
            byte[] json = rs.jsonBytes(entity);
            this.evalSyncBytes(JsonArrayLuaScripts.REPLACE_OR_APPEND_BYTES, new byte[][]{key},
                    new byte[][]{delFilter, json});
        }

        // ---- 通用 evalsha / eval byte[] 入口 (供 RediSearch / 业务自定义 LUA 用) ----

        /**
         * 业务作用：按脚本摘要执行已缓存的 Lua 脚本。
         * 服务端未缓存该脚本时报错，调用方需回退到按内容执行。
         * <p>
         * 本方法把命令<b>排入</b>批次而不等待结果：批次统一发出后不收集本命令的响应，
         * 因此命令执行失败调用方<b>无法感知</b>。需要确认执行结果时应改用同名的非异步方法。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param sha Lua 脚本摘要
         * @param keys 缓存键集合
         * @param args 脚本参数
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void evalshaAsync(String sha, String[] keys, Object... args) {
            Objects.requireNonNull(sha);
            byte[][] ks = serializeKeys(keys);
            byte[][] as = serializeArgs(args);
            this.enqueueAsync(OP_EVALSHA, null, sha, ScriptOutputType.OBJECT, 0L, 0L, new Object[]{ks, as});
        }

        /**
         * 业务作用：按脚本摘要执行已缓存的 Lua 脚本。
         * 服务端未缓存该脚本时报错，调用方需回退到按内容执行。
         * <p>
         * 本方法把命令排入批次并标记为需要结果：批次统一发出后会等待本命令的响应，
         * 因此执行失败可被调用方感知。代价是批次的整体耗时受最慢的一条同步命令约束。
         * <p>
         * 当前线程未开启批次时不缓冲，本命令<b>立即单独发出</b>。
         *
         * @param sha Lua 脚本摘要
         * @param keys 缓存键集合
         * @param args 脚本参数
         * 返回: 无返回值；命令的执行结果不回传给调用方。
         */
        public void evalsha(String sha, String[] keys, Object... args) {
            Objects.requireNonNull(sha);
            byte[][] ks = serializeKeys(keys);
            byte[][] as = serializeArgs(args);
            this.enqueueSync(OP_EVALSHA, null, sha, ScriptOutputType.OBJECT, 0L, 0L, new Object[]{ks, as});
        }

        /**
         * 业务作用：byte[] 版 eval 异步: keys / args 已是 byte[] 视图, 不重复 UTF-8 编码. ARRAY 模式入口用.
         *
         * @param script Lua 脚本
         * @param keys   缓存键集合
         * @param args   脚本参数
         */
        private void evalAsyncBytes(byte[] script, byte[][] keys, byte[][] args) {
            this.enqueueAsync(OP_EVAL_ASYNC, null, null, null, 0L, 0L, new Object[]{script, keys, args});
        }

        /**
         * 业务作用：byte[] 版 eval 同步.
         *
         * @param script Lua 脚本
         * @param keys   缓存键集合
         * @param args   脚本参数
         */
        private void evalSyncBytes(byte[] script, byte[][] keys, byte[][] args) {
            this.enqueueSync(OP_EVAL_ASYNC, null, null, null, 0L, 0L, new Object[]{script, keys, args});
        }

        // ==================== pipeline 执行 ====================

        /**
         * 业务作用：执行 pipeline, 无 execTag 限制
         */
        public void pipeline() {
            this.pipeline((Object) null);
        }

        /**
         * 业务作用：执行 pipeline, 只有 execTag 匹配时才 flush.
         * <p>
         * 用 {@link Objects#equals} 比较, 不是 {@code ==} 引用比较:
         * tag 可以是 {@code this} (引用相等) / 字符串字面量 / 动态拼接字符串 / 任意对象, 都按 equals 语义匹配.
         *
         * @param execTag 批次执行标识，只有匹配的收尾调用才会真正发出批次
         */
        public void pipeline(Object execTag) {
            Object tag = this.execTag.get();
            if (tag == null || Objects.equals(tag, execTag)) this.pipelineForce();
        }

        /**
         * 业务作用：自动 flush: 命令累积数 (CmdBuffer.count + LF.size()) 达到 pipelineLength 时触发.
         * 中间 flush, flush 完恢复 pipeline 模式继续缓冲 (区别于 pipelineForce 会关闭 open).
         * <p>
         * <b>private</b>: 这是"中间 flush 保持 open"语义, 只能内部 autoflush 用. 最终 flush 必须走 {@link #pipelineForce} /
         * {@link #pipeline()} 关闭 session，否则会把 open 状态遗留给调用线程并污染后续批处理。
         */
        private void pipelineAutoFlush() {
            Boolean wasOpen = open.get();
            Object tag = execTag.get();
            this.pipelineForce();
            if (Boolean.TRUE.equals(wasOpen)) {
                open.set(true);
                if (tag != null) execTag.set(tag);
            }
        }

        /**
         * 业务作用：强制执行 pipeline, 忽略 execTag.
         * <p>
         * 当前 session 没任何命令缓冲时, 仍执行 {@link #clear()} 清理 ThreadLocal —
         * 给 "open 了但 drain 全异常 / open 了但什么都没 enqueue" 的兜底路径用 (RedisProxy.before tick 的 finally).
         * 不留 dangling open/execTag/CMD ThreadLocal, 避免污染下一次 session 状态.
         */
        public void pipelineForce() {
            CmdBuffer buf = CMD.get();
            RecycleLinkedList<Function<RedisClusterAsyncCommands<byte[], byte[]>, RedisFuture<?>>> funcs = LF.get();
            boolean hasCmds = buf != null && buf.count > 0;
            boolean hasFuncs = ColUtils.isNotEmpty(funcs);
            if (!hasCmds && !hasFuncs) {
                this.clear();
                return;
            }
            this.doFlush(funcs, buf);
        }

        /**
         * 业务作用：外部传入 funcs (读命令) 直接 flush. 不走 ThreadLocal 缓冲, 一次性发完即返回结果.
         * 用于 pipeline(LinkedHashMap) 这类 get-with-converter 路径.
         * <p>
         * 与 LettucePipeline 的 pipeline(funcs, consumers) 区别: 写命令的 Consumer list 已被 CmdBuffer 替代,
         * 业务侧不再传 consumers. 如果业务方有自定义写命令 lambda 需求, 用 add(Function) 单独入队即可.
         *
         * @param funcs 见上述说明
         */
        public RecycleLinkedMap<Function<RedisClusterAsyncCommands<byte[], byte[]>, RedisFuture<?>>, RedisFuture<?>> pipeline(
                List<Function<RedisClusterAsyncCommands<byte[], byte[]>, RedisFuture<?>>> funcs) {

            boolean hasFunc = ColUtils.isNotEmpty(funcs);
            RecycleLinkedMap<Function<RedisClusterAsyncCommands<byte[], byte[]>, RedisFuture<?>>, RedisFuture<?>>
                    futures = RecycleLinkedMap.of();
            // 空 funcs: 不借连接/不发空包, 直接返回空 map (调用方负责 recycle)
            if (!hasFunc) return futures;
            PipelineConnectionPool pool = redisProxy.getPipelinePool();
            StatefulConnection<byte[], byte[]> connection = null;
            RedisClusterAsyncCommands<byte[], byte[]> commands = null;
            boolean flushed = false;
            try {
                try {
                    // borrow 放进 try: 失败也走 outer catch 回收 futures (futures 在 borrow 前已分配)
                    connection = pool.borrow();
                    commands = PipelineConnectionPool.async(connection);
                    commands.setAutoFlushCommands(false);
                    if (hasFunc) {
                        for (var f : funcs) {
                            if (f == null) continue;
                            RedisFuture<?> rf = f.apply(commands);
                            // 公开扩展点: function 必须 return c.xxx(...); 返回 null 会让 awaitAll 拿到 null future 出难定位的异常, fail-fast
                            if (rf == null)
                                throw new IllegalStateException("pipeline function returned null RedisFuture");
                            futures.put(f, rf);
                        }
                    }
                    commands.flushCommands();
                    flushed = true;
                } finally {
                    // borrow 成功才还原 autoFlush + 归还连接; borrow 失败 (connection==null) 跳过, 但 clear() 始终执行.
                    // flush 成功且还原成功 → release 回池; 否则 invalidate 关闭 (buffer 可能残留未 flush 命令, 见 doFlush).
                    if (connection != null) {
                        boolean restoreOk = false;
                        try {
                            commands.setAutoFlushCommands(true);
                            restoreOk = true;
                        } catch (Exception ignored) {
                        }
                        if (flushed && restoreOk) {
                            pool.release(connection);
                        } else {
                            pool.invalidate(connection);
                        }
                    }
                    // 不调 this.clear(): 本方法用显式 funcs 参数 + 独立借的连接, 不碰 ThreadLocal 的 CMD/LF/open.
                    // 若调用方同线程已 open session, clear() 会把外层 session 的缓冲命令一起清掉 (公开 API 误用风险).
                }
                if (hasFunc) {
                    this.awaitAllOrTimeout(pool, ColUtils.toArray(RedisFuture.class, futures.values()));
                }
                return futures;
            } catch (Throwable ex) {
                // 异常路径回收自身 futures: 调用方拿不到返回引用, 不会再 recycle (防泄漏 + 防双重回收)
                futures.recycle();
                throw ex;
            }
        }

        /**
         * 业务作用：pipeline get-with-consumer: 批量读命令 + 各自的结果消费函数, 一次拿全部结果.
         * Function 引用做 key 关联 future, 遍历 futures 按 key 反查 consumer 回调.
         *
         * @param map 见上述说明
         */
        public void pipeline(LinkedHashMap<Function<RedisClusterAsyncCommands<byte[], byte[]>, RedisFuture<?>>, Consumer<Object>> map) {
            if (MapUtils.isEmpty(map)) return;
            RecycleLinkedList<Function<RedisClusterAsyncCommands<byte[], byte[]>, RedisFuture<?>>> funcs = RecycleLinkedList.of();
            for (var entry : map.entrySet()) funcs.add(entry.getKey());
            // pipeline(funcs) 抛异常时其内部已回收自身 futures, 这里只需保证 funcs 归还 (try/finally 包住调用)
            RecycleLinkedMap<Function<RedisClusterAsyncCommands<byte[], byte[]>, RedisFuture<?>>, RedisFuture<?>> futures;
            try {
                futures = this.pipeline(funcs);
            } finally {
                funcs.recycle();
            }
            try {
                for (var entry : futures.entrySet()) {
                    RedisFuture<?> rf = entry.getValue();
                    if (rf == null) continue;
                    Object o = rf.get();
                    Consumer<Object> c = map.get(entry.getKey());
                    if (c != null) c.accept(o);
                }
            } catch (Throwable e) {
                throw new CacheException("pipeline反序列化异常: {}", e.getMessage(), e);
            } finally {
                futures.recycle();
            }
        }

        /**
         * 业务作用：统一 flush: CmdBuffer (平坦数组所有写命令) + LF (读命令 / sync via lambda) 在同一个连接、同一次 flushCommands 发送.
         * <p>
         * 执行顺序 (嵌套 try/finally 保证 actions 在 ThreadLocal 完整时运行, clear 始终在最外执行):
         * <ol>
         *   <li>内 try: borrow → async(conn) → setAutoFlushCommands(false) → dispatch (CmdBuffer sync future 进 syncFutures, 返回首个 sync 分发异常) → LF (future 也并入) → flushCommands</li>
         *   <li>内 finally: 还原 autoFlush + 归还连接 (flush 成功且还原成功 → release, 否则 invalidate 关闭; 尽早归还, await 不持连接)</li>
         *   <li>awaitAll syncFutures: 等所有 sync 命令完成</li>
         *   <li>syncDispatchError gate: dispatch 阶段有 sync 写命令分发失败则在此上抛, 跳过 successActions</li>
         *   <li>successActions: action() 逐个跑, 此时 ThreadLocal 仍完整</li>
         *   <li>外 finally: syncFutures.recycle() + clear() 总会执行</li>
         * </ol>
         *
         * @param funcs 见上述说明
         * @param buf   见上述说明
         */
        private void doFlush(
                RecycleLinkedList<Function<RedisClusterAsyncCommands<byte[], byte[]>, RedisFuture<?>>> funcs,
                CmdBuffer buf) {

            boolean hasFuncs = ColUtils.isNotEmpty(funcs);
            boolean hasCmds = buf != null && buf.count > 0;
            // ① 收 sync future 的容器 (CmdBuffer sync 槽 + 全部 LF), 都为空就不分配
            RecycleLinkedList<RedisFuture<?>> syncFutures = (hasFuncs || hasCmds) ? RecycleLinkedList.of() : null;
            // ② 从 pipeline 专用连接池借独占连接 (Semaphore 控制, 虚拟线程友好).
            // borrow 放进 outer try, 失败时走 outer catch fail 整个 CmdBuffer 的所有 lf (防业务线程死锁).
            PipelineConnectionPool pool = redisProxy.getPipelinePool();
            StatefulConnection<byte[], byte[]> connection = null;
            // dispatched: dispatch 完成后置 true. true 后所有 buf.lfs 已通过 completeLf 拿到 lettuce future,
            // 后续异常 (funcs.apply / flushCommands / awaitAll / actions) 不再需要兜底 fail —
            // lettuce 内部 channel 失败会自动 fail pending RedisFuture, 业务 lf.getFinally 递归 rf.get 自然解阻塞.
            boolean dispatched = false;
            // flushed: flushCommands 成功返回后置 true. 只有 flush 成功且 autoFlush 还原成功, 连接才允许回池;
            // 否则连接 buffer 可能残留未 flush 命令, 必须 invalidate (关闭) 而非 release, 防跨借用方命令串扰.
            boolean flushed = false;
            // sync 写命令 (无 lf) 分发时的首个异常, 由 dispatch 返回; awaitAll 后、successActions 前 gate.
            Throwable syncDispatchError = null;
            try {
                // 连接池只由 @EnableRedis 的自动装配注入。RedisProxy 的构造是 public,
                // 手工 new 出来的实例没有连接池；这里给出明确的装配原因与可用入口。
                // 异常在 try 内抛出，下方 catch 会先 fail 全部 lf 再上抛，避免业务线程永久等待。
                if (pool == null) {
                    throw new CacheException("RedisProxy[{}] 没有 pipeline 连接池, 无法执行批次。"
                            + "连接池由 @EnableRedis 的自动装配注入, 手工 new 出来的 RedisProxy 拿不到它 —— "
                            + "这类实例只能使用 RedisTemplate 直通的命令, 批次相关的 API 一律不可用",
                            redisProxy.getQualifier());
                }
                connection = pool.borrow();
                RedisClusterAsyncCommands<byte[], byte[]> c = null;
                try {
                    // async(connection) 放进 inner try: 极端情况 (连接类型/cluster 不一致等) 抛异常时也走 finally invalidate, 不漏 permit
                    c = PipelineConnectionPool.async(connection);
                    // ③ 关闭自动 flush, 让所有命令累积到本地 buffer, 直到 flushCommands 才发网络
                    c.setAutoFlushCommands(false);

                    // ④ CmdBuffer 路径: switch 分发, 零 lambda; sync 槽 (ops 高位置 1) 的 future 进 syncFutures.
                    // dispatch 内部 per-cmd try-catch 隔离单条异常, 不会向外抛.
                    if (hasCmds) {
                        syncDispatchError = this.dispatch(c, buf, syncFutures);
                    }
                    // 至此 buf 内所有 lf 都已 completeLf(rf), 无需兜底 fail
                    dispatched = true;

                    // ⑤ Function 路径 (读命令 / 需自定义 lambda 的 sync 写) — future 也并入 syncFutures
                    if (hasFuncs) {
                        for (var f : funcs) {
                            if (f == null) continue;
                            RedisFuture<?> rf = f.apply(c);
                            // function 必须 return RedisFuture; null 会让 awaitAll 拿到 null future, fail-fast
                            if (rf == null)
                                throw new IllegalStateException("pipeline function returned null RedisFuture");
                            syncFutures.add(rf);
                        }
                    }

                    // ⑥ 一次网络包发送所有累积的命令 (lettuce 底层一次 socket write)
                    c.flushCommands();
                    flushed = true;
                } finally {
                    // ⑦ autoFlush 还原 + 归还连接. flush 成功且还原成功 → release 回池;
                    // 否则 (flush 前抛异常 / flushCommands 抛 / 还原抛) → invalidate 关闭连接:
                    // 连接 buffer 可能残留未 flush 命令, 回池会被下个 borrower 串出, 关闭让 lettuce fail pending future.
                    // 尽早归还: await 不需持有 (RedisFuture 由 netty 事件循环驱动完成)
                    boolean restoreOk = false;
                    try {
                        // c==null 表示 async() 抛在 setAutoFlushCommands(false) 之前, 没有 autoFlush 状态需还原
                        if (c != null) c.setAutoFlushCommands(true);
                        restoreOk = true;
                    } catch (Exception ignored) {
                    }
                    if (flushed && restoreOk) {
                        pool.release(connection);
                    } else {
                        pool.invalidate(connection);
                    }
                }

                // ⑨ 同步等所有 sync 命令完成 (-1 表示无超时); async 槽 future 已被 dispatch 丢弃, 不等
                if (syncFutures != null && !syncFutures.isEmpty()) {
                    this.awaitAllOrTimeout(pool, ColUtils.toArray(RedisFuture.class, syncFutures));
                }

                // ⑨.5 sync 写命令分发时失败 (CCE / 未知 op): future 未进 syncFutures, awaitAll 看不到,
                // 这里在 successActions 前 gate, 与运行时 sync 失败 (awaitAll 抛) 一致: 跳过 successActions 并上抛.
                if (syncDispatchError != null) {
                    throw new CacheException("pipeline sync 命令分发失败: {}", syncDispatchError.getMessage(), syncDispatchError);
                }

                // ⑩ pipeline 真正成功后跑 actions; cachedIterator 复用 (零迭代器分配); 用 .action() 直跑, 异常会上抛中断后续 + 跳出本方法 (与 v1 语义一致)
                RecycleLinkedList<Action> actions = pipelineSuccessActions.get();
                if (ColUtils.isNotEmpty(actions)) {
                    Iterator<Action> iter = actions.cachedIterator();
                    while (iter.hasNext()) iter.next().action();
                }
            } catch (Throwable ex) {
                // dispatch 之前的异常 (borrow 超时 / setAutoFlushCommands 抛) → buf 内 lf 没一个被 complete,
                // 业务线程会在 lf.getFinally 永久阻塞. 这里统一 completeExceptionally 解阻塞.
                // dispatched=true 后异常 (funcs.apply / flushCommands / awaitAll / actions):
                //   - buf.lfs 都已 complete(rf), 这里再 fail 因 done=true 是 no-op, 安全无副作用;
                //   - lettuce 在 flushCommands 失败时会内部 fail pending rf, 业务 lf.getFinally 递归 rf.get 自然解阻塞.
                if (!dispatched && hasCmds) {
                    for (int i = 0; i < buf.count; i++) {
                        LettuceFuture<?> lf = buf.lfs[i];
                        if (lf != null) lf.completeExceptionally(ex);
                    }
                }
                throw ex;
            } finally {
                // ⑪ syncFutures 归还池 + ThreadLocal 清理: 即使 await/action 抛异常也保证执行
                if (syncFutures != null) syncFutures.recycle();
                this.clear();
            }
        }

        /**
         * 业务作用：未开启 pipeline 时, 单条写命令立即执行 (获取连接 → dispatch → flush → 释放连接).
         * <p>
         * sync=true: awaitAll 等单条 future 完成.
         * sync=false: 不 await, 但挂 exceptionally 监听异常打 ERROR (避免 lettuce 异步失败静默).
         * 出方法前回收 extra 中的 {@link RecycleLinkedMap} (与 {@link CmdBuffer#restore} 同义,
         * 只是这条 nonOpen 路径不经 CmdBuffer, 需就地回收).
         *
         * @param sync  是否等待执行结果
         * @param op    见上述说明
         * @param a1    见上述说明
         * @param a2    见上述说明
         * @param a3    见上述说明
         * @param l     见上述说明
         * @param l2    见上述说明
         * @param extra 见上述说明
         */
        private void doFlushSingle(boolean sync, short op, byte[] a1, Object a2, Object a3, long l, long l2, Object extra) {
            // ① 从 pipeline 专用连接池借独占连接 (borrow 放进 try: 失败也走 finally 回收 extra, 防 RecycleLinkedMap 泄漏)
            PipelineConnectionPool pool = redisProxy.getPipelinePool();
            StatefulConnection<byte[], byte[]> connection = null;
            RedisClusterAsyncCommands<byte[], byte[]> c = null;
            RedisFuture<?> f = null;
            boolean flushed = false;
            try {
                connection = pool.borrow();
                c = PipelineConnectionPool.async(connection);
                // ② 同 doFlush: 关闭 auto-flush → dispatchOne (返回 future) → flushCommands
                c.setAutoFlushCommands(false);
                f = this.dispatchOne(c, op, a1, a2, a3, l, l2, extra);
                c.flushCommands();
                flushed = true;
            } finally {
                // ③ borrow 成功才还原 autoFlush + 归还连接; borrow 失败 (connection==null) 只回收 extra.
                // flush 成功且还原成功 → release 回池; 否则 invalidate 关闭 (buffer 可能残留未 flush 命令, 见 doFlush).
                if (connection != null) {
                    boolean restoreOk = false;
                    try {
                        c.setAutoFlushCommands(true);
                        restoreOk = true;
                    } catch (Exception ignored) {
                    }
                    if (flushed && restoreOk) {
                        pool.release(connection);
                    } else {
                        pool.invalidate(connection);
                    }
                }
                // OP_XADD_MULTI 等的 RecycleLinkedMap extra 无论 borrow 是否成功都回收
                if (extra instanceof RecycleLinkedMap rm) rm.recycle();
            }
            // ④ sync: awaitAll 等命令完成
            // ⑤ async: 不 await, 挂带 op+key+extras 的池化 logger (用于排查协议类错误, 见 attachAsyncErrorLog)
            if (f == null) return;
            if (sync) {
                this.awaitAllOrTimeout(pool, f);
            } else {
                attachAsyncErrorLog(f, op, a1, extra);
            }
        }

        /**
         * 业务作用：awaitAll 等所有 sync future 完成, 超时抛 {@link CacheException} 给业务线程 (而非静默继续).
         * <p>
         * 超时时长取 {@link PipelineConnectionPool#awaitTimeout()} (来自 redis 命令超时配置 timeout).
         * 配置 ≤0 时返回无限等待哨兵，{@code LettuceFutures.awaitAll} 不会进入超时分支。
         * 命令本身失败 (future 异常完成) 时 awaitAll 内部直接抛 lettuce 异常, 也不走此超时分支.
         *
         * @param pool    见上述说明
         * @param futures 见上述说明
         */
        private void awaitAllOrTimeout(PipelineConnectionPool pool, RedisFuture<?>... futures) {
            if (LettuceFutures.awaitAll(pool.awaitTimeout(), futures)) return;
            throw new CacheException("pipeline await 超时, timeout={}ms", pool.getAwaitTimeoutMs());
        }

        // ==================== 命令分发: switch(op) → lettuce async API ====================

        /**
         * 业务作用：批量分发: 遍历 CmdBuffer 的并行数组, 逐条 switch 到对应 lettuce 命令.
         * <p>
         * 每条 ops 字节高位 (SYNC_BIT) = 1 表示 sync, dispatchOne 返回的 RedisFuture:
         * <ul>
         *   <li>sync 槽: future 收进 syncFutures, doFlush 末尾统一 awaitAll</li>
         *   <li>async 槽: 不 await, 但挂 {@link #attachAsyncErrorLog} 监听异常打 ERROR 日志
         *       (否则 lettuce 异步执行失败 exception 只存在被丢弃的 future 里, 完全静默无痕)</li>
         * </ul>
         * <p>
         * 返回首个 sync 写命令 (lf==null && SYNC_BIT) 的分发异常 (无则 null): 这类命令没有 lf 承接,
         * 又不在 syncFutures 里被 awaitAll 看到, 不返回上去就会出现"sync 写分发失败却照跑 successActions"的不一致.
         * doFlush 拿到后在 successActions 前抛出, 与运行时 sync 失败 (awaitAll 抛) 行为对齐.
         *
         * @param c           见上述说明
         * @param buf         见上述说明
         * @param syncFutures 见上述说明
         */
        private Throwable dispatch(RedisClusterAsyncCommands<byte[], byte[]> c, CmdBuffer buf, RecycleLinkedList<RedisFuture<?>> syncFutures) {
            Throwable firstSyncError = null;
            for (int i = 0; i < buf.count; i++) {
                short rawOp = buf.ops[i];
                // ① 解码: 位 14 (0x4000) = sync 标记, 低 14 位 = 真 OP 码 (0-16383)
                boolean sync = (rawOp & SYNC_BIT) != 0;
                short op = (short) (rawOp & 0x3FFF);
                // ② 单条分发, 拿 lettuce 返回的 RedisFuture.
                // per-cmd try-catch: dispatchOne 可能抛 CCE (extras 类型错配) / IllegalStateException
                // (lettuce 同步校验 reject), 若不隔离则后续所有 task 的 lf 永远不会 complete, 业务线程死锁.
                RedisFuture<?> f;
                LettuceFuture<?> lf = buf.lfs[i];
                try {
                    f = this.dispatchOne(c, op, buf.arg1[i], buf.arg2[i], buf.arg3[i], buf.longArg[i], buf.longArg2[i], buf.extras[i]);
                } catch (Throwable ex) {
                    // lf 读命令: 异常直达业务线程; sync 写命令: 没 lf 承接, 记录首个异常交 doFlush 上抛.
                    // 两种情况都 log + continue (保留逐条隔离, 不中断后续命令分发).
                    if (lf != null) {
                        lf.completeExceptionally(ex);
                    } else if (sync && firstSyncError == null) {
                        firstSyncError = ex;
                    }
                    log.error("[lettuce-pipeline] dispatchOne failed op={}", op, ex);
                    continue;
                }
                if (f == null) continue;
                // ③ 读命令: 回填 lf, 业务线程在 getFinally 内递归 get RedisFuture 拿真值/异常
                // (lf 与 SYNC_BIT 互斥, 业务侧不会同时设置, 这里 lf 优先, 失败暴露给业务而非 awaitAll)
                if (lf != null) {
                    this.completeLf(lf, f);
                    continue;
                }
                if (sync) {
                    // sync 槽: 收 future 由 doFlush 末尾统一 awaitAll
                    syncFutures.add(f);
                } else {
                    // ④ async 槽: 不 await, 挂带 op+key+extras 的池化 logger 打异常.
                    // 否则 lettuce 异步执行失败时 exception 只存在于被丢弃的 future, 完全静默
                    // 统一进入失败记录，避免异步命令异常在无日志的情况下静默丢失。
                    attachAsyncErrorLog(f, op, buf.arg1[i], buf.extras[i]);
                }
            }
            return firstSyncError;
        }

        /**
         * 业务作用：单条命令分发: switch(op) 直接调 lettuce async API, 返回 lettuce 的 RedisFuture.
         * <p>
         * 每个 case 对应一种 Redis 命令, 参数从并行数组的对应 slot 取出。
         * 返回值由 caller (dispatch / doFlushSingle) 决定是否收集 await: sync 收, async 弃。
         * <p>
         * 注意 OP_ZADD / OP_ZINCRBY: longArg 存的是 score 的 doubleToRawLongBits 编码,
         * 分发时用 longBitsToDouble 还原.
         * <p>
         * OP_XTRIM_MINID / OP_XDEL / OP_XACK: 这三个 lettuce API 直接收 String,
         * arg2/arg3 直接存原 String, 这里 (String) 强转后传给 lettuce, 零中间对象.
         * 其它 op 的 arg2/arg3 都是 byte[], 在 case 内 (byte[]) 强转, 单态 call site
         * 下 JIT 会折叠 CHECKCAST. arg1 永远是 byte[] key, 类型直传, 无 cast.
         * <p>
         * OP_HDECR_BY_AND_DEL: Lua 脚本 EVAL, KEYS=[a1], ARGV=[a2, str(longArg)].
         *
         * @param c     见上述说明
         * @param op    见上述说明
         * @param a1    见上述说明
         * @param a2    见上述说明
         * @param a3    见上述说明
         * @param l     见上述说明
         * @param l2    见上述说明
         * @param extra 见上述说明
         */
        @SuppressWarnings("unchecked")
        private RedisFuture<?> dispatchOne(RedisClusterAsyncCommands<byte[], byte[]> c, short op,
                                           byte[] a1, Object a2, Object a3, long l, long l2, Object extra) {
            return switch (op) {
                // -- key 通用 --
                case OP_DEL -> c.del(a1);
                case OP_EXPIRE -> c.pexpire(a1, l);
                case OP_EXPIRE_AT -> c.pexpireat(a1, l);
                case OP_PERSIST -> c.persist(a1);
                case OP_HEXPIRE -> c.hexpire(a1, Duration.ofMillis(l), (byte[]) a2);
                // -- hash --
                case OP_HSET -> c.hset(a1, (byte[]) a2, (byte[]) a3);
                case OP_HDEL -> c.hdel(a1, (byte[]) a2);
                case OP_HINCRBY -> c.hincrby(a1, (byte[]) a2, l);
                case OP_HSET_NX -> c.hsetnx(a1, (byte[]) a2, (byte[]) a3);
                case OP_HDECR_BY_AND_DEL -> {
                    byte[] luaScript = RedisProxy.hDecrByAndDelScriptBytes();
                    byte[][] ks = {a1};
                    byte[][] as = {(byte[]) a2, keySerializer.serialize(String.valueOf(l))};
                    // 用 OBJECT 而非 VALUE: hDecrBy 后判 0 then del 的 Lua 返回 Integer (decrement 后余值),
                    // VALUE 模式无法承载脚本返回的 Integer；OBJECT 与该 Lua 的返回形态一致。
                    yield c.eval(luaScript, ScriptOutputType.OBJECT, ks, as);
                }
                // -- set --
                case OP_SADD -> c.sadd(a1, (byte[]) a3);
                case OP_SREM -> c.srem(a1, (byte[]) a3);
                // -- list --
                case OP_LPUSH -> c.lpush(a1, (byte[]) a3);
                case OP_RPUSH -> c.rpush(a1, (byte[]) a3);
                case OP_LPUSH_X -> c.lpushx(a1, (byte[]) a3);
                case OP_RPUSH_X -> c.rpushx(a1, (byte[]) a3);
                case OP_LREM -> c.lrem(a1, l, (byte[]) a3);
                case OP_LSET -> c.lset(a1, l, (byte[]) a3);
                case OP_LTRIM -> c.ltrim(a1, l, l2);
                // -- string --
                case OP_SET -> c.set(a1, (byte[]) a3);
                case OP_SET_EX -> c.psetex(a1, l, (byte[]) a3);
                case OP_INCR -> c.incr(a1);
                case OP_DECR -> c.decr(a1);
                case OP_INCRBY -> c.incrby(a1, l);
                case OP_DECRBY -> c.decrby(a1, l);
                case OP_SET_NX -> c.setnx(a1, (byte[]) a3);
                case OP_SETRANGE -> c.setrange(a1, l, (byte[]) a3);
                case OP_APPEND -> c.append(a1, (byte[]) a3);
                // -- stream / pubsub --
                case OP_XADD -> c.xadd(a1, (byte[]) a2, (byte[]) a3);
                case OP_XADD_MULTI -> c.xadd(a1, (Map<byte[], byte[]>) extra);
                case OP_XTRIM_MAXLEN -> c.xtrim(a1, l);
                case OP_XTRIM_MINID -> {
                    XTrimArgs args = new XTrimArgs();
                    args.minId((String) a2);
                    yield c.xtrim(a1, args);
                }
                case OP_XDEL -> c.xdel(a1, (String) a2);
                case OP_XACK -> c.xack(a1, (byte[]) a2, (String) a3);
                case OP_PUB -> c.publish(a1, (byte[]) a3);
                // -- sorted set --
                case OP_ZADD -> c.zadd(a1, Double.longBitsToDouble(l), (byte[]) a3);
                case OP_ZREM -> c.zrem(a1, (byte[]) a3);
                case OP_ZINCRBY -> c.zincrby(a1, Double.longBitsToDouble(l), (byte[]) a3);
                case OP_ZREMRANGE -> c.zremrangebyrank(a1, l, l2);
                case OP_ZREMRANGE_BY_LEX -> c.zremrangebylex(a1, Range.create((byte[]) a2, (byte[]) a3));
                case OP_ZREMRANGE_BY_SCORE -> c.zremrangebyscore(a1,
                        Range.create(Double.longBitsToDouble(l), Double.longBitsToDouble(l2)));
                // -- script --
                case OP_EVAL_ASYNC -> {
                    Object[] e = (Object[]) extra;
                    byte[] script = (byte[]) e[0];
                    byte[][] keys = (byte[][]) e[1];
                    byte[][] args = (byte[][]) e[2];
                    // 用 OBJECT 而非 VALUE: lettuce 按 RESP 实际类型解析 (Integer→Long / Status→String / Bulk→byte[] / Array→List),
                    // 兼容所有返回类型. 原 VALUE 只能接 Bulk/null, 遇 Integer 返回会 ValueOutput.set(long) 协议爆.
                    yield c.eval(script, ScriptOutputType.OBJECT, keys, args);
                }
                case OP_SCRIPT_FLUSH -> c.scriptFlush();
                case OP_SCRIPT_KILL -> c.scriptKill();
                // -- 读命令 (有返回值) --
                case OP_DEL_MULTI -> c.del((byte[][]) extra);
                case OP_DUMP -> c.dump(a1);
                case OP_EXISTS -> c.exists(a1);
                case OP_HEXPIRE_MULTI -> c.hexpire(a1, Duration.ofMillis(l), (byte[][]) extra);
                case OP_HDEL_MULTI -> c.hdel(a1, (byte[][]) extra);
                case OP_HEXISTS -> c.hexists(a1, (byte[]) a2);
                case OP_HGET -> c.hget(a1, (byte[]) a2);
                case OP_HGETALL -> c.hgetall(a1);
                case OP_PTTL -> c.pttl(a1);
                case OP_KEYS -> c.keys(a1);
                case OP_GET -> c.get(a1);
                case OP_GETRANGE -> c.getrange(a1, l, l2);
                case OP_GETSET -> c.getset(a1, (byte[]) a3);
                case OP_GETDEL -> c.getdel(a1);
                case OP_MGET -> c.mget((byte[][]) extra);
                case OP_STRLEN -> c.strlen(a1);
                case OP_MSET -> c.mset((Map<byte[], byte[]>) extra);
                case OP_LPOP -> c.lpop(a1);
                case OP_LPOP_COUNT -> c.lpop(a1, l);
                case OP_LRANGE -> c.lrange(a1, l, l2);
                case OP_RPOP -> c.rpop(a1);
                case OP_RPOP_COUNT -> c.rpop(a1, l);
                // -- hash 读 --
                case OP_HMGET -> c.hmget(a1, (byte[][]) extra);
                case OP_HKEYS -> c.hkeys(a1);
                case OP_HLEN -> c.hlen(a1);
                case OP_HMSET -> c.hmset(a1, (Map<byte[], byte[]>) extra);
                // -- list 读 --
                case OP_LINDEX -> c.lindex(a1, l);
                case OP_LLEN -> c.llen(a1);
                case OP_LPUSH_MULTI -> c.lpush(a1, (byte[][]) extra);
                case OP_RPUSH_MULTI -> c.rpush(a1, (byte[][]) extra);
                // -- set --
                case OP_SADD_MULTI -> c.sadd(a1, (byte[][]) extra);
                case OP_SCARD -> c.scard(a1);
                case OP_SDIFF -> c.sdiff((byte[][]) extra);
                case OP_SINTER -> c.sinter((byte[][]) extra);
                case OP_SISMEMBER -> c.sismember(a1, (byte[]) a3);
                case OP_SMISMEMBER -> c.smismember(a1, (byte[][]) extra);
                case OP_SMEMBERS -> c.smembers(a1);
                case OP_SPOP -> c.spop(a1);
                case OP_SPOP_COUNT -> c.spop(a1, l);
                case OP_SRANDMEMBER -> c.srandmember(a1);
                case OP_SRANDMEMBER_COUNT -> c.srandmember(a1, l);
                case OP_SREM_MULTI -> c.srem(a1, (byte[][]) extra);
                case OP_SUNION -> c.sunion((byte[][]) extra);
                // -- zset --
                case OP_ZADD_MULTI -> c.zadd(a1, (Object[]) extra);
                case OP_ZREM_MULTI -> c.zrem(a1, (byte[][]) extra);
                case OP_ZREMRANGE_BY_LEX_RANGE -> c.zremrangebylex(a1, (Range<byte[]>) extra);
                case OP_ZREMRANGE_BY_SCORE_RANGE -> c.zremrangebyscore(a1, (Range<? extends Number>) extra);
                case OP_ZCARD -> c.zcard(a1);
                case OP_ZCOUNT_LEX -> c.zlexcount(a1, (Range<byte[]>) extra);
                case OP_ZCOUNT_SCORE -> c.zcount(a1, (Range<? extends Number>) extra);
                case OP_ZRANGE -> c.zrange(a1, l, l2);
                case OP_ZRANGEBYLEX -> c.zrangebylex(a1, (Range<byte[]>) extra);
                case OP_ZRANGEBYLEX_LIMIT -> c.zrangebylex(a1, (Range<byte[]>) extra, (Limit) a2);
                case OP_ZRANGEBYSCORE -> c.zrangebyscore(a1, (Range<? extends Number>) extra);
                case OP_ZRANGEBYSCORE_LIMIT -> c.zrangebyscore(a1, (Range<? extends Number>) extra, (Limit) a2);
                case OP_ZRANK -> c.zrank(a1, (byte[]) a3);
                case OP_ZREVRANGE -> c.zrevrange(a1, l, l2);
                case OP_ZREVRANGEBYSCORE -> c.zrevrangebyscore(a1, (Range<? extends Number>) extra);
                case OP_ZREVRANGEBYSCORE_LIMIT -> c.zrevrangebyscore(a1, (Range<? extends Number>) extra, (Limit) a2);
                case OP_ZREVRANK -> c.zrevrank(a1, (byte[]) a3);
                case OP_ZSCORE -> c.zscore(a1, (byte[]) a3);
                case OP_ZRANGE_WITHSCORES -> c.zrangeWithScores(a1, l, l2);
                case OP_ZREVRANGE_WITHSCORES -> c.zrevrangeWithScores(a1, l, l2);
                case OP_ZRANGEBYSCORE_WITHSCORES -> c.zrangebyscoreWithScores(a1, (Range<? extends Number>) extra);
                case OP_ZRANGEBYSCORE_WITHSCORES_LIMIT ->
                        c.zrangebyscoreWithScores(a1, (Range<? extends Number>) extra, (Limit) a2);
                case OP_ZREVRANGEBYSCORE_WITHSCORES ->
                        c.zrevrangebyscoreWithScores(a1, (Range<? extends Number>) extra);
                case OP_ZREVRANGEBYSCORE_WITHSCORES_LIMIT ->
                        c.zrevrangebyscoreWithScores(a1, (Range<? extends Number>) extra, (Limit) a2);
                case OP_ZPOPMIN -> c.zpopmin(a1);
                case OP_ZPOPMIN_COUNT -> c.zpopmin(a1, l);
                case OP_ZPOPMAX -> c.zpopmax(a1);
                case OP_ZPOPMAX_COUNT -> c.zpopmax(a1, l);
                case OP_ZMSCORE -> c.zmscore(a1, (byte[][]) extra);
                case OP_ZRANDMEMBER -> c.zrandmember(a1);
                case OP_ZRANDMEMBER_COUNT -> c.zrandmember(a1, l);
                // -- string / key 扩展 --
                case OP_TTL -> c.ttl(a1);
                case OP_TYPE -> c.type(a1);
                case OP_EXISTS_MULTI -> c.exists((byte[][]) extra);
                case OP_INCRBYFLOAT -> c.incrbyfloat(a1, Double.longBitsToDouble(l));
                // -- hash 扩展 --
                case OP_HVALS -> c.hvals(a1);
                case OP_HRANDFIELD -> c.hrandfield(a1);
                case OP_HRANDFIELD_COUNT -> c.hrandfield(a1, l);
                case OP_HINCRBYFLOAT -> c.hincrbyfloat(a1, (byte[]) a2, Double.longBitsToDouble(l));
                // -- list 扩展 --
                case OP_LINSERT_BEFORE -> c.linsert(a1, true, (byte[]) a2, (byte[]) a3);
                case OP_LINSERT_AFTER -> c.linsert(a1, false, (byte[]) a2, (byte[]) a3);
                case OP_LPOS -> c.lpos(a1, (byte[]) a3);
                case OP_LPOS_COUNT -> c.lpos(a1, (byte[]) a3, (int) l, null);
                // -- set 扩展 --
                case OP_SMOVE -> c.smove(a1, (byte[]) a2, (byte[]) a3);
                // -- script --
                case OP_SCRIPT_LOAD -> c.scriptLoad(a1);
                case OP_SCRIPT_EXISTS -> c.scriptExists((String[]) extra);
                case OP_EVALSHA -> {
                    Object[] e = (Object[]) extra;
                    byte[][] keys = (byte[][]) e[0];
                    byte[][] args = (byte[][]) e[1];
                    yield c.evalsha((String) a2, (ScriptOutputType) a3, keys, args);
                }
                case OP_EVAL -> {
                    Object[] e = (Object[]) extra;
                    byte[][] keys = (byte[][]) e[0];
                    byte[][] args = (byte[][]) e[1];
                    yield c.eval(a1, (ScriptOutputType) a3, keys, args);
                }
                // -- stream --
                case OP_XTRIM_ARGS -> c.xtrim(a1, (XTrimArgs) extra);
                case OP_XDEL_MULTI -> c.xdel(a1, (String[]) extra);
                case OP_XLEN -> c.xlen(a1);
                case OP_XRANGE -> c.xrange(a1, (Range<String>) extra);
                case OP_XRANGE_LIMIT -> c.xrange(a1, (Range<String>) extra, Limit.from((int) l));
                case OP_XREVRANGE -> c.xrevrange(a1, (Range<String>) extra);
                case OP_XREVRANGE_LIMIT -> c.xrevrange(a1, (Range<String>) extra, Limit.from((int) l));
                case OP_XINFO_GROUPS -> c.xinfoGroups(a1);
                case OP_XGROUP_CREATE ->
                        c.xgroupCreate((XReadArgs.StreamOffset<byte[]>) extra, (byte[]) a3, XGroupCreateArgs.Builder.mkstream());
                case OP_XINFO_CONSUMERS -> c.xinfoConsumers(a1, (byte[]) a3);
                case OP_XACK_MULTI -> c.xack(a1, (byte[]) a3, (String[]) extra);
                case OP_XGROUP_DESTROY -> c.xgroupDestroy(a1, (byte[]) a3);
                case OP_XAUTOCLAIM -> c.xautoclaim(a1, (XAutoClaimArgs<byte[]>) extra);
                // ---- RediSearch / RedisJSON (走 c.dispatch 自定义命令) ----
                // JSON.SET key path value, 返回 OK → StatusOutput; SET_FIELD 跟 SET 同 dispatch 路径, OP 分别只为业务侧统计/钩子
                case OP_JSON_SET, OP_JSON_SET_FIELD -> {
                    CommandArgs<byte[], byte[]> args = new CommandArgs<>(ByteArrayCodec.INSTANCE)
                            .addKey(a1).addValue((byte[]) a2).addValue((byte[]) a3);
                    yield c.dispatch(CMD_JSON_SET, new StatusOutput<>(ByteArrayCodec.INSTANCE), args);
                }
                // JSON.DEL key [path], 返回删除子文档数 → IntegerOutput
                case OP_JSON_DEL -> {
                    CommandArgs<byte[], byte[]> args = new CommandArgs<>(ByteArrayCodec.INSTANCE).addKey(a1);
                    if (a2 != null) args.addValue((byte[]) a2);
                    yield c.dispatch(CMD_JSON_DEL, new IntegerOutput<>(ByteArrayCodec.INSTANCE), args);
                }
                // JSON.GET key [path], 返回 JSON 字符串 → ValueOutput (byte[])
                case OP_JSON_GET -> {
                    CommandArgs<byte[], byte[]> args = new CommandArgs<>(ByteArrayCodec.INSTANCE).addKey(a1);
                    if (a2 != null) args.addValue((byte[]) a2);
                    yield c.dispatch(CMD_JSON_GET, new ValueOutput<>(ByteArrayCodec.INSTANCE), args);
                }
                // JSON.NUMINCRBY key path delta, 返回新值字符串 (legacy "3" / jsonpath "[3]") → ValueOutput
                // delta 负数即 decr; 字段不存在 / 非数字时 Redis 端报错, lettuce 回调里 future 异常上抛
                case OP_JSON_NUMINCRBY -> {
                    CommandArgs<byte[], byte[]> args = new CommandArgs<>(ByteArrayCodec.INSTANCE)
                            .addKey(a1).addValue((byte[]) a2).addValue((byte[]) a3);
                    yield c.dispatch(CMD_JSON_NUMINCRBY, new ValueOutput<>(ByteArrayCodec.INSTANCE), args);
                }
                // FT.SEARCH / FT.AGGREGATE / FT.CREATE / FT.INFO / FT.DROPINDEX: a1=index bytes, extras=(String[]) 剩余 args.
                // 返回 nested 结构 (int/long/double/string/list/map 混合), 走 NestedMultiOutput 统一处理.
                case OP_FT_SEARCH, OP_FT_AGGREGATE, OP_FT_CREATE, OP_FT_INFO, OP_FT_DROPINDEX -> {
                    ProtocolKeyword cmd = ftCmdFor(op);
                    CommandArgs<byte[], byte[]> args = new CommandArgs<>(ByteArrayCodec.INSTANCE).addValue(a1);
                    if (extra instanceof String[] sa) {
                        for (String s : sa) args.addValue(s.getBytes(StandardCharsets.UTF_8));
                    }
                    yield c.dispatch(cmd, new NestedMultiOutput<>(ByteArrayCodec.INSTANCE), args);
                }
                // 未登记的 op 一律抛出, 不返回 null: 返回 null 会让带 lf 的读命令永远不 complete (业务线程死锁),
                // 或让单条 sync 命令静默"成功". 抛出后 dispatch 的 per-cmd catch 会 completeExceptionally 暴露给业务,
                // 单条路径 (doFlushSingle) 直接上抛给调用方.
                default -> throw new IllegalStateException("unsupported pipeline op: " + op);
            };
        }

        /**
         * 业务作用：把 FT_* OP 映射到对应 ProtocolKeyword (内联 switch, 切分编译期等价表).
         *
         * @param op 见上述说明
         * @return 见上述说明。
         */
        private static ProtocolKeyword ftCmdFor(short op) {
            return switch (op) {
                case OP_FT_SEARCH -> CMD_FT_SEARCH;
                case OP_FT_AGGREGATE -> CMD_FT_AGGREGATE;
                case OP_FT_CREATE -> CMD_FT_CREATE;
                case OP_FT_INFO -> CMD_FT_INFO;
                case OP_FT_DROPINDEX -> CMD_FT_DROPINDEX;
                default -> throw new IllegalStateException("not a FT op: " + op);
            };
        }

        // ==================== 清理 ====================

        /**
         * 业务作用：只清理当前线程的 session ThreadLocal (open/execTag/CMD/LF), 不发命令 (= abort, 丢弃已入队的命令).
         * <p>
         * 给手动 {@code open(...)} / {@link #openIsolated} 的方法在 try/finally 里兜底用: 入队/序列化/flush 前半段抛异常时,
         * 保证 open/CMD/LF 不残留污染后续调用 (尤其定时任务线程会复用). 成功 flush 后调用是幂等 no-op (ThreadLocal 已清).
         * <p>
         * <b>public</b>: 外部包用 {@link #openIsolated} 时, 异常路径可调本方法 abort 临时 session (丢弃未发命令 + 清 ThreadLocal),
         * 不必为了清理而调 {@code pipeline()/pipelineForce()} 把残缺命令发出去.
         */
        public void clearSession() {
            this.clear();
        }

        /**
         * 业务作用：当前线程在本 Actuator 上是否已 open 一个 session (用于判断是否处于嵌套场景).
         *
         * @return 见上述说明。
         */
        boolean isSessionOpen() {
            return Boolean.TRUE.equals(this.open.get());
        }

        /**
         * 业务作用：pipeline 执行完毕 (或 abort) 清理当前线程的 session ThreadLocal + 归还池化对象.
         * <p>
         * <b>CmdBuffer</b>: 一律 recycle 回池 + CMD.remove, 不论线程类型 — buf 是池化资源, 还池后会被其他线程借走,
         * ThreadLocal 留指针就是悬挂引用。
         * <p>
         * <b>LF / pipelineSuccessActions</b>: {@code fullRemove}(虚拟线程 或 throwaway 实例)走 recycle+remove;
         * 否则 (CACHE 单例 + 平台线程) 仅 clear 容器、保留 ThreadLocal 引用复用 (单例长存, 容器是轻量链表壳子).
         * throwaway 实例不会被复用, 保留容器只会在 ThreadLocalMap 留残条目, 故和虚拟线程一样全 remove。
         */
        private void clear() {
            open.remove();
            execTag.remove();
            boolean fullRemove = throwaway || Thread.currentThread().isVirtual();

            // CmdBuffer 池化: 不论何种线程都 recycle 回池 + 清 ThreadLocal 引用
            CmdBuffer buf = CMD.get();
            if (buf != null) {
                buf.recycle();
                CMD.remove();
            }

            if (fullRemove) {
                var lf = LF.get();
                if (lf != null) {
                    lf.recycle();
                    LF.remove();
                }
                RecycleLinkedList<Action> actions = pipelineSuccessActions.get();
                if (actions != null) {
                    actions.recycle();
                    pipelineSuccessActions.remove();
                }
                return;
            }
            var funcs = LF.get();
            if (ColUtils.isNotEmpty(funcs)) funcs.clear();
            RecycleLinkedList<Action> actions = pipelineSuccessActions.get();
            if (ColUtils.isNotEmpty(actions)) actions.clear();
        }

        /**
         * 业务作用：给 async (fire-and-forget) 命令的 lettuce future 挂异常日志.
         * <p>
         * 用 {@link BiConsumerRecycler} 池化携带 op + key + extras 上下文 — static {@link #ASYNC_ERROR_BIC} 策略 0 capture,
         * recycler 从池借后填槽位, whenComplete 调用 (成功/失败都回调) 末尾 ofRecycle 自动归池, 不泄漏.
         * 用 whenComplete 而非 exceptionally 是因后者成功路径不回调, ofRecycle 不会触发归池.
         *
         * @param f      见上述说明
         * @param op     见上述说明
         * @param key    缓存键
         * @param extras 见上述说明
         */
        @SuppressWarnings({"rawtypes", "unchecked"})
        private static void attachAsyncErrorLog(RedisFuture<?> f, short op, byte[] key, Object extras) {
            BiConsumerRecycler<Object, Throwable> br = BiConsumerRecycler.ofRecycle(ASYNC_ERROR_BIC);
            // extras 若是池化 RecycleLinkedMap (OP_XADD_MULTI/OP_HMSET), session 结束会被 recycle 回池, 异步回调时读到的是
            // 已清空/被复用的内容 → 日志串数据. 这类不持有引用 (日志显示 null); 其余 byte[][]/Object[]/String[] 是新建的, 安全.
            Object safeExtras = extras instanceof RecycleLinkedMap ? null : extras;
            br.val(0, op).ref(0, key).ref(1, safeExtras);
            f.whenComplete((BiConsumerRecycler) br);
        }

        /**
         * static 共享策略, 0 capture. lambda 内从 recycler 槽位读 op + key + extras, 异常时 log, 成功时 noop.
         */
        @SuppressWarnings({"rawtypes"})
        private static final Consumer3<Object, Throwable, BiConsumerRecycler<Object, Throwable>> ASYNC_ERROR_BIC = (v, ex, br) -> {
            if (ex == null) return;
            short op = (short) br.longVal(0);
            byte[] key = br.ref(0);
            Object extras = br.ref(1);
            log.error("[lettuce-pipeline] async cmd failed op={} key={} extras={}",
                    op, keyHex(key), extrasForLog(extras), ex);
        };

        /**
         * 业务作用：key 转可读形式 (排障用): 优先 UTF-8, 不可打印则 hex.
         *
         * @param key 缓存键
         * @return 见上述说明。
         */
        private static String keyHex(byte[] key) {
            if (key == null) return "null";
            try {
                String s = new String(key, StandardCharsets.UTF_8);
                for (int i = 0; i < s.length(); i++) {
                    char c = s.charAt(i);
                    if (c < 0x20 || c > 0x7E) return bytesToHex(key);
                }
                return s;
            } catch (Exception e) {
                return bytesToHex(key);
            }
        }

        /**
         * 业务作用：把字节转成十六进制文本，供日志中展示二进制内容。
         * 直接按字符串打印二进制会产出不可读且可能截断的内容，排查时无法还原实际字节。
         *
         * @param b 待转换的字节
         * @return 十六进制文本。
         */
        private static String bytesToHex(byte[] b) {
            StringBuilder sb = new StringBuilder(b.length * 2);
            for (byte x : b) sb.append(String.format("%02x", x));
            return sb.toString();
        }

        /**
         * 业务作用：extras 转可读形式 (排障用): 递归展开 byte[] / byte[][] / Object[] / Map.
         *
         * @param extras 见上述说明
         * @return 见上述说明。
         */
        @SuppressWarnings("rawtypes")
        private static String extrasForLog(Object extras) {
            if (extras == null) return "null";
            if (extras instanceof byte[] b) return keyHex(b);
            if (extras instanceof byte[][] bs) {
                StringBuilder sb = new StringBuilder("[");
                for (int i = 0; i < bs.length; i++) {
                    if (i > 0) sb.append(", ");
                    sb.append(keyHex(bs[i]));
                }
                return sb.append("]").toString();
            }
            if (extras instanceof Object[] arr) {
                StringBuilder sb = new StringBuilder("[");
                for (int i = 0; i < arr.length; i++) {
                    if (i > 0) sb.append(", ");
                    sb.append(extrasForLog(arr[i]));
                }
                return sb.append("]").toString();
            }
            if (extras instanceof Map m) {
                StringBuilder sb = new StringBuilder("{");
                boolean first = true;
                for (Object o : m.entrySet()) {
                    Map.Entry e = (Map.Entry) o;
                    if (!first) sb.append(", ");
                    sb.append(extrasForLog(e.getKey())).append("=").append(extrasForLog(e.getValue()));
                    first = false;
                }
                return sb.append("}").toString();
            }
            return String.valueOf(extras);
        }
    }
}
