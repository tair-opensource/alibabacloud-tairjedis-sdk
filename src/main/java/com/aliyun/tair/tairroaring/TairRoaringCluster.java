package com.aliyun.tair.tairroaring;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairroaring.factory.RoaringBuilderFactory;
import com.aliyun.tair.util.JoinParameters;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.util.SafeEncoder;
import static redis.clients.jedis.Protocol.toByteArray;


import java.util.ArrayList;
import java.util.List;

public class TairRoaringCluster {
    private JedisCluster jc;

    public TairRoaringCluster(JedisCluster jc) {
        this.jc = jc;
    }

    /**
     * TR.SETBIT    TR.SETBIT <key> <offset> <value>
     * setting the value at the offset in roaringbitmap
     *
     * @param key roaring key
     * @param offset the bit offset
     * @param value the bit value
     * @return Success: long; Fail: error
     */
    public long trsetbit(final String key, long offset, final String value) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key),ModuleCommand.TRSETBIT, SafeEncoder.encode(key), toByteArray(offset), SafeEncoder.encode(value));
        return BuilderFactory.LONG.build(obj);
    }
    public long trsetbit(final String key, long offset, long value) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key),ModuleCommand.TRSETBIT, SafeEncoder.encode(key), toByteArray(offset), toByteArray(value));
        return BuilderFactory.LONG.build(obj);
    }
    public long trsetbit(byte[] key, long offset, byte[] value) {
        Object obj = jc.sendCommand(key,ModuleCommand.TRSETBIT, key, toByteArray(offset), value);
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * TR.SETBITS    TR.SETBITS <key> <offset> [<offset2> <offset3> ... <offsetn>]
     * setting the value at the offset in roaringbitmap
     *
     * @param key roaring key
     * @param fields the bit offset
     * @return Success: long; Fail: error
     */
    public long trsetbits(final String key, long... fields) {
        final List<byte[]> args = new ArrayList<byte[]>();
        for (long value : fields) {
            args.add(toByteArray(value));
        }
        Object obj = jc.sendCommand(SafeEncoder.encode(key),ModuleCommand.TRSETBITS,
                JoinParameters.joinParameters(SafeEncoder.encode(key),  args.toArray(new byte[args.size()][])));
        return BuilderFactory.LONG.build(obj);
    }
    public long trsetbits(byte[] key, long... fields) {
        final List<byte[]> args = new ArrayList<byte[]>();
        for (long value : fields) {
            args.add(toByteArray(value));
        }
        Object obj = jc.sendCommand(key,ModuleCommand.TRSETBITS,
                JoinParameters.joinParameters(key, args.toArray(new byte[args.size()][])));
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * TR.GETBIT    TR.GETBIT <key> <offset>
     * getting the bit on the offset of roaringbitmap
     *
     * @param key roaring key
     * @param offset the bit offset
     * @return Success: long; Fail: error
     */
    public long trgetbit(final String key, long offset) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key),ModuleCommand.TRGETBIT, SafeEncoder.encode(key), toByteArray(offset));
        return BuilderFactory.LONG.build(obj);
    }
    public long trgetbit(byte[] key, long offset) {
        Object obj = jc.sendCommand(key,ModuleCommand.TRGETBIT, key, toByteArray(offset));
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * TR.GETBITS    TR.GETBITS <key> <offset> [<offset2> <offset3> ... <offsetn>]
     * get the value at the offset in roaringbitmap
     *
     * @param key roaring key
     * @param fields the bit offset
     * @return Success: array long; Fail: error
     */
    public List<Long> trgetbits(final String key, long... fields) {
        final List<byte[]> args = new ArrayList<byte[]>();
        for (long value : fields) {
            args.add(toByteArray(value));
        }
        Object obj = jc.sendCommand(SafeEncoder.encode(key),ModuleCommand.TRGETBITS,
                JoinParameters.joinParameters(SafeEncoder.encode(key),  args.toArray(new byte[args.size()][])));
        return BuilderFactory.LONG_LIST.build(obj);
    }
    public List<Long> trgetbits(byte[] key, long... fields) {
        final List<byte[]> args = new ArrayList<byte[]>();
        for (long value : fields) {
            args.add(toByteArray(value));
        }
        Object obj = jc.sendCommand(key,ModuleCommand.TRGETBITS,
                JoinParameters.joinParameters(key, args.toArray(new byte[args.size()][])));
        return BuilderFactory.LONG_LIST.build(obj);
    }


    /**
     * TR.CLEARBITS    TR.CLEARBITS <key> <offset> [<offset2> <offset3> ... <offsetn>]
     * remove the value at the offset in roaringbitmap
     *
     * @param key roaring key
     * @param fields the bit offset
     * @return Success: long; Fail: error
     */
    public long trclearbits(final String key, long... fields) {
        final List<byte[]> args = new ArrayList<byte[]>();
        for (long value : fields) {
            args.add(toByteArray(value));
        }
        Object obj = jc.sendCommand(SafeEncoder.encode(key),ModuleCommand.TRCLEARBITS,
                JoinParameters.joinParameters(SafeEncoder.encode(key),  args.toArray(new byte[args.size()][])));
        return BuilderFactory.LONG.build(obj);
    }
    public long trclearbits(byte[] key, long... fields) {
        final List<byte[]> args = new ArrayList<byte[]>();
        for (long value : fields) {
            args.add(toByteArray(value));
        }
        Object obj = jc.sendCommand(key,ModuleCommand.TRCLEARBITS,
                JoinParameters.joinParameters(key, args.toArray(new byte[args.size()][])));
        return BuilderFactory.LONG.build(obj);
    }


    /**
     * TR.RANGE TR.RANGE <key> <start> <end>
     * retrive the setted bit between the closed range, return them with int array
     *
     * @param key roaring bitmap key
     * @param start range start
     * @param end range end
     * @return Success: array long; Fail: error
     */
    public List<Long> trrange(final String key, long start, long end) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key),ModuleCommand.TRRANGE, SafeEncoder.encode(key), toByteArray(start), toByteArray(end));
        return BuilderFactory.LONG_LIST.build(obj);
    }
    public List<Long> trrange(byte[] key, long start, long end) {
        Object obj = jc.sendCommand(key,ModuleCommand.TRRANGE, key, toByteArray(start), toByteArray(end));
        return BuilderFactory.LONG_LIST.build(obj);
    }

    /**
     * TR.RANGEBITARRAY TR.RANGEBITARRAY <key> <start> <end>
     * retrive the setted bit between the closed range, return them by the string bit-array
     *
     * @param key roaring bitmap key
     * @param start range start
     * @param end range end
     * @return Success: string; Fail: error
     */
    public String trrangebitarray(final String key, long start, long end) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key),ModuleCommand.TRRANGEBITARRAY, SafeEncoder.encode(key), toByteArray(start), toByteArray(end));
        return BuilderFactory.STRING.build(obj);
    }
    public String trrangebitarray(byte[] key, long start, long end) {
        Object obj = jc.sendCommand(key,ModuleCommand.TRRANGEBITARRAY, key, toByteArray(start), toByteArray(end));
        return BuilderFactory.STRING.build(obj);
    }


    /**
     * TR.APPENDBITARRAY TR.APPENDBITARRAY <key> <offset> <value>
     * append the bit array after the offset in roaringbitmap
     *
     * @param key roaring key
     * @param offset the bit offset
     * @param value the bit value
     * @return Success: long; Fail: error
     */
    public long trappendbitarray(final String key, long offset, final String value) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key),ModuleCommand.TRAPPENDBITARRAY, SafeEncoder.encode(key), toByteArray(offset), SafeEncoder.encode(value));
        return BuilderFactory.LONG.build(obj);
    }
    public long trappendbitarray(final String key, long offset, byte[] value) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key),ModuleCommand.TRAPPENDBITARRAY, SafeEncoder.encode(key), toByteArray(offset), value);
        return BuilderFactory.LONG.build(obj);
    }
    public long trappendbitarray(byte[] key, long offset, byte[] value) {
        Object obj = jc.sendCommand(key,ModuleCommand.TRAPPENDBITARRAY, key, toByteArray(offset), value);
        return BuilderFactory.LONG.build(obj);
    }


    /**
     * TR.SETRANGE TR.SETRANGE <key> <start> <end>
     * 设置 Roaring bitmap中range 区间内元素为1-bit, 返回设置成功的 bit 数
     *
     * @param key roaring bitmap key
     * @param start range start
     * @param end range end
     * @return Success: array long; Fail: error
     */
    public long trsetrange(final String key, long start, long end) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key),ModuleCommand.TRSETRANGE, SafeEncoder.encode(key), toByteArray(start), toByteArray(end));
        return BuilderFactory.LONG.build(obj);
    }
    public long trsetrange(byte[] key, long start, long end) {
        Object obj = jc.sendCommand(key,ModuleCommand.TRSETRANGE, key, toByteArray(start), toByteArray(end));
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * TR.FLIPRANGE TR.FLIPRANGE <key> <start> <end>
     *  翻转 Roaring bitmap 中range 区间内所有bit, 返回设置成功的 bit 数
     *
     * @param key roaring bitmap key
     * @param start range start
     * @param end range end
     * @return Success: array long; Fail: error
     */
    public long trfliprange(final String key, long start, long end) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key),ModuleCommand.TRFLIPRANGE, SafeEncoder.encode(key), toByteArray(start), toByteArray(end));
        return BuilderFactory.LONG.build(obj);
    }
    public long trfliprange(byte[] key, long start, long end) {
        Object obj = jc.sendCommand(key,ModuleCommand.TRFLIPRANGE, key, toByteArray(start), toByteArray(end));
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * TR.BITCOUNT	TR.BITCOUNT <key> [<start> <end>]
     * counting bit set as 1 in the roaringbitmap
     * start and end are optional, you can count 1-bit in range by passing start and end
     *
     * @param key roaring key
     * @param start range start
     * @param end range end
     * @return Success: long; Fail: error
     */
    public long trbitcount(final String key, long start, long end) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key),ModuleCommand.TRBITCOUNT, SafeEncoder.encode(key), toByteArray(start), toByteArray(end));
        return BuilderFactory.LONG.build(obj);
    }
    public long trbitcount(final String key) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key),ModuleCommand.TRBITCOUNT, SafeEncoder.encode(key));
        return BuilderFactory.LONG.build(obj);
    }
    public long trbitcount(byte[] key) {
        Object obj = jc.sendCommand(key,ModuleCommand.TRBITCOUNT, key);
        return BuilderFactory.LONG.build(obj);
    }
    public long trbitcount(byte[] key, long start, long end) {
        Object obj = jc.sendCommand(key,ModuleCommand.TRBITCOUNT, key, toByteArray(start), toByteArray(end));
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * TR.MIN	TR.MIN <key>
     * 返回key对应的bitmap集合中首个bit值为1的偏移量，不存在时返回-1。
     *
     * @param key roaring key
     * @return Success: long; Fail: error
     */
    public long trmin(final String key) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key),ModuleCommand.TRMIN, SafeEncoder.encode(key));
        return BuilderFactory.LONG.build(obj);
    }

    public long trmin(byte[] key) {
        Object obj = jc.sendCommand(key,ModuleCommand.TRMIN, key);
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * TR.MAX	TR.MAX <key>
     * 返回key对应的bitmap集合中bit值为1的最大偏移量，不存在时返回-1。
     *
     * @param key roaring key
     * @return Success: long; Fail: error
     */
    public long trmax(final String key) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key),ModuleCommand.TRMAX, SafeEncoder.encode(key));
        return BuilderFactory.LONG.build(obj);
    }

    public long trmax(byte[] key) {
        Object obj = jc.sendCommand(key,ModuleCommand.TRMAX, key);
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * TR.OPTIMIZE	TR.OPTIMIZE <key>
     * 优化Roaring bitmap的存储空间。如果目标对象相对较大，且创建后以只读操作为主，可以主动执行此命令。
     *
     * @param key roaring key
     * @return Success: +OK; Fail: error
     */
    public String troptimize(final String key) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key),ModuleCommand.TROPTIMIZE, SafeEncoder.encode(key));
        return BuilderFactory.STRING.build(obj);
    }
    public String troptimize(byte[] key) {
        Object obj = jc.sendCommand(key,ModuleCommand.TROPTIMIZE, key);
        return BuilderFactory.STRING.build(obj);
    }


    /**
     * TR.STAT	TR.STAT <key>
     * 返回当前bitmap的统计信息, 包括各种 roaringbitmap 容器的数量以及内存使用状况等信息。
     *
     * @param key roaring key
     * @return Success: string; Fail: error
     */
    public String trstat(final String key, boolean json) {
        if (json == true) {
            Object obj = jc.sendCommand(SafeEncoder.encode(key),ModuleCommand.TRSTAT, SafeEncoder.encode(key), SafeEncoder.encode("JSON"));
            return BuilderFactory.STRING.build(obj);
        }
        Object obj = jc.sendCommand(SafeEncoder.encode(key),ModuleCommand.TRSTAT, SafeEncoder.encode(key));
        return BuilderFactory.STRING.build(obj);
    }
    public String trstat(byte[] key, boolean json) {
        if (json == true) {
            Object obj = jc.sendCommand(key,ModuleCommand.TRSTAT, key, SafeEncoder.encode("JSON"));
            return BuilderFactory.STRING.build(obj);
        }
        Object obj = jc.sendCommand(key,ModuleCommand.TRSTAT, key);
        return BuilderFactory.STRING.build(obj);
    }


    /**
     * TR.BITPOS	TR.BITPOS <key> <value> [counting]
     * 传入一个value值（1或者0），在目标Key（TairRoaring数据结构）中查找首个被设置为指定值的bit位，并返回该bit位的偏移量（offset），偏移量（offset）从0开始。
     *  通过传入额外参数 counting 可以控制查找第 counting 个元素，如果 counting 为负则表示从后先前查找.
     *
     * @param key roaring key
     * @param value bit value
     * @param count count of the bit, negetive count indecate the reverse iteration
     * @return Success: long; Fail: error
     */
    public long trbitpos(final String key, final String value, long count) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key),ModuleCommand.TRBITPOS, SafeEncoder.encode(key), SafeEncoder.encode(value), toByteArray(count));
        return BuilderFactory.LONG.build(obj);
    }
    public long trbitpos(final String key, final String value) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key),ModuleCommand.TRBITPOS, SafeEncoder.encode(key), SafeEncoder.encode(value));
        return BuilderFactory.LONG.build(obj);
    }
    public long trbitpos(final String key, long value) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key),ModuleCommand.TRBITPOS, SafeEncoder.encode(key), toByteArray(value));
        return BuilderFactory.LONG.build(obj);
    }
    public long trbitpos(final String key, long value, long count) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key),ModuleCommand.TRBITPOS, SafeEncoder.encode(key), toByteArray(value), toByteArray(count));
        return BuilderFactory.LONG.build(obj);
    }
    public long trbitpos(byte[] key, byte[] value) {
        Object obj = jc.sendCommand(key,ModuleCommand.TRBITPOS, key, value);
        return BuilderFactory.LONG.build(obj);
    }
    public long trbitpos(byte[] key, byte[] value, byte[] count) {
        Object obj = jc.sendCommand(key,ModuleCommand.TRBITPOS, key, value, count);
        return BuilderFactory.LONG.build(obj);
    }


    /**
     * TR.BITOP	TR.BITOP <destkey> <operation> <key> [<key2> <key3>...]
     * 对Roaring bitmap执行集合运算操作，计算结果存储在destkey中。
     * 说明 集群架构暂不支持该命令。
     *
     * @param destkey   result store int destkey
     * @param operation operation type: AND OR NOT XOR DIFF
     * @param keys   operation joining keys
     * @return Success: long; Fail: error
     */
    public long trbitop(final String destkey, final String operation, final String... keys) {
        Object obj = jc.sendCommand(SafeEncoder.encode(destkey),ModuleCommand.TRBITOP,
            JoinParameters.joinParameters(SafeEncoder.encode(destkey), SafeEncoder.encode(operation), SafeEncoder.encodeMany(keys)));
        return BuilderFactory.LONG.build(obj);
    }
    public long trbitop(byte[] destkey, byte[] operation, byte[]... keys) {
        Object obj = jc.sendCommand(destkey,ModuleCommand.TRBITOP, JoinParameters.joinParameters(operation, keys));
        return BuilderFactory.LONG.build(obj);
    }


    /**
     * TR.RANK TR.RANK <key> <offset>
     * 计算给定 key 对应的 roaringbitmap 对象中小于等于 offset 元素的个数
     *
     * @param key roaring key
     * @param offset bit ranking offset
     * @return Success: long; Fail: error
     */
    public long trrank(final String key, long offset) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.TRRANK, SafeEncoder.encode(key), toByteArray(offset));
        return BuilderFactory.LONG.build(obj);
    }
    public long trrank(byte[] key, byte[] offset) {
        Object obj = jc.sendCommand(key, ModuleCommand.TRRANK, key, offset);
        return BuilderFactory.LONG.build(obj);
    }


    /**
     * TR.BITOPCARD	TR.BITOPCARD <operation> <key> [<key2> <key3>...]
     * 对Roaring bitmap执行集合运算操作，返回结算结果中 1-bit 数
     * 说明 集群架构暂不支持该命令。
     *
     * @param operation operation type: AND OR NOT XOR DIFF
     * @param keys   operation joining keys
     * @return Success: long; Fail: error
     */
    public long trbitopcard(final String operation , final String... keys) {
        Object obj = jc.sendCommand(SafeEncoder.encode(keys[0]), ModuleCommand.TRBITOPCARD,
            JoinParameters.joinParameters(SafeEncoder.encode(operation), SafeEncoder.encodeMany(keys)));
        return BuilderFactory.LONG.build(obj);
    }
    public long trbitopcard(byte[] operation, byte[]... keys) {
        Object obj = jc.sendCommand(keys[0], ModuleCommand.TRBITOPCARD, JoinParameters.joinParameters(operation, keys));
        return BuilderFactory.LONG.build(obj);
    }


    /**
     * TR.SCAN TR.SCAN <key> <cursor> [COUNT <count>]
     * iterating element from cursor, COUNT indecate the max elements count per request
     *
     * @param key roaring bitmap key
     * @param cursor scan cursor, 0 stand for the very first value
     * @param count iteration counting by scan
     * @return Success: cursor and array long; Fail: error
     */
    public ScanResult<Long> trscan(final String  key, long cursor, long count) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key),ModuleCommand.TRSCAN, SafeEncoder.encode(key), toByteArray(cursor), SafeEncoder.encode("COUNT"), toByteArray(count));
        return RoaringBuilderFactory.TRSCAN_RESULT_LONG.build(obj);
    }
    public ScanResult<Long> trscan(final String key, long cursor) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key),ModuleCommand.TRSCAN, SafeEncoder.encode(key), toByteArray(cursor));
        return RoaringBuilderFactory.TRSCAN_RESULT_LONG.build(obj);
    }
    public ScanResult<byte[]> trscan(byte[] key, byte[] cursor) {
        Object obj = jc.sendCommand(key,ModuleCommand.TRSCAN, key, cursor);
        return RoaringBuilderFactory.TRSCAN_RESULT_BYTE.build(obj);
    }
    public ScanResult<byte[]> trscan(byte[] key, byte[] cursor, byte[] count) {
        Object obj = jc.sendCommand(key,ModuleCommand.TRSCAN, key, cursor, SafeEncoder.encode("COUNT"), count);
        return RoaringBuilderFactory.TRSCAN_RESULT_BYTE.build(obj);
    }


    /**
     * TR.LOAD TR.LOAD <key> <value>
     * Loading CRoaring serilizing style data into a empty key
     *
     * @param key   result store int key
     * @param value data
     * @return Success: long; Fail: error
     */
    public long trload(final String key, byte[] value) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key),ModuleCommand.TRLOAD, SafeEncoder.encode(key), value);
        return BuilderFactory.LONG.build(obj);
    }
    public long trload(byte[] key, byte[] value) {
        Object obj = jc.sendCommand(key,ModuleCommand.TRLOAD, key, value);
        return BuilderFactory.LONG.build(obj);
    }


    /**
     * TR.LOADSTRING TR.LOAD <key> <stringkey>
     * Loading string into into a empty roaringbitmap
     *
     * @param key   result store int key
     * @param stringkey string, aka origional bitmap
     * @return Success: long; Fail: error
     */
    public long trloadstring(final String key, final String stringkey) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key),ModuleCommand.TRLOADSTRING, SafeEncoder.encode(key), SafeEncoder.encode(stringkey));
        return BuilderFactory.LONG.build(obj);
    }
    public long trloadstring(byte[] key, byte[] stringkey) {
        Object obj = jc.sendCommand(key,ModuleCommand.TRLOADSTRING, key, stringkey);
        return BuilderFactory.LONG.build(obj);
    }


    /**
     * TR.DIFF	TR.DIFF <destkey> <key1> <key2>
     * 计算key1与key2对应Roaring Bitmap的差集，并将结果储到destkey所指的键中。
     * 说明 集群架构暂不支持该命令。
     *
     * @param destkey   result store int key
     * @param key1 operation diff key
     * @param key2 operation diff key
     * @return Success: OK; Fail: error
     */
    public String trdiff(final String destkey, final String key1, final String key2) {
        Object obj = jc.sendCommand(SafeEncoder.encode(destkey),ModuleCommand.TRDIFF,
            SafeEncoder.encode(destkey), SafeEncoder.encode(key1), SafeEncoder.encode(key2));
        return BuilderFactory.STRING.build(obj);
    }

    public String trdiff(byte[] destkey, byte[] key1, byte[] key2) {
        Object obj = jc.sendCommand(destkey, ModuleCommand.TRDIFF, destkey, key1, key2);
        return BuilderFactory.STRING.build(obj);
    }


    /**
     * TR.SETINTARRAY	TR.SETINTARRAY <key> <value1> [<value2> <value3> ... <valueN>]
     * 根据传入的整形数组来设置对应的Roaring bitmap, 该命令会覆盖已存在的Roaring bitmap对象。
     *
     * @param key roaring bitmap key
     * @param fields bit offset value
     * @return Success: +OK; Fail: error
     */
    public String trsetintarray(final String key, long... fields) {
        final List<byte[]> args = new ArrayList<byte[]>();
        for (long value : fields) {
            args.add(toByteArray(value));
        }
        Object obj = jc.sendCommand(SafeEncoder.encode(key),ModuleCommand.TRSETINTARRAY,
            JoinParameters.joinParameters(SafeEncoder.encode(key), args.toArray(new byte[args.size()][])));
        return BuilderFactory.STRING.build(obj);
    }

    public String trsetintarray(byte[] key, long... fields) {
        final List<byte[]> args = new ArrayList<byte[]>();
        for (long value : fields) {
            args.add(toByteArray(value));
        }
        Object obj = jc.sendCommand(key,ModuleCommand.TRSETINTARRAY,
                JoinParameters.joinParameters(key, args.toArray(new byte[args.size()][])));
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * TR.APPENDINTARRAY	TR.APPENDINTARRAY <key> <value1> [<value2> <value3> ... <valueN>]
     * 将bitmap中指定bit位的值（value）设置为1，支持传入多个值。
     *
     * @param key roaring bitmap key
     * @param fields bit offset value
     * @return Success: +OK; Fail: error
     */
    public String trappendintarray(final String key, long... fields) {
        final List<byte[]> args = new ArrayList<byte[]>();
        for (long value : fields) {
            args.add(toByteArray(value));
        }
        Object obj = jc.sendCommand(SafeEncoder.encode(key),ModuleCommand.TRAPPENDINTARRAY,
                JoinParameters.joinParameters(SafeEncoder.encode(key),  args.toArray(new byte[args.size()][])));
        return BuilderFactory.STRING.build(obj);
    }
    public String trappendintarray(byte[] key, long... fields) {
        final List<byte[]> args = new ArrayList<byte[]>();
        for (long value : fields) {
            args.add(toByteArray(value));
        }
        Object obj = jc.sendCommand(key,ModuleCommand.TRAPPENDINTARRAY,
                JoinParameters.joinParameters(key, args.toArray(new byte[args.size()][])));
        return BuilderFactory.STRING.build(obj);
    }


    /**
     * TR.SETBITARRAY	TR.SETBITARRAY <key> <value>
     * 根据传入的bit（由0和1组成的字符串），来创建一个位图（bitmap）。执行该命令时，Redis会逐个字符判断，只操作bit为1的字符，如果目标Key已存在则会覆盖已有数据。
     *
     * @param key roaring bitmap key
     * @param value bit offset value
     * @return Success: +OK; Fail: error
     */
    public String trsetbitarray(final String key, final String value) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key),ModuleCommand.TRSETBITARRAY, SafeEncoder.encode(key), SafeEncoder.encode(value));
        return BuilderFactory.STRING.build(obj);
    }
    public String trsetbitarray(byte[] key, byte[] value) {
        Object obj = jc.sendCommand(key,ModuleCommand.TRSETBITARRAY, key, value);
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * TR.JACCARD TR.JACCARD <key1> <key2>
     * 计算key1与key2对应Roaring Bitmap的jaccard index, 返回对应的值.
     * 说明 集群架构暂不支持该命令。
     *
     * @param key1 operation key
     * @param key2 operation key
     * @return Success: double; Fail: error
     */
    public Double trjaccard(final String key1, final String key2) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key1),ModuleCommand.TRJACCARD, SafeEncoder.encode(key1), SafeEncoder.encode(key2));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double trjaccard(byte[] key1, byte[] key2) {
        Object obj = jc.sendCommand(key1,ModuleCommand.TRJACCARD, key1, key2);
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * TR.CONTAINS TR.CONTAINS <key1> <key2>
     * 计算 key1 对应的 roaringbitmap 是否是 key2 对应 roaringbitmap 的子集，返回 1 是子集 0 不是子集
     * 说明 集群架构暂不支持该命令。
     *
     * @param key1 operation key
     * @param key2 operation key
     * @return Success: double; Fail: error
     */
    public Boolean trcontains(final String key1, final String key2) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key1),ModuleCommand.TRCONTAINS, SafeEncoder.encode(key1), SafeEncoder.encode(key2));
        return BuilderFactory.BOOLEAN.build(obj);
    }

    public Boolean trcontains(byte[] key1, byte[] key2) {
        Object obj = jc.sendCommand(key1,ModuleCommand.TRCONTAINS, key1, key2);
        return BuilderFactory.BOOLEAN.build(obj);
    }
}
