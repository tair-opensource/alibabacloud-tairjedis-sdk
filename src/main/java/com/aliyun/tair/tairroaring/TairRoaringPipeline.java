package com.aliyun.tair.tairroaring;

import java.util.ArrayList;
import java.util.List;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.util.JoinParameters;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.util.SafeEncoder;

import static redis.clients.jedis.Protocol.toByteArray;

public class TairRoaringPipeline extends Pipeline {
    /**
     * TR.SETBIT    TR.SETBIT <key> <offset> <value>
     * setting the value at the offset in roaringbitmap
     *
     * @param key roaring key
     * @param offset the bit offset
     * @param value the bit value
     * @return Success: long; Fail: error
     */
    public Response<Long> trsetbit(final String key, long offset, final String value) {
        getClient("").sendCommand(ModuleCommand.TRSETBIT, SafeEncoder.encode(key), toByteArray(offset), SafeEncoder.encode(value));
        return getResponse(BuilderFactory.LONG);
    }
    public Response<Long> trsetbit(byte[] key, long offset, byte[] value) {
        getClient("").sendCommand(ModuleCommand.TRSETBIT, key, toByteArray(offset), value);
        return getResponse(BuilderFactory.LONG);
    }
    public Response<Long> trsetbit(final String key, long offset, long value) {
        getClient("").sendCommand(ModuleCommand.TRSETBIT, SafeEncoder.encode(key), toByteArray(offset), toByteArray(value));
        return getResponse(BuilderFactory.LONG);
    }


    /**
     * TR.GETBIT    TR.GETBIT <key> <offset>
     * getting the bit on the offset of roaringbitmap
     *
     * @param key roaring key
     * @param offset the bit offset
     * @return Success: long; Fail: error
     */
    public Response<Long> trgetbit(final String key, long offset) {
        getClient("").sendCommand(ModuleCommand.TRGETBIT, SafeEncoder.encode(key), toByteArray(offset));
        return getResponse(BuilderFactory.LONG);
    }
    public Response<Long> trgetbit(final String key, final String offset) {
        getClient("").sendCommand(ModuleCommand.TRGETBIT, SafeEncoder.encode(key), SafeEncoder.encode(offset));
        return getResponse(BuilderFactory.LONG);
    }
    public Response<Long> trgetbit(byte[] key, long offset) {
        getClient("").sendCommand(ModuleCommand.TRGETBIT, key, toByteArray(offset));
        return getResponse(BuilderFactory.LONG);
    }


    /**
     * TR.BITCOUNT	TR.BITCOUNT <key>
     * counting bit set as 1 in the roaringbitmap
     *
     * @param key roaring key
     * @return Success: long; Fail: error
     */
    public Response<Long> trbitcount(final String key) {
        getClient("").sendCommand(ModuleCommand.TRBITCOUNT, SafeEncoder.encode(key));
        return getResponse(BuilderFactory.LONG);
    }
    public Response<Long> trbitcount(byte[] key) {
        getClient("").sendCommand(ModuleCommand.TRBITCOUNT, key);
        return getResponse(BuilderFactory.LONG);
    }

    /**
     * TR.MIN	TR.MIN <key>
     * 返回key对应的bitmap集合中首个bit值为1的偏移量，不存在时返回-1。
     *
     * @param key roaring key
     * @return Success: long; Fail: error
     */
    public Response<Long> trmin(final String key) {
        getClient("").sendCommand(ModuleCommand.TRMIN, SafeEncoder.encode(key));
        return getResponse(BuilderFactory.LONG);
    }
    public Response<Long> trmin(byte[] key) {
        getClient("").sendCommand(ModuleCommand.TRMIN, key);
        return getResponse(BuilderFactory.LONG);
    }

    /**
     * TR.MAX	TR.MAX <key>
     * 返回key对应的bitmap集合中bit值为1的最大偏移量，不存在时返回-1。
     *
     * @param key roaring key
     * @return Success: long; Fail: error
     */
    public Response<Long> trmax(final String key) {
        getClient("").sendCommand(ModuleCommand.TRMAX, SafeEncoder.encode(key));
        return getResponse(BuilderFactory.LONG);
    }
    public Response<Long> trmax(byte[] key) {
        getClient("").sendCommand(ModuleCommand.TRMAX, key);
        return getResponse(BuilderFactory.LONG);
    }


    /**
     * TR.OPTIMIZE	TR.OPTIMIZE <key>
     * 优化Roaring bitmap的存储空间。如果目标对象相对较大，且创建后以只读操作为主，可以主动执行此命令。
     *
     * @param key roaring key
     * @return Success: +OK; Fail: error
     */
    public Response<String> troptimize(final String key) {
        getClient("").sendCommand(ModuleCommand.TROPTIMIZE, SafeEncoder.encode(key));
        return getResponse(BuilderFactory.STRING);
    }
    public Response<String> troptimize(byte[] key) {
        getClient("").sendCommand(ModuleCommand.TROPTIMIZE, key);
        return getResponse(BuilderFactory.STRING);
    }


    /**
     * TR.STAT	TR.STAT <key>
     * 返回当前bitmap的统计信息, 包括各种 roaringbitmap 容器的数量以及内存使用状况等信息。
     *
     * @param key roaring key
     * @return Success: string; Fail: error
     */
    public Response<String> trstat(final String key) {
        getClient("").sendCommand(ModuleCommand.TRSTAT, SafeEncoder.encode(key));
        return getResponse(BuilderFactory.STRING);
    }
    public Response<String> trstat(byte[] key) {
        getClient("").sendCommand(ModuleCommand.TRSTAT, key);
        return getResponse(BuilderFactory.STRING);
    }


    /**
     * TR.BITPOS	TR.BITPOS <key> <value>
     * 传入一个value值（1或者0），在目标Key（TairRoaring数据结构）中查找首个被设置为指定值的bit位，并返回该bit位的偏移量（offset），偏移量（offset）从0开始。
     *
     * @param key roaring key
     * @param value bit value
     * @return Success: long; Fail: error
     */
    public Response<Long> trbitpos(final String key, final String value) {
        getClient("").sendCommand(ModuleCommand.TRBITPOS, SafeEncoder.encode(key), SafeEncoder.encode(value));
        return getResponse(BuilderFactory.LONG);
    }
    public Response<Long> trbitpos(final String key, long value) {
        getClient("").sendCommand(ModuleCommand.TRBITPOS, SafeEncoder.encode(key), toByteArray(value));
        return getResponse(BuilderFactory.LONG);
    }
    public Response<Long> trbitpos(byte[] key, byte[] value) {
        getClient("").sendCommand(ModuleCommand.TRBITPOS, key, value);
        return getResponse(BuilderFactory.LONG);
    }


    /**
     * TR.BITOP	TR.BITOP <destkey> <operation> <key> [<key2> <key3>...]
     * 对Roaring bitmap执行集合运算操作，计算结果存储在destkey中。
     * 说明 集群架构暂不支持该命令。
     *
     * @param key   result store int destkey
     * @param operation operation type: AND OR NOT XOR DIFF
     * @param key   operation joining keys
     * @return Success: long; Fail: error
     */
    public Response<Long> trbitop(final String key, final String operation, final String... fields) {
        getClient("").sendCommand(ModuleCommand.TRBITOP,
            JoinParameters.joinParameters(SafeEncoder.encode(key), SafeEncoder.encode(operation), SafeEncoder.encodeMany(fields)));
        return getResponse(BuilderFactory.LONG);
    }
    public Response<Long> trbitop(byte[] key, byte[] operation, byte[]... fields) {
        getClient("").sendCommand(ModuleCommand.TRBITOP, JoinParameters.joinParameters(key, operation, fields));
        return getResponse(BuilderFactory.LONG);
    }

    /**
     * TR.DIFF	TR.DIFF <destkey> <key1> <key2>
     * 计算key1与key2对应Roaring Bitmap的差集，并将结果储到destkey所指的键中。
     * 说明 集群架构暂不支持该命令。
     *
     * @param key   result store int key
     * @param fields   operation joining keys
     * @return Success: OK; Fail: error
     */
    public Response<String> trdiff(final String key, final String... fields) {
        getClient("").sendCommand(ModuleCommand.TRDIFF,
            JoinParameters.joinParameters(SafeEncoder.encode(key), SafeEncoder.encodeMany(fields)));
        return getResponse(BuilderFactory.STRING);
    }
    public Response<String> trdiff(byte[] key, byte[]... fields) {
        getClient("").sendCommand(ModuleCommand.TRDIFF, JoinParameters.joinParameters(key, fields));
        return getResponse(BuilderFactory.STRING);
    }


    /**
     * TR.SETINTARRAY	TR.SETINTARRAY <key> <value1> [<value2> <value3> ... <valueN>]
     * 根据传入的整形数组来设置对应的Roaring bitmap, 该命令会覆盖已存在的Roaring bitmap对象。
     *
     * @param key roaring bitmap key
     * @param fields bit offset value
     * @return Success: +OK; Fail: error
     */
    public Response<String> trsetintarray(final String key, long... fields) {
        final List<byte[]> args = new ArrayList<byte[]>();
        for (long value : fields) {
            args.add(toByteArray(value));
        }
        getClient("").sendCommand(ModuleCommand.TRSETINTARRAY,
            JoinParameters.joinParameters(SafeEncoder.encode(key), args.toArray(new byte[args.size()][])));
        return getResponse(BuilderFactory.STRING);
    }
    public Response<String> trsetintarray(byte[] key, long... fields) {
        final List<byte[]> args = new ArrayList<byte[]>();
        for (long value : fields) {
            args.add(toByteArray(value));
        }
        getClient("").sendCommand(ModuleCommand.TRSETINTARRAY,
                JoinParameters.joinParameters(key, args.toArray(new byte[args.size()][])));
        return getResponse(BuilderFactory.STRING);
    }


    /**
     * TR.APPENDINTARRAY	TR.APPENDINTARRAY <key> <value1> [<value2> <value3> ... <valueN>]
     * 将bitmap中指定bit位的值（value）设置为1，支持传入多个值。
     *
     * @param key roaring bitmap key
     * @param fields bit offset value
     * @return Success: +OK; Fail: error
     */
    public Response<String> trappendintarray(final String key, long... fields) {
        final List<byte[]> args = new ArrayList<byte[]>();
        for (long value : fields) {
            args.add(toByteArray(value));
        }
        getClient("").sendCommand(ModuleCommand.TRAPPENDINTARRAY,
                JoinParameters.joinParameters(SafeEncoder.encode(key),  args.toArray(new byte[args.size()][])));
        return getResponse(BuilderFactory.STRING);
    }
    public Response<String> trappendintarray(byte[] key, long... fields) {
        final List<byte[]> args = new ArrayList<byte[]>();
        for (long value : fields) {
            args.add(toByteArray(value));
        }
        getClient("").sendCommand(ModuleCommand.TRAPPENDINTARRAY,
                JoinParameters.joinParameters(key, args.toArray(new byte[args.size()][])));
        return getResponse(BuilderFactory.STRING);
    }


    /**
     * TR.SETBITARRAY	TR.SETBITARRAY <key> <value>
     * 根据传入的bit（由0和1组成的字符串），来创建一个位图（bitmap）。执行该命令时，Redis会逐个字符判断，只操作bit为1的字符，如果目标Key已存在则会覆盖已有数据。
     *
     * @param key roaring bitmap key
     * @param value bit offset value
     * @return Success: +OK; Fail: error
     */
    public Response<String> trsetbitarray(final String key, long value) {
        getClient("").sendCommand(ModuleCommand.TRSETBITARRAY, SafeEncoder.encode(key), toByteArray(value));
        return getResponse(BuilderFactory.STRING);
    }
    public Response<String> trsetbitarray(byte[] key, byte[] value) {
        getClient("").sendCommand(ModuleCommand.TRSETBITARRAY, key, value);
        return getResponse(BuilderFactory.STRING);
    }


    /**
     * TR.RANGEINTARRAY	TR.RANGEINTARRAY <key> <start> <end>
     * 获取Roaring bitmap中bit值为1所对应的int数组的元素范围，下标为元素范围的元素，如果下标超过对应int数组长度则用0补齐。
     *
     * @param key roaring bitmap key
     * @param start range start
     * @param end range end
     * @return Success: array long; Fail: error
     */
    public Response<List<Long>> trrangeintarray(final String key, long start, long end) {
        getClient("").sendCommand(ModuleCommand.TRRANGEINTARRAY, SafeEncoder.encode(key), toByteArray(start), toByteArray(end));
        return getResponse(BuilderFactory.LONG_LIST);
    }
    public Response<List<Long>> trrangeintarray(byte[] key, long start, long end) {
        getClient("").sendCommand(ModuleCommand.TRRANGEINTARRAY, key, toByteArray(start), toByteArray(end));
        return getResponse(BuilderFactory.LONG_LIST);
    }
}
