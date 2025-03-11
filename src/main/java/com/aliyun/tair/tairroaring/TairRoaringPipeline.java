package com.aliyun.tair.tairroaring;

import java.util.ArrayList;
import java.util.List;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.jedis3.ScanResult;
import com.aliyun.tair.tairroaring.factory.RoaringBuilderFactory;
import io.valkey.BuilderFactory;
import io.valkey.CommandArguments;
import io.valkey.CommandObject;
import io.valkey.Jedis;
import io.valkey.Pipeline;
import io.valkey.Response;

import static io.valkey.Protocol.toByteArray;

public class TairRoaringPipeline extends Pipeline {
    public TairRoaringPipeline(Jedis jedis) {
        super(jedis);
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
    public Response<Long> trsetbit(final String key, long offset, final String value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRSETBIT)
            .key(key)
            .add(offset)
            .add(value), BuilderFactory.LONG));
    }
    public Response<Long> trsetbit(final String key, long offset, long value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRSETBIT)
            .key(key)
            .add(offset)
            .add(value), BuilderFactory.LONG));
    }
    public Response<Long> trsetbit(byte[] key, long offset, byte[] value) {
         return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRSETBIT)
            .key(key)
            .add(offset)
            .add(value), BuilderFactory.LONG));
    }

    /**
     * TR.SETBITS    TR.SETBITS <key> <offset> [<offset2> <offset3> ... <offsetn>]
     * setting the value at the offset in roaringbitmap
     *
     * @param fields the bit offset
     * @param key roaring key
     * @return Success: long; Fail: error
     */
    public Response<Long> trsetbits(final String key, long... fields) {
        final List<byte[]> args = new ArrayList<byte[]>();
        for (long value : fields) {
            args.add(toByteArray(value));
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRSETBITS)
            .key(key)
            .addObjects(args), BuilderFactory.LONG));
    }
    public Response<Long> trsetbits(byte[] key, long... fields) {
        final List<byte[]> args = new ArrayList<byte[]>();
        for (long value : fields) {
            args.add(toByteArray(value));
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRSETBITS)
            .key(key)
            .addObjects(args), BuilderFactory.LONG));
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
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRGETBIT)
            .key(key)
            .add(offset), BuilderFactory.LONG));
    }
    public Response<Long> trgetbit(byte[] key, long offset) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRGETBIT)
            .key(key)
            .add(offset), BuilderFactory.LONG));
    }

    /**
     * TR.GETBITS    TR.GETBITS <key> <offset> [<offset2> <offset3> ... <offsetn>]
     * get the value at the offset in roaringbitmap
     *
     * @param key roaring key
     * @param fields the bit offset
     * @return Success: array long; Fail: error
     */
    public Response<List<Long>> trgetbits(final String key, long... fields) {
        final List<byte[]> args = new ArrayList<byte[]>();
        for (long value : fields) {
            args.add(toByteArray(value));
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRGETBITS)
            .key(key)
            .addObjects(args), BuilderFactory.LONG_LIST));
    }
    public Response<List<Long>> trgetbits(byte[] key, long... fields) {
        final List<byte[]> args = new ArrayList<byte[]>();
        for (long value : fields) {
            args.add(toByteArray(value));
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRGETBITS)
            .key(key)
            .addObjects(args), BuilderFactory.LONG_LIST));
    }


    /**
     * TR.CLEARBITS    TR.CLEARBITS <key> <offset> [<offset2> <offset3> ... <offsetn>]
     * remove the value at the offset in roaringbitmap
     *
     * @param key roaring key
     * @param fields the bit offset
     * @return Success: long; Fail: error
     */
    public Response<Long> trclearbits(final String key, long... fields) {
        final List<byte[]> args = new ArrayList<byte[]>();
        for (long value : fields) {
            args.add(toByteArray(value));
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRCLEARBITS)
            .key(key)
            .addObjects(args), BuilderFactory.LONG));
    }
    public Response<Long> trclearbits(byte[] key, long... fields) {
        final List<byte[]> args = new ArrayList<byte[]>();
        for (long value : fields) {
            args.add(toByteArray(value));
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRCLEARBITS)
            .key(key)
            .addObjects(args), BuilderFactory.LONG));
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
    public Response<List<Long>> trrange(final String key, long start, long end) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRRANGE)
            .key(key)
            .add(start)
            .add(end), BuilderFactory.LONG_LIST));
    }
    public Response<List<Long>> trrange(byte[] key, long start, long end) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRRANGE)
            .key(key)
            .add(start)
            .add(end), BuilderFactory.LONG_LIST));
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
    public Response<String> trrangebitarray(final String key, long start, long end) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRRANGEBITARRAY)
            .key(key)
            .add(start)
            .add(end), BuilderFactory.STRING));
    }
    public Response<String> trrangebitarray(byte[] key, long start, long end) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRRANGEBITARRAY)
            .key(key)
            .add(start)
            .add(end), BuilderFactory.STRING));
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
    public Response<Long> trappendbitarray(final String key, long offset, final String value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRAPPENDBITARRAY)
            .key(key)
            .add(offset)
            .add(value), BuilderFactory.LONG));
    }
    public Response<Long> trappendbitarray(final String key, long offset, byte[] value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRAPPENDBITARRAY)
            .key(key)
            .add(offset)
            .add(value), BuilderFactory.LONG));
    }
    public Response<Long> trappendbitarray(byte[] key, long offset, byte[] value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRAPPENDBITARRAY)
            .key(key)
            .add(offset)
            .add(value), BuilderFactory.LONG));
    }


    /**
     * TR.SETRANGE TR.SETRANGE <key> <start> <end>
     * set all the elements between min (included) and max (included).
     *
     * @param key roaring bitmap key
     * @param start range start
     * @param end range end
     * @return Success: long; Fail: error
     */
    public Response<Long> trsetrange(final String key, long start, long end) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRSETRANGE)
            .key(key)
            .add(start)
            .add(end), BuilderFactory.LONG));
    }
    public Response<Long> trsetrange(byte[] key, long start, long end) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRSETRANGE)
            .key(key)
            .add(start)
            .add(end), BuilderFactory.LONG));
    }

    /**
     * TR.FLIPRANGE TR.FLIPRANGE <key> <start> <end>
     * flip all elements in the roaring bitmap within a specified interval: [range_start, range_end].
     *
     * @param key roaring bitmap key
     * @param start range start
     * @param end range end
     * @return Success: array long; Fail: error
     */
    public Response<Long> trfliprange(final String key, long start, long end) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRFLIPRANGE)
            .key(key)
            .add(start)
            .add(end), BuilderFactory.LONG));
    }
    public Response<Long> trfliprange(byte[] key, long start, long end) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRFLIPRANGE)
            .key(key)
            .add(start)
            .add(end), BuilderFactory.LONG));
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
    public Response<Long> trbitcount(final String key, long start, long end) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRBITCOUNT)
            .key(key)
            .add(start)
            .add(end), BuilderFactory.LONG));
    }
    public Response<Long> trbitcount(final String key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRBITCOUNT)
            .key(key), BuilderFactory.LONG));
    }
    public Response<Long> trbitcount(byte[] key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRBITCOUNT)
            .key(key), BuilderFactory.LONG));
    }
    public Response<Long> trbitcount(byte[] key, long start, long end) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRBITCOUNT)
            .key(key)
            .add(start)
            .add(end), BuilderFactory.LONG));
    }


    /**
     * TR.MIN	TR.MIN <key>
     * return the minimum element's offset set in the roaring bitmap
     *
     * @param key roaring key
     * @return Success: long; Fail: error
     */
    public Response<Long> trmin(final String key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRMIN)
            .key(key), BuilderFactory.LONG));
    }
    public Response<Long> trmin(byte[] key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRMIN)
            .key(key), BuilderFactory.LONG));
    }


    /**
     * TR.MAX	TR.MAX <key>
     * return the maximum element's offset set in the roaring bitmap
     *
     * @param key roaring key
     * @return Success: long; Fail: error
     */
    public Response<Long> trmax(final String key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRMAX)
            .key(key), BuilderFactory.LONG));
    }
    public Response<Long> trmax(byte[] key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRMAX)
            .key(key), BuilderFactory.LONG));
    }

    /**
     * TR.OPTIMIZE	TR.OPTIMIZE <key>
     * optimize memory usage by trying to use RLE container instead of int array or bitset.
     * it will also run shrink_to_fit on bitmap, this may cause memory reallocation.
     * optimize will try this function but did not make a guarantee that any change would happen
     *
     * @param key roaring key
     * @return Success: +OK; Fail: error
     */
    public Response<String> troptimize(final String key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TROPTIMIZE)
            .key(key), BuilderFactory.STRING));
    }
    public Response<String> troptimize(byte[] key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TROPTIMIZE)
            .key(key), BuilderFactory.STRING));
    }


    /**
     * TR.STAT	TR.STAT <key>
     * return roaring bitmap statistic information, you can get JSON formatted result by passing json = true.
     *
     * @param key roaring key
     * @return Success: string; Fail: error
     */
    public Response<String> trstat(final String key, boolean json) {
        if (json) {
            return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRSTAT)
                .key(key)
                .add("JSON"), BuilderFactory.STRING));
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRSTAT)
            .key(key), BuilderFactory.STRING));
    }
    public Response<String> trstat(byte[] key, boolean json) {
        if (json) {
            return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRSTAT)
                .key(key)
                .add("JSON"), BuilderFactory.STRING));
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRSTAT)
            .key(key), BuilderFactory.STRING));
    }


    /**
     * TR.BITPOS	TR.BITPOS <key> <value> [counting]
     * return the first element set as value at index, where the smallest element is at index 0.
     * counting is an optional argument, you can pass positive Counting to indicate the command count for the n-th element from the top
     * or pass an negative Counting to count from the n-th element form the bottom.
     *
     * @param key roaring key
     * @param value bit value
     * @param count count of the bit, negetive count indecate the reverse iteration
     * @return Success: long; Fail: error
     */
    public Response<Long> trbitpos(final String key, final String value, long count) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRBITPOS)
            .key(key)
            .add(value)
            .add(count), BuilderFactory.LONG));
    }

    public Response<Long> trbitpos(final String key, final String value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRBITPOS)
            .key(key)
            .add(value), BuilderFactory.LONG));
    }

    public Response<Long> trbitpos(final String key, long value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRBITPOS)
            .key(key)
            .add(value), BuilderFactory.LONG));
    }

    public Response<Long> trbitpos(final String key, long value, long count) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRBITPOS)
            .key(key)
            .add(value)
            .add(count), BuilderFactory.LONG));
    }

    public Response<Long> trbitpos(byte[] key, byte[] value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRBITPOS)
            .key(key)
            .add(value), BuilderFactory.LONG));
    }

    public Response<Long> trbitpos(byte[] key, byte[] value, byte[] count) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRBITPOS)
            .key(key)
            .add(value)
            .add(count), BuilderFactory.LONG));
    }


    /**
     * TR.RANK TR.RANK <key> <offset>
     * rank returns the number of elements that are smaller or equal to offset.
     *
     * @param key roaring key
     * @param offset bit ranking offset
     * @return Success: long; Fail: error
     */
    public Response<Long> trrank(final String key, long offset) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRRANK)
            .key(key)
            .add(offset), BuilderFactory.LONG));
    }

    public Response<Long> trrank(byte[] key, byte[] offset) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRRANK)
            .key(key)
            .add(offset), BuilderFactory.LONG));
    }


    /**
     * TR.BITOP	TR.BITOP <destkey> <operation> <key> [<key2> <key3>...]
     * call bitset computation on given roaring bitmaps, store the result into destkey
     * return the cardinality of result.
     * operation can be passed to AND OR XOR NOT DIFF.
     *
     * @param destkey   result store int destkey
     * @param operation operation type: AND OR NOT XOR DIFF
     * @param keys   operation joining keys
     * @return Success: long; Fail: error
     */
    public Response<Long> trbitop(final String destkey, final String operation, final String... keys) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRBITOP)
            .key(destkey)
            .add(operation)
            .addObjects((Object[]) keys), BuilderFactory.LONG));
    }

    public Response<Long> trbitop(byte[] destkey, byte[] operation, byte[]... keys) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRBITOP)
            .key(destkey)
            .add(operation)
            .addObjects((Object[]) keys), BuilderFactory.LONG));
    }


    /**
     * TR.BITOPCARD	TR.BITOPCARD <operation> <key> [<key2> <key3>...]
     * call bitset computation on given roaring bitmaps, return the cardinality of result.
     * operation can be passed to AND OR XOR NOT DIFF.
     *
     * @param operation operation type: AND OR NOT XOR DIFF
     * @param keys   operation joining keys
     * @return Success: long; Fail: error
     */
    public Response<Long> trbitopcard(final String operation, final String... keys) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRBITOPCARD)
            .add(operation)
            .addObjects((Object[]) keys), BuilderFactory.LONG));
    }

    public Response<Long> trbitopcard(byte[] operation, byte[]... keys) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRBITOPCARD)
            .add(operation)
            .addObjects((Object[]) keys), BuilderFactory.LONG));
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
    public Response<ScanResult<Long>> trscan(final String key, long cursor, long count) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRSCAN)
            .key(key)
            .add(cursor)
            .add("COUNT")
            .add(count), RoaringBuilderFactory.TRSCAN_RESULT_LONG));
    }
    public Response<ScanResult<Long>> trscan(final String key, long cursor) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRSCAN)
            .key(key)
            .add(cursor), RoaringBuilderFactory.TRSCAN_RESULT_LONG));
    }
    public Response<ScanResult<byte[]>> trscan(byte[] key, byte[] cursor) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRSCAN)
            .key(key)
            .add(cursor), RoaringBuilderFactory.TRSCAN_RESULT_BYTE));
    }
    public Response<ScanResult<byte[]>> trscan(byte[] key, byte[] cursor, byte[] count) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRSCAN)
            .key(key)
            .add(cursor)
            .add("COUNT")
            .add(count), RoaringBuilderFactory.TRSCAN_RESULT_BYTE));
    }


    /**
     * TR.LOADSTRING TR.LOAD <key> <stringkey>
     * Loading string into into a empty roaringbitmap
     *
     * @param key   result store int key
     * @param stringkey string, aka origional bitmap
     * @return Success: long; Fail: error
     */
    public Response<Long> trloadstring(final String key, final String stringkey) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRLOADSTRING)
            .key(key)
            .add(stringkey), BuilderFactory.LONG));
    }

    public Response<Long> trloadstring(byte[] key, byte[] stringkey) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRLOADSTRING)
            .key(key)
            .add(stringkey), BuilderFactory.LONG));
    }


    /**
     * TR.DIFF	TR.DIFF <destkey> <key1> <key2>
     * caculate the difference (andnot) by key1 and key2, store the result into destkey
     *
     * @param destkey   result store int key
     * @param key1 operation diff key
     * @param key2 operation diff key
     * @return Success: OK; Fail: error
     */
    public Response<String> trdiff(final String destkey, final String key1, final String key2) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRDIFF)
            .key(destkey)
            .add(key1)
            .add(key2), BuilderFactory.STRING));
    }
    public Response<String> trdiff(byte[] destkey, byte[] key1, byte[] key2) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRDIFF)
            .key(destkey)
            .add(key1)
            .add(key2), BuilderFactory.STRING));
    }


    /**
     * TR.SETINTARRAY	TR.SETINTARRAY <key> <value1> [<value2> <value3> ... <valueN>]
     * reset the bitmap by given integer array.
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
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRSETINTARRAY)
            .key(key)
            .addObjects(args), BuilderFactory.STRING));
    }

    public Response<String> trsetintarray(byte[] key, long... fields) {
        final List<byte[]> args = new ArrayList<byte[]>();
        for (long value : fields) {
            args.add(toByteArray(value));
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRSETINTARRAY)
            .key(key)
            .addObjects(args), BuilderFactory.STRING));
    }

    /**
     * TR.APPENDINTARRAY	TR.APPENDINTARRAY <key> <value1> [<value2> <value3> ... <valueN>]
     * add elements to the roaring bitmap.
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
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRAPPENDINTARRAY)
            .key(key)
            .addObjects(args), BuilderFactory.STRING));
    }
    public Response<String> trappendintarray(byte[] key, long... fields) {
        final List<byte[]> args = new ArrayList<byte[]>();
        for (long value : fields) {
            args.add(toByteArray(value));
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRAPPENDINTARRAY)
            .key(key)
            .addObjects(args), BuilderFactory.STRING));
    }


    /**
     * TR.SETBITARRAY	TR.SETBITARRAY <key> <value>
     * reset the roaring bitmap by given 01-bit string bitset.
     *
     * @param key roaring bitmap key
     * @param value bit offset value
     * @return Success: +OK; Fail: error
     */
    public Response<String> trsetbitarray(final String key, final String value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRSETBITARRAY)
            .key(key)
            .add(value), BuilderFactory.STRING));
    }
    public Response<String> trsetbitarray(byte[] key, byte[] value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRSETBITARRAY)
            .key(key)
            .add(value), BuilderFactory.STRING));
    }

    /**
     * TR.JACCARD TR.JACCARD <key1> <key2>
     * caculate roaringbitmap Jaccard index on key1 and key2.
     *
     * @param key1 operation key
     * @param key2 operation key
     * @return Success: double; Fail: error
     */
    public Response<Double> trjaccard(final String key1, final String key2) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRJACCARD)
            .key(key1)
            .add(key2), BuilderFactory.DOUBLE));
    }
    public Response<Double> trjaccard(byte[] key1, byte[] key2) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRJACCARD)
            .key(key1)
            .add(key2), BuilderFactory.DOUBLE));
    }

    /**
     * TR.CONTAINS TR.CONTAINS <key1> <key2>
     * return wether roaring bitmap key1 is a sub-set of key2
     *
     * @param key1 operation key
     * @param key2 operation key
     * @return Success: double; Fail: error
     */
    public Response<Boolean> trcontains(final String key1, final String key2) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRCONTAINS)
            .key(key1)
            .add(key2), BuilderFactory.BOOLEAN));
    }

    public Response<Boolean> trcontains(byte[] key1, byte[] key2) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TRCONTAINS)
            .key(key1)
            .add(key2), BuilderFactory.BOOLEAN));
    }
}
