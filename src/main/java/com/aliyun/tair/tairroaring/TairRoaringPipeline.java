package com.aliyun.tair.tairroaring;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.util.JoinParameters;
import com.sun.org.apache.xpath.internal.operations.Bool;
import redis.clients.jedis.BuilderFactory;
import com.aliyun.tair.tairroaring.factory.RoaringBuilderFactory;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.util.SafeEncoder;
import static redis.clients.jedis.Protocol.toByteArray;


import java.util.ArrayList;
import java.util.List;

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
    public Response<Long> trsetbit(final String key, long offset, long value) {
         getClient("").sendCommand(ModuleCommand.TRSETBIT, SafeEncoder.encode(key), toByteArray(offset), toByteArray(value));
        return getResponse(BuilderFactory.LONG);
    }
    public Response<Long> trsetbit(byte[] key, long offset, byte[] value) {
         getClient("").sendCommand(ModuleCommand.TRSETBIT, key, toByteArray(offset), value);
        return getResponse(BuilderFactory.LONG);
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
        getClient("").sendCommand(ModuleCommand.TRSETBITS,
                JoinParameters.joinParameters(SafeEncoder.encode(key),  args.toArray(new byte[args.size()][])));
        return getResponse(BuilderFactory.LONG);
    }
    public Response<Long> trsetbits(byte[] key, long... fields) {
        final List<byte[]> args = new ArrayList<byte[]>();
        for (long value : fields) {
            args.add(toByteArray(value));
        }
         getClient("").sendCommand(ModuleCommand.TRSETBITS,
                JoinParameters.joinParameters(key, args.toArray(new byte[args.size()][])));
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
    public Response<Long> trgetbit(byte[] key, long offset) {
         getClient("").sendCommand(ModuleCommand.TRGETBIT, key, toByteArray(offset));
        return getResponse(BuilderFactory.LONG);
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
         getClient("").sendCommand(ModuleCommand.TRGETBITS,
                JoinParameters.joinParameters(SafeEncoder.encode(key),  args.toArray(new byte[args.size()][])));
        return getResponse(BuilderFactory.LONG_LIST);
    }
    public Response<List<Long>> trgetbits(byte[] key, long... fields) {
        final List<byte[]> args = new ArrayList<byte[]>();
        for (long value : fields) {
            args.add(toByteArray(value));
        }
         getClient("").sendCommand(ModuleCommand.TRGETBITS,
                JoinParameters.joinParameters(key, args.toArray(new byte[args.size()][])));
        return getResponse(BuilderFactory.LONG_LIST);
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
         getClient("").sendCommand(ModuleCommand.TRCLEARBITS,
                JoinParameters.joinParameters(SafeEncoder.encode(key),  args.toArray(new byte[args.size()][])));
        return getResponse(BuilderFactory.LONG);
    }
    public Response<Long> trclearbits(byte[] key, long... fields) {
        final List<byte[]> args = new ArrayList<byte[]>();
        for (long value : fields) {
            args.add(toByteArray(value));
        }
         getClient("").sendCommand(ModuleCommand.TRCLEARBITS,
                JoinParameters.joinParameters(key, args.toArray(new byte[args.size()][])));
        return getResponse(BuilderFactory.LONG);
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
         getClient("").sendCommand(ModuleCommand.TRRANGE, SafeEncoder.encode(key), toByteArray(start), toByteArray(end));
        return getResponse(BuilderFactory.LONG_LIST);
    }
    public Response<List<Long>> trrange(byte[] key, long start, long end) {
         getClient("").sendCommand(ModuleCommand.TRRANGE, key, toByteArray(start), toByteArray(end));
        return getResponse(BuilderFactory.LONG_LIST);
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
         getClient("").sendCommand(ModuleCommand.TRRANGEBITARRAY, SafeEncoder.encode(key), toByteArray(start), toByteArray(end));
        return getResponse(BuilderFactory.STRING);
    }
    public Response<String> trrangebitarray(byte[] key, long start, long end) {
         getClient("").sendCommand(ModuleCommand.TRRANGEBITARRAY, key, toByteArray(start), toByteArray(end));
        return getResponse(BuilderFactory.STRING);
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
         getClient("").sendCommand(ModuleCommand.TRAPPENDBITARRAY, SafeEncoder.encode(key), toByteArray(offset), SafeEncoder.encode(value));
        return getResponse(BuilderFactory.LONG);
    }
    public Response<Long> trappendbitarray(final String key, long offset, byte[] value) {
         getClient("").sendCommand(ModuleCommand.TRAPPENDBITARRAY, SafeEncoder.encode(key), toByteArray(offset), value);
        return getResponse(BuilderFactory.LONG);
    }
    public Response<Long> trappendbitarray(byte[] key, long offset, byte[] value) {
         getClient("").sendCommand(ModuleCommand.TRAPPENDBITARRAY, key, toByteArray(offset), value);
        return getResponse(BuilderFactory.LONG);
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
         getClient("").sendCommand(ModuleCommand.TRSETRANGE, SafeEncoder.encode(key), toByteArray(start), toByteArray(end));
        return getResponse(BuilderFactory.LONG);
    }
    public Response<Long> trsetrange(byte[] key, long start, long end) {
         getClient("").sendCommand(ModuleCommand.TRSETRANGE, key, toByteArray(start), toByteArray(end));
        return getResponse(BuilderFactory.LONG);
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
         getClient("").sendCommand(ModuleCommand.TRFLIPRANGE, SafeEncoder.encode(key), toByteArray(start), toByteArray(end));
        return getResponse(BuilderFactory.LONG);
    }
    public Response<Long> trfliprange(byte[] key, long start, long end) {
         getClient("").sendCommand(ModuleCommand.TRFLIPRANGE, key, toByteArray(start), toByteArray(end));
        return getResponse(BuilderFactory.LONG);
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
         getClient("").sendCommand(ModuleCommand.TRBITCOUNT, SafeEncoder.encode(key), toByteArray(start), toByteArray(end));
        return getResponse(BuilderFactory.LONG);
    }
    public Response<Long> trbitcount(final String key) {
        getClient("").sendCommand(ModuleCommand.TRBITCOUNT, SafeEncoder.encode(key));
        return getResponse(BuilderFactory.LONG);
    }
    public Response<Long> trbitcount(byte[] key) {
        getClient("").sendCommand(ModuleCommand.TRBITCOUNT, key);
        return getResponse(BuilderFactory.LONG);
    }
    public Response<Long> trbitcount(byte[] key, long start, long end) {
         getClient("").sendCommand(ModuleCommand.TRBITCOUNT, key, toByteArray(start), toByteArray(end));
        return getResponse(BuilderFactory.LONG);
    }


    /**
     * TR.MIN	TR.MIN <key>
     * return the minimum element's offset set in the roaring bitmap
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
     * return the maximum element's offset set in the roaring bitmap
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
     * optimize memory usage by trying to use RLE container instead of int array or bitset.
     * it will also run shrink_to_fit on bitmap, this may cause memory reallocation.
     * optimize will try this function but did not make a guarantee that any change would happen
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
     * return roaring bitmap statistic information, you can get JSON formatted result by passing json = true.
     *
     * @param key roaring key
     * @return Success: string; Fail: error
     */
    public Response<String> trstat(final String key, boolean json) {
        if (json = true) {
             getClient("").sendCommand(ModuleCommand.TRSTAT, SafeEncoder.encode(key), SafeEncoder.encode("JSON"));
            return getResponse(BuilderFactory.STRING);
        }
         getClient("").sendCommand(ModuleCommand.TRSTAT, SafeEncoder.encode(key));
        return getResponse(BuilderFactory.STRING);
    }
    public Response<String> trstat(byte[] key, boolean json) {
        if (json == true) {
             getClient("").sendCommand(ModuleCommand.TRSTAT, key, SafeEncoder.encode("JSON"));
            return getResponse(BuilderFactory.STRING);
        }
         getClient("").sendCommand(ModuleCommand.TRSTAT, key);
        return getResponse(BuilderFactory.STRING);
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
        getClient("").sendCommand(ModuleCommand.TRBITPOS, SafeEncoder.encode(key), SafeEncoder.encode(value), toByteArray(count));
        return getResponse(BuilderFactory.LONG);
    }
    public Response<Long> trbitpos(final String key, final String value) {
        getClient("").sendCommand(ModuleCommand.TRBITPOS, SafeEncoder.encode(key), SafeEncoder.encode(value));
        return getResponse(BuilderFactory.LONG);
    }
    public Response<Long> trbitpos(final String key, long value) {
        getClient("").sendCommand(ModuleCommand.TRBITPOS, SafeEncoder.encode(key), toByteArray(value));
        return getResponse(BuilderFactory.LONG);
    }
    public Response<Long> trbitpos(final String key, long value, long count) {
        getClient("").sendCommand(ModuleCommand.TRBITPOS, SafeEncoder.encode(key), toByteArray(value), toByteArray(count));
        return getResponse(BuilderFactory.LONG);
    }
    public Response<Long> trbitpos(byte[] key, byte[] value) {
        getClient("").sendCommand(ModuleCommand.TRBITPOS, key, value);
        return getResponse(BuilderFactory.LONG);
    }
    public Response<Long> trbitpos(byte[] key, byte[] value, byte[] count) {
        getClient("").sendCommand(ModuleCommand.TRBITPOS, key, value, count);
        return getResponse(BuilderFactory.LONG);
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
        getClient("").sendCommand(ModuleCommand.TRRANK, SafeEncoder.encode(key), toByteArray(offset));
        return getResponse(BuilderFactory.LONG);
    }
    public Response<Long> trrank(byte[] key, byte[] offset) {
        getClient("").sendCommand(ModuleCommand.TRRANK, key, offset);
        return getResponse(BuilderFactory.LONG);
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
         getClient("").sendCommand(ModuleCommand.TRBITOP,
            JoinParameters.joinParameters(SafeEncoder.encode(destkey), SafeEncoder.encode(operation), SafeEncoder.encodeMany(keys)));
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> trbitop(byte[] destkey, byte[] operation, byte[]... keys) {
        getClient("").sendCommand(ModuleCommand.TRBITOP, JoinParameters.joinParameters(destkey, operation, keys));
        return getResponse(BuilderFactory.LONG);
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
    public Response<Long> trbitopcard(final String operation , final String... keys) {
         getClient("").sendCommand(ModuleCommand.TRBITOPCARD,
            JoinParameters.joinParameters(SafeEncoder.encode(operation), SafeEncoder.encodeMany(keys)));
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> trbitopcard(byte[] operation, byte[]... keys) {
         getClient("").sendCommand(ModuleCommand.TRBITOPCARD, JoinParameters.joinParameters(operation, keys));
        return getResponse(BuilderFactory.LONG);
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
    public Response<ScanResult<Long>> trscan(final String  key, long cursor, long count) {
        getClient("").sendCommand(ModuleCommand.TRSCAN, SafeEncoder.encode(key), toByteArray(cursor), SafeEncoder.encode("COUNT"), toByteArray(count));
        return getResponse(RoaringBuilderFactory.TRSCAN_RESULT_LONG);
    }
    public Response<ScanResult<Long>> trscan(final String key, long cursor) {
         getClient("").sendCommand(ModuleCommand.TRSCAN, SafeEncoder.encode(key), toByteArray(cursor));
        return getResponse(RoaringBuilderFactory.TRSCAN_RESULT_LONG);
    }
    public Response<ScanResult<byte[]>> trscan(byte[] key, byte[] cursor) {
         getClient("").sendCommand(ModuleCommand.TRSCAN, key, cursor);
        return getResponse(RoaringBuilderFactory.TRSCAN_RESULT_BYTE);
    }
    public Response<ScanResult<byte[]>> trscan(byte[] key, byte[] cursor, byte[] count) {
         getClient("").sendCommand(ModuleCommand.TRSCAN, key, cursor, SafeEncoder.encode("COUNT"), count);
        return getResponse(RoaringBuilderFactory.TRSCAN_RESULT_BYTE);
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
         getClient("").sendCommand(ModuleCommand.TRLOADSTRING, SafeEncoder.encode(key), SafeEncoder.encode(stringkey));
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> trloadstring(byte[] key, byte[] stringkey) {
         getClient("").sendCommand(ModuleCommand.TRLOADSTRING, key, stringkey);
        return getResponse(BuilderFactory.LONG);
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
         getClient("").sendCommand(ModuleCommand.TRDIFF, SafeEncoder.encode(destkey),
                 SafeEncoder.encode(key1), SafeEncoder.encode(key2));
        return getResponse(BuilderFactory.STRING);
    }
    public Response<String> trdiff(byte[] destkey, byte[] key1, byte[] key2) {
         getClient("").sendCommand(ModuleCommand.TRDIFF, destkey, key1, key2);
        return getResponse(BuilderFactory.STRING);
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
     * reset the roaring bitmap by given 01-bit string bitset.
     *
     * @param key roaring bitmap key
     * @param value bit offset value
     * @return Success: +OK; Fail: error
     */
    public Response<String> trsetbitarray(final String key, final String value) {
         getClient("").sendCommand(ModuleCommand.TRSETBITARRAY, SafeEncoder.encode(key), SafeEncoder.encode(value));
        return getResponse(BuilderFactory.STRING);
    }
    public Response<String> trsetbitarray(byte[] key, byte[] value) {
         getClient("").sendCommand(ModuleCommand.TRSETBITARRAY, key, value);
        return getResponse(BuilderFactory.STRING);
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
        getClient("").sendCommand(ModuleCommand.TRJACCARD, SafeEncoder.encode(key1), SafeEncoder.encode(key2));
        return getResponse(BuilderFactory.DOUBLE);
    }
    public Response<Double> trjaccard(byte[] key1, byte[] key2) {
        getClient("").sendCommand(ModuleCommand.TRJACCARD, key1, key2);
        return getResponse(BuilderFactory.DOUBLE);
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
        getClient("").sendCommand(ModuleCommand.TRCONTAINS, SafeEncoder.encode(key1), SafeEncoder.encode(key2));
        return getResponse(BuilderFactory.BOOLEAN);
    }

    public Response<Boolean> trcontains(byte[] key1, byte[] key2) {
        getClient("").sendCommand(ModuleCommand.TRCONTAINS, key1, key2);
        return getResponse(BuilderFactory.BOOLEAN);
    }
}
