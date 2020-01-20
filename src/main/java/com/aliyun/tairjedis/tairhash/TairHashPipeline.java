package com.aliyun.tairjedis.tairhash;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.aliyun.tairjedis.ModuleCommand;
import com.aliyun.tairjedis.tairhash.params.ExhgetwithverResult;
import com.aliyun.tairjedis.tairhash.params.ExhincrByParams;
import com.aliyun.tairjedis.tairhash.params.ExhmsetwithoptsParams;
import com.aliyun.tairjedis.tairhash.factory.HashBuilderFactory;
import com.aliyun.tairjedis.tairhash.params.ExhsetParams;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.util.SafeEncoder;

import static redis.clients.jedis.Protocol.toByteArray;

public class TairHashPipeline extends Pipeline {

    public Response<Long> exhset(final String key, final String field, final String value) {
        getClient("").sendCommand(ModuleCommand.EXHSET, key, field, value);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> exhset(final byte[] key, final byte[] field, final byte[] value) {
        getClient("").sendCommand(ModuleCommand.EXHSET, key, field, value);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> exhset(final String key, final String field, final String value, final ExhsetParams params) {
        getClient("").sendCommand(ModuleCommand.EXHSET,
            params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(field), SafeEncoder.encode(value)));
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> exhset(final byte[] key, final byte[] field, final byte[] value, final ExhsetParams params) {
        getClient("").sendCommand(ModuleCommand.EXHSET, params.getByteParams(key, field, value));
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> exhsetnx(final String key, final String field, final String value) {
        getClient("").sendCommand(ModuleCommand.EXHSETNX, key, field, value);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> exhsetnx(final byte[] key, final byte[] field, final byte[] value) {
        getClient("").sendCommand(ModuleCommand.EXHSETNX, key, field, value);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<String> exhmset(final String key, final Map<String, String> hash) {
        final Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>(hash.size());
        for (final Entry<String, String> entry : hash.entrySet()) {
            bhash.put(SafeEncoder.encode(entry.getKey()), SafeEncoder.encode(entry.getValue()));
        }
        return exhmset(SafeEncoder.encode(key), bhash);
    }

    public Response<String> exhmset(final byte[] key, final Map<byte[], byte[]> hash) {
        final List<byte[]> params = new ArrayList<byte[]>();
        params.add(key);

        for (final Entry<byte[], byte[]> entry : hash.entrySet()) {
            params.add(entry.getKey());
            params.add(entry.getValue());
        }

        getClient("").sendCommand(ModuleCommand.EXHMSET, params.toArray(new byte[params.size()][]));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> exhmsetwithopts(final String key, final List<ExhmsetwithoptsParams<String>> params) {
        List<ExhmsetwithoptsParams<byte[]>> bexhash = new ArrayList<ExhmsetwithoptsParams<byte[]>>();
        for (ExhmsetwithoptsParams<String> entry : params) {
            bexhash.add(new ExhmsetwithoptsParams<byte[]>(SafeEncoder.encode(entry.getField()),
                SafeEncoder.encode(entry.getValue()), entry.getVer(), entry.getExp()));
        }
        return exhmsetwithopts(SafeEncoder.encode(key), bexhash);
    }

    public Response<String> exhmsetwithopts(final byte[] key, final List<ExhmsetwithoptsParams<byte[]>> params) {
        final List<byte[]> p = new ArrayList<byte[]>();
        p.add(key);

        for (final ExhmsetwithoptsParams<byte[]> entry : params) {
            p.add(entry.getField());
            p.add(entry.getValue());
            p.add(toByteArray(entry.getVer()));
            p.add(toByteArray(entry.getExp()));
        }

        getClient("").sendCommand(ModuleCommand.EXHMSETWITHOPTS, p.toArray(new byte[params.size()][]));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<Boolean> exhpexpire(final String key, final String field, final int milliseconds) {
        return exhpexpire(SafeEncoder.encode(key), SafeEncoder.encode(field), milliseconds);
    }

    public Response<Boolean> exhpexpire(final byte[] key, final byte[] field, final int milliseconds) {
        getClient("").sendCommand(ModuleCommand.EXHPEXPIRE, key, field, toByteArray(milliseconds));
        return getResponse(BuilderFactory.BOOLEAN);
    }

    public Response<Boolean> exhpexpireAt(final String key, final String field, final long unixTime) {
        return exhpexpireAt(SafeEncoder.encode(key), SafeEncoder.encode(field), unixTime);
    }

    public Response<Boolean> exhpexpireAt(final byte[] key, final byte[] field, final long unixTime) {
        getClient("").sendCommand(ModuleCommand.EXHPEXPIREAT, key, field, toByteArray(unixTime));
        return getResponse(BuilderFactory.BOOLEAN);
    }

    public Response<Boolean> exhexpire(final String key, final String field, final int seconds) {
        return exhexpire(SafeEncoder.encode(key), SafeEncoder.encode(field), seconds);
    }

    public Response<Boolean> exhexpire(final byte[] key, final byte[] field, final int seconds) {
        getClient("").sendCommand(ModuleCommand.EXHEXPIRE, key, field, toByteArray(seconds));
        return getResponse(BuilderFactory.BOOLEAN);
    }

    public Response<Boolean> exhexpireAt(final String key, final String field, final long unixTime) {
        return exhexpireAt(SafeEncoder.encode(key), SafeEncoder.encode(field), unixTime);
    }

    public Response<Boolean> exhexpireAt(final byte[] key, final byte[] field, final long unixTime) {
        getClient("").sendCommand(ModuleCommand.EXHEXPIREAT, key, field, toByteArray(unixTime));
        return getResponse(BuilderFactory.BOOLEAN);
    }

    public Response<Long> exhpttl(final String key, final String field) {
        return exhpttl(SafeEncoder.encode(key), SafeEncoder.encode(field));
    }

    public Response<Long> exhpttl(final byte[] key, final byte[] field) {
        getClient("").sendCommand(ModuleCommand.EXHPTTL, key, field);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> exhttl(final String key, final String field) {
        return exhttl(SafeEncoder.encode(key), SafeEncoder.encode(field));
    }

    public Response<Long> exhttl(final byte[] key, final byte[] field) {
        getClient("").sendCommand(ModuleCommand.EXHTTL, key, field);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> exhver(final String key, final String field) {
        return exhver(SafeEncoder.encode(key), SafeEncoder.encode(field));
    }

    public Response<Long> exhver(final byte[] key, final byte[] field) {
        getClient("").sendCommand(ModuleCommand.EXHVER, key, field);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Boolean> exhsetver(final String key, final String field, final long version) {
        return exhsetver(SafeEncoder.encode(key), SafeEncoder.encode(field), version);
    }

    public Response<Boolean> exhsetver(final byte[] key, final byte[] field, final long version) {
        getClient("").sendCommand(ModuleCommand.EXHSETVER, key, field, toByteArray(version));
        return getResponse(BuilderFactory.BOOLEAN);
    }

    public Response<Long> exhincrBy(final String key, final String field, final long value) {
        return exhincrBy(SafeEncoder.encode(key), SafeEncoder.encode(field), value);
    }

    public Response<Long> exhincrBy(byte[] key, byte[] field, long value) {
        getClient("").sendCommand(ModuleCommand.EXHINCRBY, key, field, toByteArray(value));
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> exhincrBy(final String key, final String field, final long value,
        final ExhincrByParams params) {
        return exhincrBy(SafeEncoder.encode(key), SafeEncoder.encode(field), value, params);
    }

    public Response<Long> exhincrBy(final byte[] key, final byte[] field, final long value,
        final ExhincrByParams params) {
        getClient("").sendCommand(ModuleCommand.EXHINCRBY,
            params.getByteParams(key, field, toByteArray(value)));
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Double> exhincrByFloat(final String key, final String field, final double value) {
        return exhincrByFloat(SafeEncoder.encode(key), SafeEncoder.encode(field), value);
    }

    public Response<Double> exhincrByFloat(byte[] key, byte[] field, final double value) {
        getClient("").sendCommand(ModuleCommand.EXHINCRBYFLOAT, key, field, toByteArray(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> exhincrByFloat(final String key, final String field, final double value,
        final ExhincrByParams params) {
        return exhincrByFloat(SafeEncoder.encode(key), SafeEncoder.encode(field), value, params);
    }

    public Response<Double> exhincrByFloat(byte[] key, byte[] field, double value, ExhincrByParams params) {
        getClient("").sendCommand(ModuleCommand.EXINCRBYFLOAT,
            params.getByteParams(key, field, toByteArray(value)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<String> exhget(final String key, final String field) {
        getClient("").sendCommand(ModuleCommand.EXHGET, key, field);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<byte[]> exhget(final byte[] key, final byte[] field) {
        getClient("").sendCommand(ModuleCommand.EXHGET, key, field);
        return getResponse(BuilderFactory.BYTE_ARRAY);
    }

    public Response<ExhgetwithverResult<String>> exhgetwithver(final String key, final String field) {
        getClient("").sendCommand(ModuleCommand.EXHGETWITHVER, key, field);
        return getResponse(HashBuilderFactory.EXHGETWITHVER_RESULT_STRING);
    }

    public Response<ExhgetwithverResult<byte[]>> exhgetwithver(byte[] key, byte[] field) {
        getClient("").sendCommand(ModuleCommand.EXHGETWITHVER, key, field);
        return getResponse(HashBuilderFactory.EXHGETWITHVER_RESULT_BYTE);
    }

    public Response<List<String>> exhmget(final String key, final String... fields) {
        getClient("").sendCommand(ModuleCommand.EXHMGET,
            joinParameters(SafeEncoder.encode(key), SafeEncoder.encodeMany(fields)));
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<byte[]>> exhmget(byte[] key, byte[]... fields) {
        getClient("").sendCommand(ModuleCommand.EXHMGET, joinParameters(key, fields));
        return getResponse(BuilderFactory.BYTE_ARRAY_LIST);
    }

    public Response<List<ExhgetwithverResult<String>>> exhmgetwithver(final String key, final String... fields) {
        getClient("").sendCommand(ModuleCommand.EXHMGETWITHVER,
            joinParameters(SafeEncoder.encode(key), SafeEncoder.encodeMany(fields)));
        return getResponse(HashBuilderFactory.EXHMGETWITHVER_RESULT_STRING_LIST);
    }

    public Response<List<ExhgetwithverResult<byte[]>>> exhmgetwithver(byte[] key, byte[]... fields) {
        getClient("").sendCommand(ModuleCommand.EXHMGETWITHVER, joinParameters(key, fields));
        return getResponse(HashBuilderFactory.EXHMGETWITHVER_RESULT_BYTE_LIST);
    }

    public Response<Long> exhdel(final String key, final String... fields) {
        return exhdel(SafeEncoder.encode(key), SafeEncoder.encodeMany(fields));
    }

    public Response<Long> exhdel(byte[] key, byte[]... fields) {
        getClient("").sendCommand(ModuleCommand.EXHDEL, joinParameters(key, fields));
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> exhlen(final String key) {
        return exhlen(SafeEncoder.encode(key));
    }

    public Response<Long> exhlen(byte[] key) {
        getClient("").sendCommand(ModuleCommand.EXHLEN, key);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Boolean> exhexists(final String key, final String field) {
        return exhexists(SafeEncoder.encode(key), SafeEncoder.encode(field));
    }

    public Response<Boolean> exhexists(byte[] key, byte[] field) {
        getClient("").sendCommand(ModuleCommand.EXHEXISTS, key, field);
        return getResponse(BuilderFactory.BOOLEAN);
    }

    public Response<Long> exhstrlen(final String key, final String field) {
        return exhstrlen(SafeEncoder.encode(key), SafeEncoder.encode(field));
    }

    public Response<Long> exhstrlen(byte[] key, byte[] field) {
        getClient("").sendCommand(ModuleCommand.EXHSTRLEN, key, field);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Set<String>> exhkeys(final String key) {
        getClient("").sendCommand(ModuleCommand.EXHKEYS, key);
        return getResponse(BuilderFactory.STRING_ZSET);
    }

    public Response<Set<byte[]>> exhkeys(byte[] key) {
        getClient("").sendCommand(ModuleCommand.EXHKEYS, key);
        return getResponse(BuilderFactory.BYTE_ARRAY_ZSET);
    }

    public Response<List<String>> exhvals(final String key) {
        getClient("").sendCommand(ModuleCommand.EXHVALS, key);
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<byte[]>> exhvals(byte[] key) {
        getClient("").sendCommand(ModuleCommand.EXHVALS, key);
        return getResponse(BuilderFactory.BYTE_ARRAY_LIST);
    }

    public Response<Map<String, String>> exhgetAll(final String key) {
        getClient("").sendCommand(ModuleCommand.EXHGETALL, key);
        return getResponse(BuilderFactory.STRING_MAP);
    }

    public Response<Map<byte[], byte[]>> exhgetAll(byte[] key) {
        getClient("").sendCommand(ModuleCommand.EXHGETALL, key);
        return getResponse(BuilderFactory.BYTE_ARRAY_MAP);
    }

    public Response<ScanResult<Entry<String, String>>> exhscan(final String key, final String op, final String subkey) {
        return exhscan(key, op, subkey, new ScanParams());
    }

    public Response<ScanResult<Entry<String, String>>> exhscan(final String key, final String op, final String subkey,
        final ScanParams params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(op));
        args.add(SafeEncoder.encode(subkey));
        args.addAll(params.getParams());

        getClient("").sendCommand(ModuleCommand.EXHSCAN, args.toArray(new byte[args.size()][]));
        return getResponse(HashBuilderFactory.EXHSCAN_RESULT_STRING);
    }

    public Response<ScanResult<Entry<byte[], byte[]>>> exhscan(final byte[] key, final byte[] op, final byte[] subkey) {
        return exhscan(key, op, subkey, new ScanParams());
    }

    public Response<ScanResult<Entry<byte[], byte[]>>> exhscan(final byte[] key, final byte[] op, final byte[] subkey,
        final ScanParams params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(key);
        args.add(op);
        args.add(subkey);
        args.addAll(params.getParams());

        getClient("").sendCommand(ModuleCommand.EXHSCAN, args.toArray(new byte[args.size()][]));
        return getResponse(HashBuilderFactory.EXHSCAN_RESULT_BYTE);
    }

    /* ================ Common function ================ */

    private byte[][] joinParameters(byte[] first, byte[][] rest) {
        byte[][] result = new byte[rest.length + 1][];
        result[0] = first;
        System.arraycopy(rest, 0, result, 1, rest.length);
        return result;
    }

}
