package com.aliyun.tair.tairhash;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.jedis3.Jedis3BuilderFactory;
import com.aliyun.tair.jedis3.ScanParams;
import com.aliyun.tair.jedis3.ScanResult;
import com.aliyun.tair.tairhash.factory.HashBuilderFactory;
import com.aliyun.tair.tairhash.params.ExhgetwithverResult;
import com.aliyun.tair.tairhash.params.ExhincrByFloatParams;
import com.aliyun.tair.tairhash.params.ExhincrByParams;
import com.aliyun.tair.tairhash.params.ExhmsetwithoptsParams;
import com.aliyun.tair.tairhash.params.ExhsetParams;
import io.valkey.BuilderFactory;
import io.valkey.CommandArguments;
import io.valkey.CommandObject;
import io.valkey.Jedis;
import io.valkey.Pipeline;
import io.valkey.Response;
import io.valkey.util.SafeEncoder;

import static io.valkey.Protocol.toByteArray;

public class TairHashPipeline extends Pipeline {
    public TairHashPipeline(Jedis jedis) {
        super(jedis);
    }

    public Response<Long> exhset(final byte[] key, final byte[] field, final byte[] value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHSET)
            .key(key)
            .add(field)
            .add(value), BuilderFactory.LONG));
    }

    public Response<Long> exhset(final String key, final String field, final String value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHSET)
            .key(key)
            .add(field)
            .add(value), BuilderFactory.LONG));
    }

    public Response<Long> exhset(final byte[] key, final byte[] field, final byte[] value, final ExhsetParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHSET)
            .addObjects(params.getByteParams(key, field, value)), BuilderFactory.LONG));
    }

    public Response<Long> exhset(final String key, final String field, final String value, final ExhsetParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHSET).addObjects(
            params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(field), SafeEncoder.encode(value))),
            BuilderFactory.LONG));
    }

    public Response<Long> exhsetnx(final byte[] key, final byte[] field, final byte[] value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHSETNX)
            .key(key)
            .add(field)
            .add(value), BuilderFactory.LONG));
    }

    public Response<Long> exhsetnx(final String key, final String field, final String value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHSETNX)
            .key(key)
            .add(field)
            .add(value), BuilderFactory.LONG));
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
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHMSET).addObjects(params),
            BuilderFactory.STRING));
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
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHMSETWITHOPTS).addObjects(p),
            BuilderFactory.STRING));
    }

    public Response<Boolean> exhpexpire(final String key, final String field, final int milliseconds) {
        return exhpexpire(SafeEncoder.encode(key), SafeEncoder.encode(field), milliseconds, false);
    }

    public Response<Boolean> exhpexpire(final String key, final String field, final int milliseconds,
        boolean noactive) {
        return exhpexpire(SafeEncoder.encode(key), SafeEncoder.encode(field), milliseconds, noactive);
    }

    public Response<Boolean> exhpexpire(final byte[] key, final byte[] field, final int milliseconds) {
        return exhpexpire(key, field, milliseconds, false);
    }

    public Response<Boolean> exhpexpire(final byte[] key, final byte[] field, final int milliseconds,
        boolean noactive) {
        CommandArguments args = new CommandArguments(ModuleCommand.EXHPEXPIRE);
        args.add(key).add(field).add(milliseconds);
        if (noactive) {
            args.add("noactive");
        }
        return appendCommand(new CommandObject<>(args, BuilderFactory.BOOLEAN));
    }

    public Response<Boolean> exhpexpireAt(final String key, final String field, final long unixTime) {
        return exhpexpireAt(SafeEncoder.encode(key), SafeEncoder.encode(field), unixTime, false);
    }

    public Response<Boolean> exhpexpireAt(final String key, final String field, final long unixTime, boolean noactive) {
        return exhpexpireAt(SafeEncoder.encode(key), SafeEncoder.encode(field), unixTime, noactive);
    }

    public Response<Boolean> exhpexpireAt(final byte[] key, final byte[] field, final long unixTime) {
        return exhpexpireAt(key, field, unixTime, false);
    }

    public Response<Boolean> exhpexpireAt(final byte[] key, final byte[] field, final long unixTime, boolean noactive) {
        CommandArguments args = new CommandArguments(ModuleCommand.EXHPEXPIREAT);
        args.add(key).add(field).add(unixTime);
        if (noactive) {
            args.add("noactive");
        }
        return appendCommand(new CommandObject<>(args, BuilderFactory.BOOLEAN));
    }

    public Response<Boolean> exhexpire(final String key, final String field, final int seconds) {
        return exhexpire(SafeEncoder.encode(key), SafeEncoder.encode(field), seconds, false);
    }

    public Response<Boolean> exhexpire(final String key, final String field, final int seconds, boolean noactive) {
        return exhexpire(SafeEncoder.encode(key), SafeEncoder.encode(field), seconds, noactive);
    }

    public Response<Boolean> exhexpire(final byte[] key, final byte[] field, final int seconds) {
        return exhexpire(key, field, seconds, false);
    }

    public Response<Boolean> exhexpire(final byte[] key, final byte[] field, final int seconds, boolean noactive) {
        CommandArguments args = new CommandArguments(ModuleCommand.EXHEXPIRE);
        args.add(key).add(field).add(seconds);
        if (noactive) {
            args.add("noactive");
        }
        return appendCommand(new CommandObject<>(args, BuilderFactory.BOOLEAN));
    }

    public Response<Boolean> exhexpireAt(final String key, final String field, final long unixTime) {
        return exhexpireAt(SafeEncoder.encode(key), SafeEncoder.encode(field), unixTime, false);
    }

    public Response<Boolean> exhexpireAt(final String key, final String field, final long unixTime, boolean noactive) {
        return exhexpireAt(SafeEncoder.encode(key), SafeEncoder.encode(field), unixTime, noactive);
    }

    public Response<Boolean> exhexpireAt(final byte[] key, final byte[] field, final long unixTime) {
        return exhexpireAt(key, field, unixTime, false);
    }

    public Response<Boolean> exhexpireAt(final byte[] key, final byte[] field, final long unixTime, boolean noactive) {
        CommandArguments args = new CommandArguments(ModuleCommand.EXHEXPIREAT);
        args.add(key).add(field).add(unixTime);
        if (noactive) {
            args.add("noactive");
        }
        return appendCommand(new CommandObject<>(args, BuilderFactory.BOOLEAN));
    }

    public Response<Long> exhpttl(final String key, final String field) {
        return exhpttl(SafeEncoder.encode(key), SafeEncoder.encode(field));
    }

    public Response<Long> exhpttl(final byte[] key, final byte[] field) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHPTTL)
            .add(key)
            .add(field), BuilderFactory.LONG));
    }

    public Response<Long> exhttl(final String key, final String field) {
        return exhttl(SafeEncoder.encode(key), SafeEncoder.encode(field));
    }

    public Response<Long> exhttl(final byte[] key, final byte[] field) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHTTL)
            .add(key)
            .add(field), BuilderFactory.LONG));
    }

    public Response<Long> exhver(final String key, final String field) {
        return exhver(SafeEncoder.encode(key), SafeEncoder.encode(field));
    }

    public Response<Long> exhver(final byte[] key, final byte[] field) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHVER)
            .add(key)
            .add(field), BuilderFactory.LONG));
    }

    public Response<Boolean> exhsetver(final String key, final String field, final long version) {
        return exhsetver(SafeEncoder.encode(key), SafeEncoder.encode(field), version);
    }

    public Response<Boolean> exhsetver(final byte[] key, final byte[] field, final long version) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHSETVER)
            .add(key)
            .add(field)
            .add(version), BuilderFactory.BOOLEAN));
    }

    public Response<Long> exhincrBy(final String key, final String field, final long value) {
        return exhincrBy(SafeEncoder.encode(key), SafeEncoder.encode(field), value);
    }

    public Response<Long> exhincrBy(byte[] key, byte[] field, long value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHINCRBY)
            .add(key)
            .add(field)
            .add(value), BuilderFactory.LONG));
    }

    public Response<Long> exhincrBy(final String key, final String field, final long value,
        final ExhincrByParams params) {
        return exhincrBy(SafeEncoder.encode(key), SafeEncoder.encode(field), value, params);
    }

    public Response<Long> exhincrBy(final byte[] key, final byte[] field, final long value,
        final ExhincrByParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHINCRBY).addObjects(
            params.getByteParams(key, field, toByteArray(value))), BuilderFactory.LONG));
    }

    public Response<Double> exhincrByFloat(final String key, final String field, final double value) {
        return exhincrByFloat(SafeEncoder.encode(key), SafeEncoder.encode(field), value);
    }

    public Response<Double> exhincrByFloat(byte[] key, byte[] field, final double value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHINCRBYFLOAT)
            .add(key)
            .add(field)
            .add(value), BuilderFactory.DOUBLE));
    }

    public Response<Double> exhincrByFloat(final String key, final String field, final double value,
        final ExhincrByFloatParams params) {
        return exhincrByFloat(SafeEncoder.encode(key), SafeEncoder.encode(field), value, params);
    }

    public Response<Double> exhincrByFloat(byte[] key, byte[] field, double value, ExhincrByFloatParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHINCRBYFLOAT).addObjects(
            params.getByteParams(key, field, toByteArray(value))), BuilderFactory.DOUBLE));
    }

    public Response<String> exhget(final String key, final String field) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHGET)
            .add(key)
            .add(field), BuilderFactory.STRING));
    }

    public Response<byte[]> exhget(final byte[] key, final byte[] field) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHGET)
            .add(key)
            .add(field), Jedis3BuilderFactory.BYTE_ARRAY));
    }

    public Response<ExhgetwithverResult<String>> exhgetwithver(final String key, final String field) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHGETWITHVER)
            .add(key)
            .add(field), HashBuilderFactory.EXHGETWITHVER_RESULT_STRING));
    }

    public Response<ExhgetwithverResult<byte[]>> exhgetwithver(byte[] key, byte[] field) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHGETWITHVER)
            .add(key)
            .add(field), HashBuilderFactory.EXHGETWITHVER_RESULT_BYTE));
    }

    public Response<List<String>> exhmget(final String key, final String... fields) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHMGET)
            .add(key)
            .addObjects(fields), BuilderFactory.STRING_LIST));
    }

    public Response<List<byte[]>> exhmget(byte[] key, byte[]... fields) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHMGET)
            .add(key)
            .addObjects(fields), Jedis3BuilderFactory.BYTE_ARRAY_LIST));
    }

    public Response<List<ExhgetwithverResult<String>>> exhmgetwithver(final String key, final String... fields) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHMGETWITHVER)
            .add(key)
            .addObjects(fields), HashBuilderFactory.EXHMGETWITHVER_RESULT_STRING_LIST));
    }

    public Response<List<ExhgetwithverResult<byte[]>>> exhmgetwithver(byte[] key, byte[]... fields) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHMGETWITHVER)
            .add(key)
            .addObjects(fields), HashBuilderFactory.EXHMGETWITHVER_RESULT_BYTE_LIST));
    }

    public Response<Long> exhdel(final String key, final String... fields) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHDEL)
            .add(key)
            .addObjects(fields), BuilderFactory.LONG));
    }

    public Response<Long> exhdel(byte[] key, byte[]... fields) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHDEL)
            .add(key)
            .addObjects(fields), BuilderFactory.LONG));
    }

    public Response<Long> exhlen(final String key) {
        return exhlen(SafeEncoder.encode(key), false);
    }

    public Response<Long> exhlen(final String key, boolean noexp) {
        return exhlen(SafeEncoder.encode(key), noexp);
    }

    public Response<Long> exhlen(byte[] key) {
        return exhlen(key, false);
    }

    public Response<Long> exhlen(byte[] key, boolean noexp) {
        CommandArguments args = new CommandArguments(ModuleCommand.EXHLEN);
        args.add(key);
        if (noexp) {
            args.add("noexp");
        }
        return appendCommand(new CommandObject<>(args, BuilderFactory.LONG));
    }

    public Response<Boolean> exhexists(final String key, final String field) {
        return exhexists(SafeEncoder.encode(key), SafeEncoder.encode(field));
    }

    public Response<Boolean> exhexists(byte[] key, byte[] field) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHEXISTS)
            .add(key)
            .add(field), BuilderFactory.BOOLEAN));
    }

    public Response<Long> exhstrlen(final String key, final String field) {
        return exhstrlen(SafeEncoder.encode(key), SafeEncoder.encode(field));
    }

    public Response<Long> exhstrlen(byte[] key, byte[] field) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHSTRLEN)
            .add(key)
            .add(field), BuilderFactory.LONG));
    }

    public Response<Set<String>> exhkeys(final String key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHKEYS)
            .add(key), Jedis3BuilderFactory.STRING_ZSET));
    }

    public Response<Set<byte[]>> exhkeys(byte[] key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHKEYS)
            .add(key), Jedis3BuilderFactory.BYTE_ARRAY_ZSET));
    }

    public Response<List<String>> exhvals(final String key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHVALS)
            .add(key), BuilderFactory.STRING_LIST));
    }

    public Response<List<byte[]>> exhvals(byte[] key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHVALS)
            .add(key), Jedis3BuilderFactory.BYTE_ARRAY_LIST));
    }

    public Response<Map<String, String>> exhgetAll(final String key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHGETALL)
            .add(key), BuilderFactory.STRING_MAP));
    }

    public Response<Map<byte[], byte[]>> exhgetAll(byte[] key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHGETALL)
            .add(key), Jedis3BuilderFactory.BYTE_ARRAY_MAP));
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

        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHSCAN).addObjects(args),
            HashBuilderFactory.EXHSCAN_RESULT_STRING));
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

        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHSCAN).addObjects(args),
            HashBuilderFactory.EXHSCAN_RESULT_BYTE));
    }

    public Response<ScanResult<Entry<String, String>>> exhscanunorder(final String key, final String cursor) {
        return exhscanunorder(key, cursor, new ScanParams());
    }

    public Response<ScanResult<Entry<byte[], byte[]>>> exhscanunorder(final byte[] key, final byte[] cursor) {
        return exhscanunorder(key, cursor, new ScanParams());
    }

    public Response<ScanResult<Entry<String, String>>> exhscanunorder(final String key, final String cursor,
        final ScanParams params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(cursor));
        args.addAll(params.getParams());

        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHSCANUNORDER).addObjects(args),
            HashBuilderFactory.EXHSCAN_RESULT_STRING));
    }

    public Response<ScanResult<Entry<byte[], byte[]>>> exhscanunorder(final byte[] key, final byte[] cursor,
        final ScanParams params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(key);
        args.add(cursor);
        args.addAll(params.getParams());

        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXHSCANUNORDER).addObjects(args),
            HashBuilderFactory.EXHSCAN_RESULT_BYTE));
    }
}
