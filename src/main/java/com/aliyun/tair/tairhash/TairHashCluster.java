package com.aliyun.tair.tairhash;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairhash.factory.HashBuilderFactory;
import com.aliyun.tair.tairhash.params.*;
import com.aliyun.tair.util.JoinParameters;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.util.SafeEncoder;

import static redis.clients.jedis.Protocol.toByteArray;

public class TairHashCluster {
    private JedisCluster jc;

    public TairHashCluster(JedisCluster jc) {
        this.jc = jc;
    }

    public Long exhset(final String key, final String field, final String value) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXHSET, key, field, value);
        return BuilderFactory.LONG.build(obj);
    }

    public Long exhset(final byte[] key, final byte[] field, final byte[] value) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXHSET, key, field, value);
        return BuilderFactory.LONG.build(obj);
    }

    public Long exhset(final String key, final String field, final String value, final ExhsetParams params) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.EXHSET,
            params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(field), SafeEncoder.encode(value)));
        return BuilderFactory.LONG.build(obj);
    }

    public Long exhset(final byte[] key, final byte[] field, final byte[] value, final ExhsetParams params) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXHSET, params.getByteParams(key, field, value));
        return BuilderFactory.LONG.build(obj);
    }

    public Long exhsetnx(final String key, final String field, final String value) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXHSETNX, key, field, value);
        return BuilderFactory.LONG.build(obj);
    }

    public Long exhsetnx(final byte[] key, final byte[] field, final byte[] value) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXHSETNX, key, field, value);
        return BuilderFactory.LONG.build(obj);
    }

    public String exhmset(final String key, final Map<String, String> hash) {
        final Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>(hash.size());
        for (final Entry<String, String> entry : hash.entrySet()) {
            bhash.put(SafeEncoder.encode(entry.getKey()), SafeEncoder.encode(entry.getValue()));
        }
        return exhmset(SafeEncoder.encode(key), bhash);
    }

    public String exhmset(final byte[] key, final Map<byte[], byte[]> hash) {
        final List<byte[]> params = new ArrayList<byte[]>();
        params.add(key);

        for (final Entry<byte[], byte[]> entry : hash.entrySet()) {
            params.add(entry.getKey());
            params.add(entry.getValue());
        }

        Object obj = jc.sendCommand(key, ModuleCommand.EXHMSET, params.toArray(new byte[params.size()][]));
        return BuilderFactory.STRING.build(obj);
    }

    public String exhmsetwithopts(final String key, final List<ExhmsetwithoptsParams<String>> params) {
        List<ExhmsetwithoptsParams<byte[]>> bexhash = new ArrayList<ExhmsetwithoptsParams<byte[]>>();
        for (ExhmsetwithoptsParams<String> entry : params) {
            bexhash.add(new ExhmsetwithoptsParams<byte[]>(SafeEncoder.encode(entry.getField()),
                SafeEncoder.encode(entry.getValue()), entry.getVer(), entry.getExp()));
        }
        return exhmsetwithopts(SafeEncoder.encode(key), bexhash);
    }

    public String exhmsetwithopts(final byte[] key, final List<ExhmsetwithoptsParams<byte[]>> params) {
        final List<byte[]> p = new ArrayList<byte[]>();
        p.add(key);

        for (final ExhmsetwithoptsParams<byte[]> entry : params) {
            p.add(entry.getField());
            p.add(entry.getValue());
            p.add(toByteArray(entry.getVer()));
            p.add(toByteArray(entry.getExp()));
        }

        Object obj = jc.sendCommand(key, ModuleCommand.EXHMSETWITHOPTS, p.toArray(new byte[params.size()][]));
        return BuilderFactory.STRING.build(obj);
    }

    public Boolean exhpexpire(final String key, final String field, final int milliseconds) {
        return exhpexpire(SafeEncoder.encode(key), SafeEncoder.encode(field), milliseconds);
    }

    public Boolean exhpexpire(final String key, final String field, final int milliseconds,boolean noactive) {
        return exhpexpire(SafeEncoder.encode(key), SafeEncoder.encode(field), milliseconds,noactive);
    }

    public Boolean exhpexpire(final byte[] key, final byte[] field, final int milliseconds) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXHPEXPIRE, key, field, toByteArray(milliseconds));
        return BuilderFactory.BOOLEAN.build(obj);
    }

    public Boolean exhpexpire(final byte[] key, final byte[] field, final int milliseconds,boolean noactive) {
        Object obj;
        if(noactive){
            obj = jc.sendCommand(key, ModuleCommand.EXHPEXPIRE, key, field, toByteArray(milliseconds),SafeEncoder.encode("noactive"));
        } else {
            obj = jc.sendCommand(key, ModuleCommand.EXHPEXPIRE, key, field, toByteArray(milliseconds));
        }

        return BuilderFactory.BOOLEAN.build(obj);
    }

    public Boolean exhpexpireAt(final String key, final String field, final long unixTime) {
        return exhpexpireAt(SafeEncoder.encode(key), SafeEncoder.encode(field), unixTime);
    }

    public Boolean exhpexpireAt(final String key, final String field, final long unixTime,boolean noactive) {
        return exhpexpireAt(SafeEncoder.encode(key), SafeEncoder.encode(field), unixTime,noactive);
    }

    public Boolean exhpexpireAt(final byte[] key, final byte[] field, final long unixTime) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXHPEXPIREAT, key, field, toByteArray(unixTime));
        return BuilderFactory.BOOLEAN.build(obj);
    }

    public Boolean exhpexpireAt(final byte[] key, final byte[] field, final long unixTime,boolean noactive) {
        Object obj;
        if(noactive) {
            obj = jc.sendCommand(key, ModuleCommand.EXHPEXPIREAT, key, field, toByteArray(unixTime),SafeEncoder.encode("noactive"));
        } else {
            obj = jc.sendCommand(key, ModuleCommand.EXHPEXPIREAT, key, field, toByteArray(unixTime));
        }
        return BuilderFactory.BOOLEAN.build(obj);
    }

    public Boolean exhexpire(final String key, final String field, final int seconds,boolean noactive) {
        return exhexpire(SafeEncoder.encode(key), SafeEncoder.encode(field), seconds,noactive);
    }

    public Boolean exhexpire(final String key, final String field, final int seconds) {
        return exhexpire(SafeEncoder.encode(key), SafeEncoder.encode(field), seconds);
    }

    public Boolean exhexpire(final byte[] key, final byte[] field, final int seconds) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXHEXPIRE, key, field, toByteArray(seconds));
        return BuilderFactory.BOOLEAN.build(obj);
    }

    public Boolean exhexpire(final byte[] key, final byte[] field, final int seconds,boolean noactive) {
        Object obj;
        if(noactive){
            obj  = jc.sendCommand(key, ModuleCommand.EXHEXPIRE, key, field, toByteArray(seconds),SafeEncoder.encode("noactive"));
        }else {
            obj  = jc.sendCommand(key, ModuleCommand.EXHEXPIRE, key, field, toByteArray(seconds));
        }

        return BuilderFactory.BOOLEAN.build(obj);
    }

    public Boolean exhexpireAt(final String key, final String field, final long unixTime) {
        return exhexpireAt(SafeEncoder.encode(key), SafeEncoder.encode(field), unixTime);
    }

    public Boolean exhexpireAt(final String key, final String field, final long unixTime,boolean noactive) {
        return exhexpireAt(SafeEncoder.encode(key), SafeEncoder.encode(field), unixTime,noactive);
    }

    public Boolean exhexpireAt(final byte[] key, final byte[] field, final long unixTime) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXHEXPIREAT, key, field, toByteArray(unixTime));
        return BuilderFactory.BOOLEAN.build(obj);
    }

    public Boolean exhexpireAt(final byte[] key, final byte[] field, final long unixTime,boolean noactive) {
        Object obj;
        if(noactive){
            obj = jc.sendCommand(key, ModuleCommand.EXHEXPIREAT, key, field, toByteArray(unixTime),SafeEncoder.encode("noactive"));
        }else {
            obj = jc.sendCommand(key, ModuleCommand.EXHEXPIREAT, key, field, toByteArray(unixTime));
        }

        return BuilderFactory.BOOLEAN.build(obj);
    }

    public Long exhpttl(final String key, final String field) {
        return exhpttl(SafeEncoder.encode(key), SafeEncoder.encode(field));
    }

    public Long exhpttl(final byte[] key, final byte[] field) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXHPTTL, key, field);
        return BuilderFactory.LONG.build(obj);
    }

    public Long exhttl(final String key, final String field) {
        return exhttl(SafeEncoder.encode(key), SafeEncoder.encode(field));
    }

    public Long exhttl(final byte[] key, final byte[] field) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXHTTL, key, field);
        return BuilderFactory.LONG.build(obj);
    }

    public Long exhver(final String key, final String field) {
        return exhver(SafeEncoder.encode(key), SafeEncoder.encode(field));
    }

    public Long exhver(final byte[] key, final byte[] field) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXHVER, key, field);
        return BuilderFactory.LONG.build(obj);
    }

    public Boolean exhsetver(final String key, final String field, final long version) {
        return exhsetver(SafeEncoder.encode(key), SafeEncoder.encode(field), version);
    }

    public Boolean exhsetver(final byte[] key, final byte[] field, final long version) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXHSETVER, key, field, toByteArray(version));
        return BuilderFactory.BOOLEAN.build(obj);
    }

    public Long exhincrBy(final String key, final String field, final long value) {
        return exhincrBy(SafeEncoder.encode(key), SafeEncoder.encode(field), value);
    }

    public Long exhincrBy(byte[] key, byte[] field, long value) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXHINCRBY, key, field, toByteArray(value));
        return BuilderFactory.LONG.build(obj);
    }

    public Long exhincrBy(final String key, final String field, final long value, final ExhincrByParams params) {
        return exhincrBy(SafeEncoder.encode(key), SafeEncoder.encode(field), value, params);
    }

    public Long exhincrBy(final byte[] key, final byte[] field, final long value, final ExhincrByParams params) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXHINCRBY,
            params.getByteParams(key, field, toByteArray(value)));
        return BuilderFactory.LONG.build(obj);
    }

    public Double exhincrByFloat(final String key, final String field, final double value) {
        return exhincrByFloat(SafeEncoder.encode(key), SafeEncoder.encode(field), value);
    }

    public Double exhincrByFloat(byte[] key, byte[] field, final double value) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXHINCRBYFLOAT, key, field, toByteArray(value));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double exhincrByFloat(final String key, final String field, final double value,
        final ExhincrByFloatParams params) {
        return exhincrByFloat(SafeEncoder.encode(key), SafeEncoder.encode(field), value, params);
    }

    public Double exhincrByFloat(byte[] key, byte[] field, double value, ExhincrByFloatParams params) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXHINCRBYFLOAT,
            params.getByteParams(key, field, toByteArray(value)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public String exhget(final String key, final String field) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXHGET, key, field);
        return BuilderFactory.STRING.build(obj);
    }

    public byte[] exhget(final byte[] key, final byte[] field) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXHGET, key, field);
        return BuilderFactory.BYTE_ARRAY.build(obj);
    }

    public ExhgetwithverResult<String> exhgetwithver(final String key, final String field) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXHGETWITHVER, key, field);
        return HashBuilderFactory.EXHGETWITHVER_RESULT_STRING.build(obj);
    }

    public ExhgetwithverResult<byte[]> exhgetwithver(byte[] key, byte[] field) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXHGETWITHVER, key, field);
        return HashBuilderFactory.EXHGETWITHVER_RESULT_BYTE.build(obj);
    }

    public List<String> exhmget(final String key, final String... fields) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.EXHMGET,
            JoinParameters.joinParameters(SafeEncoder.encode(key), SafeEncoder.encodeMany(fields)));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<byte[]> exhmget(byte[] key, byte[]... fields) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXHMGET, JoinParameters.joinParameters(key, fields));
        return BuilderFactory.BYTE_ARRAY_LIST.build(obj);
    }

    public List<ExhgetwithverResult<String>> exhmgetwithver(final String key, final String... fields) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.EXHMGETWITHVER,
            JoinParameters.joinParameters(SafeEncoder.encode(key), SafeEncoder.encodeMany(fields)));
        return HashBuilderFactory.EXHMGETWITHVER_RESULT_STRING_LIST.build(obj);
    }

    public List<ExhgetwithverResult<byte[]>> exhmgetwithver(byte[] key, byte[]... fields) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXHMGETWITHVER, JoinParameters.joinParameters(key, fields));
        return HashBuilderFactory.EXHMGETWITHVER_RESULT_BYTE_LIST.build(obj);
    }

    public Long exhdel(final String key, final String... fields) {
        return exhdel(SafeEncoder.encode(key), SafeEncoder.encodeMany(fields));
    }

    public Long exhdel(byte[] key, byte[]... fields) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXHDEL, JoinParameters.joinParameters(key, fields));
        return BuilderFactory.LONG.build(obj);
    }

    public Long exhlen(final String key) {
        return exhlen(SafeEncoder.encode(key));
    }

    public Long exhlen(final String key,boolean noexp) {
        return exhlen(SafeEncoder.encode(key),noexp);
    }

    public Long exhlen(byte[] key) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXHLEN, key);
        return BuilderFactory.LONG.build(obj);
    }

    public Long exhlen(byte[] key,boolean noexp) {
        Object obj;
        if(noexp){
            obj = jc.sendCommand(key, ModuleCommand.EXHLEN, key,SafeEncoder.encode("noexp"));
        }else {
            obj = jc.sendCommand(key, ModuleCommand.EXHLEN, key);
        }

        return BuilderFactory.LONG.build(obj);
    }

    public Boolean exhexists(final String key, final String field) {
        return exhexists(SafeEncoder.encode(key), SafeEncoder.encode(field));
    }

    public Boolean exhexists(byte[] key, byte[] field) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXHEXISTS, key, field);
        return BuilderFactory.BOOLEAN.build(obj);
    }

    public Long exhstrlen(final String key, final String field) {
        return exhstrlen(SafeEncoder.encode(key), SafeEncoder.encode(field));
    }

    public Long exhstrlen(byte[] key, byte[] field) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXHSTRLEN, key, field);
        return BuilderFactory.LONG.build(obj);
    }

    public Set<String> exhkeys(final String key) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXHKEYS, key);
        return BuilderFactory.STRING_ZSET.build(obj);
    }

    public Set<byte[]> exhkeys(byte[] key) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXHKEYS, key);
        return BuilderFactory.BYTE_ARRAY_ZSET.build(obj);
    }

    public List<String> exhvals(final String key) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXHVALS, key);
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<byte[]> exhvals(byte[] key) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXHVALS, key);
        return BuilderFactory.BYTE_ARRAY_LIST.build(obj);
    }

    public Map<String, String> exhgetAll(final String key) {
        try {
            Object obj = jc.sendCommand(key, ModuleCommand.EXHGETALL, key);
            return BuilderFactory.STRING_MAP.build(obj);
        } catch (ClassCastException e) {
            return new HashMap<>();
        }
    }

    public Map<byte[], byte[]> exhgetAll(byte[] key) {
        try {
            Object obj = jc.sendCommand(key, ModuleCommand.EXHGETALL, key);
            return BuilderFactory.BYTE_ARRAY_MAP.build(obj);
        } catch (ClassCastException e) {
            return new HashMap<>();
        }
    }

    public ScanResult<Entry<String, String>> exhscan(final String key, final String op, final String subkey) {
        return exhscan(key, op, subkey, new ScanParams());
    }

    public ScanResult<Entry<String, String>> exhscan(final String key, final String op, final String subkey,
        final ScanParams params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(op));
        args.add(SafeEncoder.encode(subkey));
        args.addAll(params.getParams());

        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.EXHSCAN, args.toArray(new byte[args.size()][]));
        return HashBuilderFactory.EXHSCAN_RESULT_STRING.build(obj);
    }

    public ScanResult<Entry<String, String>> exhscan(final String key, final String op, final String subkey,
                                                     final ExhscanParams params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(op));
        args.add(SafeEncoder.encode(subkey));
        args.addAll(params.getParams());

        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.EXHSCAN, args.toArray(new byte[args.size()][]));
        return HashBuilderFactory.EXHSCAN_RESULT_STRING.build(obj);
    }

    public ScanResult<Entry<byte[], byte[]>> exhscan(final byte[] key, final byte[] op, final byte[] subkey) {
        return exhscan(key, op, subkey, new ScanParams());
    }

    public ScanResult<Entry<byte[], byte[]>> exhscan(final byte[] key, final byte[] op, final byte[] subkey,
        final ScanParams params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(key);
        args.add(op);
        args.add(subkey);
        args.addAll(params.getParams());

        Object obj = jc.sendCommand(key, ModuleCommand.EXHSCAN, args.toArray(new byte[args.size()][]));
        return HashBuilderFactory.EXHSCAN_RESULT_BYTE.build(obj);
    }

    public ScanResult<Entry<byte[], byte[]>> exhscan(final byte[] key, final byte[] op, final byte[] subkey,
                                                     final ExhscanParams params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(key);
        args.add(op);
        args.add(subkey);
        args.addAll(params.getParams());

        Object obj = jc.sendCommand(key, ModuleCommand.EXHSCAN, args.toArray(new byte[args.size()][]));
        return HashBuilderFactory.EXHSCAN_RESULT_BYTE.build(obj);
    }
}
