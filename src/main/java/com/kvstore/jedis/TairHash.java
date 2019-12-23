package com.kvstore.jedis;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.params.Params;
import redis.clients.jedis.util.SafeEncoder;

import static redis.clients.jedis.Protocol.toByteArray;

/**
 * @author bodong.ybd
 * @date 2019/12/23
 */
public class TairHash {
    private Jedis jedis;
    private JedisPool jedisPool;

    public TairHash(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    public TairHash(Jedis jedis) {
        this.jedis = jedis;
    }

    private Jedis getJedis() {
        if (jedisPool != null) {
            return jedisPool.getResource();
        }
        return jedis;
    }

    public Long exhset(final String key, final String field, final String value) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHSET, key, field, value);
        return BuilderFactory.LONG.build(obj);
    }

    public Long exhset(final byte[] key, final byte[] field, final byte[] value) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHSET, key, field, value);
        return BuilderFactory.LONG.build(obj);
    }

    public Long exhset(final String key, final String field, final String value, final ExhsetParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHSET,
            params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(field), SafeEncoder.encode(value)));
        return BuilderFactory.LONG.build(obj);
    }

    public Long exhset(final byte[] key, final byte[] field, final byte[] value, final ExhsetParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHSET, params.getByteParams(key, field, value));
        return BuilderFactory.LONG.build(obj);
    }

    public Long exhsetnx(final String key, final String field, final String value) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHSETNX, key, field, value);
        return BuilderFactory.LONG.build(obj);
    }

    public Long exhsetnx(final byte[] key, final byte[] field, final byte[] value) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHSETNX, key, field, value);
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

        Object obj = getJedis().sendCommand(ModuleCommand.EXHMSET, params.toArray(new byte[params.size()][]));
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

        Object obj = getJedis().sendCommand(ModuleCommand.EXHMSETWITHOPTS, p.toArray(new byte[params.size()][]));
        return BuilderFactory.STRING.build(obj);
    }

    public Boolean exhpexpire(final String key, final String field, final int milliseconds) {
        return exhpexpire(SafeEncoder.encode(key), SafeEncoder.encode(field), milliseconds);
    }

    public Boolean exhpexpire(final byte[] key, final byte[] field, final int milliseconds) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHPEXPIRE, key, field, toByteArray(milliseconds));
        return BuilderFactory.LONG.build(obj) == 1;
    }

    public Boolean exhpexpireAt(final String key, final String field, final long unixTime) {
        return exhpexpireAt(SafeEncoder.encode(key), SafeEncoder.encode(field), unixTime);
    }

    public Boolean exhpexpireAt(final byte[] key, final byte[] field, final long unixTime) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHPEXPIREAT, key, field, toByteArray(unixTime));
        return BuilderFactory.LONG.build(obj) == 1;
    }

    public Boolean exhexpire(final String key, final String field, final int seconds) {
        return exhexpire(SafeEncoder.encode(key), SafeEncoder.encode(field), seconds);
    }

    public Boolean exhexpire(final byte[] key, final byte[] field, final int seconds) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHEXPIRE, key, field, toByteArray(seconds));
        return BuilderFactory.LONG.build(obj) == 1;
    }

    public Boolean exhexpireAt(final String key, final String field, final long unixTime) {
        return exhexpireAt(SafeEncoder.encode(key), SafeEncoder.encode(field), unixTime);
    }

    public Boolean exhexpireAt(final byte[] key, final byte[] field, final long unixTime) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHEXPIREAT, key, field, toByteArray(unixTime));
        return BuilderFactory.LONG.build(obj) == 1;
    }

    public Long exhpttl(final String key, final String field) {
        return exhpttl(SafeEncoder.encode(key), SafeEncoder.encode(field));
    }

    public Long exhpttl(final byte[] key, final byte[] field) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHPTTL, key, field);
        return BuilderFactory.LONG.build(obj);
    }

    public Long exhttl(final String key, final String field) {
        return exhttl(SafeEncoder.encode(key), SafeEncoder.encode(field));
    }

    public Long exhttl(final byte[] key, final byte[] field) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHTTL, key, field);
        return BuilderFactory.LONG.build(obj);
    }

    public Long exhver(final String key, final String field) {
        return exhver(SafeEncoder.encode(key), SafeEncoder.encode(field));
    }

    public Long exhver(final byte[] key, final byte[] field) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHVER, key, field);
        return BuilderFactory.LONG.build(obj);
    }

    public Boolean exhsetver(final String key, final String field, final long version) {
        return exhsetver(SafeEncoder.encode(key), SafeEncoder.encode(field), version);
    }

    public Boolean exhsetver(final byte[] key, final byte[] field, final long version) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHSETVER, key, field, toByteArray(version));
        return BuilderFactory.LONG.build(obj) == 1;
    }

    public Long exhincrBy(final String key, final String field, final long value) {
        return exhincrBy(SafeEncoder.encode(key), SafeEncoder.encode(field), value);
    }

    public Long exhincrBy(byte[] key, byte[] field, long value) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHINCRBY, field, toByteArray(value));
        return BuilderFactory.LONG.build(obj);
    }

    public Long exhincrBy(final String key, final String field, final long value, final ExhincrByParams params) {
        return exhincrBy(SafeEncoder.encode(key), SafeEncoder.encode(field), value, params);
    }

    public Long exhincrBy(final byte[] key, final byte[] field, final long value, final ExhincrByParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHINCRBY,
            params.getByteParams(key, field, toByteArray(value)));
        return BuilderFactory.LONG.build(obj);
    }

    public Double exhincrByFloat(final String key, final String field, final double value) {
        return exhincrByFloat(SafeEncoder.encode(key), SafeEncoder.encode(field), value);
    }

    public Double exhincrByFloat(byte[] key, byte[] field, final double value) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHINCRBYFLOAT, key, field, toByteArray(value));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double exhincrByFloat(final String key, final String field, final double value,
        final ExhincrByParams params) {
        return exhincrByFloat(SafeEncoder.encode(key), SafeEncoder.encode(field), value, params);
    }

    public Double exhincrByFloat(byte[] key, byte[] field, double value, ExhincrByParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXINCRBYFLOAT,
            params.getByteParams(key, field, toByteArray(value)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public ExhgetwithverResult<String> exhgetwithver(final String key, final String field) {
        getJedis().getClient().sendCommand(ModuleCommand.EXHGETWITHVER, key, field);
        List<Object> result = getJedis().getClient().getObjectMultiBulkReply();
        if (null == result || 0 == result.size()) {
            return null;
        } else {
            String value = SafeEncoder.encode((byte[])result.get(0));
            long version = (Long)result.get(1);
            return new ExhgetwithverResult<String>(value, version);
        }
    }

    public ExhgetwithverResult<byte[]> exhgetwithver(byte[] key, byte[] field) {
        getJedis().getClient().sendCommand(ModuleCommand.EXHGETWITHVER, key, field);
        List<Object> result = getJedis().getClient().getObjectMultiBulkReply();
        if (null == result || 0 == result.size()) {
            return null;
        } else {
            byte[] value = (byte[])result.get(0);
            long version = (Long)result.get(1);
            return new ExhgetwithverResult<byte[]>(value, version);
        }
    }

    public List<String> exhmget(final String key, final String... fields) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHMGET,
            joinParameters(SafeEncoder.encode(key), SafeEncoder.encodeMany(fields)));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<byte[]> exhmget(byte[] key, byte[]... fields) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHMGET, joinParameters(key, fields));
        return BuilderFactory.BYTE_ARRAY_LIST.build(obj);
    }

    public List<ExhgetwithverResult<String>> exhmgetwithver(final String key, final String... fields) {
        getJedis().getClient().sendCommand(ModuleCommand.EXHMGETWITHVER,
            joinParameters(SafeEncoder.encode(key), SafeEncoder.encodeMany(fields)));
        List<Object> result = getJedis().getClient().getObjectMultiBulkReply();
        if (null == result || 0 == result.size()) {
            return null;
        } else {
            List<ExhgetwithverResult<String>> results = new ArrayList<ExhgetwithverResult<String>>();
            for (Object o : result) {
                if (o == null) {
                    results.add(null);
                } else {
                    List<Object> lo = (List<Object>)o;
                    String value = SafeEncoder.encode((byte[])lo.get(0));
                    long version = (Long)lo.get(1);
                    results.add(new ExhgetwithverResult<String>(value, version));
                }
            }
            return results;
        }
    }

    public List<ExhgetwithverResult<byte[]>> exhmgetwithver(byte[] key, byte[]... fields) {
        getJedis().getClient().sendCommand(ModuleCommand.EXHMGETWITHVER, joinParameters(key, fields));
        List<Object> result = getJedis().getClient().getObjectMultiBulkReply();
        if (null == result || 0 == result.size()) {
            return null;
        } else {
            List<ExhgetwithverResult<byte[]>> results = new ArrayList<ExhgetwithverResult<byte[]>>();
            for (Object o : result) {
                if (o == null) {
                    results.add(null);
                } else {
                    List<Object> lo = (List<Object>)o;
                    byte[] value = (byte[])lo.get(0);
                    long version = (Long)lo.get(1);
                    results.add(new ExhgetwithverResult<byte[]>(value, version));
                }
            }
            return results;
        }
    }

    public Long exhdel(final String key, final String... fields) {
        return exhdel(SafeEncoder.encode(key), SafeEncoder.encodeMany(fields));
    }

    public Long exhdel(byte[] key, byte[]... fields) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHDEL, joinParameters(key, fields));
        return BuilderFactory.LONG.build(obj);
    }

    public Long exhlen(final String key) {
        return exhlen(SafeEncoder.encode(key));
    }

    public Long exhlen(byte[] key) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHLEN, key);
        return BuilderFactory.LONG.build(obj);
    }

    public Boolean exhexists(final String key, final String field) {
        return exhexists(SafeEncoder.encode(key), SafeEncoder.encode(field));
    }

    public Boolean exhexists(byte[] key, byte[] field) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHEXISTS, key, field);
        return BuilderFactory.LONG.build(obj) == 1;
    }

    public Long exhstrlen(final String key, final String field) {
        return exhstrlen(SafeEncoder.encode(key), SafeEncoder.encode(field));
    }

    public Long exhstrlen(byte[] key, byte[] field) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHSTRLEN, key, field);
        return BuilderFactory.LONG.build(obj);
    }

    public Set<String> exhkeys(final String key) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHKEYS, key);
        return BuilderFactory.STRING_ZSET.build(obj);
    }

    public Set<byte[]> exhkeys(byte[] key) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHKEYS, key);
        return BuilderFactory.BYTE_ARRAY_ZSET.build(obj);
    }

    public List<String> exhvals(final String key) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHVALS, key);
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<byte[]> exhvals(byte[] key) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHVALS, key);
        return BuilderFactory.BYTE_ARRAY_LIST.build(obj);
    }

    public Map<String, String> exhgetAll(final String key) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHGETALL, key);
        return BuilderFactory.STRING_MAP.build(obj);
    }

    public Map<byte[], byte[]> exhgetAll(byte[] key) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHGETALL, key);
        return BuilderFactory.BYTE_ARRAY_MAP.build(obj);
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
        getJedis().getClient().sendCommand(ModuleCommand.EXHSCAN, args.toArray(new byte[args.size()][]));

        List<Object> result = getJedis().getClient().getObjectMultiBulkReply();
        String newcursor = new String((byte[])result.get(0));
        List<Map.Entry<String, String>> results = new ArrayList<Map.Entry<String, String>>();
        List<byte[]> rawResults = (List<byte[]>)result.get(1);
        Iterator<byte[]> iterator = rawResults.iterator();
        while (iterator.hasNext()) {
            results.add(new AbstractMap.SimpleEntry<String, String>(SafeEncoder.encode(iterator.next()),
                SafeEncoder.encode(iterator.next())));
        }
        return new ScanResult<Map.Entry<String, String>>(newcursor, results);
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
        getJedis().getClient().sendCommand(ModuleCommand.EXHSCAN, args.toArray(new byte[args.size()][]));

        List<Object> result = getJedis().getClient().getObjectMultiBulkReply();
        byte[] newcursor = (byte[])result.get(0);
        List<Map.Entry<byte[], byte[]>> results = new ArrayList<Map.Entry<byte[], byte[]>>();
        List<byte[]> rawResults = (List<byte[]>)result.get(1);
        Iterator<byte[]> iterator = rawResults.iterator();
        while (iterator.hasNext()) {
            results.add(new AbstractMap.SimpleEntry<byte[], byte[]>(iterator.next(), iterator.next()));
        }
        return new ScanResult<Map.Entry<byte[], byte[]>>(newcursor, results);
    }

    /* ================ Common function ================ */

    private byte[][] joinParameters(byte[] first, byte[][] rest) {
        byte[][] result = new byte[rest.length + 1][];
        result[0] = first;
        System.arraycopy(rest, 0, result, 1, rest.length);
        return result;
    }

    /* ================ Common Class ================ */

    public static class ExhsetParams extends Params {
        private static final String XX = "xx";
        private static final String NX = "nx";

        private static final String PX = "px";
        private static final String EX = "ex";
        private static final String EXAT = "exat";
        private static final String PXAT = "pxat";

        private static final String VER = "ver";
        private static final String ABS = "abs";

        public ExhsetParams() {}

        public static ExhsetParams ExhsetParams() {
            return new ExhsetParams();
        }

        /**
         * Only set the key if it already exist.
         *
         * @return SetParams
         */
        public ExhsetParams xx() {
            addParam(XX);
            return this;
        }

        /**
         * Only set the key if it does not already exist.
         *
         * @return SetParams
         */
        public ExhsetParams nx() {
            addParam(NX);
            return this;
        }

        /**
         * Set the specified expire time, in seconds.
         *
         * @param secondsToExpire
         * @return SetParams
         */
        public ExhsetParams ex(int secondsToExpire) {
            addParam(EX, secondsToExpire);
            return this;
        }

        /**
         * Set the specified expire time, in milliseconds.
         *
         * @param millisecondsToExpire
         * @return SetParams
         */
        public ExhsetParams px(long millisecondsToExpire) {
            addParam(PX, millisecondsToExpire);
            return this;
        }

        /**
         * Set the specified absolute expire time, in seconds.
         *
         * @param secondsToExpire
         * @return SetParams
         */
        public ExhsetParams exat(int secondsToExpire) {
            addParam(EXAT, secondsToExpire);
            return this;
        }

        /**
         * Set the specified absolute expire time, in milliseconds.
         *
         * @param millisecondsToExpire
         * @return SetParams
         */
        public ExhsetParams pxat(long millisecondsToExpire) {
            addParam(PXAT, millisecondsToExpire);
            return this;
        }

        /**
         * Set if version equal or not exist
         *
         * @param version
         * @return SetParams
         */
        public ExhsetParams ver(long version) {
            addParam(VER, version);
            return this;
        }

        /**
         * Set version to absoluteVersion
         *
         * @param absoluteVersion
         * @return SetParams
         */
        public ExhsetParams abs(long absoluteVersion) {
            addParam(ABS, absoluteVersion);
            return this;
        }

        private void addParamWithValue(ArrayList<byte[]> byteParams, String option) {
            if (contains(option)) {
                byteParams.add(SafeEncoder.encode(option));
                byteParams.add(SafeEncoder.encode(String.valueOf(getParam(option))));
            }
        }

        public byte[][] getByteParams(byte[]... args) {
            ArrayList<byte[]> byteParams = new ArrayList<byte[]>();
            for (byte[] arg : args) {
                byteParams.add(arg);
            }

            if (contains(XX)) {
                byteParams.add(SafeEncoder.encode(XX));
            }
            if (contains(NX)) {
                byteParams.add(SafeEncoder.encode(NX));
            }

            addParamWithValue(byteParams, EX);
            addParamWithValue(byteParams, PX);
            addParamWithValue(byteParams, EXAT);
            addParamWithValue(byteParams, PXAT);

            addParamWithValue(byteParams, VER);
            addParamWithValue(byteParams, ABS);

            return byteParams.toArray(new byte[byteParams.size()][]);
        }
    }

    public class ExhmsetwithoptsParams<T> {
        private T field;
        private T value;
        private long ver;
        private long exp;

        public ExhmsetwithoptsParams(T field, T value, long ver, long exp) {
            this.field = field;
            this.value = value;
            this.ver = ver;
            this.exp = exp;
        }

        public T getField() {
            return field;
        }

        public void setField(T field) {
            this.field = field;
        }

        public T getValue() {
            return value;
        }

        public void setValue(T value) {
            this.value = value;
        }

        public long getVer() {
            return ver;
        }

        public void setVer(long ver) {
            this.ver = ver;
        }

        public long getExp() {
            return exp;
        }

        public void setExp(long exp) {
            this.exp = exp;
        }
    }

    public static class ExhincrByParams extends Params {
        private static final String EX = "ex";
        private static final String EXAT = "exat";
        private static final String PX = "px";
        private static final String PXAT = "pxat";

        public ExhincrByParams() {
        }

        public static ExhincrByParams ExhincrByParams() {
            return new ExhincrByParams();
        }

        public ExhincrByParams ex(int secondsToExpire) {
            if (!contains(EXAT)) {
                addParam(EX, secondsToExpire);
            }
            return this;
        }

        public ExhincrByParams exat(long unixTime) {
            if (!contains(EX)) {
                addParam(EXAT, unixTime);
            }
            return this;
        }

        public ExhincrByParams px(long millisecondsToExpire) {
            if (!contains(PXAT)) {
                addParam(PX, millisecondsToExpire);
            }
            return this;
        }

        public ExhincrByParams pxat(long millisecondsToExpire) {
            if (!contains(PX)) {
                addParam(PXAT, millisecondsToExpire);
            }
            return this;
        }

        private void addParamWithValue(ArrayList<byte[]> byteParams, String option) {
            if (contains(option)) {
                byteParams.add(SafeEncoder.encode(option));
                byteParams.add(SafeEncoder.encode(String.valueOf(getParam(option))));
            }
        }

        public byte[][] getByteParams(byte[]... args) {
            ArrayList<byte[]> byteParams = new ArrayList<byte[]>();
            for (byte[] arg : args) {
                byteParams.add(arg);
            }

            addParamWithValue(byteParams, EX);
            addParamWithValue(byteParams, PX);
            addParamWithValue(byteParams, EXAT);
            addParamWithValue(byteParams, PXAT);
            return byteParams.toArray(new byte[byteParams.size()][]);
        }
    }

    public class ExhgetwithverResult<T> {
        private T value;
        private long ver;

        public ExhgetwithverResult(T value, long ver) {
            this.value = value;
            this.ver = ver;
        }

        public T getValue() {
            return value;
        }

        public long getVer() {
            return ver;
        }

        public void setValue(T value) {
            this.value = value;
        }

        public void setVer(long ver) {
            this.ver = ver;
        }
    }
}
