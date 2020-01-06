package com.kvstore.jedis.tairhash;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.kvstore.jedis.ModuleCommand;
import com.kvstore.jedis.tairhash.factory.HashBuilderFactory;
import com.kvstore.jedis.tairhash.params.ExhgetwithverResult;
import com.kvstore.jedis.tairhash.params.ExhincrByParams;
import com.kvstore.jedis.tairhash.params.ExhmsetwithoptsParams;
import com.kvstore.jedis.tairhash.params.ExhsetParams;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.util.SafeEncoder;

import static redis.clients.jedis.Protocol.toByteArray;

/**
 * @author bodong.ybd & dwan
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

    /**
     * Set the string value of a exhash field.
     *
     * @param key   the key
     * @param field the field type: key
     * @param value the value
     * @return integer-reply specifically:
     * {@literal 1} if {@code field} is a new field in the hash and {@code value} was set. {@literal 0} if
     * {@code field} already exists in the hash and the value was updated.
     */
    public Long exhset(final String key, final String field, final String value) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHSET, key, field, value);
        return BuilderFactory.LONG.build(obj);
    }

    public Long exhset(final byte[] key, final byte[] field, final byte[] value) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHSET, key, field, value);
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * Set the string value of a exhash field.
     *
     * @param key   the key
     * @param field the field type: key
     * @param value the value
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time] [NX|XX] [VER version | ABS version]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * `NX` - only set the key if it does not already exists
     * `XX` - only set the key if it already exists
     * `VER` - Set if version matched or not exist
     * `ABS` - Set with abs version
     * @return integer-reply specifically:
     * {@literal 1} if {@code field} is a new field in the hash and {@code value} was set. {@literal 0} if
     * {@code field} already exists in the hash and the value was updated.
     */
    public Long exhset(final String key, final String field, final String value, final ExhsetParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHSET,
            params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(field), SafeEncoder.encode(value)));
        return BuilderFactory.LONG.build(obj);
    }

    public Long exhset(final byte[] key, final byte[] field, final byte[] value, final ExhsetParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHSET, params.getByteParams(key, field, value));
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * Set the value of a exhash field, only if the field does not exist.
     *
     * @param key   the key
     * @param field the field type: key
     * @param value the value
     * @return integer-reply specifically:
     * {@code 1} if {@code field} is a new field in the hash and {@code value} was set. {@code 0} if {@code field}
     * already exists in the hash and no operation was performed.
     */
    public Long exhsetnx(final String key, final String field, final String value) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHSETNX, key, field, value);
        return BuilderFactory.LONG.build(obj);
    }

    public Long exhsetnx(final byte[] key, final byte[] field, final byte[] value) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHSETNX, key, field, value);
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * Set multiple hash fields to multiple values.
     *
     * @param key  the key
     * @param hash the null
     * @return String simple-string-reply
     */
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

    /**
     * set multiple hash fields with version
     *
     * @param key    the key
     * @param params the params
     * @return success: OK
     */
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

    /**
     * Set expire time (milliseconds).
     *
     * @param key     the key
     * @param field   the field
     * @param milliseconds time is milliseconds
     * @return Success: true, fail: false.
     */
    public Boolean exhpexpire(final String key, final String field, final int milliseconds) {
        return exhpexpire(SafeEncoder.encode(key), SafeEncoder.encode(field), milliseconds);
    }

    public Boolean exhpexpire(final byte[] key, final byte[] field, final int milliseconds) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHPEXPIRE, key, field, toByteArray(milliseconds));
        return BuilderFactory.BOOLEAN.build(obj);
    }

    /**
     * Set the expiration for a key as a UNIX timestamp (milliseconds).
     *
     * @param key      the key
     * @param field    the field
     * @param unixTime timestamp the timestamp type: posix time, time is milliseconds
     * @return Success: true, fail: false.
     */
    public Boolean exhpexpireAt(final String key, final String field, final long unixTime) {
        return exhpexpireAt(SafeEncoder.encode(key), SafeEncoder.encode(field), unixTime);
    }

    public Boolean exhpexpireAt(final byte[] key, final byte[] field, final long unixTime) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHPEXPIREAT, key, field, toByteArray(unixTime));
        return BuilderFactory.BOOLEAN.build(obj);
    }

    /**
     * Set expire time (seconds).
     *
     * @param key     the key
     * @param field   the field
     * @param seconds time is seconds
     * @return Success: true, fail: false.
     */
    public Boolean exhexpire(final String key, final String field, final int seconds) {
        return exhexpire(SafeEncoder.encode(key), SafeEncoder.encode(field), seconds);
    }

    public Boolean exhexpire(final byte[] key, final byte[] field, final int seconds) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHEXPIRE, key, field, toByteArray(seconds));
        return BuilderFactory.BOOLEAN.build(obj);
    }

    /**
     * Set the expiration for a key as a UNIX timestamp (seconds).
     *
     * @param key      the key
     * @param field    the field
     * @param unixTime timestamp the timestamp type: posix time, time is seconds
     * @return Success: true, fail: false.
     */
    public Boolean exhexpireAt(final String key, final String field, final long unixTime) {
        return exhexpireAt(SafeEncoder.encode(key), SafeEncoder.encode(field), unixTime);
    }

    public Boolean exhexpireAt(final byte[] key, final byte[] field, final long unixTime) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHEXPIREAT, key, field, toByteArray(unixTime));
        return BuilderFactory.BOOLEAN.build(obj);
    }

    /**
     * Get ttl (milliseconds).
     *
     * @param key   the key
     * @param field the field
     * @return ttl
     */
    public Long exhpttl(final String key, final String field) {
        return exhpttl(SafeEncoder.encode(key), SafeEncoder.encode(field));
    }

    public Long exhpttl(final byte[] key, final byte[] field) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHPTTL, key, field);
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * Get ttl (seconds).
     *
     * @param key   the key
     * @param field the field
     * @return ttl
     */
    public Long exhttl(final String key, final String field) {
        return exhttl(SafeEncoder.encode(key), SafeEncoder.encode(field));
    }

    public Long exhttl(final byte[] key, final byte[] field) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHTTL, key, field);
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * Get version
     *
     * @param key   the key
     * @param field the field
     * @return version
     */
    public Long exhver(final String key, final String field) {
        return exhver(SafeEncoder.encode(key), SafeEncoder.encode(field));
    }

    public Long exhver(final byte[] key, final byte[] field) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHVER, key, field);
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * Set the field version.
     *
     * @param key     the key
     * @param field   the field
     * @param version the version
     */
    public Boolean exhsetver(final String key, final String field, final long version) {
        return exhsetver(SafeEncoder.encode(key), SafeEncoder.encode(field), version);
    }

    public Boolean exhsetver(final byte[] key, final byte[] field, final long version) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHSETVER, key, field, toByteArray(version));
        return BuilderFactory.BOOLEAN.build(obj);
    }

    /**
     * Increment the integer value of a hash field by the given number.
     *
     * @param key   the key
     * @param field the field type: key
     * @return Long integer-reply the value at {@code field} after the increment operation.
     */
    public Long exhincrBy(final String key, final String field, final long value) {
        return exhincrBy(SafeEncoder.encode(key), SafeEncoder.encode(field), value);
    }

    public Long exhincrBy(byte[] key, byte[] field, long value) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHINCRBY, key, field, toByteArray(value));
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * Increment the integer value of a hash field by the given number.
     *
     * @param key   the key
     * @param field the field type: key
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Long integer-reply the value at {@code field} after the increment operation.
     */
    public Long exhincrBy(final String key, final String field, final long value, final ExhincrByParams params) {
        return exhincrBy(SafeEncoder.encode(key), SafeEncoder.encode(field), value, params);
    }

    public Long exhincrBy(final byte[] key, final byte[] field, final long value, final ExhincrByParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHINCRBY,
            params.getByteParams(key, field, toByteArray(value)));
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * Increment the float value of a hash field by the given amount.
     *
     * @param key   the key
     * @param field the field type: key
     * @param value the increment type: double
     * @return Double bulk-string-reply the value of {@code field} after the increment.
     */
    public Double exhincrByFloat(final String key, final String field, final double value) {
        return exhincrByFloat(SafeEncoder.encode(key), SafeEncoder.encode(field), value);
    }

    public Double exhincrByFloat(byte[] key, byte[] field, final double value) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHINCRBYFLOAT, key, field, toByteArray(value));
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * Increment the float value of a hash field by the given amount.
     *
     * @param key   the key
     * @param field the field type: key
     * @param value the increment type: double
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Double bulk-string-reply the value of {@code field} after the increment.
     */
    public Double exhincrByFloat(final String key, final String field, final double value,
        final ExhincrByParams params) {
        return exhincrByFloat(SafeEncoder.encode(key), SafeEncoder.encode(field), value, params);
    }

    public Double exhincrByFloat(byte[] key, byte[] field, double value, ExhincrByParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXINCRBYFLOAT,
            params.getByteParams(key, field, toByteArray(value)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * Get the value of a exhash field.
     *
     * @param key   the key
     * @param field the field type: key
     * @return K bulk-string-reply the value associated with {@code field}
     * or {@literal null} when {@code field} is not present
     * in the hash or {@code key} does not exist.
     */
    public String exhget(final String key, final String field) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHGET, key, field);
        return BuilderFactory.STRING.build(obj);
    }

    public byte[] exhget(final byte[] key, final byte[] field) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHGET, key, field);
        return BuilderFactory.BYTE_ARRAY.build(obj);
    }

    /**
     * Get the value and the version of a exhash field.
     *
     * @param key   the key
     * @param field the field type: key
     * @return ExhgetwithverResult the value and the version associated with {@code field}
     * or {@literal null} when {@code field} is not present
     * in the hash or {@code key} does not exist.
     */
    public ExhgetwithverResult<String> exhgetwithver(final String key, final String field) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHGETWITHVER, key, field);
        return HashBuilderFactory.EXHGETWITHVER_RESULT_STRING.build(obj);
    }

    public ExhgetwithverResult<byte[]> exhgetwithver(byte[] key, byte[] field) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHGETWITHVER, key, field);
        return HashBuilderFactory.EXHGETWITHVER_RESULT_BYTE.build(obj);
    }

    /**
     * Get the values of all the given hash fields.
     *
     * @param key    the key
     * @param fields the field type: key
     * @return List&lt;K&gt; array-reply list of values associated with the given fields
     */
    public List<String> exhmget(final String key, final String... fields) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHMGET,
            joinParameters(SafeEncoder.encode(key), SafeEncoder.encodeMany(fields)));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<byte[]> exhmget(byte[] key, byte[]... fields) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHMGET, joinParameters(key, fields));
        return BuilderFactory.BYTE_ARRAY_LIST.build(obj);
    }

    /**
     * Get the values and version of all the given hash fields.
     *
     * @param key    the key
     * @param fields the field type: key
     * @return List&lt;K&gt; array-reply list of values associated with the given fields
     */
    public List<ExhgetwithverResult<String>> exhmgetwithver(final String key, final String... fields) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHMGETWITHVER,
            joinParameters(SafeEncoder.encode(key), SafeEncoder.encodeMany(fields)));
        return HashBuilderFactory.EXHMGETWITHVER_RESULT_STRING_LIST.build(obj);
    }

    public List<ExhgetwithverResult<byte[]>> exhmgetwithver(byte[] key, byte[]... fields) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHMGETWITHVER, joinParameters(key, fields));
        return HashBuilderFactory.EXHMGETWITHVER_RESULT_BYTE_LIST.build(obj);
    }

    /**
     * Delete one or more hash fields.
     *
     * @param key    the key
     * @param fields the field type: key
     * @return Long integer-reply the number of fields that were removed from the hash
     * not including specified but non existing fields.
     */
    public Long exhdel(final String key, final String... fields) {
        return exhdel(SafeEncoder.encode(key), SafeEncoder.encodeMany(fields));
    }

    public Long exhdel(byte[] key, byte[]... fields) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHDEL, joinParameters(key, fields));
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * Get the number of fields in a hash.
     *
     * @param key the key
     * @return Long integer-reply number of fields in the hash, or {@code 0} when {@code key} does not exist.
     */
    public Long exhlen(final String key) {
        return exhlen(SafeEncoder.encode(key));
    }

    public Long exhlen(byte[] key) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHLEN, key);
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * Determine if a hash field exists.
     *
     * @param key   the key
     * @param field the field type: key
     * @return Boolean integer-reply specifically:
     * {@literal true} if the hash contains {@code field}.
     * {@literal false} if the hash does not contain {@code field}, or {@code key} does not exist.
     */
    public Boolean exhexists(final String key, final String field) {
        return exhexists(SafeEncoder.encode(key), SafeEncoder.encode(field));
    }

    public Boolean exhexists(byte[] key, byte[] field) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHEXISTS, key, field);
        return BuilderFactory.BOOLEAN.build(obj);
    }

    /**
     * Get the length of a hash field.
     *
     * @param key   the key
     * @param field the field
     * @return the length
     */
    public Long exhstrlen(final String key, final String field) {
        return exhstrlen(SafeEncoder.encode(key), SafeEncoder.encode(field));
    }

    public Long exhstrlen(byte[] key, byte[] field) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHSTRLEN, key, field);
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * Get all the fields in a hash.
     *
     * @param key the key
     * @return Set&lt;K&gt; array-reply list of fields in the hash, or an empty list when {@code key} does not exist.
     */
    public Set<String> exhkeys(final String key) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHKEYS, key);
        return BuilderFactory.STRING_ZSET.build(obj);
    }

    public Set<byte[]> exhkeys(byte[] key) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHKEYS, key);
        return BuilderFactory.BYTE_ARRAY_ZSET.build(obj);
    }

    /**
     * Get all the values in a hash.
     *
     * @param key the key
     * @return List&lt;K&gt; array-reply list of values in the hash, or an empty list when {@code key} does not exist.
     */
    public List<String> exhvals(final String key) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHVALS, key);
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<byte[]> exhvals(byte[] key) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHVALS, key);
        return BuilderFactory.BYTE_ARRAY_LIST.build(obj);
    }

    /**
     * Get all the fields and values in a hash.
     *
     * @param key the key
     * @return Map&lt;K,K&gt; array-reply list of fields and their values stored in the hash
     * or an empty list when {@code key} does not exist.
     */
    public Map<String, String> exhgetAll(final String key) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHGETALL, key);
        return BuilderFactory.STRING_MAP.build(obj);
    }

    public Map<byte[], byte[]> exhgetAll(byte[] key) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXHGETALL, key);
        return BuilderFactory.BYTE_ARRAY_MAP.build(obj);
    }

    /**
     * Exhscan a exhash
     *
     * @param key    the key
     * @param op     the op
     * @param subkey the subkey
     * @return A ScanResult
     */
    public ScanResult<Entry<String, String>> exhscan(final String key, final String op, final String subkey) {
        return exhscan(key, op, subkey, new ScanParams());
    }

    public ScanResult<Entry<byte[], byte[]>> exhscan(final byte[] key, final byte[] op, final byte[] subkey) {
        return exhscan(key, op, subkey, new ScanParams());
    }

    /**
     * Exhscan a exhash
     *
     * @param key    the key
     * @param op     the op
     * @param subkey the subkey
     * @param params the params: [MATCH pattern] [COUNT count]
     * `MATCH` - Set the pattern which is used to filter the results
     * `COUNT` - Set the number of fields in a single scan (default is 10)
     * @return A ScanResult
     */
    public ScanResult<Entry<String, String>> exhscan(final String key, final String op, final String subkey,
                                                     final ScanParams params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(op));
        args.add(SafeEncoder.encode(subkey));
        args.addAll(params.getParams());

        Object obj = getJedis().sendCommand(ModuleCommand.EXHSCAN, args.toArray(new byte[args.size()][]));
        return HashBuilderFactory.EXHSCAN_RESULT_STRING.build(obj);
    }

    public ScanResult<Entry<byte[], byte[]>> exhscan(final byte[] key, final byte[] op, final byte[] subkey,
        final ScanParams params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(key);
        args.add(op);
        args.add(subkey);
        args.addAll(params.getParams());

        Object obj = getJedis().sendCommand(ModuleCommand.EXHSCAN, args.toArray(new byte[args.size()][]));
        return HashBuilderFactory.EXHSCAN_RESULT_BYTE.build(obj);
    }

    /* ================ Common function ================ */

    private byte[][] joinParameters(byte[] first, byte[][] rest) {
        byte[][] result = new byte[rest.length + 1][];
        result[0] = first;
        System.arraycopy(rest, 0, result, 1, rest.length);
        return result;
    }
}
