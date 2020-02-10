package com.aliyun.tair.tairdoc;

import java.util.List;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairdoc.params.JsonsetParams;
import com.aliyun.tair.tairdoc.params.JsongetParams;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.util.SafeEncoder;

import static redis.clients.jedis.Protocol.toByteArray;

public class TairDoc {
    private Jedis jedis;

    public TairDoc(Jedis jedis) {
        this.jedis = jedis;
    }

    private Jedis getJedis() {
        return jedis;
    }

    /**
     * JSON.SET <key> <path> <json> [NX|XX]
     * Sets the JSON value at `path` in `key`
     *
     * For new Redis keys the `path` must be the root. For existing keys, when the entire `path` exists,
     * the value that it contains is replaced with the `json` value.
     *
     * `NX` - only set the key if it does not already exists
     * `XX` - only set the key if it already exists
     *
     * Reply: Simple String `OK` if executed correctly, or Null if the specified `NX` or `XX`
     * conditions were not met.
     */
    public String jsonset(final String key, final String path, final String json) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONSET, key, path, json);
        return BuilderFactory.STRING.build(obj);
    }

    public String jsonset(final String key, final String path, final String json, final JsonsetParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONSET,
            params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(path), SafeEncoder.encode(json)));
        return BuilderFactory.STRING.build(obj);
    }

    public String jsonset(final byte[] key, final byte[] path, final byte[] json) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONSET, key, path, json);
        return BuilderFactory.STRING.build(obj);
    }

    public String jsonset(final byte[] key, final byte[] path, final byte[] json, final JsonsetParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONSET, params.getByteParams(key, path, json));
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * JSON.GET <key> [PATH] [FORMAT <XML/YAML>] [ROOTNAME <root>] [ARRNAME <arr>]
     * Return the value at `path` in JSON serialized form.
     *
     * `PATH` the path of json
     * `FORMAT` doc format，XML or YAML
     * `ROOTNAME` XML root name
     * `ARRNAME` XML array name
     *
     * Reply: Bulk String, specifically the JSON serialization or XML/YAML.
     */
    public String jsonget(final String key) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONGET, key);
        return BuilderFactory.STRING.build(obj);
    }

    public String jsonget(final String key, final String path) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONGET, key, path);
        return BuilderFactory.STRING.build(obj);
    }

    public String jsonget(final String key, final String path, final JsongetParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONGET,
            params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(path)));
        return BuilderFactory.STRING.build(obj);
    }

    public byte[] jsonget(final byte[] key) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONGET, key);
        return BuilderFactory.BYTE_ARRAY.build(obj);
    }

    public byte[] jsonget(final byte[] key, final byte[] path) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONGET, key, path);
        return BuilderFactory.BYTE_ARRAY.build(obj);
    }

    public byte[] jsonget(final byte[] key, final byte[] path, final JsongetParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONGET,
            params.getByteParams(key, path));
        return BuilderFactory.BYTE_ARRAY.build(obj);
    }

    /**
     * JSON.MGET <key> [<key> ...] <path>
     * Returns the values at `path` from multiple `key`s. Non-existing keys and non-existing paths
     * are reported as null.
     * Reply: Array of Bulk Strings, specifically the JSON serialization of
     * the value at each key's path.
     */
    public List<String> jsonmget(String... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONMGET, args);
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<byte[]> jsonmget(byte[]... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONMGET, args);
        return BuilderFactory.BYTE_ARRAY_LIST.build(obj);
    }

    /**
     * JSON.DEL <key> [path]
     * Delete a value.
     *
     * `path` defaults to root if not provided. Non-existing keys as well as non-existing paths are
     * ignored. Deleting an object's root is equivalent to deleting the key from Redis.
     *
     * Reply: Integer, specifically the number of paths deleted (0 or 1).
     */
    public Long jsondel(final String key) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONDEL, key);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsondel(final String key, final String path) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONDEL, key, path);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsondel(final byte[] key) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONDEL, key);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsondel(final byte[] key, final byte[] path) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONDEL, key, path);
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * JSON.TYPE <key> [path]
     * Reports the type of JSON value at `path`.
     * `path` defaults to root if not provided. If the `key` or `path` do not exist, null is returned.
     * Reply: Simple string, specifically the type.
     */
    public String jsontype(final String key) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONTYPE, key);
        return BuilderFactory.STRING.build(obj);
    }

    public String jsontype(final String key, final String path) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONTYPE, key, path);
        return BuilderFactory.STRING.build(obj);
    }

    public byte[] jsontype(final byte[] key) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONTYPE, key);
        return BuilderFactory.BYTE_ARRAY.build(obj);
    }

    public byte[] jsontype(final byte[] key, final byte[] path) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONTYPE, key, path);
        return BuilderFactory.BYTE_ARRAY.build(obj);
    }

    /**
     * JSON.NUMICRBY <key> [path] <value>
     * long value range: [-2^53, 2^53] [-9007199254740992, 9007199254740992]
     * double value range: [Double.MIN_VALUE, Double.MAX_VALUE]
     *
     * Increments the value stored under `path` by `value`.
     * `path` must exist path and must be a number value.
     * Reply: int number, specifically the resulting.
     */
    public Double jsonnumincrBy(final String key, final Double value) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONNUMINCRBY, SafeEncoder.encode(key), toByteArray(value));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double jsonnumincrBy(final String key, final String path, final Double value) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONNUMINCRBY, SafeEncoder.encode(key),
            SafeEncoder.encode(path), toByteArray(value));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double jsonnumincrBy(final byte[] key, final Double value) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONNUMINCRBY, key, toByteArray(value));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double jsonnumincrBy(final byte[] key, final byte[] path, final Double value) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONNUMINCRBY, key, path, toByteArray(value));
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * JSON.STRAPPEND <key> [path] <json-string>
     * Append the `json-string` value(s) the string at `path`.
     * `path` defaults to root if not provided.
     * Reply: Integer, -1 : key not exists, other: specifically the string's new length.
     */
    public Long jsonstrAppend(final String key, final String json) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONSTRAPPEND, key, json);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonstrAppend(final String key, final String path, final String json) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONSTRAPPEND, key, path, json);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonstrAppend(final byte[] key, final byte[] json) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONSTRAPPEND, key, json);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonstrAppend(final byte[] key, final byte[] path, final byte[] json) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONSTRAPPEND, key, path, json);
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * JSON.STRLEN <key> [path]
     * Report the length of the JSON value at `path` in `key`.
     *
     * `path` defaults to root if not provided. If the `key` or `path` do not exist, null is returned.
     *
     * Reply: Integer, specifically the length of the value.
     */
    public Long jsonstrlen(final String key) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONSTRLEN, key);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonstrlen(final String key, final String path) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONSTRLEN, key, path);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonstrlen(final byte[] key) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONSTRLEN, key);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonstrlen(final byte[] key, final byte[] path) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONSTRLEN, key, path);
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * JSON.ARRAPPEND <key> <path> <json> [<json> ...]
     * Append the `json` value(s) into the array at `path` after the last element in it.
     * Reply: Integer, specifically the array's new size
     */
    public Long jsonarrAppend(String... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONARRAPPEND, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonarrAppend(byte[]... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONARRAPPEND, args);
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * JSON.ARRPOP <key> <path> [index]
     * Remove and return element from the index in the array.
     *
     * `path` the array pointer. `index` is the position in the array to start
     * popping from (defaults to -1, meaning the last element). Out of range indices are rounded to
     * their respective array ends. Popping an empty array yields null.
     *
     * Reply: Bulk String, specifically the popped JSON value.
     */
    public String jsonarrPop(final String key, final String path) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONARRPOP, key, path);
        return BuilderFactory.STRING.build(obj);
    }

    public String jsonarrPop(final String key, final String path, int index) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONARRPOP, key, path, String.valueOf(index));
        return BuilderFactory.STRING.build(obj);
    }

    public byte[] jsonarrPop(final byte[] key, final byte[] path) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONARRPOP, key, path);
        return BuilderFactory.BYTE_ARRAY.build(obj);
    }

    public byte[] jsonarrPop(final byte[] key, final byte[] path, int index) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONARRPOP, key, path, toByteArray(index));
        return BuilderFactory.BYTE_ARRAY.build(obj);
    }

    /**
     * JSON.ARRINSERT <key> <path> <index> <json> [<json> ...]
     * Insert the `json` value(s) into the array at `path` before the `index` (shifts to the right).
     *
     * The index must be in the array's range. Inserting at `index` 0 prepends to the array.
     * Negative index values are interpreted as starting from the end.
     *
     * Reply: Integer, specifically the array's new size
     */
    public Long jsonarrInsert(String... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONARRINSERT, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonarrInsert(byte[]... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONARRINSERT, args);
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * JSON.ARRLEN <key> [path]
     * Report the length of the array at `path` in `key`.
     *
     * `path` defaults to root if not provided. If the `key` or `path` do not exist, null is returned.
     *
     * Reply: Integer, specifically the length of the array.
     */
    public Long jsonArrlen(final String key) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONARRLEN, key);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonArrlen(final String key, final String path) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONARRLEN, key, path);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonarrLen(final byte[] key) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONARRLEN, key);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonarrLen(final byte[] key, final byte[] path) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONARRLEN, key, path);
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * JSON.ARRTRIM <key> <path> <start> <stop>
     * Trim an array so that it contains only the specified inclusive range of elements.
     *
     * Reply: Integer, specifically the array's new size.
     */
    public Long jsonarrTrim(final String key, final String path, final int start, final int stop) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONARRTRIM, key, path, String.valueOf(start),
            String.valueOf(stop));
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonarrTrim(final byte[] key, final byte[] path, final int start, final int stop) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONARRTRIM, key, path, toByteArray(start),
            toByteArray(stop));
        return BuilderFactory.LONG.build(obj);
    }
}
