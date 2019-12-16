import java.util.List;

import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * use TairDoc with Jedis.
 *
 * @author bodong.ybd
 * @date 2019/12/11
 */
public class TairDoc {
    private Jedis jedis;
    private JedisPool jedisPool;

    public TairDoc(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    public TairDoc(Jedis jedis) {
        this.jedis = jedis;
    }

    private Jedis getJedis() {
        if (jedisPool != null) {
            return jedisPool.getResource();
        }
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
    public String jsonset(String... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONSET, args);
        return BuilderFactory.STRING.build(obj);
    }

    public String jsonset(byte[]... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONSET, args);
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
    public String jsonget(String... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONGET, args);
        return BuilderFactory.STRING.build(obj);
    }

    public byte[] jsonget(byte[]... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONGET, args);
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
    public Long jsondel(String... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONDEL, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsondel(byte[]... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONDEL, args);
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * JSON.TYPE <key> [path]
     * Reports the type of JSON value at `path`.
     * `path` defaults to root if not provided. If the `key` or `path` do not exist, null is returned.
     * Reply: Simple string, specifically the type.
     */
    public String jsontype(String... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONTYPE, args);
        return BuilderFactory.STRING.build(obj);
    }

    public byte[] jsontype(byte[]... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONTYPE, args);
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
    public Double jsonnumincrBy(String... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONNUMINCRBY, args);
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double jsonnumincrBy(byte[]... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONNUMINCRBY, args);
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * JSON.STRAPPEND <key> [path] <json-string>
     * Append the `json-string` value(s) the string at `path`.
     * `path` defaults to root if not provided.
     * Reply: Integer, -1 : key not exists, other: specifically the string's new length.
     */
    public Long jsonstrAppend(String... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONSTRAPPEND, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonstrAppend(byte[]... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONSTRAPPEND, args);
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
    public Long jsonstrlen(String... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONSTRLEN, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonstrlen(byte[]... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONSTRLEN, args);
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
    public String jsonarrPop(String... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONARRPOP, args);
        return BuilderFactory.STRING.build(obj);
    }

    public byte[] jsonarrPop(byte[]... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONARRPOP, args);
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
    public Long jsonArrlen(String... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONARRLEN, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonarrLen(byte[]... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONARRLEN, args);
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * JSON.ARRTRIM <key> <path> <start> <stop>
     * Trim an array so that it contains only the specified inclusive range of elements.
     *
     * Reply: Integer, specifically the array's new size.
     */
    public Long jsonarrTrim(String... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONARRTRIM, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonarrTrim(byte[]... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.JSONARRTRIM, args);
        return BuilderFactory.LONG.build(obj);
    }
}
