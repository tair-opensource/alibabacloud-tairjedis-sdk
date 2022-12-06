package com.aliyun.tair.tairstring;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairstring.params.*;
import com.aliyun.tair.tairstring.results.ExcasResult;
import com.aliyun.tair.tairstring.results.ExgetResult;
import com.aliyun.tair.tairstring.factory.StringBuilderFactory;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;
import java.util.List;

import static redis.clients.jedis.Protocol.toByteArray;

public class TairString {
    private Jedis jedis;
    private JedisPool jedisPool;

    public TairString(Jedis jedis) {
        this.jedis = jedis;
    }

    public TairString(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    private Jedis getJedis() {
        if (jedisPool != null) {
            return jedisPool.getResource();
        }
        return jedis;
    }

    private void releaseJedis(Jedis jedis) {
        if (jedisPool != null) {
            jedis.close();
        }
    }

    /**
     * Compare And Set.
     *
     * @param key       the key
     * @param oldvalue  the oldvalue
     * @param newvalue  the newvalue
     * @return Success: 1; Not exist: -1; Fail: 0.
     */
    public Long cas(String key, String oldvalue, String newvalue) {
        return cas(SafeEncoder.encode(key), SafeEncoder.encode(oldvalue), SafeEncoder.encode(newvalue));
    }

    public Long cas(byte[] key, byte[] oldvalue, byte[] newvalue) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CAS, key, oldvalue, newvalue);
            return BuilderFactory.LONG.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Compare And Set.
     *
     * @param key       the key
     * @param oldvalue  the oldvalue
     * @param newvalue  thenewvalue
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: 1; Not exist: -1; Fail: 0.
     */
    public Long cas(String key, String oldvalue, String newvalue, CasParams params) {
        return cas(SafeEncoder.encode(key), SafeEncoder.encode(oldvalue), SafeEncoder.encode(newvalue), params);
    }

    public Long cas(byte[] key, byte[] oldvalue, byte[] newvalue, CasParams params) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CAS, params.getByteParams(key, oldvalue, newvalue));
            return BuilderFactory.LONG.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Compare And Delete.
     *
     * @param key       the key
     * @param value     the value
     * @return Success: 1; Not exist: -1; Fail: 0.
     */
    public Long cad(String key, String value) {
        return cad(SafeEncoder.encode(key), SafeEncoder.encode(value));
    }

    public Long cad(byte[] key, byte[] value) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CAD, key, value);
            return BuilderFactory.LONG.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Set the string value of the key.
     *
     * @param key   the key
     * @param value the value
     * @return Success: OK; Fail: error.
     */
    public String exset(String key, String value) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.EXSET, key, value);
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public String exset(byte[] key, byte[] value) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.EXSET, key, value);
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Set the string value of the key.
     *
     * @param key   the key
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
     * @return Success: OK; Fail: error.
     */
    public String exset(String key, String value, ExsetParams params) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.EXSET, params.getByteParams(key, value));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public String exset(byte[] key, byte[] value, ExsetParams params) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.EXSET, params.getByteParams(key, value));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Get the value of the key and set expire time.
     *
     * @param key   the key
     * @return List, Success: [value, version]; Fail: error.
     */
    public ExgetResult<String> exgetex(String key) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.EXGETEX, key);
            return StringBuilderFactory.EXGET_RESULT_STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public ExgetResult<byte[]> exgetex(byte[] key) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.EXGETEX, key);
            return StringBuilderFactory.EXGET_RESULT_BYTE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Set the string value of the key.
     *
     * @param key   the key
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time] [NX|XX] [VER version | ABS version]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * `KEEPTTL` - Remove the time to live associated with the key.
     * @return Success: OK; Fail: error.
     */
    public ExgetResult<String> exgetex(String key, ExgetexParams params) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.EXGETEX, params.getByteParams(key));
            return StringBuilderFactory.EXGET_RESULT_STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public ExgetResult<byte[]> exgetex(byte[] key, ExgetexParams params) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.EXGETEX, params.getByteParams(key));
            return StringBuilderFactory.EXGET_RESULT_BYTE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Get the value of the key.
     *
     * @param key   the key
     * @return List, Success: [value, version]; Fail: error.
     */
    public ExgetResult<String> exget(String key) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.EXGET, key);
            return StringBuilderFactory.EXGET_RESULT_STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public ExgetResult<byte[]> exget(byte[] key) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.EXGET, key);
            return StringBuilderFactory.EXGET_RESULT_BYTE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Get the value of the key.
     *
     * @param keys   the keys
     * @return List, Success: [value, version]; Fail: error.
     */
    public List<ExgetResult<String>> exmgetString(ArrayList<String> keys) {
        final List<byte[]> params = new ArrayList<byte[]>();
        for (String key : keys) {
            params.add(SafeEncoder.encode(key));
        }

        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.EXMGET, params.toArray(new byte[params.size()][]));
            return StringBuilderFactory.EXGET_MULTI_RESULT_STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public List<ExgetResult<byte[]>> exmget(ArrayList<byte[]> keys) {
        final List<byte[]> params = new ArrayList<byte[]>();
        for (byte[] key : keys) {
            params.add(key);
        }

        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.EXMGET, params.toArray(new byte[params.size()][]));
            return StringBuilderFactory.EXGET_MULTI_RESULT_BYTE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Set the version for the key.
     *
     * @param key     the key
     * @param version the version
     * @return Success: 1; Not exist: 0; Fail: error.
     */
    public Long exsetver(String key, long version) {
        return exsetver(SafeEncoder.encode(key), version);
    }

    public Long exsetver(byte[] key, long version) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.EXSETVER, key, toByteArray(version));
            return BuilderFactory.LONG.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Increment the integer value of the key by the given number.
     *
     * @param key   the key
     * @param incr  the incr
     * @return Success: value of key; Fail: error.
     */
    public Long exincrBy(String key, long incr) {
        return exincrBy(SafeEncoder.encode(key), incr);
    }

    public Long exincrBy(byte[] key, long incr) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.EXINCRBY, key, toByteArray(incr));
            return BuilderFactory.LONG.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Increment the integer value of the key by the given number.
     *
     * @param key   the key
     * @param incr  the incr
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time] [VER version | ABS version]
     *               [MIN minval] [MAX maxval]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * `VER` - Set if version matched or not exist
     * `ABS` - Set with abs version
     * `MIN` - Set the min value for the value.
     * `MAX` - Set the max value for the value.
     * `DEF` - Set the default value for the init value.
     * @return Success: value of key; Fail: error.
     */
    public Long exincrBy(String key, long incr, ExincrbyParams params) {
        return exincrBy(SafeEncoder.encode(key), incr, params);
    }

    public Long exincrBy(byte[] key, long incr, ExincrbyParams params) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.EXINCRBY, params.getByteParams(key, toByteArray(incr)));
            return BuilderFactory.LONG.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Increment the float value of the key by the given number.
     *
     * @param key   the key
     * @param incr  the incr
     * @return Success: value of key; Fail: error.
     */
    public Double exincrByFloat(String key, Double incr) {
        return exincrByFloat(SafeEncoder.encode(key), incr);
    }

    public Double exincrByFloat(byte[] key, Double incr) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.EXINCRBYFLOAT, key, toByteArray(incr));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Increment the float value of the key by the given number.
     *
     * @param key   the key
     * @param incr  the incr
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time] [VER version | ABS version]
     *               [MIN minval] [MAX maxval]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * `VER` - Set if version matched or not exist
     * `ABS` - Set with abs version
     * `MIN` - Set the min value for the value.
     * `MAX` - Set the max value for the value.
     * @return Success: value of key; Fail: error.
     */
    public Double exincrByFloat(String key, Double incr, ExincrbyFloatParams params) {
        return exincrByFloat(SafeEncoder.encode(key), incr, params);
    }

    public Double exincrByFloat(byte[] key, Double incr, ExincrbyFloatParams params) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.EXINCRBYFLOAT, params.getByteParams(key, toByteArray(incr)));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Compare And Set.
     *
     * @param key       the key
     * @param newvalue  the newvalue
     * @param version   the version
     * @return List, Success: ["OK", "", version]; Fail: ["Err", value, version].
     */
    public ExcasResult<String> excas(String key, String newvalue, long version) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.EXCAS, key, newvalue, String.valueOf(version));
            if (obj instanceof Long && ((Long)obj == -1L)) {
                return null;
            }
            return StringBuilderFactory.EXCAS_RESULT_STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public ExcasResult<byte[]> excas(byte[] key, byte[] newvalue, long version) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.EXCAS, key, newvalue, toByteArray(version));
            if (obj instanceof Long && ((Long)obj == -1L)) {
                return null;
            }
            return StringBuilderFactory.EXCAS_RESULT_BYTE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Compare And Delete.
     *
     * @param key       the key
     * @param version     the version
     * @return Success: 1; Not exist: -1; Fail: 0.
     */
    public Long excad(String key, long version) {
        return excad(SafeEncoder.encode(key), version);
    }

    public Long excad(byte[] key, long version) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.EXCAD, key, toByteArray(version));
            return BuilderFactory.LONG.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }
}
