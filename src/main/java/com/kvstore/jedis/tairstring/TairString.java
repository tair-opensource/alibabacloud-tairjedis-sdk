package com.kvstore.jedis.tairstring;

import com.kvstore.jedis.ModuleCommand;
import com.kvstore.jedis.tairstring.params.CasParams;
import com.kvstore.jedis.tairstring.params.ExincrbyFloatParams;
import com.kvstore.jedis.tairstring.params.ExincrbyParams;
import com.kvstore.jedis.tairstring.params.ExsetParams;
import com.kvstore.jedis.tairstring.results.ExcasResult;
import com.kvstore.jedis.tairstring.results.ExgetResult;
import com.kvstore.jedis.tairstring.factory.StringBuilderFactory;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.util.SafeEncoder;

import static redis.clients.jedis.Protocol.toByteArray;

/**
 * @author bodong.ybd & dwan
 * @date 2019/12/16
 */
public class TairString {
    private Jedis jedis;
    private JedisPool jedisPool;

    public TairString(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    public TairString(Jedis jedis) {
        this.jedis = jedis;
    }

    private Jedis getJedis() {
        if (jedisPool != null) {
            return jedisPool.getResource();
        }
        return jedis;
    }

    public Long cas(String key, String oldvalue, String newvalue) {
        return cas(SafeEncoder.encode(key), SafeEncoder.encode(oldvalue), SafeEncoder.encode(newvalue));
    }

    public Long cas(byte[] key, byte[] oldvalue, byte[] newvalue) {
        Object obj = getJedis().sendCommand(ModuleCommand.CAS, key, oldvalue, newvalue);
        return BuilderFactory.LONG.build(obj);
    }

    public Long cas(String key, String oldvalue, String newvalue, CasParams params) {
        return cas(SafeEncoder.encode(key), SafeEncoder.encode(oldvalue), SafeEncoder.encode(newvalue), params);
    }

    public Long cas(byte[] key, byte[] oldvalue, byte[] newvalue, CasParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.CAS, params.getByteParams(key, oldvalue, newvalue));
        return BuilderFactory.LONG.build(obj);
    }

    public Long cad(String key, String value) {
        return cad(SafeEncoder.encode(key), SafeEncoder.encode(value));
    }

    public Long cad(byte[] key, byte[] value) {
        Object obj = getJedis().sendCommand(ModuleCommand.CAD, key, value);
        return BuilderFactory.LONG.build(obj);
    }

    public String exset(String key, String value) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXSET, key, value);
        return BuilderFactory.STRING.build(obj);
    }

    public String exset(byte[] key, byte[] value) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXSET, key, value);
        return BuilderFactory.STRING.build(obj);
    }

    public String exset(String key, String value, ExsetParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXSET, params.getByteParams(key, value));
        return BuilderFactory.STRING.build(obj);
    }

    public String exset(byte[] key, byte[] value, ExsetParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXSET, params.getByteParams(key, value));
        return BuilderFactory.STRING.build(obj);
    }

    public ExgetResult<String> exget(String key) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXGET, key);
        return StringBuilderFactory.EXGET_RESULT_STRING.build(obj);
    }

    public ExgetResult<byte[]> exget(byte[] key) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXGET, key);
        return StringBuilderFactory.EXGET_RESULT_BYTE.build(obj);
    }

    public Long exsetver(String key, long version) {
        return exsetver(SafeEncoder.encode(key), version);
    }

    public Long exsetver(byte[] key, long version) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXSETVER, key, toByteArray(version));
        return BuilderFactory.LONG.build(obj);
    }

    public Long exincrBy(String key, long incr) {
        return exincrBy(SafeEncoder.encode(key), incr);
    }

    public Long exincrBy(byte[] key, long incr) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXINCRBY, key, toByteArray(incr));
        return BuilderFactory.LONG.build(obj);
    }

    public Long exincrBy(String key, long incr, ExincrbyParams params) {
        return exincrBy(SafeEncoder.encode(key), incr, params);
    }

    public Long exincrBy(byte[] key, long incr, ExincrbyParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXINCRBY, params.getByteParams(key, toByteArray(incr)));
        return BuilderFactory.LONG.build(obj);
    }

    public Double exincrByFloat(String key, Double incr) {
        return exincrByFloat(SafeEncoder.encode(key), incr);
    }

    public Double exincrByFloat(byte[] key, Double incr) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXINCRBYFLOAT, key, toByteArray(incr));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double exincrByFloat(String key, Double incr, ExincrbyFloatParams params) {
        return exincrByFloat(SafeEncoder.encode(key), incr, params);
    }

    public Double exincrByFloat(byte[] key, Double incr, ExincrbyFloatParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXINCRBYFLOAT, params.getByteParams(key, toByteArray(incr)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public ExcasResult<String> excas(String key, String newvalue, long version) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXCAS, key, newvalue, String.valueOf(version));
        return StringBuilderFactory.EXCAS_RESULT_STRING.build(obj);
    }

    public ExcasResult<byte[]> excas(byte[] key, byte[] newvalue, long version) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXCAS, key, newvalue, toByteArray(version));
        return StringBuilderFactory.EXCAS_RESULT_BYTE.build(obj);
    }

    public Long excad(String key, long version) {
        return excad(SafeEncoder.encode(key), version);
    }

    public Long excad(byte[] key, long version) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXCAD, key, toByteArray(version));
        return BuilderFactory.LONG.build(obj);
    }
}
