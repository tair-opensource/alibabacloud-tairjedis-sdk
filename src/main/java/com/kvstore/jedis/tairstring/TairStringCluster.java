package com.kvstore.jedis.tairstring;

import com.kvstore.jedis.ModuleCommand;
import com.kvstore.jedis.tairstring.params.ExincrbyFloatParams;
import com.kvstore.jedis.tairstring.params.ExincrbyParams;
import com.kvstore.jedis.tairstring.params.ExsetParams;
import com.kvstore.jedis.tairstring.results.ExcasResult;
import com.kvstore.jedis.tairstring.results.ExgetResult;
import com.kvstore.jedis.tairstring.factory.StringBuilderFactory;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.util.SafeEncoder;

import static redis.clients.jedis.Protocol.toByteArray;

/**
 * @author bodong.ybd & dwan
 * @date 2019/12/17
 */
public class TairStringCluster {
    private JedisCluster jc;

    public TairStringCluster(JedisCluster jc) {
        this.jc = jc;
    }

    public Long cas(String sampleKey, String... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.CAS, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Long cas(byte[] sampleKey, byte[]... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.CAS, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Long cad(String sampleKey, String... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.CAD, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Long cad(byte[] sampleKey, byte[]... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.CAD, args);
        return BuilderFactory.LONG.build(obj);
    }

    public String exset(String sampleKey, String key, String value) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.EXSET, key, value);
        return BuilderFactory.STRING.build(obj);
    }

    public String exset(byte[] sampleKey, byte[] key, byte[] value) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.EXSET, key, value);
        return BuilderFactory.STRING.build(obj);
    }

    public String exset(String sampleKey, String key, String value, ExsetParams params) {
        Object obj = jc.sendCommand(SafeEncoder.encode(sampleKey), ModuleCommand.EXSET, params.getByteParams(key, value));
        return BuilderFactory.STRING.build(obj);
    }

    public String exset(byte[] sampleKey, byte[] key, byte[] value, ExsetParams params) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.EXSET, params.getByteParams(key, value));
        return BuilderFactory.STRING.build(obj);
    }

    public ExgetResult<String> exget(String sampleKey, String key) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.EXGET, key);
        return StringBuilderFactory.EXGET_RESULT_STRING.build(obj);
    }

    public ExgetResult<byte[]> exget(byte[] sampleKey, byte[] key) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.EXGET, key);
        return StringBuilderFactory.EXGET_RESULT_BYTE.build(obj);
    }

    public Long exsetver(String sampleKey, String key, long version) {
        return exsetver(SafeEncoder.encode(sampleKey), SafeEncoder.encode(key), version);
    }

    public Long exsetver(byte[] sampleKey, byte[] key, long version) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.EXSETVER, key, toByteArray(version));
        return BuilderFactory.LONG.build(obj);
    }

    public Long exincrBy(String sampleKey, String key, long incr) {
        return exincrBy(SafeEncoder.encode(sampleKey), SafeEncoder.encode(key), incr);
    }

    public Long exincrBy(byte[] sampleKey, byte[] key, long incr) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.EXINCRBY, key, toByteArray(incr));
        return BuilderFactory.LONG.build(obj);
    }

    public Long exincrBy(String sampleKey, String key, long incr, ExincrbyParams params) {
        return exincrBy(SafeEncoder.encode(sampleKey), SafeEncoder.encode(key), incr, params);
    }

    public Long exincrBy(byte[] sampleKey, byte[] key, long incr, ExincrbyParams params) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.EXINCRBY, params.getByteParams(key, toByteArray(incr)));
        return BuilderFactory.LONG.build(obj);
    }

    public Double exincrByFloat(String sampleKey, String key, Double incr) {
        return exincrByFloat(SafeEncoder.encode(sampleKey), SafeEncoder.encode(key), incr);
    }

    public Double exincrByFloat(byte[] sampleKey, byte[] key, Double incr) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.EXINCRBYFLOAT, key, toByteArray(incr));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double exincrByFloat(String sampleKey, String key, Double incr, ExincrbyFloatParams params) {
        return exincrByFloat(SafeEncoder.encode(sampleKey), SafeEncoder.encode(key), incr, params);
    }

    public Double exincrByFloat(byte[] sampleKey, byte[] key, Double incr, ExincrbyFloatParams params) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.EXINCRBYFLOAT, params.getByteParams(key, toByteArray(incr)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public ExcasResult<String> excas(String sampleKey, String key, String newvalue, long version) {

        Object obj = jc.sendCommand(sampleKey, ModuleCommand.EXCAS, key, newvalue, String.valueOf(version));
        return StringBuilderFactory.EXCAS_RESULT_STRING.build(obj);
    }

    public ExcasResult<byte[]> excas(byte[] sampleKey, byte[] key, byte[] newvalue, long version) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.EXCAS, key, newvalue, toByteArray(version));
        return StringBuilderFactory.EXCAS_RESULT_BYTE.build(obj);
    }

    public Long excad(String sampleKey, String key, long version) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.EXCAD, key, String.valueOf(version));
        return BuilderFactory.LONG.build(obj);
    }

    public Long excad(byte[] sampleKey, byte[] key, long version) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.EXCAD, key, toByteArray(version));
        return BuilderFactory.LONG.build(obj);
    }
}
