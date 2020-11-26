package com.aliyun.tair.tairstring;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairstring.params.CasParams;
import com.aliyun.tair.tairstring.params.ExincrbyFloatParams;
import com.aliyun.tair.tairstring.params.ExincrbyParams;
import com.aliyun.tair.tairstring.params.ExsetParams;
import com.aliyun.tair.tairstring.results.ExcasResult;
import com.aliyun.tair.tairstring.results.ExgetResult;
import com.aliyun.tair.tairstring.factory.StringBuilderFactory;
import com.aliyun.tair.tairstring.results.ExincrbyVersionResult;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.util.SafeEncoder;

import static redis.clients.jedis.Protocol.toByteArray;

public class TairStringCluster {
    private JedisCluster jc;

    public TairStringCluster(JedisCluster jc) {
        this.jc = jc;
    }

    public Long cas(String key, String oldvalue, String newvalue) {
        return cas(SafeEncoder.encode(key), SafeEncoder.encode(oldvalue), SafeEncoder.encode(newvalue));
    }

    public Long cas(byte[] key, byte[] oldvalue, byte[] newvalue) {
        Object obj = jc.sendCommand(key, ModuleCommand.CAS, key, oldvalue, newvalue);
        return BuilderFactory.LONG.build(obj);
    }

    public Long cas(String key, String oldvalue, String newvalue, CasParams params) {
        return cas(SafeEncoder.encode(key), SafeEncoder.encode(oldvalue), SafeEncoder.encode(newvalue), params);
    }

    public Long cas(byte[] key, byte[] oldvalue, byte[] newvalue, CasParams params) {
        Object obj = jc.sendCommand(key, ModuleCommand.CAS, params.getByteParams(key, oldvalue, newvalue));
        return BuilderFactory.LONG.build(obj);
    }

    public Long cad(String key, String value) {
        return cad(SafeEncoder.encode(key), SafeEncoder.encode(value));
    }

    public Long cad(byte[] key, byte[] value) {
        Object obj = jc.sendCommand(key, ModuleCommand.CAD, key, value);
        return BuilderFactory.LONG.build(obj);
    }

    public String exset(String key, String value) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXSET, key, value);
        return BuilderFactory.STRING.build(obj);
    }

    public String exset(byte[] key, byte[] value) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXSET, key, value);
        return BuilderFactory.STRING.build(obj);
    }

    public String exset(String key, String value, ExsetParams params) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.EXSET, params.getByteParams(key, value));
        return BuilderFactory.STRING.build(obj);
    }

    public String exset(byte[] key, byte[] value, ExsetParams params) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXSET, params.getByteParams(key, value));
        return BuilderFactory.STRING.build(obj);
    }

    public Long exsetVersion(String key, String value, ExsetParams params) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.EXSET,
            params.getByteParams(key, value, "WITHVERSION"));
        return BuilderFactory.LONG.build(obj);
    }

    public Long exsetVersion(byte[] key, byte[] value, ExsetParams params) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXSET,
            params.getByteParams(key, value, "WITHVERSION".getBytes()));
        return BuilderFactory.LONG.build(obj);
    }

    public ExgetResult<String> exget(String key) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXGET, key);
        return StringBuilderFactory.EXGET_RESULT_STRING.build(obj);
    }

    public ExgetResult<byte[]> exget(byte[] key) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXGET, key);
        return StringBuilderFactory.EXGET_RESULT_BYTE.build(obj);
    }

    public ExgetResult<String> exgetFlags(String key) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXGET, key, "WITHFLAGS");
        return StringBuilderFactory.EXGET_RESULT_STRING.build(obj);
    }

    public ExgetResult<byte[]> exgetFlags(byte[] key) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXGET, key, "WITHFLAGS".getBytes());
        return StringBuilderFactory.EXGET_RESULT_BYTE.build(obj);
    }

    public Long exsetver(String key, long version) {
        return exsetver(SafeEncoder.encode(key), version);
    }

    public Long exsetver(byte[] key, long version) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXSETVER, key, toByteArray(version));
        return BuilderFactory.LONG.build(obj);
    }

    public Long exincrBy(String key, long incr) {
        return exincrBy(SafeEncoder.encode(key), incr);
    }

    public Long exincrBy(byte[] key, long incr) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXINCRBY, key, toByteArray(incr));
        return BuilderFactory.LONG.build(obj);
    }

    public Long exincrBy(String key, long incr, ExincrbyParams params) {
        return exincrBy(SafeEncoder.encode(key), incr, params);
    }

    public Long exincrBy(byte[] key, long incr, ExincrbyParams params) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXINCRBY, params.getByteParams(key, toByteArray(incr)));
        return BuilderFactory.LONG.build(obj);
    }

    public ExincrbyVersionResult exincrByVersion(String key, long incr, ExincrbyParams params) {
        return exincrByVersion(SafeEncoder.encode(key), incr, params);
    }

    public ExincrbyVersionResult exincrByVersion(byte[] key, long incr, ExincrbyParams params) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXINCRBY,
            params.getByteParams(key, toByteArray(incr), "WITHVERSION".getBytes()));
        return StringBuilderFactory.EXINCRBY_VERSION_RESULT_STRING.build(obj);
    }

    public Double exincrByFloat(String key, Double incr) {
        return exincrByFloat(SafeEncoder.encode(key), incr);
    }

    public Double exincrByFloat(byte[] key, Double incr) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXINCRBYFLOAT, key, toByteArray(incr));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double exincrByFloat(String key, Double incr, ExincrbyFloatParams params) {
        return exincrByFloat(SafeEncoder.encode(key), incr, params);
    }

    public Double exincrByFloat(byte[] key, Double incr, ExincrbyFloatParams params) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXINCRBYFLOAT, params.getByteParams(key, toByteArray(incr)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public ExcasResult<String> excas(String key, String newvalue, long version) {

        Object obj = jc.sendCommand(key, ModuleCommand.EXCAS, key, newvalue, String.valueOf(version));
        return StringBuilderFactory.EXCAS_RESULT_STRING.build(obj);
    }

    public ExcasResult<byte[]> excas(byte[] key, byte[] newvalue, long version) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXCAS, key, newvalue, toByteArray(version));
        return StringBuilderFactory.EXCAS_RESULT_BYTE.build(obj);
    }

    public Long excad(String key, long version) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXCAD, key, String.valueOf(version));
        return BuilderFactory.LONG.build(obj);
    }

    public Long excad(byte[] key, long version) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXCAD, key, toByteArray(version));
        return BuilderFactory.LONG.build(obj);
    }

    public Long exappend(String key, String value, String nxxx, String verabs, long version) {
        return exappend(SafeEncoder.encode(key), SafeEncoder.encode(value), nxxx, verabs, version);
    }

    public Long exappend(byte[] key, byte[] value, String nxxx, String verabs, long version) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXAPPEND, key, value, SafeEncoder.encode(nxxx),
            SafeEncoder.encode(verabs), toByteArray(version));
        return BuilderFactory.LONG.build(obj);
    }

    public Long exprepend(String key, String value, String nxxx, String verabs, long version) {
        return exprepend(SafeEncoder.encode(key), SafeEncoder.encode(value), nxxx, verabs, version);
    }

    public Long exprepend(byte[] key, byte[] value, String nxxx, String verabs, long version) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXPREPEND, key, value, SafeEncoder.encode(nxxx),
            SafeEncoder.encode(verabs), toByteArray(version));
        return BuilderFactory.LONG.build(obj);
    }

    public ExgetResult<String> exgae(String key, String expxwithat, long time) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXGAE, key, expxwithat, Long.toString(time));
        return StringBuilderFactory.EXGET_RESULT_STRING.build(obj);
    }

    public ExgetResult<byte[]> exgae(byte[] key, String expxwithat, long time) {
        Object obj = jc.sendCommand(key, ModuleCommand.EXGAE, key, SafeEncoder.encode(expxwithat),
            toByteArray(time));
        return StringBuilderFactory.EXGET_RESULT_BYTE.build(obj);
    }
}
