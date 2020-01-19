package com.aliyun.tairjedis.tairstring;

import com.aliyun.tairjedis.ModuleCommand;
import com.aliyun.tairjedis.tairstring.params.CasParams;
import com.aliyun.tairjedis.tairstring.params.ExincrbyFloatParams;
import com.aliyun.tairjedis.tairstring.params.ExincrbyParams;
import com.aliyun.tairjedis.tairstring.params.ExsetParams;
import com.aliyun.tairjedis.tairstring.results.ExcasResult;
import com.aliyun.tairjedis.tairstring.results.ExgetResult;
import com.aliyun.tairjedis.tairstring.factory.StringBuilderFactory;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.util.SafeEncoder;

import static redis.clients.jedis.Protocol.toByteArray;

public class TairStringPipeline extends Pipeline {

    public Response<Long> cas(String key, String oldvalue, String newvalue) {
        return cas(SafeEncoder.encode(key), SafeEncoder.encode(oldvalue), SafeEncoder.encode(newvalue));
    }

    public Response<Long> cas(byte[] key, byte[] oldvalue, byte[] newvalue) {
        getClient("").sendCommand(ModuleCommand.CAS, key, oldvalue, newvalue);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> cas(String key, String oldvalue, String newvalue, CasParams params) {
        return cas(SafeEncoder.encode(key), SafeEncoder.encode(oldvalue), SafeEncoder.encode(newvalue), params);
    }

    public Response<Long> cas(byte[] key, byte[] oldvalue, byte[] newvalue, CasParams params) {
        getClient("").sendCommand(ModuleCommand.CAS, params.getByteParams(key, oldvalue, newvalue));
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> cad(String key, String value) {
        return cad(SafeEncoder.encode(key), SafeEncoder.encode(value));
    }

    public Response<Long> cad(byte[] key, byte[] value) {
        getClient("").sendCommand(ModuleCommand.CAD, key, value);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<String> exset(String key, String value) {
        getClient("").sendCommand(ModuleCommand.EXSET, key, value);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> exset(byte[] key, byte[] value) {
        getClient("").sendCommand(ModuleCommand.EXSET, key, value);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> exset(String key, String value, ExsetParams params) {
        getClient("").sendCommand(ModuleCommand.EXSET, params.getByteParams(key, value));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> exset(byte[] key, byte[] value, ExsetParams params) {
        getClient("").sendCommand(ModuleCommand.EXSET, params.getByteParams(key, value));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<ExgetResult<String>> exget(String key) {
        getClient("").sendCommand(ModuleCommand.EXGET, key);
        return getResponse(StringBuilderFactory.EXGET_RESULT_STRING);
    }

    public Response<ExgetResult<byte[]>> exget(byte[] key) {
        getClient("").sendCommand(ModuleCommand.EXGET, key);
        return getResponse(StringBuilderFactory.EXGET_RESULT_BYTE);
    }

    public Response<Long> exsetver(String key, long version) {
        return exsetver(SafeEncoder.encode(key), version);
    }

    public Response<Long> exsetver(byte[] key, long version) {
        getClient("").sendCommand(ModuleCommand.EXSETVER, key, toByteArray(version));
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> exincrBy(String key, long incr) {
        return exincrBy(SafeEncoder.encode(key), incr);
    }

    public Response<Long> exincrBy(byte[] key, long incr) {
        getClient("").sendCommand(ModuleCommand.EXINCRBY, key, toByteArray(incr));
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> exincrBy(String key, long incr, ExincrbyParams params) {
        return exincrBy(SafeEncoder.encode(key), incr, params);
    }

    public Response<Long> exincrBy(byte[] key, long incr, ExincrbyParams params) {
        getClient("").sendCommand(ModuleCommand.EXINCRBY, params.getByteParams(key, toByteArray(incr)));
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Double> exincrByFloat(String key, Double incr) {
        return exincrByFloat(SafeEncoder.encode(key), incr);
    }

    public Response<Double> exincrByFloat(byte[] key, Double incr) {
        getClient("").sendCommand(ModuleCommand.EXINCRBYFLOAT, key, toByteArray(incr));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> exincrByFloat(String key, Double incr, ExincrbyFloatParams params) {
        return exincrByFloat(SafeEncoder.encode(key), incr, params);
    }

    public Response<Double> exincrByFloat(byte[] key, Double incr, ExincrbyFloatParams params) {
        getClient("").sendCommand(ModuleCommand.EXINCRBYFLOAT, params.getByteParams(key, toByteArray(incr)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<ExcasResult<String>> excas(String key, String value, long version) {
        getClient("").sendCommand(ModuleCommand.EXCAS, key, value, String.valueOf(version));
        return getResponse(StringBuilderFactory.EXCAS_RESULT_STRING);
    }

    public Response<ExcasResult<byte[]>> excas(byte[] key, byte[] value, long version) {
        getClient("").sendCommand(ModuleCommand.EXCAS, key, value, toByteArray(version));
        return getResponse(StringBuilderFactory.EXCAS_RESULT_BYTE);
    }

    public Response<Long> excad(String key, long version) {
        return excad(SafeEncoder.encode(key), version);
    }

    public Response<Long> excad(byte[] key, long version) {
        getClient("").sendCommand(ModuleCommand.EXCAD, key, toByteArray(version));
        return getResponse(BuilderFactory.LONG);
    }
}
