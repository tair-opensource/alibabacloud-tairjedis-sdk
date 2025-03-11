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
import io.valkey.BuilderFactory;
import io.valkey.CommandArguments;
import io.valkey.CommandObject;
import io.valkey.Jedis;
import io.valkey.Pipeline;
import io.valkey.Response;
import io.valkey.util.SafeEncoder;

import static io.valkey.Protocol.toByteArray;

public class TairStringPipeline extends Pipeline {
    public TairStringPipeline(Jedis jedis) {
        super(jedis);
    }

    public Response<Long> cas(String key, String oldvalue, String newvalue) {
        return cas(SafeEncoder.encode(key), SafeEncoder.encode(oldvalue), SafeEncoder.encode(newvalue));
    }

    public Response<Long> cas(byte[] key, byte[] oldvalue, byte[] newvalue) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CAS)
            .add(key)
            .add(oldvalue)
            .add(newvalue), BuilderFactory.LONG));
    }

    public Response<Long> cas(String key, String oldvalue, String newvalue, CasParams params) {
        return cas(SafeEncoder.encode(key), SafeEncoder.encode(oldvalue), SafeEncoder.encode(newvalue), params);
    }

    public Response<Long> cas(byte[] key, byte[] oldvalue, byte[] newvalue, CasParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CAS)
            .addObjects(params.getByteParams(key, oldvalue, newvalue)), BuilderFactory.LONG));
    }

    public Response<Long> cad(String key, String value) {
        return cad(SafeEncoder.encode(key), SafeEncoder.encode(value));
    }

    public Response<Long> cad(byte[] key, byte[] value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CAD)
            .add(key)
            .add(value), BuilderFactory.LONG));
    }

    public Response<String> exset(String key, String value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXSET)
            .add(key)
            .add(value), BuilderFactory.STRING));
    }

    public Response<String> exset(byte[] key, byte[] value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXSET)
            .add(key)
            .add(value), BuilderFactory.STRING));
    }

    public Response<String> exset(String key, String value, ExsetParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXSET)
            .addObjects(params.getByteParams(key, value)), BuilderFactory.STRING));
    }

    public Response<String> exset(byte[] key, byte[] value, ExsetParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXSET)
            .addObjects(params.getByteParams(key, value)), BuilderFactory.STRING));
    }

    public Response<Long> exsetVersion(String key, String value, ExsetParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXSET)
            .addObjects(params.getByteParams(key, value, "WITHVERSION")), BuilderFactory.LONG));
    }

    public Response<Long> exsetVersion(byte[] key, byte[] value, ExsetParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXSET)
            .addObjects(params.getByteParams(key, value, "WITHVERSION".getBytes())), BuilderFactory.LONG));
    }

    public Response<ExgetResult<String>> exget(String key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXGET)
            .add(key), StringBuilderFactory.EXGET_RESULT_STRING));
    }

    public Response<ExgetResult<byte[]>> exget(byte[] key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXGET)
            .add(key), StringBuilderFactory.EXGET_RESULT_BYTE));
    }

    public Response<ExgetResult<String>> exgetFlags(String key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXGET)
            .add(key).add("WITHFLAGS"), StringBuilderFactory.EXGET_RESULT_STRING));
    }

    public Response<ExgetResult<byte[]>> exgetFlags(byte[] key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXGET)
            .add(key).add("WITHFLAGS"), StringBuilderFactory.EXGET_RESULT_BYTE));
    }

    public Response<Long> exsetver(String key, long version) {
        return exsetver(SafeEncoder.encode(key), version);
    }

    public Response<Long> exsetver(byte[] key, long version) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXSETVER)
            .add(key)
            .add(version), BuilderFactory.LONG));
    }

    public Response<Long> exincrBy(String key, long incr) {
        return exincrBy(SafeEncoder.encode(key), incr);
    }

    public Response<Long> exincrBy(byte[] key, long incr) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXINCRBY)
            .add(key)
            .add(incr), BuilderFactory.LONG));
    }

    public Response<Long> exincrBy(String key, long incr, ExincrbyParams params) {
        return exincrBy(SafeEncoder.encode(key), incr, params);
    }

    public Response<Long> exincrBy(byte[] key, long incr, ExincrbyParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXINCRBY)
            .addObjects(params.getByteParams(key, toByteArray(incr))), BuilderFactory.LONG));
    }

    public Response<ExincrbyVersionResult> exincrByVersion(String key, long incr, ExincrbyParams params) {
        return exincrByVersion(SafeEncoder.encode(key), incr, params);
    }

    public Response<ExincrbyVersionResult> exincrByVersion(byte[] key, long incr, ExincrbyParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXINCRBY)
            .addObjects(params.getByteParams(key, toByteArray(incr), "WITHVERSION".getBytes())),
            StringBuilderFactory.EXINCRBY_VERSION_RESULT_STRING));
    }

    public Response<Double> exincrByFloat(String key, Double incr) {
        return exincrByFloat(SafeEncoder.encode(key), incr);
    }

    public Response<Double> exincrByFloat(byte[] key, Double incr) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXINCRBYFLOAT)
            .add(key)
            .add(incr), BuilderFactory.DOUBLE));
    }

    public Response<Double> exincrByFloat(String key, Double incr, ExincrbyFloatParams params) {
        return exincrByFloat(SafeEncoder.encode(key), incr, params);
    }

    public Response<Double> exincrByFloat(byte[] key, Double incr, ExincrbyFloatParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXINCRBYFLOAT)
            .addObjects(params.getByteParams(key, toByteArray(incr))), BuilderFactory.DOUBLE));
    }

    public Response<ExcasResult<String>> excas(String key, String value, long version) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXCAS)
            .add(key)
            .add(value)
            .add(version), StringBuilderFactory.EXCAS_RESULT_STRING));
    }

    public Response<ExcasResult<byte[]>> excas(byte[] key, byte[] value, long version) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXCAS)
            .add(key)
            .add(value)
            .add(version), StringBuilderFactory.EXCAS_RESULT_BYTE));
    }

    public Response<Long> excad(String key, long version) {
        return excad(SafeEncoder.encode(key), version);
    }

    public Response<Long> excad(byte[] key, long version) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXCAD)
            .add(key)
            .add(version), BuilderFactory.LONG));
    }

    public Response<Long> exappend(String key, String value, String nxxx, String verabs, long version) {
        return exappend(SafeEncoder.encode(key), SafeEncoder.encode(value), nxxx, verabs, version);
    }

    public Response<Long> exappend(byte[] key, byte[] value, String nxxx, String verabs, long version) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXAPPEND)
            .add(key)
            .add(value)
            .add(nxxx)
            .add(verabs)
            .add(version), BuilderFactory.LONG));
    }

    public Response<Long> exprepend(String key, String value, String nxxx, String verabs, long version) {
        return exprepend(SafeEncoder.encode(key), SafeEncoder.encode(value), nxxx, verabs, version);
    }

    public Response<Long> exprepend(byte[] key, byte[] value, String nxxx, String verabs, long version) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXPREPEND)
            .add(key)
            .add(value)
            .add(nxxx)
            .add(verabs)
            .add(version), BuilderFactory.LONG));
    }

    public Response<ExgetResult<String>> exgae(String key, String expxwithat, long time) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXGAE)
            .add(key)
            .add(expxwithat)
            .add(time), StringBuilderFactory.EXGET_RESULT_STRING));
    }

    public Response<ExgetResult<byte[]>> exgae(byte[] key, String expxwithat, long time) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXGAE)
            .add(key)
            .add(expxwithat)
            .add(time), StringBuilderFactory.EXGET_RESULT_BYTE));
    }
}
