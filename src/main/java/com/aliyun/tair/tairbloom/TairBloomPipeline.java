package com.aliyun.tair.tairbloom;

import java.util.List;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairbloom.factory.BloomBuilderFactory;
import com.aliyun.tair.tairbloom.params.BfinsertParams;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.CommandArguments;
import redis.clients.jedis.CommandObject;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.util.SafeEncoder;

public class TairBloomPipeline extends Pipeline {

    public TairBloomPipeline(Jedis jedis) {
        super(jedis);
    }

    public Response<String> bfreserve(String key, long initCapacity, double errorRate) {
        return bfreserve(SafeEncoder.encode(key), initCapacity, errorRate);
    }

    public Response<String> bfreserve(byte[] key, long initCapacity, double errorRate) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.BFRESERVE)
            .key(key)
            .add(errorRate)
            .add(initCapacity), BuilderFactory.STRING));
    }

    public Response<Boolean> bfadd(String key, String item) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.BFADD)
            .key(key)
            .add(item), BuilderFactory.BOOLEAN));
    }

    public Response<Boolean> bfadd(byte[] key, byte[] item) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.BFADD)
            .key(key)
            .add(item), BuilderFactory.BOOLEAN));
    }

    public Response<Boolean[]> bfmadd(String key, String... items) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.BFMADD)
            .key(key)
            .addObjects(items), BloomBuilderFactory.BFMADD_RESULT_BOOLEAN_LIST));
    }

    public Response<Boolean[]> bfmadd(byte[] key, byte[]... items) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.BFMADD)
            .key(key)
            .addObjects(items), BloomBuilderFactory.BFMADD_RESULT_BOOLEAN_LIST));
    }

    public Response<Boolean> bfexists(String key, String value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.BFEXISTS)
            .key(key)
            .add(value), BuilderFactory.BOOLEAN));
    }

    public Response<Boolean> bfexists(byte[] key, byte[] value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.BFEXISTS)
            .key(key)
            .add(value), BuilderFactory.BOOLEAN));
    }

    public Response<Boolean[]> bfmexists(String key, String... value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.BFMEXISTS)
            .key(key)
            .addObjects(value), BloomBuilderFactory.BFMADD_RESULT_BOOLEAN_LIST));
    }

    public Response<Boolean[]> bfmexists(byte[] key, byte[]... value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.BFMEXISTS)
            .key(key)
            .addObjects(value), BloomBuilderFactory.BFMADD_RESULT_BOOLEAN_LIST));
    }

    public Response<Boolean[]> bfinsert(String key, BfinsertParams params, String... items) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.BFINSERT)
            .addObjects(params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encodeMany(items))),
            BloomBuilderFactory.BFINSERT_RESULT_BOOLEAN_LIST));
    }

    public Response<Boolean[]> bfinsert(byte[] key, BfinsertParams params, byte[]... items) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.BFINSERT)
            .addObjects(params.getByteParams(key, items)),
            BloomBuilderFactory.BFINSERT_RESULT_BOOLEAN_LIST));
    }

    public Response<List<String>> bfdebug(String key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.BFDEBUG)
            .key(key), BuilderFactory.STRING_LIST));
    }

    public Response<List<String>> bfdebug(byte[] key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.BFDEBUG)
            .key(key), BuilderFactory.STRING_LIST));
    }
}
