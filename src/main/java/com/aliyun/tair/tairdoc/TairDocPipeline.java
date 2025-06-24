package com.aliyun.tair.tairdoc;

import java.util.List;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.jedis3.Jedis3BuilderFactory;
import com.aliyun.tair.tairdoc.params.JsonsetParams;
import com.aliyun.tair.tairdoc.params.JsongetParams;
import io.valkey.BuilderFactory;
import io.valkey.CommandArguments;
import io.valkey.CommandObject;
import io.valkey.Jedis;
import io.valkey.Pipeline;
import io.valkey.Response;
import io.valkey.util.SafeEncoder;

public class TairDocPipeline extends Pipeline {
    public TairDocPipeline(Jedis jedis) {
        super(jedis);
    }

    public Response<String> jsonset(final String key, final String path, final String json) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONSET)
            .key(key)
            .add(path)
            .add(json), BuilderFactory.STRING));
    }

    public Response<String> jsonset(final String key, final String path, final String json,
        final JsonsetParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONSET).addObjects(
            params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(path), SafeEncoder.encode(json))),
            BuilderFactory.STRING));
    }

    public Response<String> jsonset(final byte[] key, final byte[] path, final byte[] json) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONSET)
            .key(key)
            .add(path)
            .add(json), BuilderFactory.STRING));
    }

    public Response<String> jsonset(final byte[] key, final byte[] path, final byte[] json,
        final JsonsetParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONSET)
            .addObjects(params.getByteParams(key, path, json)), BuilderFactory.STRING));
    }

    public Response<String> jsonget(final String key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONGET)
            .key(key), BuilderFactory.STRING));
    }

    public Response<String> jsonget(final String key, final String path) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONGET)
            .key(key)
            .add(path), BuilderFactory.STRING));
    }

    public Response<String> jsonget(final String key, final String path, final JsongetParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONGET)
            .addObjects(params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(path))),
            BuilderFactory.STRING));
    }

    public Response<byte[]> jsonget(final byte[] key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONGET)
            .key(key), Jedis3BuilderFactory.BYTE_ARRAY));
    }

    public Response<byte[]> jsonget(final byte[] key, final byte[] path) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONGET)
            .key(key)
            .add(path), Jedis3BuilderFactory.BYTE_ARRAY));
    }

    public Response<byte[]> jsonget(final byte[] key, final byte[] path, final JsongetParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONGET)
            .addObjects(params.getByteParams(key, path)), Jedis3BuilderFactory.BYTE_ARRAY));
    }

    public Response<List<String>> jsonmget(String... args) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONMGET)
            .addObjects((Object[])args), BuilderFactory.STRING_LIST));
    }

    public Response<List<byte[]>> jsonmget(byte[]... args) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONMGET)
            .addObjects((Object[])args), Jedis3BuilderFactory.BYTE_ARRAY_LIST));
    }

    public Response<Long> jsondel(final String key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONDEL)
            .key(key), BuilderFactory.LONG));
    }

    public Response<Long> jsondel(final String key, final String path) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONDEL)
            .key(key)
            .add(path), BuilderFactory.LONG));
    }

    public Response<Long> jsondel(final byte[] key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONDEL)
            .key(key), BuilderFactory.LONG));
    }

    public Response<Long> jsondel(final byte[] key, final byte[] path) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONDEL)
            .key(key)
            .add(path), BuilderFactory.LONG));
    }

    public Response<String> jsontype(final String key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONTYPE)
            .key(key), BuilderFactory.STRING));
    }

    public Response<String> jsontype(final String key, final String path) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONTYPE)
            .key(key)
            .add(path), BuilderFactory.STRING));
    }

    public Response<byte[]> jsontype(final byte[] key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONTYPE)
            .key(key), Jedis3BuilderFactory.BYTE_ARRAY));
    }

    public Response<byte[]> jsontype(final byte[] key, final byte[] path) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONTYPE)
            .key(key)
            .add(path), Jedis3BuilderFactory.BYTE_ARRAY));
    }

    public Response<Double> jsonnumincrBy(final String key, final Double value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONNUMINCRBY)
            .key(key)
            .add(value), BuilderFactory.DOUBLE));
    }

    public Response<Double> jsonnumincrBy(final String key, final String path, final Double value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONNUMINCRBY)
            .key(key)
            .add(path)
            .add(value), BuilderFactory.DOUBLE));
    }

    public Response<Double> jsonnumincrBy(final byte[] key, final Double value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONNUMINCRBY)
            .key(key)
            .add(value), BuilderFactory.DOUBLE));
    }

    public Response<Double> jsonnumincrBy(final byte[] key, final byte[] path, final Double value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONNUMINCRBY)
            .key(key)
            .add(path)
            .add(value), BuilderFactory.DOUBLE));
    }

    public Response<Long> jsonstrAppend(final String key, final String json) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONSTRAPPEND)
            .key(key)
            .add(json), BuilderFactory.LONG));
    }

    public Response<Long> jsonstrAppend(final String key, final String path, final String json) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONSTRAPPEND)
            .key(key)
            .add(path)
            .add(json), BuilderFactory.LONG));
    }

    public Response<Long> jsonstrAppend(final byte[] key, final byte[] json) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONSTRAPPEND)
            .key(key)
            .add(json),
            BuilderFactory.LONG));
    }

    public Response<Long> jsonstrAppend(final byte[] key, final byte[] path, final byte[] json) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONSTRAPPEND)
            .key(key)
            .add(path)
            .add(json), BuilderFactory.LONG));
    }

    public Response<Long> jsonstrlen(final String key) {
        return appendCommand(
            new CommandObject<>(new CommandArguments(ModuleCommand.JSONSTRLEN)
                .key(key), BuilderFactory.LONG));
    }

    public Response<Long> jsonstrlen(final String key, final String path) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONSTRLEN)
            .key(key)
            .add(path),
            BuilderFactory.LONG));
    }

    public Response<Long> jsonstrlen(final byte[] key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONSTRLEN)
                .key(key), BuilderFactory.LONG));
    }

    public Response<Long> jsonstrlen(final byte[] key, final byte[] path) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONSTRLEN)
            .key(key)
            .add(path), BuilderFactory.LONG));
    }

    public Response<Long> jsonarrAppend(String... args) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONARRAPPEND).addObjects((Object[])args),
                BuilderFactory.LONG));
    }

    public Response<Long> jsonarrAppend(byte[]... args) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONARRAPPEND)
            .addObjects((Object[])args), BuilderFactory.LONG));
    }

    public Response<String> jsonarrPop(final String key, final String path) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONARRPOP)
            .key(key)
            .add(path), BuilderFactory.STRING));
    }

    public Response<String> jsonarrPop(final String key, final String path, int index) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONARRPOP)
            .key(key)
            .add(path)
            .add(index), BuilderFactory.STRING));
    }

    public Response<byte[]> jsonarrPop(final byte[] key, final byte[] path) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONARRPOP)
            .key(key)
            .add(path), Jedis3BuilderFactory.BYTE_ARRAY));
    }

    public Response<byte[]> jsonarrPop(final byte[] key, final byte[] path, int index) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONARRPOP)
            .key(key)
            .add(path)
            .add(index), Jedis3BuilderFactory.BYTE_ARRAY));
    }

    public Response<Long> jsonarrInsert(String... args) {
        return appendCommand(
            new CommandObject<>(new CommandArguments(ModuleCommand.JSONARRINSERT).addObjects((Object[])args),
                BuilderFactory.LONG));
    }

    public Response<Long> jsonarrInsert(byte[]... args) {
        return appendCommand(
            new CommandObject<>(new CommandArguments(ModuleCommand.JSONARRINSERT).addObjects((Object[])args),
                BuilderFactory.LONG));
    }

    public Response<Long> jsonArrLen(final String key) {
        return appendCommand(
            new CommandObject<>(new CommandArguments(ModuleCommand.JSONARRLEN).key(key), BuilderFactory.LONG));
    }

    public Response<Long> jsonArrLen(final String key, final String path) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONARRLEN).key(key).add(path),
            BuilderFactory.LONG));
    }

    public Response<Long> jsonArrLen(final byte[] key) {
        return appendCommand(
            new CommandObject<>(new CommandArguments(ModuleCommand.JSONARRLEN).key(key), BuilderFactory.LONG));
    }

    public Response<Long> jsonArrLen(final byte[] key, final byte[] path) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONARRLEN).key(key).add(path),
            BuilderFactory.LONG));
    }

    public Response<Long> jsonarrTrim(final String key, final String path, final int start, final int stop) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONARRTRIM).key(key).add(path)
            .add(start).add(stop), BuilderFactory.LONG));
    }

    public Response<Long> jsonarrTrim(final byte[] key, final byte[] path, final int start, final int stop) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.JSONARRTRIM).key(key).add(path)
            .add(start).add(stop), BuilderFactory.LONG));
    }

    public Response<String> jsonMerge(final String key, final String path, final String value) {
        getClient("").sendCommand(ModuleCommand.JSONMERGE, key, path, value);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> jsonMerge(final byte[] key, final byte[] path, final byte[] value) {
        getClient("").sendCommand(ModuleCommand.JSONMERGE, key, path, value);
        return getResponse(BuilderFactory.STRING);
    }
}
