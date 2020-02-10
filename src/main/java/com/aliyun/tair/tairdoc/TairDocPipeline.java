package com.aliyun.tair.tairdoc;

import java.util.List;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairdoc.params.JsonsetParams;
import com.aliyun.tair.tairdoc.params.JsongetParams;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.util.SafeEncoder;

import static redis.clients.jedis.Protocol.toByteArray;

public class TairDocPipeline extends Pipeline {
    public Response<String> jsonset(final String key, final String path, final String json) {
        getClient("").sendCommand(ModuleCommand.JSONSET, key, path, json);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> jsonset(final String key, final String path, final String json, final JsonsetParams params) {
        getClient("").sendCommand(ModuleCommand.JSONSET,
            params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(path), SafeEncoder.encode(json)));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> jsonset(final byte[] key, final byte[] path, final byte[] json) {
        getClient("").sendCommand(ModuleCommand.JSONSET, key, path, json);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> jsonset(final byte[] key, final byte[] path, final byte[] json, final JsonsetParams params) {
        getClient("").sendCommand(ModuleCommand.JSONSET, params.getByteParams(key, path, json));
        return getResponse(BuilderFactory.STRING);
    }
    
    public Response<String> jsonget(final String key) {
        getClient("").sendCommand(ModuleCommand.JSONGET, key);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> jsonget(final String key, final String path) {
        getClient("").sendCommand(ModuleCommand.JSONGET, key, path);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> jsonget(final String key, final String path, final JsongetParams params) {
        getClient("").sendCommand(ModuleCommand.JSONGET,
            params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(path)));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<byte[]> jsonget(final byte[] key) {
        getClient("").sendCommand(ModuleCommand.JSONGET, key);
        return getResponse(BuilderFactory.BYTE_ARRAY);
    }

    public Response<byte[]> jsonget(final byte[] key, final byte[] path) {
        getClient("").sendCommand(ModuleCommand.JSONGET, key, path);
        return getResponse(BuilderFactory.BYTE_ARRAY);
    }

    public Response<byte[]> jsonget(final byte[] key, final byte[] path, final JsongetParams params) {
        getClient("").sendCommand(ModuleCommand.JSONGET,
            params.getByteParams(key, path));
        return getResponse(BuilderFactory.BYTE_ARRAY);
    }
    
    public Response<List<String>> jsonmget(String... args) {
        getClient("").sendCommand(ModuleCommand.JSONMGET, args);
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<byte[]>> jsonmget(byte[]... args) {
        getClient("").sendCommand(ModuleCommand.JSONMGET, args);
        return getResponse(BuilderFactory.BYTE_ARRAY_LIST);
    }
    
    public Response<Long> jsondel(final String key) {
        getClient("").sendCommand(ModuleCommand.JSONDEL, key);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> jsondel(final String key, final String path) {
        getClient("").sendCommand(ModuleCommand.JSONDEL, key, path);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> jsondel(final byte[] key) {
        getClient("").sendCommand(ModuleCommand.JSONDEL, key);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> jsondel(final byte[] key, final byte[] path) {
        getClient("").sendCommand(ModuleCommand.JSONDEL, key, path);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<String> jsontype(final String key) {
        getClient("").sendCommand(ModuleCommand.JSONTYPE, key);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> jsontype(final String key, final String path) {
        getClient("").sendCommand(ModuleCommand.JSONTYPE, key, path);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<byte[]> jsontype(final byte[] key) {
        getClient("").sendCommand(ModuleCommand.JSONTYPE, key);
        return getResponse(BuilderFactory.BYTE_ARRAY);
    }

    public Response<byte[]> jsontype(final byte[] key, final byte[] path) {
        getClient("").sendCommand(ModuleCommand.JSONTYPE, key, path);
        return getResponse(BuilderFactory.BYTE_ARRAY);
    }

    public Response<Double> jsonnumincrBy(final String key, final Double value) {
        getClient("").sendCommand(ModuleCommand.JSONNUMINCRBY, SafeEncoder.encode(key), toByteArray(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> jsonnumincrBy(final String key, final String path, final Double value) {
        getClient("").sendCommand(ModuleCommand.JSONNUMINCRBY, SafeEncoder.encode(key),
            SafeEncoder.encode(path), toByteArray(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> jsonnumincrBy(final byte[] key, final Double value) {
        getClient("").sendCommand(ModuleCommand.JSONNUMINCRBY, key, toByteArray(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> jsonnumincrBy(final byte[] key, final byte[] path, final Double value) {
        getClient("").sendCommand(ModuleCommand.JSONNUMINCRBY, key, path, toByteArray(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Long> jsonstrAppend(final String key, final String json) {
        getClient("").sendCommand(ModuleCommand.JSONSTRAPPEND, key, json);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> jsonstrAppend(final String key, final String path, final String json) {
        getClient("").sendCommand(ModuleCommand.JSONSTRAPPEND, key, path, json);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> jsonstrAppend(final byte[] key, final byte[] json) {
        getClient("").sendCommand(ModuleCommand.JSONSTRAPPEND, key, json);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> jsonstrAppend(final byte[] key, final byte[] path, final byte[] json) {
        getClient("").sendCommand(ModuleCommand.JSONSTRAPPEND, key, path, json);
        return getResponse(BuilderFactory.LONG);
    }
    
    public Response<Long> jsonstrlen(final String key) {
        getClient("").sendCommand(ModuleCommand.JSONSTRLEN, key);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> jsonstrlen(final String key, final String path) {
        getClient("").sendCommand(ModuleCommand.JSONSTRLEN, key, path);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> jsonstrlen(final byte[] key) {
        getClient("").sendCommand(ModuleCommand.JSONSTRLEN, key);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> jsonstrlen(final byte[] key, final byte[] path) {
        getClient("").sendCommand(ModuleCommand.JSONSTRLEN, key, path);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> jsonarrAppend(String... args) {
        getClient("").sendCommand(ModuleCommand.JSONARRAPPEND, args);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> jsonarrAppend(byte[]... args) {
        getClient("").sendCommand(ModuleCommand.JSONARRAPPEND, args);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<String> jsonarrPop(final String key, final String path) {
        getClient("").sendCommand(ModuleCommand.JSONARRPOP, key, path);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> jsonarrPop(final String key, final String path, int index) {
        getClient("").sendCommand(ModuleCommand.JSONARRPOP, key, path, String.valueOf(index));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<byte[]> jsonarrPop(final byte[] key, final byte[] path) {
        getClient("").sendCommand(ModuleCommand.JSONARRPOP, key, path);
        return getResponse(BuilderFactory.BYTE_ARRAY);
    }

    public Response<byte[]> jsonarrPop(final byte[] key, final byte[] path, int index) {
        getClient("").sendCommand(ModuleCommand.JSONARRPOP, key, path, toByteArray(index));
        return getResponse(BuilderFactory.BYTE_ARRAY);
    }
    
    public Response<Long> jsonarrInsert(String... args) {
        getClient("").sendCommand(ModuleCommand.JSONARRINSERT, args);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> jsonarrInsert(byte[]... args) {
        getClient("").sendCommand(ModuleCommand.JSONARRINSERT, args);
        return getResponse(BuilderFactory.LONG);
    }
    
    public Response<Long> jsonArrlen(final String key) {
        getClient("").sendCommand(ModuleCommand.JSONARRLEN, key);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> jsonArrlen(final String key, final String path) {
        getClient("").sendCommand(ModuleCommand.JSONARRLEN, key, path);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> jsonarrLen(final byte[] key) {
        getClient("").sendCommand(ModuleCommand.JSONARRLEN, key);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> jsonarrLen(final byte[] key, final byte[] path) {
        getClient("").sendCommand(ModuleCommand.JSONARRLEN, key, path);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> jsonarrTrim(final String key, final String path, final int start, final int stop) {
        getClient("").sendCommand(ModuleCommand.JSONARRTRIM, key, path, String.valueOf(start),
            String.valueOf(stop));
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> jsonarrTrim(final byte[] key, final byte[] path, final int start, final int stop) {
        getClient("").sendCommand(ModuleCommand.JSONARRTRIM, key, path, toByteArray(start),
            toByteArray(stop));
        return getResponse(BuilderFactory.LONG);
    }
}
