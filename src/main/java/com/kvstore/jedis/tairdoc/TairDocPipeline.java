package com.kvstore.jedis.tairdoc;

import java.util.List;

import com.kvstore.jedis.ModuleCommand;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

/**
 * @author bodong.ybd
 * @date 2019/12/11
 */
public class TairDocPipeline extends Pipeline {
    public Response<String> jsonset(String... args) {
        getClient("").sendCommand(ModuleCommand.JSONSET, args);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> jsonset(byte[]... args) {
        getClient("").sendCommand(ModuleCommand.JSONSET, args);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> jsonget(String... args) {
        getClient("").sendCommand(ModuleCommand.JSONGET, args);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<byte[]> jsonget(byte[]... args) {
        getClient("").sendCommand(ModuleCommand.JSONGET, args);
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

    public Response<Long> jsondel(String... args) {
        getClient("").sendCommand(ModuleCommand.JSONDEL, args);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> jsondel(byte[]... args) {
        getClient("").sendCommand(ModuleCommand.JSONDEL, args);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<String> jsontype(String... args) {
        getClient("").sendCommand(ModuleCommand.JSONTYPE, args);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<byte[]> jsontype(byte[]... args) {
        getClient("").sendCommand(ModuleCommand.JSONTYPE, args);
        return getResponse(BuilderFactory.BYTE_ARRAY);
    }

    public Response<Double> jsonnumincrBy(String... args) {
        getClient("").sendCommand(ModuleCommand.JSONNUMINCRBY, args);
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> jsonnumincrBy(byte[]... args) {
        getClient("").sendCommand(ModuleCommand.JSONNUMINCRBY, args);
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Long> jsonstrAppend(String... args) {
        getClient("").sendCommand(ModuleCommand.JSONSTRAPPEND, args);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> jsonstrAppend(byte[]... args) {
        getClient("").sendCommand(ModuleCommand.JSONSTRAPPEND, args);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> jsonstrlen(String... args) {
        getClient("").sendCommand(ModuleCommand.JSONSTRLEN, args);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> jsonstrlen(byte[]... args) {
        getClient("").sendCommand(ModuleCommand.JSONSTRLEN, args);
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

    public Response<String> jsonarrPop(String... args) {
        getClient("").sendCommand(ModuleCommand.JSONARRPOP, args);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<byte[]> jsonarrPop(byte[]... args) {
        getClient("").sendCommand(ModuleCommand.JSONARRPOP, args);
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

    public Response<Long> jsonArrlenCommand(String... args) {
        getClient("").sendCommand(ModuleCommand.JSONARRLEN, args);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> jsonarrLen(byte[]... args) {
        getClient("").sendCommand(ModuleCommand.JSONARRLEN, args);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> jsonarrTrim(String... args) {
        getClient("").sendCommand(ModuleCommand.JSONARRTRIM, args);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> jsonarrTrim(byte[]... args) {
        getClient("").sendCommand(ModuleCommand.JSONARRTRIM, args);
        return getResponse(BuilderFactory.LONG);
    }
}
