package com.aliyun.tair.tairdoc;

import java.util.List;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.jedis3.Jedis3BuilderFactory;
import com.aliyun.tair.tairdoc.params.JsonsetParams;
import com.aliyun.tair.tairdoc.params.JsongetParams;
import com.aliyun.tair.util.JoinParameters;
import io.valkey.BuilderFactory;
import io.valkey.JedisCluster;
import io.valkey.util.SafeEncoder;

import static io.valkey.Protocol.toByteArray;

public class TairDocCluster {
    private JedisCluster jc;

    public TairDocCluster(JedisCluster jc) {
        this.jc = jc;
    }

    public String jsonset(final String key, final String path, final String json) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONSET, key, path, json);
        return BuilderFactory.STRING.build(obj);
    }

    public String jsonset(final String key, final String path, final String json, final JsonsetParams params) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.JSONSET,
            params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(path), SafeEncoder.encode(json)));
        return BuilderFactory.STRING.build(obj);
    }

    public String jsonset(final byte[] key, final byte[] path, final byte[] json) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONSET, key, path, json);
        return BuilderFactory.STRING.build(obj);
    }

    public String jsonset(final byte[] key, final byte[] path, final byte[] json, final JsonsetParams params) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONSET, params.getByteParams(key, path, json));
        return BuilderFactory.STRING.build(obj);
    }

    public String jsonget(final String key) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONGET, key);
        return BuilderFactory.STRING.build(obj);
    }

    public String jsonget(final String key, final String path) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONGET, key, path);
        return BuilderFactory.STRING.build(obj);
    }

    public String jsonget(final String key, final String path, final JsongetParams params) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.JSONGET,
            params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(path)));
        return BuilderFactory.STRING.build(obj);
    }

    public byte[] jsonget(final byte[] key) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONGET, key);
        return Jedis3BuilderFactory.BYTE_ARRAY.build(obj);
    }

    public byte[] jsonget(final byte[] key, final byte[] path) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONGET, key, path);
        return Jedis3BuilderFactory.BYTE_ARRAY.build(obj);
    }

    public byte[] jsonget(final byte[] key, final byte[] path, final JsongetParams params) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONGET,
            params.getByteParams(key, path));
        return Jedis3BuilderFactory.BYTE_ARRAY.build(obj);
    }

    public List<String> jsonmget(String sampleKey, String... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.JSONMGET, args);
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<byte[]> jsonmget(byte[] sampleKey, byte[]... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.JSONMGET, args);
        return Jedis3BuilderFactory.BYTE_ARRAY_LIST.build(obj);
    }

    public Long jsondel(final String key) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONDEL, key);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsondel(final String key, final String path) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONDEL, key, path);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsondel(final byte[] key) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONDEL, key);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsondel(final byte[] key, final byte[] path) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONDEL, key, path);
        return BuilderFactory.LONG.build(obj);
    }

    public String jsontype(final String key) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONTYPE, key);
        return BuilderFactory.STRING.build(obj);
    }

    public String jsontype(final String key, final String path) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONTYPE, key, path);
        return BuilderFactory.STRING.build(obj);
    }

    public byte[] jsontype(final byte[] key) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONTYPE, key);
        return Jedis3BuilderFactory.BYTE_ARRAY.build(obj);
    }

    public byte[] jsontype(final byte[] key, final byte[] path) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONTYPE, key, path);
        return Jedis3BuilderFactory.BYTE_ARRAY.build(obj);
    }

    public Double jsonnumincrBy(final String key, final Double value) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.JSONNUMINCRBY, SafeEncoder.encode(key),
            toByteArray(value));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double jsonnumincrBy(final String key, final String path, final Double value) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.JSONNUMINCRBY, SafeEncoder.encode(key),
            SafeEncoder.encode(path), toByteArray(value));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double jsonnumincrBy(final byte[] key, final Double value) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONNUMINCRBY, key, toByteArray(value));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double jsonnumincrBy(final byte[] key, final byte[] path, final Double value) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONNUMINCRBY, key, path, toByteArray(value));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Long jsonstrAppend(final String key, final String json) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONSTRAPPEND, key, json);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonstrAppend(final String key, final String path, final String json) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONSTRAPPEND, key, path, json);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonstrAppend(final byte[] key, final byte[] json) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONSTRAPPEND, key, json);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonstrAppend(final byte[] key, final byte[] path, final byte[] json) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONSTRAPPEND, key, path, json);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonstrlen(final String key) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONSTRLEN, key);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonstrlen(final String key, final String path) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONSTRLEN, key, path);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonstrlen(final byte[] key) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONSTRLEN, key);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonstrlen(final byte[] key, final byte[] path) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONSTRLEN, key, path);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonarrAppend(String sampleKey, String... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.JSONARRAPPEND,
            JoinParameters.joinParameters(sampleKey, args));
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonarrAppend(byte[] sampleKey, byte[]... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.JSONARRAPPEND,
            JoinParameters.joinParameters(sampleKey, args));
        return BuilderFactory.LONG.build(obj);
    }

    public String jsonarrPop(final String key, final String path) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONARRPOP, key, path);
        return BuilderFactory.STRING.build(obj);
    }

    public String jsonarrPop(final String key, final String path, int index) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONARRPOP, key, path, String.valueOf(index));
        return BuilderFactory.STRING.build(obj);
    }

    public byte[] jsonarrPop(final byte[] key, final byte[] path) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONARRPOP, key, path);
        return Jedis3BuilderFactory.BYTE_ARRAY.build(obj);
    }

    public byte[] jsonarrPop(final byte[] key, final byte[] path, int index) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONARRPOP, key, path, toByteArray(index));
        return Jedis3BuilderFactory.BYTE_ARRAY.build(obj);
    }

    public Long jsonarrInsert(String sampleKey, String... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.JSONARRINSERT,
            JoinParameters.joinParameters(sampleKey, args));
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonarrInsert(byte[] sampleKey, byte[]... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.JSONARRINSERT,
            JoinParameters.joinParameters(sampleKey, args));
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonArrLen(final String key) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONARRLEN, key);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonArrlen(final String key, final String path) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONARRLEN, key, path);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonArrLen(final byte[] key) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONARRLEN, key);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonArrLen(final byte[] key, final byte[] path) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONARRLEN, key, path);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonarrTrim(final String key, final String path, final int start, final int stop) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONARRTRIM, key, path, String.valueOf(start),
            String.valueOf(stop));
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonarrTrim(final byte[] key, final byte[] path, final int start, final int stop) {
        Object obj = jc.sendCommand(key, ModuleCommand.JSONARRTRIM, key, path, toByteArray(start),
            toByteArray(stop));
        return BuilderFactory.LONG.build(obj);
    }
}
