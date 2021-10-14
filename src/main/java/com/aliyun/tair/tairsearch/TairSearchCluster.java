package com.aliyun.tair.tairsearch;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairsearch.params.TFTDelDocParams;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.util.SafeEncoder;

public class TairSearchCluster {
    private JedisCluster jc;

    public TairSearchCluster(JedisCluster jc) {
        this.jc = jc;
    }

    public String tftmappingindex(String key, String request) {
        return tftmappingindex(SafeEncoder.encode(key), SafeEncoder.encode(request));
    }

    public String tftmappingindex(byte[] key, byte[] request) {
        Object obj = jc.sendCommand(key, ModuleCommand.TFTMAPPINGINDEX, key, request);
        return BuilderFactory.STRING.build(obj);
    }

    public String tftadddoc(String key, String request) {
        return tftadddoc(SafeEncoder.encode(key), SafeEncoder.encode(request));
    }

    public String tftadddoc(byte[] key, byte[] request) {
        Object obj = jc.sendCommand(key, ModuleCommand.TFTADDDOC, key, request);
        return BuilderFactory.STRING.build(obj);
    }

    public String tftadddoc(String key, String request, String docId) {
        return tftadddoc(SafeEncoder.encode(key), SafeEncoder.encode(request), SafeEncoder.encode(docId));
    }

    public String tftadddoc(byte[] key, byte[] request, byte[] docId) {
        Object obj = jc.sendCommand(key, ModuleCommand.TFTADDDOC, key, request, SafeEncoder.encode("WITH_ID"), docId);
        return BuilderFactory.STRING.build(obj);
    }

    public String tftgetdoc(String key, String docId) {
        return tftgetdoc(SafeEncoder.encode(key), SafeEncoder.encode(docId));
    }

    public String tftgetdoc(byte[] key, byte[] docId) {
        Object obj = jc.sendCommand(key, ModuleCommand.TFTGETDOC, key, docId);
        return BuilderFactory.STRING.build(obj);
    }

    public String tftgetdoc(String key, String docId, String request) {
        return tftgetdoc(SafeEncoder.encode(key), SafeEncoder.encode(docId), SafeEncoder.encode(request));
    }

    public String tftgetdoc(byte[] key, byte[] docId, byte[] request) {
        Object obj = jc.sendCommand(key, ModuleCommand.TFTGETDOC, key, docId, request);
        return BuilderFactory.STRING.build(obj);
    }

    public Long tftdeldoc(String key,  String... docId) {
        TFTDelDocParams params = new TFTDelDocParams();
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.TFTDELDOC, params.getByteParams(key, docId));
        return BuilderFactory.LONG.build(obj);
    }

    public Long tftdeldoc(byte[] key, byte[]... docId) {
        TFTDelDocParams params = new TFTDelDocParams();
        Object obj = jc.sendCommand(key, ModuleCommand.TFTDELDOC, params.getByteParams(key, docId));
        return BuilderFactory.LONG.build(obj);
    }

    public String tftgetindexmappings(String key) {
        return tftgetindexmappings(SafeEncoder.encode(key));
    }

    public String tftgetindexmappings(byte[] key) {
        Object obj = jc.sendCommand(key, ModuleCommand.TFTGETINDEX, key, SafeEncoder.encode("mappings"));
        return BuilderFactory.STRING.build(obj);
    }

    public String tftgetindexsettings(String key) {
        return tftgetindexsettings(SafeEncoder.encode(key));
    }

    public String tftgetindexsettings(byte[] key) {
        Object obj = jc.sendCommand(key, ModuleCommand.TFTGETINDEX, key, SafeEncoder.encode("settings"));
        return BuilderFactory.STRING.build(obj);
    }

    public String tftsearch(String key, String request) {
        return tftsearch(SafeEncoder.encode(key), SafeEncoder.encode(request));
    }

    public String tftsearch(byte[] key, byte[] request) {
        Object obj = jc.sendCommand(key, ModuleCommand.TFTSEARCH, key, request);
        return BuilderFactory.STRING.build(obj);
    }
}
