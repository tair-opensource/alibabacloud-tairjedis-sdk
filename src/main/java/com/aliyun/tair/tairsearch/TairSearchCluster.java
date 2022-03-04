package com.aliyun.tair.tairsearch;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairsearch.params.TFTAddDocParams;
import com.aliyun.tair.tairsearch.params.TFTDelDocParams;
import com.aliyun.tair.tairsearch.params.TFTScanParams;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.ScanResult;
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

    public String tftcreateindex(String key, String request) {
        return tftcreateindex(SafeEncoder.encode(key), SafeEncoder.encode(request));
    }

    public String tftcreateindex(byte[] key, byte[] request) {
        Object obj = jc.sendCommand(key, ModuleCommand.TFTCREATEINDEX, key, request);
        return BuilderFactory.STRING.build(obj);
    }

    public String tftupdateindex(String key, String request) {
        return tftupdateindex(SafeEncoder.encode(key), SafeEncoder.encode(request));
    }

    public String tftupdateindex(byte[] key, byte[] request) {
        Object obj = jc.sendCommand(key, ModuleCommand.TFTUPDATEINDEX, key, request);
        return BuilderFactory.STRING.build(obj);
    }

    public String tftgetindexmappings(String key) {
        return tftgetindexmappings(SafeEncoder.encode(key));
    }

    public String tftgetindexmappings(byte[] key) {
        Object obj = jc.sendCommand(key, ModuleCommand.TFTGETINDEX, key, SafeEncoder.encode("mappings"));
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

    public String tftmadddoc(String key, Map<String /* docId */, String /* docContent */> docs) {
        TFTAddDocParams params = new TFTAddDocParams();
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.TFTMADDDOC, params.getByteParams(key, docs));
        return BuilderFactory.STRING.build(obj);
    }

    public String tftmadddoc(byte[] key, Map<byte[] /* docId */, byte[] /* docContent */> docs) {
        TFTAddDocParams params = new TFTAddDocParams();
        Object obj = jc.sendCommand(key, ModuleCommand.TFTMADDDOC, params.getByteParams(key, docs));
        return BuilderFactory.STRING.build(obj);
    }

    public String tftupdatedoc(String key, String docId, String docContent) {
        return tftupdatedoc(SafeEncoder.encode(key), SafeEncoder.encode(docId), SafeEncoder.encode(docContent));
    }

    public String tftupdatedoc(byte[] key, byte[] docId, byte[] docContent) {
        Object obj = jc.sendCommand(key, ModuleCommand.TFTUPDATEDOC, key, docId, docContent);
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

    public String tftdeldoc(String key,  String... docId) {
        TFTDelDocParams params = new TFTDelDocParams();
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.TFTDELDOC, params.getByteParams(key, docId));
        return BuilderFactory.STRING.build(obj);
    }

    public String tftdeldoc(byte[] key, byte[]... docId) {
        TFTDelDocParams params = new TFTDelDocParams();
        Object obj = jc.sendCommand(key, ModuleCommand.TFTDELDOC, params.getByteParams(key, docId));
        return BuilderFactory.STRING.build(obj);
    }

    public String tftdelall(String index) {
        return tftdelall(SafeEncoder.encode(index));
    }

    public String tftdelall(byte[] key) {
        Object obj = jc.sendCommand(key, ModuleCommand.TFTDELALL, key);
        return BuilderFactory.STRING.build(obj);
    }

    public String tftsearch(String key, String request) {
        return tftsearch(SafeEncoder.encode(key), SafeEncoder.encode(request));
    }

    public String tftsearch(byte[] key, byte[] request) {
        Object obj = jc.sendCommand(key, ModuleCommand.TFTSEARCH, key, request);
        return BuilderFactory.STRING.build(obj);
    }

    public String tftsearch(String key, String request, boolean use_cache) {
        return tftsearch(SafeEncoder.encode(key), SafeEncoder.encode(request), use_cache);
    }

    public String tftsearch(byte[] key, byte[] request, boolean use_cache) {
        Object obj;
        if (use_cache) {
            obj = jc.sendCommand(key, ModuleCommand.TFTSEARCH, key, request, SafeEncoder.encode("use_cache"));
        } else {
            obj = jc.sendCommand(key, ModuleCommand.TFTSEARCH, key, request);
        }
        return BuilderFactory.STRING.build(obj);
    }

    public Long tftexists(String key, String docId) {
        return tftexists(SafeEncoder.encode(key), SafeEncoder.encode(docId));
    }

    public Long tftexists(byte[] key, byte[] docId) {
        Object obj = jc.sendCommand(key, ModuleCommand.TFTEXISTS, key, docId);
        return BuilderFactory.LONG.build(obj);
    }

    public Long tftdocnum(String key) {
        return tftdocnum(SafeEncoder.encode(key));
    }

    public Long tftdocnum(byte[] key) {
        Object obj = jc.sendCommand(key, ModuleCommand.TFTDOCNUM, key);
        return BuilderFactory.LONG.build(obj);
    }

    public ScanResult<String> tftscandocid(String key, String cursor) {
        Object obj = jc.sendCommand(key, ModuleCommand.TFTSCANDOCID, key, cursor);
        List<Object> result = (List<Object>)obj;
        String newcursor = new String((byte[]) result.get(0));
        List<String> results = new ArrayList<>();
        List<byte[]> rawResults = (List<byte[]>) result.get(1);
        for (byte[] bs : rawResults) {
            results.add(SafeEncoder.encode(bs));
        }
        return new ScanResult<>(newcursor, results);
    }

    public ScanResult<byte[]> tftscandocid(byte[] key, byte[] cursor) {
        Object obj = jc.sendCommand(key, ModuleCommand.TFTSCANDOCID, key, cursor);
        List<Object> result = (List<Object>)obj;
        byte[] newcursor = (byte[]) result.get(0);
        List<byte[]> rawResults = (List<byte[]>) result.get(1);
        return new ScanResult<>(newcursor, rawResults);
    }

    public ScanResult<String> tftscandocid(String key, String cursor, final TFTScanParams params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(cursor));
        args.addAll(params.getParams());
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.TFTSCANDOCID, args.toArray(new byte[args.size()][]));
        List<Object> result = (List<Object>)obj;
        String newcursor = new String((byte[]) result.get(0));
        List<String> results = new ArrayList<>();
        List<byte[]> rawResults = (List<byte[]>) result.get(1);
        for (byte[] bs : rawResults) {
            results.add(SafeEncoder.encode(bs));
        }
        return new ScanResult<>(newcursor, results);
    }

    public ScanResult<byte[]> tftscandocid(byte[] key, byte[] cursor, final TFTScanParams params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(key);
        args.add(cursor);
        args.addAll(params.getParams());
        Object obj = jc.sendCommand(key, ModuleCommand.TFTSCANDOCID, args.toArray(new byte[args.size()][]));
        List<Object> result = (List<Object>)obj;
        byte[] newcursor = (byte[]) result.get(0);
        List<byte[]> rawResults = (List<byte[]>) result.get(1);
        return new ScanResult<>(newcursor, rawResults);
    }
}
