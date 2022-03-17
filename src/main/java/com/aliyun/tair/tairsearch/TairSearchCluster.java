package com.aliyun.tair.tairsearch;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairsearch.params.TFTAddDocParams;
import com.aliyun.tair.tairsearch.params.TFTDelDocParams;
import com.aliyun.tair.tairsearch.params.TFTScanParams;
import com.aliyun.tair.util.JoinParameters;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static redis.clients.jedis.Protocol.toByteArray;

public class TairSearchCluster {
    private JedisCluster jc;

    public TairSearchCluster(JedisCluster jc) {
        this.jc = jc;
    }

    public String tftmappingindex(String index, String request) {
        return tftmappingindex(SafeEncoder.encode(index), SafeEncoder.encode(request));
    }

    public String tftmappingindex(byte[] index, byte[] request) {
        Object obj = jc.sendCommand(index, ModuleCommand.TFTMAPPINGINDEX, index, request);
        return BuilderFactory.STRING.build(obj);
    }

    public String tftcreateindex(String index, String request) {
        return tftcreateindex(SafeEncoder.encode(index), SafeEncoder.encode(request));
    }

    public String tftcreateindex(byte[] index, byte[] request) {
        Object obj = jc.sendCommand(index, ModuleCommand.TFTCREATEINDEX, index, request);
        return BuilderFactory.STRING.build(obj);
    }

    public String tftupdateindex(String index, String request) {
        return tftupdateindex(SafeEncoder.encode(index), SafeEncoder.encode(request));
    }

    public String tftupdateindex(byte[] index, byte[] request) {
        Object obj = jc.sendCommand(index, ModuleCommand.TFTUPDATEINDEX, index, request);
        return BuilderFactory.STRING.build(obj);
    }

    public String tftgetindexmappings(String index) {
        return tftgetindexmappings(SafeEncoder.encode(index));
    }

    public String tftgetindexmappings(byte[] index) {
        Object obj = jc.sendCommand(index, ModuleCommand.TFTGETINDEX, index, SafeEncoder.encode("mappings"));
        return BuilderFactory.STRING.build(obj);
    }

    public String tftadddoc(String index, String request) {
        return tftadddoc(SafeEncoder.encode(index), SafeEncoder.encode(request));
    }

    public String tftadddoc(byte[] index, byte[] request) {
        Object obj = jc.sendCommand(index, ModuleCommand.TFTADDDOC, index, request);
        return BuilderFactory.STRING.build(obj);
    }

    public String tftadddoc(String index, String request, String docId) {
        return tftadddoc(SafeEncoder.encode(index), SafeEncoder.encode(request), SafeEncoder.encode(docId));
    }

    public String tftadddoc(byte[] index, byte[] request, byte[] docId) {
        Object obj = jc.sendCommand(index, ModuleCommand.TFTADDDOC, index, request, SafeEncoder.encode("WITH_ID"), docId);
        return BuilderFactory.STRING.build(obj);
    }

    public String tftmadddoc(String index, Map<String /* docId */, String /* docContent */> docs) {
        TFTAddDocParams params = new TFTAddDocParams();
        Object obj = jc.sendCommand(SafeEncoder.encode(index), ModuleCommand.TFTMADDDOC, params.getByteParams(index, docs));
        return BuilderFactory.STRING.build(obj);
    }

    public String tftmadddoc(byte[] index, Map<byte[] /* docId */, byte[] /* docContent */> docs) {
        TFTAddDocParams params = new TFTAddDocParams();
        Object obj = jc.sendCommand(index, ModuleCommand.TFTMADDDOC, params.getByteParams(index, docs));
        return BuilderFactory.STRING.build(obj);
    }

    @Deprecated
    public String tftupdatedoc(String index, String docId, String docContent) {
        return tftupdatedoc(SafeEncoder.encode(index), SafeEncoder.encode(docId), SafeEncoder.encode(docContent));
    }

    @Deprecated
    public String tftupdatedoc(byte[] index, byte[] docId, byte[] docContent) {
        Object obj = jc.sendCommand(index, ModuleCommand.TFTUPDATEDOC, index, docId, docContent);
        return BuilderFactory.STRING.build(obj);
    }

    public String tftupdatedocfield(String index, String docId, String docContent) {
        return tftupdatedoc(SafeEncoder.encode(index), SafeEncoder.encode(docId), SafeEncoder.encode(docContent));
    }

    public String tftupdatedocfield(byte[] index, byte[] docId, byte[] docContent) {
        Object obj = jc.sendCommand(index, ModuleCommand.TFTUPDATEDOCFIELD, index, docId, docContent);
        return BuilderFactory.STRING.build(obj);
    }

    public Long tftincrlongdocfield(String index, String docId, final String field, final long value) {
        return tftincrlongdocfield(SafeEncoder.encode(index), SafeEncoder.encode(docId), SafeEncoder.encode(field), value);
    }

    public Long tftincrlongdocfield(byte[] index, byte[] docId, byte[] field, long value) {
        Object obj = jc.sendCommand(index, ModuleCommand.TFTINCRLONGDOCFIELD, index, docId, field, toByteArray(value));
        return BuilderFactory.LONG.build(obj);
    }

    public Double tftincrfloatdocfield(String index, String docId, final String field, final double value) {
        return tftincrfloatdocfield(SafeEncoder.encode(index), SafeEncoder.encode(docId), SafeEncoder.encode(field), value);
    }

    public Double tftincrfloatdocfield(byte[] index, byte[] docId, byte[] field, double value) {
        Object obj = jc.sendCommand(index, ModuleCommand.TFTINCRFLOATDOCFIELD, index, docId, field, toByteArray(value));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Long tftdeldocfield(String index, String docId, final String... field) {
        return tftdeldocfield(SafeEncoder.encode(index), SafeEncoder.encode(docId), SafeEncoder.encodeMany(field));
    }

    public Long tftdeldocfield(byte[] index, byte[] docId, byte[]... field) {
        Object obj = jc.sendCommand(index, ModuleCommand.TFTDELDOCFIELD, JoinParameters.joinParameters(index, docId, field));
        return BuilderFactory.LONG.build(obj);
    }

    public String tftgetdoc(String index, String docId) {
        return tftgetdoc(SafeEncoder.encode(index), SafeEncoder.encode(docId));
    }

    public String tftgetdoc(byte[] index, byte[] docId) {
        Object obj = jc.sendCommand(index, ModuleCommand.TFTGETDOC, index, docId);
        return BuilderFactory.STRING.build(obj);
    }

    public String tftgetdoc(String index, String docId, String request) {
        return tftgetdoc(SafeEncoder.encode(index), SafeEncoder.encode(docId), SafeEncoder.encode(request));
    }

    public String tftgetdoc(byte[] index, byte[] docId, byte[] request) {
        Object obj = jc.sendCommand(index, ModuleCommand.TFTGETDOC, index, docId, request);
        return BuilderFactory.STRING.build(obj);
    }

    public String tftdeldoc(String index,  String... docId) {
        TFTDelDocParams params = new TFTDelDocParams();
        Object obj = jc.sendCommand(SafeEncoder.encode(index), ModuleCommand.TFTDELDOC, params.getByteParams(index, docId));
        return BuilderFactory.STRING.build(obj);
    }

    public String tftdeldoc(byte[] index, byte[]... docId) {
        TFTDelDocParams params = new TFTDelDocParams();
        Object obj = jc.sendCommand(index, ModuleCommand.TFTDELDOC, params.getByteParams(index, docId));
        return BuilderFactory.STRING.build(obj);
    }

    public String tftdelall(String index) {
        return tftdelall(SafeEncoder.encode(index));
    }

    public String tftdelall(byte[] index) {
        Object obj = jc.sendCommand(index, ModuleCommand.TFTDELALL, index);
        return BuilderFactory.STRING.build(obj);
    }

    public String tftsearch(String index, String request) {
        return tftsearch(SafeEncoder.encode(index), SafeEncoder.encode(request));
    }

    public String tftsearch(byte[] index, byte[] request) {
        Object obj = jc.sendCommand(index, ModuleCommand.TFTSEARCH, index, request);
        return BuilderFactory.STRING.build(obj);
    }

    public String tftsearch(String index, String request, boolean use_cache) {
        return tftsearch(SafeEncoder.encode(index), SafeEncoder.encode(request), use_cache);
    }

    public String tftsearch(byte[] index, byte[] request, boolean use_cache) {
        Object obj;
        if (use_cache) {
            obj = jc.sendCommand(index, ModuleCommand.TFTSEARCH, index, request, SafeEncoder.encode("use_cache"));
        } else {
            obj = jc.sendCommand(index, ModuleCommand.TFTSEARCH, index, request);
        }
        return BuilderFactory.STRING.build(obj);
    }

    public Long tftexists(String index, String docId) {
        return tftexists(SafeEncoder.encode(index), SafeEncoder.encode(docId));
    }

    public Long tftexists(byte[] index, byte[] docId) {
        Object obj = jc.sendCommand(index, ModuleCommand.TFTEXISTS, index, docId);
        return BuilderFactory.LONG.build(obj);
    }

    public Long tftdocnum(String index) {
        return tftdocnum(SafeEncoder.encode(index));
    }

    public Long tftdocnum(byte[] index) {
        Object obj = jc.sendCommand(index, ModuleCommand.TFTDOCNUM, index);
        return BuilderFactory.LONG.build(obj);
    }

    public ScanResult<String> tftscandocid(String index, String cursor) {
        Object obj = jc.sendCommand(index, ModuleCommand.TFTSCANDOCID, index, cursor);
        List<Object> result = (List<Object>)obj;
        String newcursor = new String((byte[]) result.get(0));
        List<String> results = new ArrayList<>();
        List<byte[]> rawResults = (List<byte[]>) result.get(1);
        for (byte[] bs : rawResults) {
            results.add(SafeEncoder.encode(bs));
        }
        return new ScanResult<>(newcursor, results);
    }

    public ScanResult<byte[]> tftscandocid(byte[] index, byte[] cursor) {
        Object obj = jc.sendCommand(index, ModuleCommand.TFTSCANDOCID, index, cursor);
        List<Object> result = (List<Object>)obj;
        byte[] newcursor = (byte[]) result.get(0);
        List<byte[]> rawResults = (List<byte[]>) result.get(1);
        return new ScanResult<>(newcursor, rawResults);
    }

    public ScanResult<String> tftscandocid(String index, String cursor, final TFTScanParams params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(SafeEncoder.encode(index));
        args.add(SafeEncoder.encode(cursor));
        args.addAll(params.getParams());
        Object obj = jc.sendCommand(SafeEncoder.encode(index), ModuleCommand.TFTSCANDOCID, args.toArray(new byte[args.size()][]));
        List<Object> result = (List<Object>)obj;
        String newcursor = new String((byte[]) result.get(0));
        List<String> results = new ArrayList<>();
        List<byte[]> rawResults = (List<byte[]>) result.get(1);
        for (byte[] bs : rawResults) {
            results.add(SafeEncoder.encode(bs));
        }
        return new ScanResult<>(newcursor, results);
    }

    public ScanResult<byte[]> tftscandocid(byte[] index, byte[] cursor, final TFTScanParams params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(index);
        args.add(cursor);
        args.addAll(params.getParams());
        Object obj = jc.sendCommand(index, ModuleCommand.TFTSCANDOCID, args.toArray(new byte[args.size()][]));
        List<Object> result = (List<Object>)obj;
        byte[] newcursor = (byte[]) result.get(0);
        List<byte[]> rawResults = (List<byte[]>) result.get(1);
        return new ScanResult<>(newcursor, rawResults);
    }
}
