package com.aliyun.tair.tairsearch;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairsearch.params.TFTAddDocParams;
import com.aliyun.tair.tairsearch.params.TFTDelDocParams;
import com.aliyun.tair.tairsearch.params.TFTScanParams;
import com.aliyun.tair.util.JoinParameters;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static redis.clients.jedis.Protocol.toByteArray;

public class TairSearchPipeline extends Pipeline {
    public Response<String> tftmappingindex(String key, String request) {
        return tftmappingindex(SafeEncoder.encode(key), SafeEncoder.encode(request));
    }

    public Response<String> tftmappingindex(byte[] key, byte[] request) {
        getClient("").sendCommand(ModuleCommand.TFTMAPPINGINDEX, key, request);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> tftcreateindex(String key, String request) {
        return tftcreateindex(SafeEncoder.encode(key), SafeEncoder.encode(request));
    }

    public Response<String> tftcreateindex(byte[] key, byte[] request) {
        getClient("").sendCommand(ModuleCommand.TFTCREATEINDEX, key, request);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> tftupdateindex(String index, String request) {
        return tftupdateindex(SafeEncoder.encode(index), SafeEncoder.encode(request));
    }

    public Response<String> tftupdateindex(byte[] index, byte[] request) {
        getClient("").sendCommand(ModuleCommand.TFTUPDATEINDEX, index, request);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> tftgetindexmappings(String key) {
        return tftgetindexmappings(SafeEncoder.encode(key));
    }

    public Response<String> tftgetindexmappings(byte[] key) {
        getClient("").sendCommand(ModuleCommand.TFTGETINDEX, key, SafeEncoder.encode("mappings"));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> tftadddoc(String key, String request) {
        return tftadddoc(SafeEncoder.encode(key), SafeEncoder.encode(request));
    }

    public Response<String> tftadddoc(byte[] key, byte[] request) {
        getClient("").sendCommand(ModuleCommand.TFTADDDOC, key, request);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> tftadddoc(String key, String request, String docId) {
        return tftadddoc(SafeEncoder.encode(key), SafeEncoder.encode(request), SafeEncoder.encode(docId));
    }

    public Response<String> tftadddoc(byte[] key, byte[] request, byte[] docId) {
        getClient("").sendCommand(ModuleCommand.TFTADDDOC, key, request, SafeEncoder.encode("WITH_ID"), docId);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> tftmadddoc(String key, Map<String /* docId */, String /* docContent */> docs) {
        TFTAddDocParams params = new TFTAddDocParams();
        getClient("").sendCommand(ModuleCommand.TFTMADDDOC, params.getByteParams(key, docs));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> tftmadddoc(byte[] key, Map<byte[] /* docId */, byte[] /* docContent */> docs) {
        TFTAddDocParams params = new TFTAddDocParams();
        getClient("").sendCommand(ModuleCommand.TFTMADDDOC, params.getByteParams(key, docs));
        return getResponse(BuilderFactory.STRING);
    }

    @Deprecated
    public Response<String> tftupdatedoc(String index, String docId, String docContent) {
        return tftupdatedoc(SafeEncoder.encode(index), SafeEncoder.encode(docId), SafeEncoder.encode(docContent));
    }

    @Deprecated
    public Response<String> tftupdatedoc(byte[] index, byte[] docId, byte[] docContent) {
        getClient("").sendCommand(ModuleCommand.TFTUPDATEDOC, index, docId, docContent);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> tftupdatedocfield(String index, String docId, String docContent) {
        return tftupdatedoc(SafeEncoder.encode(index), SafeEncoder.encode(docId), SafeEncoder.encode(docContent));
    }

    public Response<String> tftupdatedocfield(byte[] index, byte[] docId, byte[] docContent) {
        getClient("").sendCommand(ModuleCommand.TFTUPDATEDOCFIELD, index, docId, docContent);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<Long> tftincrlongdocfield(String index, String docId, final String field, final long value) {
        return tftincrlongdocfield(SafeEncoder.encode(index), SafeEncoder.encode(docId), SafeEncoder.encode(field), value);
    }

    public Response<Long> tftincrlongdocfield(byte[] index, byte[] docId, byte[] field, long value) {
        getClient("").sendCommand(ModuleCommand.TFTINCRLONGDOCFIELD, index, docId, field, toByteArray(value));
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Double> tftincrfloatdocfield(String index, String docId, final String field, final double value) {
        return tftincrfloatdocfield(SafeEncoder.encode(index), SafeEncoder.encode(docId), SafeEncoder.encode(field), value);
    }

    public Response<Double> tftincrfloatdocfield(byte[] index, byte[] docId, byte[] field, double value) {
        getClient("").sendCommand(ModuleCommand.TFTINCRFLOATDOCFIELD, index, docId, field, toByteArray(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Long> tftdeldocfield(String index, String docId, final String... field) {
        return tftdeldocfield(SafeEncoder.encode(index), SafeEncoder.encode(docId), SafeEncoder.encodeMany(field));
    }

    public Response<Long> tftdeldocfield(byte[] index, byte[] docId, byte[]... field) {
        getClient("").sendCommand(ModuleCommand.TFTDELDOCFIELD, JoinParameters.joinParameters(index, docId, field));
        return getResponse(BuilderFactory.LONG);
    }

    public Response<String> tftgetdoc(String key, String docId) {
        return tftgetdoc(SafeEncoder.encode(key), SafeEncoder.encode(docId));
    }

    public Response<String> tftgetdoc(byte[] key, byte[] docId) {
        getClient("").sendCommand(ModuleCommand.TFTGETDOC, key, docId);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> tftgetdoc(String key, String docId, String request) {
        return tftgetdoc(SafeEncoder.encode(key), SafeEncoder.encode(docId), SafeEncoder.encode(request));
    }

    public Response<String> tftgetdoc(byte[] key, byte[] docId, byte[] request) {
        getClient("").sendCommand(ModuleCommand.TFTGETDOC, key, docId, request);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> tftdeldoc(String key,  String... docId) {
        TFTDelDocParams params = new TFTDelDocParams();
        getClient("").sendCommand(ModuleCommand.TFTDELDOC, params.getByteParams(key, docId));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> tftdeldoc(byte[] key, byte[]... docId) {
        TFTDelDocParams params = new TFTDelDocParams();
        getClient("").sendCommand(ModuleCommand.TFTDELDOC, params.getByteParams(key, docId));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> tftdelall(String index) {
        return tftdelall(SafeEncoder.encode(index));
    }

    public Response<String> tftdelall(byte[] index) {
        getClient("").sendCommand(ModuleCommand.TFTDELALL, index);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> tftsearch(String key, String request) {
        return tftsearch(SafeEncoder.encode(key), SafeEncoder.encode(request));
    }

    public Response<String> tftsearch(byte[] key, byte[] request) {
        getClient("").sendCommand(ModuleCommand.TFTSEARCH, key, request);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> tftsearch(String key, String request, boolean use_cache) {
        return tftsearch(SafeEncoder.encode(key), SafeEncoder.encode(request), use_cache);
    }

    public Response<String> tftsearch(byte[] key, byte[] request, boolean use_cache) {
        if (use_cache) {
            getClient("").sendCommand(ModuleCommand.TFTSEARCH, key, request, SafeEncoder.encode("use_cache"));
        } else {
            getClient("").sendCommand(ModuleCommand.TFTSEARCH, key, request);
        }

        return getResponse(BuilderFactory.STRING);
    }

    public Response<Long> tftexists(String index, String docId) {
        return tftexists(SafeEncoder.encode(index), SafeEncoder.encode(docId));
    }

    public Response<Long> tftexists(byte[] index, byte[] docId) {
        getClient("").sendCommand(ModuleCommand.TFTEXISTS, index, docId);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> tftdocnum(String index) {
        return tftdocnum(SafeEncoder.encode(index));
    }

    public Response<Long> tftdocnum(byte[] index) {
        getClient("").sendCommand(ModuleCommand.TFTDOCNUM, index);
        return getResponse(BuilderFactory.LONG);
    }
}
