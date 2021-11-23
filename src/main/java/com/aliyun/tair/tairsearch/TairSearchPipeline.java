package com.aliyun.tair.tairsearch;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairsearch.params.TFTDelDocParams;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.util.SafeEncoder;

public class TairSearchPipeline extends Pipeline {
    public Response<String> tftmappingindex(String key, String request) {
        return tftmappingindex(SafeEncoder.encode(key), SafeEncoder.encode(request));
    }

    public Response<String> tftmappingindex(byte[] key, byte[] request) {
        getClient("").sendCommand(ModuleCommand.TFTMAPPINGINDEX, key, request);
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

    public Response<String> tftgetindexmappings(String key) {
        return tftgetindexmappings(SafeEncoder.encode(key));
    }

    public Response<String> tftgetindexmappings(byte[] key) {
        getClient("").sendCommand(ModuleCommand.TFTGETINDEX, key, SafeEncoder.encode("mappings"));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> tftgetindexsettings(String key) {
        return tftgetindexsettings(SafeEncoder.encode(key));
    }

    public Response<String> tftgetindexsettings(byte[] key) {
        getClient("").sendCommand(ModuleCommand.TFTGETINDEX, key, SafeEncoder.encode("settings"));
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
}
