package com.aliyun.tair.tairsearch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairsearch.params.TFTAddDocParams;
import com.aliyun.tair.tairsearch.params.TFTDelDocParams;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.util.SafeEncoder;


public class TairSearch {
    private Jedis jedis;

    public TairSearch(Jedis jedis) {
        this.jedis = jedis;
    }

    private Jedis getJedis() {
        return jedis;
    }

    /**
     * Create an Index and specify its schema. Note that this command will only succeed
     * if the Index does not exist.
     *
     * @param key   the index name
     * @param request the index schema
     * @return Success: OK; Fail: error
     */
    public String tftmappingindex(String key, String request) {
        return tftmappingindex(SafeEncoder.encode(key), SafeEncoder.encode(request));
    }

    public String tftmappingindex(byte[] key, byte[] request) {
        Object obj = getJedis().sendCommand(ModuleCommand.TFTMAPPINGINDEX, key, request);
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Add a document to Index.
     *
     * @param key   the index name
     * @param request the json representation of a document
     * @return Success: Json structure containing document id and version ; Fail: error.
     */
    public String tftadddoc(String key, String request) {
        return tftadddoc(SafeEncoder.encode(key), SafeEncoder.encode(request));
    }

    public String tftadddoc(byte[] key, byte[] request) {
        Object obj = getJedis().sendCommand(ModuleCommand.TFTADDDOC, key, request);
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Similar to the above but you can manually specify the document id.
     */
    public String tftadddoc(String key, String request, String docId) {
        return tftadddoc(SafeEncoder.encode(key), SafeEncoder.encode(request), SafeEncoder.encode(docId));
    }

    public String tftadddoc(byte[] key, byte[] request, byte[] docId) {
        Object obj = getJedis().sendCommand(ModuleCommand.TFTADDDOC, key, request, SafeEncoder.encode("WITH_ID"), docId);
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Add docs in batch.
     */
    public String tftmadddoc(String key, Map<String /* docContent */, String /* docId */> docs) {
        TFTAddDocParams params = new TFTAddDocParams();
        Object obj = getJedis().sendCommand(ModuleCommand.TFTMADDDOC, params.getByteParams(key, docs));
        return BuilderFactory.STRING.build(obj);
    }

    public String tftmadddoc(byte[] key, Map<byte[] /* docContent */, byte[] /* docId */> docs) {
        TFTAddDocParams params = new TFTAddDocParams();
        Object obj = getJedis().sendCommand(ModuleCommand.TFTMADDDOC, params.getByteParams(key, docs));
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Get a document from Index.
     *
     * @param key   the index name
     * @param docId the document id
     * @return Success: The content of the document; Not exists: null; Fail: error
     */
    public String tftgetdoc(String key, String docId) {
        return tftgetdoc(SafeEncoder.encode(key), SafeEncoder.encode(docId));
    }

    public String tftgetdoc(byte[] key, byte[] docId) {
        Object obj = getJedis().sendCommand(ModuleCommand.TFTGETDOC, key, docId);
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Same as above but you can specify some filtering rules through the request parameter.
     */
    public String tftgetdoc(String key, String docId, String request) {
        return tftgetdoc(SafeEncoder.encode(key), SafeEncoder.encode(docId), SafeEncoder.encode(request));
    }

    public String tftgetdoc(byte[] key, byte[] docId, byte[] request) {
        Object obj = getJedis().sendCommand(ModuleCommand.TFTGETDOC, key, docId, request);
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Delete the specified document(s) from the index.
     *
     * @param key   the index name
     * @param docId the document id(s)
     * @return Success: Number of successfully deleted documents; Fail: error
     */
    public String tftdeldoc(String key, String... docId) {
        TFTDelDocParams params = new TFTDelDocParams();
        Object obj = getJedis().sendCommand(ModuleCommand.TFTDELDOC, params.getByteParams(key, docId));
        return BuilderFactory.STRING.build(obj);
    }

    public String tftdeldoc(byte[] key, byte[]... docId) {
        TFTDelDocParams params = new TFTDelDocParams();
        Object obj = getJedis().sendCommand(ModuleCommand.TFTDELDOC, params.getByteParams(key, docId));
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Get index schema information.
     *
     * @param key the index name
     * @return Success: Schema information represented by json; Fail: error
     */
    public String tftgetindexmappings(String key) {
        return tftgetindexmappings(SafeEncoder.encode(key));
    }

    public String tftgetindexmappings(byte[] key) {
        Object obj = getJedis().sendCommand(ModuleCommand.TFTGETINDEX, key, SafeEncoder.encode("mappings"));
        return BuilderFactory.STRING.build(obj);
    }

    public String tftgetindexsettings(String key) {
        return tftgetindexsettings(SafeEncoder.encode(key));
    }

    public String tftgetindexsettings(byte[] key) {
        Object obj = getJedis().sendCommand(ModuleCommand.TFTGETINDEX, key, SafeEncoder.encode("settings"));
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Full text search in an Index.
     *
     * @param key the index name
     * @param request Search expression, for detailed grammar, please refer to the official document
     * @return Success: Query result in json format; Fail: error
     */
    public String tftsearch(String key, String request) {
        return tftsearch(SafeEncoder.encode(key), SafeEncoder.encode(request));
    }

    public String tftsearch(byte[] key, byte[] request) {
        Object obj = getJedis().sendCommand(ModuleCommand.TFTSEARCH, key, request);
        return BuilderFactory.STRING.build(obj);
    }

    public String tftsearch(String key, String request, boolean use_cache) {
        return tftsearch(SafeEncoder.encode(key), SafeEncoder.encode(request), use_cache);
    }

    public String tftsearch(byte[] key, byte[] request, boolean use_cache) {
        Object obj;
        if (use_cache) {
            obj = getJedis().sendCommand(ModuleCommand.TFTSEARCH, key, request, SafeEncoder.encode("use_cache"));
        } else {
            obj = getJedis().sendCommand(ModuleCommand.TFTSEARCH, key, request);
        }

        return BuilderFactory.STRING.build(obj);
    }
}
