package com.aliyun.tair.tairsearch;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairsearch.params.TFTAddDocParams;
import com.aliyun.tair.tairsearch.params.TFTDelDocParams;
import com.aliyun.tair.tairsearch.params.TFTScanParams;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanResult;
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
     * @param index   the index name
     * @param request the index schema
     * @return Success: OK; Fail: error
     */
    public String tftmappingindex(String index, String request) {
        return tftmappingindex(SafeEncoder.encode(index), SafeEncoder.encode(request));
    }

    public String tftmappingindex(byte[] index, byte[] request) {
        Object obj = getJedis().sendCommand(ModuleCommand.TFTMAPPINGINDEX, index, request);
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Create an Index and specify its schema. Note that this command will only succeed
     * if the Index does not exist.
     *
     * @param index   the index name
     * @param request the index schema
     * @return Success: OK; Fail: error
     */
    public String tftcreateindex(String index, String request) {
        return tftcreateindex(SafeEncoder.encode(index), SafeEncoder.encode(request));
    }

    public String tftcreateindex(byte[] index, byte[] request) {
        Object obj = getJedis().sendCommand(ModuleCommand.TFTCREATEINDEX, index, request);
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Update an existing index mapping.
     * Note that you cannot update (append) mapping properties.
     *
     * @param index   the index name
     * @param request the index schema
     * @return Success: OK; Fail: error
     */
    public String tftupdateindex(String index, String request) {
        return tftupdateindex(SafeEncoder.encode(index), SafeEncoder.encode(request));
    }

    public String tftupdateindex(byte[] index, byte[] request) {
        Object obj = getJedis().sendCommand(ModuleCommand.TFTUPDATEINDEX, index, request);
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Get index schema information.
     *
     * @param index the index name
     * @return Success: Schema information represented by json; Fail: error
     */
    public String tftgetindexmappings(String index) {
        return tftgetindexmappings(SafeEncoder.encode(index));
    }

    public String tftgetindexmappings(byte[] index) {
        Object obj = getJedis().sendCommand(ModuleCommand.TFTGETINDEX, index, SafeEncoder.encode("mappings"));
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Add a document to Index.
     *
     * @param index   the index name
     * @param request the json representation of a document
     * @return Success: Json structure containing document id and version ; Fail: error.
     */
    public String tftadddoc(String index, String request) {
        return tftadddoc(SafeEncoder.encode(index), SafeEncoder.encode(request));
    }

    public String tftadddoc(byte[] index, byte[] request) {
        Object obj = getJedis().sendCommand(ModuleCommand.TFTADDDOC, index, request);
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Similar to the above but you can manually specify the document id.
     */
    public String tftadddoc(String index, String request, String docId) {
        return tftadddoc(SafeEncoder.encode(index), SafeEncoder.encode(request), SafeEncoder.encode(docId));
    }

    public String tftadddoc(byte[] index, byte[] request, byte[] docId) {
        Object obj = getJedis().sendCommand(ModuleCommand.TFTADDDOC, index, request, SafeEncoder.encode("WITH_ID"), docId);
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Add docs in batch. This command can guarantee atomicity, that is, either all documents are
     * added successfully, or none are added.
     *
     * @param index   the index name
     * @param docs the json representation of a document
     * @return Success: OK ; Fail: error.
     */
    public String tftmadddoc(String index, Map<String /* docContent */, String /* docId */> docs) {
        TFTAddDocParams params = new TFTAddDocParams();
        Object obj = getJedis().sendCommand(ModuleCommand.TFTMADDDOC, params.getByteParams(index, docs));
        return BuilderFactory.STRING.build(obj);
    }

    public String tftmadddoc(byte[] index, Map<byte[] /* docContent */, byte[] /* docId */> docs) {
        TFTAddDocParams params = new TFTAddDocParams();
        Object obj = getJedis().sendCommand(ModuleCommand.TFTMADDDOC, params.getByteParams(index, docs));
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Update an existing doc. You can add new fields to the document, or update an existing field.
     *
     * @param index   the index name
     * @param docId the id of the document
     * @param docContent the content of the document
     * @return Success: OK ; Fail: error.
     */
    public String tftupdatedoc(String index, String docId, String docContent) {
        return tftupdatedoc(SafeEncoder.encode(index), SafeEncoder.encode(docId), SafeEncoder.encode(docContent));
    }

    public String tftupdatedoc(byte[] index, byte[] docId, byte[] docContent) {
        Object obj = getJedis().sendCommand(ModuleCommand.TFTUPDATEDOC, index, docId, docContent);
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Get a document from Index.
     *
     * @param index   the index name
     * @param docId the document id
     * @return Success: The content of the document; Not exists: null; Fail: error
     */
    public String tftgetdoc(String index, String docId) {
        return tftgetdoc(SafeEncoder.encode(index), SafeEncoder.encode(docId));
    }

    public String tftgetdoc(byte[] index, byte[] docId) {
        Object obj = getJedis().sendCommand(ModuleCommand.TFTGETDOC, index, docId);
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Same as above but you can specify some filtering rules through the request parameter.
     */
    public String tftgetdoc(String index, String docId, String request) {
        return tftgetdoc(SafeEncoder.encode(index), SafeEncoder.encode(docId), SafeEncoder.encode(request));
    }

    public String tftgetdoc(byte[] index, byte[] docId, byte[] request) {
        Object obj = getJedis().sendCommand(ModuleCommand.TFTGETDOC, index, docId, request);
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Delete the specified document(s) from the index.
     *
     * @param index   the index name
     * @param docId the document id(s)
     * @return Success: Number of successfully deleted documents; Fail: error
     */
    public String tftdeldoc(String index, String... docId) {
        TFTDelDocParams params = new TFTDelDocParams();
        Object obj = getJedis().sendCommand(ModuleCommand.TFTDELDOC, params.getByteParams(index, docId));
        return BuilderFactory.STRING.build(obj);
    }

    public String tftdeldoc(byte[] index, byte[]... docId) {
        TFTDelDocParams params = new TFTDelDocParams();
        Object obj = getJedis().sendCommand(ModuleCommand.TFTDELDOC, params.getByteParams(index, docId));
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Delete all document(s) from the index.
     *
     * @param index   the index name
     * @return Success: OK; Fail: error
     */
    public String tftdelall(String index) {
        return tftdelall(SafeEncoder.encode(index));
    }

    public String tftdelall(byte[] index) {
        Object obj = getJedis().sendCommand(ModuleCommand.TFTDELALL, index);
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Full text search in an Index.
     *
     * @param index the index name
     * @param request Search expression, for detailed grammar, please refer to the official document
     * @return Success: Query result in json format; Fail: error
     */
    public String tftsearch(String index, String request) {
        return tftsearch(SafeEncoder.encode(index), SafeEncoder.encode(request));
    }

    public String tftsearch(byte[] index, byte[] request) {
        Object obj = getJedis().sendCommand(ModuleCommand.TFTSEARCH, index, request);
        return BuilderFactory.STRING.build(obj);
    }

    public String tftsearch(String index, String request, boolean use_cache) {
        return tftsearch(SafeEncoder.encode(index), SafeEncoder.encode(request), use_cache);
    }

    public String tftsearch(byte[] index, byte[] request, boolean use_cache) {
        Object obj;
        if (use_cache) {
            obj = getJedis().sendCommand(ModuleCommand.TFTSEARCH, index, request, SafeEncoder.encode("use_cache"));
        } else {
            obj = getJedis().sendCommand(ModuleCommand.TFTSEARCH, index, request);
        }

        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Checks if the specified document exists in the index.
     *
     * @param index  the index name
     * @param docId  the id of the document
     * @return exists return 1 or return 0
     */
    public Long tftexists(String index, String docId) {
        return tftexists(SafeEncoder.encode(index), SafeEncoder.encode(docId));
    }

    public Long tftexists(byte[] index, byte[] docId) {
        Object obj = getJedis().sendCommand(ModuleCommand.TFTEXISTS, index, docId);
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * Get the number of documents contained in Index.
     *
     * @param index  the index name
     * @return the number of documents contained in Index.
     */
    public Long tftdocnum(String index) {
        return tftdocnum(SafeEncoder.encode(index));
    }

    public Long tftdocnum(byte[] index) {
        Object obj = getJedis().sendCommand(ModuleCommand.TFTDOCNUM, index);
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * Scan all document ids in index.
     *
     * @param index  the index name
     * @param cursor  the cursor used for this scan
     * @return the scan result with the results of this iteration and the new position of the cursor.
     */
    public ScanResult<String> tftscandocid(String index, String cursor) {
        Object obj = getJedis().sendCommand(ModuleCommand.TFTSCANDOCID, index, cursor);
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
        Object obj = getJedis().sendCommand(ModuleCommand.TFTSCANDOCID, index, cursor);
        List<Object> result = (List<Object>)obj;
        byte[] newcursor = (byte[]) result.get(0);
        List<byte[]> rawResults = (List<byte[]>) result.get(1);
        return new ScanResult<>(newcursor, rawResults);
    }

    /**
     * Scan all document ids in index.
     *
     * Time complexity: O(1) for every call. O(N) for a complete iteration, including enough command
     * calls for the cursor to return back to 0. N is the number of documents inside the index.
     *
     * @param index  the index name
     * @param cursor The cursor.
     * @param params the scan parameters. For example a glob-style match pattern
     * @return the scan result with the results of this iteration and the new position of the cursor.
     */
    public ScanResult<String> tftscandocid(String index, String cursor, final TFTScanParams params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(SafeEncoder.encode(index));
        args.add(SafeEncoder.encode(cursor));
        args.addAll(params.getParams());
        Object obj = getJedis().sendCommand(ModuleCommand.TFTSCANDOCID, args.toArray(new byte[args.size()][]));
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
        Object obj = getJedis().sendCommand(ModuleCommand.TFTSCANDOCID, args.toArray(new byte[args.size()][]));
        List<Object> result = (List<Object>)obj;
        byte[] newcursor = (byte[]) result.get(0);
        List<byte[]> rawResults = (List<byte[]>) result.get(1);
        return new ScanResult<>(newcursor, rawResults);
    }
}
