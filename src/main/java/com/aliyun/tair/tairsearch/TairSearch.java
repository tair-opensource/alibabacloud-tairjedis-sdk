package com.aliyun.tair.tairsearch;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairsearch.params.*;
import com.aliyun.tair.util.JoinParameters;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static redis.clients.jedis.Protocol.toByteArray;

public class TairSearch {
    private final Jedis jedis;

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
     * @param index the index name
     * @param docs  the json representation of a document
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
     * @param index      the index name
     * @param docId      the id of the document
     * @param docContent the content of the document
     * @return Success: OK ; Fail: error.
     */
    @Deprecated
    public String tftupdatedoc(String index, String docId, String docContent) {
        return tftupdatedoc(SafeEncoder.encode(index), SafeEncoder.encode(docId), SafeEncoder.encode(docContent));
    }

    @Deprecated
    public String tftupdatedoc(byte[] index, byte[] docId, byte[] docContent) {
        Object obj = getJedis().sendCommand(ModuleCommand.TFTUPDATEDOC, index, docId, docContent);
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Update doc fields. You can add new fields to the document, or update an existing field.
     * The document is automatically created if it does not exist.
     *
     * @param index      the index name
     * @param docId      the id of the document
     * @param docContent the content of the document
     * @return Success: OK ; Fail: error.
     */
    public String tftupdatedocfield(String index, String docId, String docContent) {
        return tftupdatedoc(SafeEncoder.encode(index), SafeEncoder.encode(docId), SafeEncoder.encode(docContent));
    }

    public String tftupdatedocfield(byte[] index, byte[] docId, byte[] docContent) {
        Object obj = getJedis().sendCommand(ModuleCommand.TFTUPDATEDOCFIELD, index, docId, docContent);
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Increment the integer value of a document field by the given number.
     *
     * @param index the index name
     * @param docId the document id
     * @param field The fields of the document that will be incremented
     * @param value the value to be incremented
     * @return Long integer-reply the value after the increment operation.
     */
    public Long tftincrlongdocfield(String index, String docId, final String field, final long value) {
        return tftincrlongdocfield(SafeEncoder.encode(index), SafeEncoder.encode(docId), SafeEncoder.encode(field), value);
    }

    public Long tftincrlongdocfield(byte[] index, byte[] docId, byte[] field, long value) {
        Object obj = getJedis().sendCommand(ModuleCommand.TFTINCRLONGDOCFIELD, index, docId, field, toByteArray(value));
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * Increment the double value of a document field by the given number.
     *
     * @param index the index name
     * @param docId the document id
     * @param field The fields of the document that will be incremented
     * @param value the value to be incremented
     * @return Long double-reply the value after the increment operation.
     */
    public Double tftincrfloatdocfield(String index, String docId, final String field, final double value) {
        return tftincrfloatdocfield(SafeEncoder.encode(index), SafeEncoder.encode(docId), SafeEncoder.encode(field), value);
    }

    public Double tftincrfloatdocfield(byte[] index, byte[] docId, byte[] field, double value) {
        Object obj = getJedis().sendCommand(ModuleCommand.TFTINCRFLOATDOCFIELD, index, docId, field, toByteArray(value));
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * Delete fields in the document.
     *
     * @param index the index name
     * @param docId the document id
     * @param field The fields of the document that will be deleted
     * @return Long integer-reply the number of fields that were removed from the document.
     */
    public Long tftdeldocfield(String index, String docId, final String... field) {
        return tftdeldocfield(SafeEncoder.encode(index), SafeEncoder.encode(docId), SafeEncoder.encodeMany(field));
    }

    public Long tftdeldocfield(byte[] index, byte[] docId, byte[]... field) {
        Object obj = getJedis().sendCommand(ModuleCommand.TFTDELDOCFIELD, JoinParameters.joinParameters(index, docId, field));
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * Get a document from Index.
     *
     * @param index the index name
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
     * @param index the index name
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
     * @param index the index name
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
     * @param index   the index name
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
     * Full text search for multiple Indexes.
     *
     * @param request Search expression, for detailed grammar, please refer to the official document
     * @param indexes Multiple Index names 
     * @return Success: Query result in json format; Fail: error
     */
    public String tftmsearch(String request, String... indexes) {
        TFTMSearchParams params = new TFTMSearchParams();
        Object obj = getJedis().sendCommand(ModuleCommand.TFTMSEARCH, params.getByteParams(request, indexes));
        return BuilderFactory.STRING.build(obj);
    }

    public String tftmsearch(byte[] request, byte[]... indexes) {
        TFTMSearchParams params = new TFTMSearchParams();
        Object obj = getJedis().sendCommand(ModuleCommand.TFTMSEARCH, params.getByteParams(request, indexes));
        return BuilderFactory.STRING.build(obj);
    }


    /**
     * Checks if the specified document exists in the index.
     *
     * @param index the index name
     * @param docId the id of the document
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
     * @param index the index name
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
     * @param cursor the cursor used for this scan
     * @return the scan result with the results of this iteration and the new position of the cursor.
     */
    public ScanResult<String> tftscandocid(String index, String cursor) {
        Object obj = getJedis().sendCommand(ModuleCommand.TFTSCANDOCID, index, cursor);
        List<Object> result = (List<Object>) obj;
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
        List<Object> result = (List<Object>) obj;
        byte[] newcursor = (byte[]) result.get(0);
        List<byte[]> rawResults = (List<byte[]>) result.get(1);
        return new ScanResult<>(newcursor, rawResults);
    }

    /**
     * Scan all document ids in index.
     * <p>
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
        List<Object> result = (List<Object>) obj;
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
        List<Object> result = (List<Object>) obj;
        byte[] newcursor = (byte[]) result.get(0);
        List<byte[]> rawResults = (List<byte[]>) result.get(1);
        return new ScanResult<>(newcursor, rawResults);
    }

    /**
     * Add suggestions in index.
     *
     * @param index the index name
     * @param texts the suggestions and their weight
     * @return Success:  Number of successfully added suggestions; Fail: error.
     */
    public Long tftaddsug(String index, Map<String /* docContent */, String /* docId */> texts) {
        TFTAddSugParams params = new TFTAddSugParams();
        Object obj = getJedis().sendCommand(ModuleCommand.TFTADDSUG, params.getByteParams(index, texts));
        return BuilderFactory.LONG.build(obj);
    }

    public Long tftaddsug(byte[] index, Map<byte[] /* docContent */, byte[] /* docId */> texts) {
        TFTAddSugParams params = new TFTAddSugParams();
        Object obj = getJedis().sendCommand(ModuleCommand.TFTADDSUG, params.getByteParams(index, texts));
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * Delete the specified suggestions from the index.
     *
     * @param index the index name
     * @param text  the suggestions
     * @return Success: Number of successfully deleted suggestions; Fail: error
     */
    public Long tftdelsug(String index, String... text) {
        return tftdelsug(SafeEncoder.encode(index), SafeEncoder.encodeMany(text));
    }

    public Long tftdelsug(byte[] index, byte[]... text) {
        Object obj = getJedis().sendCommand(ModuleCommand.TFTDELSUG, JoinParameters.joinParameters(index, text));
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * Gets the number of suggestions in index.
     *
     * @param index the index name
     * @return the number of autocomplete texts.
     */
    public Long tftsugnum(String index) {
        return tftsugnum(SafeEncoder.encode(index));
    }

    public Long tftsugnum(byte[] index) {
        Object obj = getJedis().sendCommand(ModuleCommand.TFTSUGNUM, index);
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * Get suggestions in index according to prefix query.
     *
     * @param index  the index name
     * @param prefix the prefix
     * @return List of the suggestions in index.
     */
    public List<String> tftgetsug(String index, String prefix) {
        Object obj = getJedis().sendCommand(ModuleCommand.TFTGETSUG, index, prefix);
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<byte[]> tftgetsug(byte[] index, byte[] prefix) {
        Object obj = getJedis().sendCommand(ModuleCommand.TFTGETSUG, index, prefix);
        return BuilderFactory.BYTE_ARRAY_LIST.build(obj);
    }

    /**
     * Get suggestions in index according to prefix query.
     *
     * @param index  the index name
     * @param prefix the prefix
     * @param params the get parameters. For example the max number of suggestions returned and use fuzzy query
     * @return List of the suggestions in index.
     */
    public List<String> tftgetsug(String index, String prefix, final TFTGetSugParams params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(SafeEncoder.encode(index));
        args.add(SafeEncoder.encode(prefix));
        args.addAll(params.getParams());
        Object obj = getJedis().sendCommand(ModuleCommand.TFTGETSUG, args.toArray(new byte[args.size()][]));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<byte[]> tftgetsug(byte[] index, byte[] prefix, final TFTGetSugParams params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(index);
        args.add(prefix);
        args.addAll(params.getParams());
        Object obj = getJedis().sendCommand(ModuleCommand.TFTGETSUG, args.toArray(new byte[args.size()][]));
        return BuilderFactory.BYTE_ARRAY_LIST.build(obj);
    }

    /**
     * Get all suggestions in index.
     *
     * @param index the index name
     * @return List of the all suggestions in index.
     */
    public List<String> tftgetallsugs(String index) {
        Object obj = getJedis().sendCommand(ModuleCommand.TFTGETALLSUGS, SafeEncoder.encode(index));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<byte[]> tftgetallsugs(byte[] index) {
        Object obj = getJedis().sendCommand(ModuleCommand.TFTGETALLSUGS, index);
        return BuilderFactory.BYTE_ARRAY_LIST.build(obj);
    }


}
