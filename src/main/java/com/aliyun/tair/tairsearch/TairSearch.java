package com.aliyun.tair.tairsearch;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairsearch.action.search.MSearchResponse;
import com.aliyun.tair.tairsearch.action.search.SearchResponse;
import com.aliyun.tair.tairsearch.params.*;
import com.aliyun.tair.tairsearch.search.builder.MSearchSourceBuilder;
import com.aliyun.tair.tairsearch.search.builder.SearchSourceBuilder;
import com.aliyun.tair.util.JoinParameters;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static redis.clients.jedis.Protocol.toByteArray;

public class TairSearch {
    private Jedis jedis;
    private JedisPool jedisPool;
    
    public TairSearch(Jedis jedis) {
        this.jedis = jedis;
    }

    public TairSearch(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    private Jedis getJedis() {
        if (jedisPool != null) {
            return jedisPool.getResource();
        }
        return jedis;
    }

    private void releaseJedis(Jedis jedis) {
        if (jedisPool != null) {
            jedis.close();
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTMAPPINGINDEX, index, request);
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTCREATEINDEX, index, request);
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTUPDATEINDEX, index, request);
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Get index schema information.
     *
     * @param index the index name
     * @return Success: Schema information represented by json; Fail: error
     */
    @Deprecated
    public String tftgetindexmappings(String index) {
        return tftgetindexmappings(SafeEncoder.encode(index));
    }

    @Deprecated
    public String tftgetindexmappings(byte[] index) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTGETINDEX, index, SafeEncoder.encode("mappings"));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public String tftgetindex(String index) {
        return tftgetindex(SafeEncoder.encode(index));
    }

    public String tftgetindex(byte[] index) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTGETINDEX, index);
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public String tftgetindex(String index, final TFTGetIndexParams params) {
        return tftgetindex(SafeEncoder.encode(index), params);
    }

    public String tftgetindex(byte[] index, final TFTGetIndexParams params) {
        Jedis jedis = getJedis();
        try {
            Object obj;
            if(params.getParams() == null){
                obj = jedis.sendCommand(ModuleCommand.TFTGETINDEX, index);
            } else {
                obj = jedis.sendCommand(ModuleCommand.TFTGETINDEX, index, params.getParams());
            }
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTADDDOC, index, request);
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Similar to the above but you can manually specify the document id.
     */
    public String tftadddoc(String index, String request, String docId) {
        return tftadddoc(SafeEncoder.encode(index), SafeEncoder.encode(request), SafeEncoder.encode(docId));
    }

    public String tftadddoc(byte[] index, byte[] request, byte[] docId) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTADDDOC, index, request, SafeEncoder.encode("WITH_ID"), docId);
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Add docs in batch. This command can guarantee atomicity, that is, either all documents are
     * added successfully, or none are added.
     *
     * @param index the index name
     * @param docs  the json representation of a document
     * @return Success: OK ; Fail: error.
     */
    @Deprecated
    public String tftmadddoc(String index, Map<String /* docContent */, String /* docId */> docs) {
        TFTAddDocParams params = new TFTAddDocParams();
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTMADDDOC, params.getByteParams(index, docs));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    @Deprecated
    public String tftmadddoc(byte[] index, Map<byte[] /* docContent */, byte[] /* docId */> docs) {
        TFTAddDocParams params = new TFTAddDocParams();
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTMADDDOC, params.getByteParams(index, docs));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public String tftmadddoc(String index, List<DocInfo> docs) {
        TFTMaddDocParams params = new TFTMaddDocParams();
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTMADDDOC, params.getByteParams(index, docs));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public String tftmadddoc(byte[] index, List<DocInfoByte> docs) {
        TFTMaddDocParams params = new TFTMaddDocParams();
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTMADDDOC, params.getByteParams(index, docs));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTUPDATEDOC, index, docId, docContent);
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTUPDATEDOCFIELD, index, docId, docContent);
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTINCRLONGDOCFIELD, index, docId, field, toByteArray(value));
            return BuilderFactory.LONG.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTINCRFLOATDOCFIELD, index, docId, field, toByteArray(value));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTDELDOCFIELD, JoinParameters.joinParameters(index, docId, field));
            return BuilderFactory.LONG.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTGETDOC, index, docId);
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Same as above but you can specify some filtering rules through the request parameter.
     */
    public String tftgetdoc(String index, String docId, String request) {
        return tftgetdoc(SafeEncoder.encode(index), SafeEncoder.encode(docId), SafeEncoder.encode(request));
    }

    public String tftgetdoc(byte[] index, byte[] docId, byte[] request) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTGETDOC, index, docId, request);
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTDELDOC, params.getByteParams(index, docId));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public String tftdeldoc(byte[] index, byte[]... docId) {
        TFTDelDocParams params = new TFTDelDocParams();
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTDELDOC, params.getByteParams(index, docId));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTDELALL, index);
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public SearchResponse tftsearch(String index, SearchSourceBuilder ssb) {
        return new SearchResponse(tftsearch(SafeEncoder.encode(index), SafeEncoder.encode(ssb.toString())));
    }

    public SearchResponse tftsearch(byte[] index, SearchSourceBuilder ssb) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTSEARCH, index, SafeEncoder.encode(ssb.toString()));
            return new SearchResponse(BuilderFactory.STRING.build(obj));
        } finally {
            releaseJedis(jedis);
        }
    }

    public SearchResponse tftsearch(String index, SearchSourceBuilder ssb, boolean use_cache) {
        return new SearchResponse(tftsearch(SafeEncoder.encode(index), SafeEncoder.encode(ssb.toString())));
    }

    public SearchResponse tftsearch(byte[] index, SearchSourceBuilder ssb, boolean use_cache) {
        Object obj;
        Jedis jedis = getJedis();
        try {
            if (use_cache) {
                obj = jedis.sendCommand(ModuleCommand.TFTSEARCH, index, SafeEncoder.encode(ssb.toString()), SafeEncoder.encode("use_cache"));
            } else {
                obj = jedis.sendCommand(ModuleCommand.TFTSEARCH, index, SafeEncoder.encode(ssb.toString()));
            }
        } finally {
            releaseJedis(jedis);
        }

        return new SearchResponse(BuilderFactory.STRING.build(obj));
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTSEARCH, index, request);
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public String tftsearch(String index, String request, boolean use_cache) {
        return tftsearch(SafeEncoder.encode(index), SafeEncoder.encode(request), use_cache);
    }

    public String tftsearch(byte[] index, byte[] request, boolean use_cache) {
        Object obj;
        Jedis jedis = getJedis();
        try {
            if (use_cache) {
                obj = jedis.sendCommand(ModuleCommand.TFTSEARCH, index, request, SafeEncoder.encode("use_cache"));
            } else {
                obj = jedis.sendCommand(ModuleCommand.TFTSEARCH, index, request);
            }
        } finally {
            releaseJedis(jedis);
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTMSEARCH, params.getByteParams(request, indexes));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public String tftmsearch(byte[] request, byte[]... indexes) {
        TFTMSearchParams params = new TFTMSearchParams();
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTMSEARCH, params.getByteParams(request, indexes));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public MSearchResponse tftmsearch(MSearchSourceBuilder ssb, String... indexes) {
        TFTMSearchParams params = new TFTMSearchParams();
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTMSEARCH, params.getByteParams(ssb.toString(), indexes));
            return new MSearchResponse(BuilderFactory.STRING.build(obj));
        } finally {
            releaseJedis(jedis);
        }
    }

    public MSearchResponse tftmsearch(MSearchSourceBuilder ssb, byte[]... indexes) {
        TFTMSearchParams params = new TFTMSearchParams();
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTMSEARCH, params.getByteParams(SafeEncoder.encode(ssb.toString()), indexes));
            return new MSearchResponse(BuilderFactory.STRING.build(obj));
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTEXISTS, index, docId);
            return BuilderFactory.LONG.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTDOCNUM, index);
            return BuilderFactory.LONG.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Scan all document ids in index.
     *
     * @param index  the index name
     * @param cursor the cursor used for this scan
     * @return the scan result with the results of this iteration and the new position of the cursor.
     */
    public ScanResult<String> tftscandocid(String index, String cursor) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTSCANDOCID, index, cursor);
            List<Object> result = (List<Object>) obj;
            String newcursor = new String((byte[]) result.get(0));
            List<String> results = new ArrayList<>();
            List<byte[]> rawResults = (List<byte[]>) result.get(1);
            for (byte[] bs : rawResults) {
                results.add(SafeEncoder.encode(bs));
            }
            return new ScanResult<>(newcursor, results);
        } finally {
            releaseJedis(jedis);
        }
    }

    public ScanResult<byte[]> tftscandocid(byte[] index, byte[] cursor) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTSCANDOCID, index, cursor);
            List<Object> result = (List<Object>) obj;
            byte[] newcursor = (byte[]) result.get(0);
            List<byte[]> rawResults = (List<byte[]>) result.get(1);
            return new ScanResult<>(newcursor, rawResults);
        } finally {
            releaseJedis(jedis);
        }
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

        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTSCANDOCID, args.toArray(new byte[args.size()][]));
            List<Object> result = (List<Object>) obj;
            String newcursor = new String((byte[]) result.get(0));
            List<String> results = new ArrayList<>();
            List<byte[]> rawResults = (List<byte[]>) result.get(1);
            for (byte[] bs : rawResults) {
                results.add(SafeEncoder.encode(bs));
            }
            return new ScanResult<>(newcursor, results);
        } finally {
            releaseJedis(jedis);
        }
    }

    public ScanResult<byte[]> tftscandocid(byte[] index, byte[] cursor, final TFTScanParams params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(index);
        args.add(cursor);
        args.addAll(params.getParams());

        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTSCANDOCID, args.toArray(new byte[args.size()][]));
            List<Object> result = (List<Object>) obj;
            byte[] newcursor = (byte[]) result.get(0);
            List<byte[]> rawResults = (List<byte[]>) result.get(1);
            return new ScanResult<>(newcursor, rawResults);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Debug analyzer.
     *
     * @param analyzerName the analyzer name
     * @param text the text to be tokenized
     * @return Success: Token information
     */
    public String tftanalyzer(String analyzerName, String text) {
        return tftanalyzer(SafeEncoder.encode(analyzerName), SafeEncoder.encode(text));
    }

    public String tftanalyzer(byte[] analyzerName, byte[] text) {
        Jedis jedis = getJedis();

        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTANALYZER, analyzerName, text);
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public String tftanalyzer(String analyzerName, String text, final TFTAnalyzerParams params) {
        return tftanalyzer(SafeEncoder.encode(analyzerName), SafeEncoder.encode(text), params);
    }

    public String tftanalyzer(byte[] analyzerName, byte[] text, final TFTAnalyzerParams params) {
        Jedis jedis = getJedis();

        try {
            /* We pass the Index_name directly to the proxy, and the proxy will correctly select the node. */
            Object obj = jedis.sendCommand(ModuleCommand.TFTANALYZER, params.getByteParams(analyzerName, text));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * explain the cost of every query phase.
     *
     * @param index the index name
     * @param ssb the SearchSourceBuilder
     * @return Success: Token information
     */
    public String tftexplaincost(String index, SearchSourceBuilder ssb) {
        return tftexplaincost(SafeEncoder.encode(index), SafeEncoder.encode(ssb.toString()));
    }

    public String tftexplaincost(byte[] index, SearchSourceBuilder ssb) {
        return tftexplaincost(index, SafeEncoder.encode(ssb.toString()));
    }

    /**
     * explain the cost of every query phase.
     *
     * @param index the index name
     * @param request the query clause
     * @return Success: Token information
     */
    public String tftexplaincost(String index, String request) {
        return tftexplaincost(SafeEncoder.encode(index), SafeEncoder.encode(request));
    }

    public String tftexplaincost(byte[] index, byte[] request) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTEXPLAINCOST, index, request);
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Add suggestions in index.
     *
     * @param index the index name
     * @param texts the suggestions and their weight
     * @return Success:  Number of successfully added suggestions; Fail: error.
     */
    public Long tftaddsug(String index, Map<String /* suggestion */, Integer /* weight */> texts) {
        TFTAddSugParams params = new TFTAddSugParams();
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTADDSUG, params.getByteParams(index, texts));
            return BuilderFactory.LONG.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Long tftaddsug(byte[] index, Map<byte[] /* suggestion */, Integer /* weight */> texts) {
        TFTAddSugParams params = new TFTAddSugParams();
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTADDSUG, params.getByteParams(index, texts));
            return BuilderFactory.LONG.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTDELSUG, JoinParameters.joinParameters(index, text));
            return BuilderFactory.LONG.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTSUGNUM, index);
            return BuilderFactory.LONG.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Get suggestions in index according to prefix query.
     *
     * @param index  the index name
     * @param prefix the prefix
     * @return List of the suggestions in index.
     */
    public List<String> tftgetsug(String index, String prefix) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTGETSUG, index, prefix);
            return BuilderFactory.STRING_LIST.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public List<byte[]> tftgetsug(byte[] index, byte[] prefix) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTGETSUG, index, prefix);
            return BuilderFactory.BYTE_ARRAY_LIST.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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

        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTGETSUG, args.toArray(new byte[args.size()][]));
            return BuilderFactory.STRING_LIST.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public List<byte[]> tftgetsug(byte[] index, byte[] prefix, final TFTGetSugParams params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(index);
        args.add(prefix);
        args.addAll(params.getParams());

        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTGETSUG, args.toArray(new byte[args.size()][]));
            return BuilderFactory.BYTE_ARRAY_LIST.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Get all suggestions in index.
     *
     * @param index the index name
     * @return List of the all suggestions in index.
     */
    public List<String> tftgetallsugs(String index) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTGETALLSUGS, SafeEncoder.encode(index));
            return BuilderFactory.STRING_LIST.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public List<byte[]> tftgetallsugs(byte[] index) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TFTGETALLSUGS, index);
            return BuilderFactory.BYTE_ARRAY_LIST.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }


}
