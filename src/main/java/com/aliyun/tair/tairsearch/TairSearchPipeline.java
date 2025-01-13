package com.aliyun.tair.tairsearch;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.jedis3.Jedis3BuilderFactory;
import com.aliyun.tair.tairsearch.action.search.SearchResponse;
import com.aliyun.tair.tairsearch.factory.SearchBuilderFactory;
import com.aliyun.tair.tairsearch.params.*;
import com.aliyun.tair.tairsearch.search.builder.SearchSourceBuilder;
import com.aliyun.tair.util.JoinParameters;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.CommandArguments;
import redis.clients.jedis.CommandObject;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static redis.clients.jedis.Protocol.toByteArray;

public class TairSearchPipeline extends Pipeline {
    public TairSearchPipeline(Jedis jedis) {
        super(jedis);
    }

    public Response<String> tftmappingindex(String key, String request) {
        return tftmappingindex(SafeEncoder.encode(key), SafeEncoder.encode(request));
    }

    public Response<String> tftmappingindex(byte[] key, byte[] request) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTMAPPINGINDEX)
            .key(key)
            .add(request), BuilderFactory.STRING));
    }

    public Response<String> tftcreateindex(String key, String request) {
        return tftcreateindex(SafeEncoder.encode(key), SafeEncoder.encode(request));
    }

    public Response<String> tftcreateindex(byte[] key, byte[] request) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTCREATEINDEX)
            .key(key)
            .add(request), BuilderFactory.STRING));
    }

    public Response<String> tftupdateindex(String index, String request) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTUPDATEINDEX)
            .key(index)
            .add(request), BuilderFactory.STRING));
    }

    public Response<String> tftupdateindex(byte[] index, byte[] request) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTUPDATEINDEX)
            .key(index)
            .add(request), BuilderFactory.STRING));
    }

    @Deprecated
    public Response<String> tftgetindexmappings(String key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTGETINDEX)
            .key(key)
            .add("mappings"), BuilderFactory.STRING));
    }

    @Deprecated
    public Response<String> tftgetindexmappings(byte[] key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTGETINDEX)
            .key(key)
            .add("mappings"), BuilderFactory.STRING));
    }

    public Response<String> tftgetindex(String index) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTGETINDEX)
            .key(index), BuilderFactory.STRING));
    }

    public Response<String> tftgetindex(byte[] index) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTGETINDEX)
            .key(index), BuilderFactory.STRING));
    }

    public Response<String> tftgetindex(String index, final TFTGetIndexParams params) {
        return tftgetindex(SafeEncoder.encode(index), params);
    }

    public Response<String> tftgetindex(byte[] index, final TFTGetIndexParams params) {
        if (params.getParams() == null) {
            return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTGETINDEX)
                .key(index), BuilderFactory.STRING));
        } else {
            return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTGETINDEX)
                .key(index)
                .add(params.getParams()), BuilderFactory.STRING));
        }
    }

    public Response<String> tftadddoc(String key, String request) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTADDDOC)
            .key(key)
            .add(request), BuilderFactory.STRING));
    }

    public Response<String> tftadddoc(byte[] key, byte[] request) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTADDDOC)
            .key(key)
            .add(request), BuilderFactory.STRING));
    }

    public Response<String> tftadddoc(String key, String request, String docId) {
        return tftadddoc(SafeEncoder.encode(key), SafeEncoder.encode(request), SafeEncoder.encode(docId));
    }

    public Response<String> tftadddoc(byte[] key, byte[] request, byte[] docId) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTADDDOC)
            .key(key)
            .add(request)
            .add("WITH_ID")
            .add(docId), BuilderFactory.STRING));
    }

    @Deprecated
    public Response<String> tftmadddoc(String key, Map<String /* docId */, String /* docContent */> docs) {
        TFTAddDocParams params = new TFTAddDocParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTMADDDOC)
            .addObjects(params.getByteParams(key, docs)), BuilderFactory.STRING));
    }

    @Deprecated
    public Response<String> tftmadddoc(byte[] key, Map<byte[] /* docId */, byte[] /* docContent */> docs) {
        TFTAddDocParams params = new TFTAddDocParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTMADDDOC)
            .addObjects(params.getByteParams(key, docs)), BuilderFactory.STRING));
    }

    public Response<String> tftmadddoc(String key, List<DocInfo> docs) {
        TFTMaddDocParams params = new TFTMaddDocParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTMADDDOC)
            .addObjects(params.getByteParams(key, docs)), BuilderFactory.STRING));
    }

    public Response<String> tftmadddoc(byte[] key, List<DocInfoByte> docs) {
        TFTMaddDocParams params = new TFTMaddDocParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTMADDDOC)
            .addObjects(params.getByteParams(key, docs)), BuilderFactory.STRING));
    }

    @Deprecated
    public Response<String> tftupdatedoc(String index, String docId, String docContent) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTUPDATEDOC)
            .key(index)
            .add(docId)
            .add(docContent), BuilderFactory.STRING));
    }

    @Deprecated
    public Response<String> tftupdatedoc(byte[] index, byte[] docId, byte[] docContent) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTUPDATEDOC)
            .key(index)
            .add(docId)
            .add(docContent), BuilderFactory.STRING));
    }

    public Response<String> tftupdatedocfield(String index, String docId, String docContent) {
        return tftupdatedoc(SafeEncoder.encode(index), SafeEncoder.encode(docId), SafeEncoder.encode(docContent));
    }

    public Response<String> tftupdatedocfield(byte[] index, byte[] docId, byte[] docContent) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTUPDATEDOCFIELD)
            .key(index)
            .add(docId)
            .add(docContent), BuilderFactory.STRING));
    }

    public Response<Long> tftincrlongdocfield(String index, String docId, final String field, final long value) {
        return tftincrlongdocfield(SafeEncoder.encode(index), SafeEncoder.encode(docId), SafeEncoder.encode(field),
            value);
    }

    public Response<Long> tftincrlongdocfield(byte[] index, byte[] docId, byte[] field, long value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTINCRLONGDOCFIELD)
            .key(index)
            .add(docId)
            .add(field)
            .add(value), BuilderFactory.LONG));
    }

    public Response<Double> tftincrfloatdocfield(String index, String docId, final String field, final double value) {
        return tftincrfloatdocfield(SafeEncoder.encode(index), SafeEncoder.encode(docId), SafeEncoder.encode(field),
            value);
    }

    public Response<Double> tftincrfloatdocfield(byte[] index, byte[] docId, byte[] field, double value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTINCRFLOATDOCFIELD)
            .key(index)
            .add(docId)
            .add(field)
            .add(value), BuilderFactory.DOUBLE));
    }

    public Response<Long> tftdeldocfield(String index, String docId, final String... field) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTDELDOCFIELD)
            .key(index)
            .add(docId)
            .addObjects((Object[])field), BuilderFactory.LONG));
    }

    public Response<Long> tftdeldocfield(byte[] index, byte[] docId, byte[]... field) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTDELDOCFIELD)
            .key(index)
            .add(docId)
            .addObjects((Object[])field), BuilderFactory.LONG));
    }

    public Response<String> tftgetdoc(String key, String docId) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTGETDOC)
            .key(key)
            .add(docId), BuilderFactory.STRING));
    }

    public Response<String> tftgetdoc(byte[] key, byte[] docId) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTGETDOC)
            .key(key)
            .add(docId), BuilderFactory.STRING));
    }

    public Response<String> tftgetdoc(String key, String docId, String request) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTGETDOC)
            .key(key)
            .add(docId)
            .add(request), BuilderFactory.STRING));
    }

    public Response<String> tftgetdoc(byte[] key, byte[] docId, byte[] request) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTGETDOC)
            .key(key)
            .add(docId)
            .add(request), BuilderFactory.STRING));
    }

    public Response<String> tftdeldoc(String key, String... docId) {
        TFTDelDocParams params = new TFTDelDocParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTDELDOC)
            .addObjects(params.getByteParams(key, docId)), BuilderFactory.STRING));
    }

    public Response<String> tftdeldoc(byte[] key, byte[]... docId) {
        TFTDelDocParams params = new TFTDelDocParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTDELDOC)
            .addObjects(params.getByteParams(key, docId)), BuilderFactory.STRING));
    }

    public Response<String> tftdelall(String index) {
        return tftdelall(SafeEncoder.encode(index));
    }

    public Response<String> tftdelall(byte[] index) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTDELALL)
            .key(index), BuilderFactory.STRING));
    }

    public Response<SearchResponse> tftsearch(String key, SearchSourceBuilder ssb) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTSEARCH)
            .key(key)
            .add(ssb.toString()), SearchBuilderFactory.SEARCH_RESPONSE));
    }

    public Response<SearchResponse> tftsearch(byte[] key, SearchSourceBuilder ssb) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTSEARCH)
            .key(key)
            .add(ssb.toString()), SearchBuilderFactory.SEARCH_RESPONSE));
    }

    public Response<SearchResponse> tftsearch(String key, SearchSourceBuilder ssb, boolean use_cache) {
        return tftsearch(SafeEncoder.encode(key), ssb, use_cache);
    }

    public Response<SearchResponse> tftsearch(byte[] key, SearchSourceBuilder ssb, boolean use_cache) {
        CommandArguments args = new CommandArguments(ModuleCommand.TFTSEARCH)
            .key(key)
            .add(ssb.toString());
        if (use_cache) {
            args.add("use_cache");
        }
        return appendCommand(new CommandObject<>(args, SearchBuilderFactory.SEARCH_RESPONSE));
    }

    public Response<String> tftsearch(String key, String request) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTSEARCH)
            .key(key)
            .add(request), BuilderFactory.STRING));
    }

    public Response<String> tftsearch(byte[] key, byte[] request) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTSEARCH)
            .key(key)
            .add(request), BuilderFactory.STRING));
    }

    public Response<String> tftsearch(String key, String request, boolean use_cache) {
        return tftsearch(SafeEncoder.encode(key), SafeEncoder.encode(request), use_cache);
    }

    public Response<String> tftsearch(byte[] key, byte[] request, boolean use_cache) {
        CommandArguments args = new CommandArguments(ModuleCommand.TFTSEARCH)
            .key(key)
            .add(request);
        if (use_cache) {
            args.add("use_cache");
        }
        return appendCommand(new CommandObject<>(args, BuilderFactory.STRING));
    }

    public Response<String> tftmsearch(String request, String... indexes) {
        TFTMSearchParams params = new TFTMSearchParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTMSEARCH)
            .addObjects(params.getByteParams(request, indexes)), BuilderFactory.STRING));
    }

    public Response<String> tftmsearch(byte[] request, byte[]... indexes) {
        TFTMSearchParams params = new TFTMSearchParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTMSEARCH)
            .addObjects(params.getByteParams(request, indexes)), BuilderFactory.STRING));
    }

    public Response<Long> tftexists(String index, String docId) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTEXISTS)
            .key(index)
            .add(docId), BuilderFactory.LONG));
    }

    public Response<Long> tftexists(byte[] index, byte[] docId) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTEXISTS)
            .key(index)
            .add(docId), BuilderFactory.LONG));
    }

    public Response<Long> tftdocnum(String index) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTDOCNUM)
            .key(index), BuilderFactory.LONG));
    }

    public Response<Long> tftdocnum(byte[] index) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTDOCNUM)
            .key(index), BuilderFactory.LONG));
    }

    public Response<String> tftanalyzer(String index_name, String text) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTANALYZER)
            .key(index_name)
            .add(text), BuilderFactory.STRING));
    }

    public Response<String> tftanalyzer(byte[] index_name, byte[] text) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTANALYZER)
            .key(index_name)
            .add(text), BuilderFactory.STRING));
    }

    public Response<String> tftanalyzer(String index_name, String text, final TFTAnalyzerParams params) {
        return tftanalyzer(SafeEncoder.encode(index_name), SafeEncoder.encode(text), params);
    }

    public Response<String> tftanalyzer(byte[] index_name, byte[] text, final TFTAnalyzerParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTANALYZER)
            .addObjects(params.getByteParams(index_name, text)), BuilderFactory.STRING));
    }

    public Response<String> tftexplaincost(String index, SearchSourceBuilder ssb) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTEXPLAINCOST)
            .key(index)
            .add(ssb.toString()), BuilderFactory.STRING));
    }

    public Response<String> tftexplaincost(byte[] index, SearchSourceBuilder ssb) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTEXPLAINCOST)
            .key(index)
            .add(ssb.toString()), BuilderFactory.STRING));
    }

    public Response<String> tftexplaincost(String index, String request) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTEXPLAINCOST)
            .key(index)
            .add(request), BuilderFactory.STRING));
    }

    public Response<String> tftexplaincost(byte[] index, byte[] request) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTEXPLAINCOST)
            .key(index)
            .add(request), BuilderFactory.STRING));
    }

    public Response<String> tftexplainscore(String index, SearchSourceBuilder ssb, String... docId) {
        return tftexplainscore(index, ssb.toString(), docId);
    }

    public Response<String> tftexplainscore(byte[] index, SearchSourceBuilder ssb, byte[]... docId) {
        return tftexplainscore(index, SafeEncoder.encode(ssb.toString()), docId);
    }

    public Response<String> tftexplainscore(String index, String request, String... docId) {
        TFTExplainScoreParams params = new TFTExplainScoreParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTEXPLAINSCORE)
            .addObjects(params.getByteParams(index, request, docId)), BuilderFactory.STRING));
    }

    public Response<String> tftexplainscore(byte[] index, byte[] request, byte[]... docId) {
        TFTExplainScoreParams params = new TFTExplainScoreParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTEXPLAINSCORE)
            .addObjects(params.getByteParams(index, request, docId)), BuilderFactory.STRING));
    }

    /**
     * Add suggestions in index.
     *
     * @param index the index name
     * @param texts the suggestions and their weight
     * @return Success:  Number of successfully added suggestions; Fail: error.
     */
    public Response<Long> tftaddsug(String index, Map<String /* suggestion */, Integer /* weight */> texts) {
        TFTAddSugParams params = new TFTAddSugParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTADDSUG)
            .addObjects(params.getByteParams(index, texts)), BuilderFactory.LONG));
    }

    public Response<Long> tftaddsug(byte[] index, Map<byte[] /* suggestion */, Integer /* weight */> texts) {
        TFTAddSugParams params = new TFTAddSugParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTADDSUG)
            .addObjects(params.getByteParams(index, texts)), BuilderFactory.LONG));
    }

    /**
     * Delete the specified suggestions from the index.
     *
     * @param index the index name
     * @param text  the suggestions
     * @return Success: Number of successfully deleted suggestions; Fail: error
     */
    public Response<Long> tftdelsug(String index, String... text) {
        return tftdelsug(SafeEncoder.encode(index), SafeEncoder.encodeMany(text));
    }

    public Response<Long> tftdelsug(byte[] index, byte[]... text) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTDELSUG)
            .key(index)
            .addObjects((Object[])text), BuilderFactory.LONG));
    }

    /**
     * Gets the number of suggestions in index.
     *
     * @param index the index name
     * @return the number of autocomplete texts.
     */
    public Response<Long> tftsugnum(String index) {
        return tftsugnum(SafeEncoder.encode(index));
    }

    public Response<Long> tftsugnum(byte[] index) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTSUGNUM)
            .key(index), BuilderFactory.LONG));
    }

    /**
     * Get suggestions in index according to prefix query.
     *
     * @param index  the index name
     * @param prefix the prefix
     * @return List of the suggestions in index.
     */
    public Response<List<String>> tftgetsug(String index, String prefix) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTGETSUG)
            .key(index)
            .add(prefix), BuilderFactory.STRING_LIST));
    }

    public Response<List<byte[]>> tftgetsug(byte[] index, byte[] prefix) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTGETSUG)
            .key(index)
            .add(prefix), Jedis3BuilderFactory.BYTE_ARRAY_LIST));
    }

    /**
     * Get suggestions in index according to prefix query.
     *
     * @param index  the index name
     * @param prefix the prefix
     * @param params the get parameters. For example the max number of suggestions returned and use fuzzy query
     * @return List of the suggestions in index.
     */
    public Response<List<String>> tftgetsug(String index, String prefix, final TFTGetSugParams params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(SafeEncoder.encode(index));
        args.add(SafeEncoder.encode(prefix));
        args.addAll(params.getParams());
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTGETSUG)
            .addObjects(args), BuilderFactory.STRING_LIST));
    }

    public Response<List<byte[]>> tftgetsug(byte[] index, byte[] prefix, final TFTGetSugParams params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(index);
        args.add(prefix);
        args.addAll(params.getParams());
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTGETSUG)
            .addObjects(args), Jedis3BuilderFactory.BYTE_ARRAY_LIST));
    }

    /**
     * Get all suggestions in index.
     *
     * @param index the index name
     * @return List of the all suggestions in index.
     */
    public Response<List<String>> tftgetallsugs(String index) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTGETALLSUGS)
            .key(index), BuilderFactory.STRING_LIST));
    }

    public Response<List<byte[]>> tftgetallsugs(byte[] index) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TFTGETALLSUGS)
            .key(index), Jedis3BuilderFactory.BYTE_ARRAY_LIST));
    }
}
