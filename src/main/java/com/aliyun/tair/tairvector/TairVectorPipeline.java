package com.aliyun.tair.tairvector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.jedis3.Jedis3BuilderFactory;
import com.aliyun.tair.tairhash.factory.HashBuilderFactory;
import com.aliyun.tair.tairvector.factory.VectorBuilderFactory;
import com.aliyun.tair.tairvector.params.DistanceMethod;
import com.aliyun.tair.tairvector.params.HscanParams;
import com.aliyun.tair.tairvector.params.IndexAlgorithm;
import com.aliyun.tair.util.JoinParameters;
import io.valkey.BuilderFactory;
import io.valkey.CommandArguments;
import io.valkey.CommandObject;
import io.valkey.Jedis;
import io.valkey.Pipeline;
import io.valkey.Response;
import com.aliyun.tair.jedis3.ScanResult;
import io.valkey.util.SafeEncoder;

import static io.valkey.Protocol.toByteArray;

public class TairVectorPipeline extends Pipeline {
    public TairVectorPipeline(Jedis jedis) {
        super(jedis);
    }

    public void quit() {
        // do nothing
    }

    /**
     * TVS.CREATEINDEX  TVS.CREATEINDEX index_name dims algorithm distance_method  [(attribute_key attribute_value) ...
     * ]
     * <p>
     * create tair-vector index
     *
     * @param index     index name
     * @param dims      vector dims
     * @param algorithm index algorithm
     * @param method    vector distance method
     * @param attrs     other columns, optional
     * @return Success: +OK; Fail: error
     */
    public Response<String> tvscreateindex(final String index, int dims, IndexAlgorithm algorithm,
        DistanceMethod method, final String... attrs) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSCREATEINDEX)
            .addObjects(JoinParameters.joinParameters(SafeEncoder.encode(index), toByteArray(dims),
                SafeEncoder.encode(algorithm.name()), SafeEncoder.encode(method.name()),
                SafeEncoder.encodeMany(attrs))), BuilderFactory.STRING));
    }

    public Response<byte[]> tvscreateindex(byte[] index, int dims, IndexAlgorithm algorithm, DistanceMethod method,
        final byte[]... params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSCREATEINDEX)
            .addObjects(JoinParameters.joinParameters(index, toByteArray(dims), SafeEncoder.encode(algorithm.name()),
                SafeEncoder.encode(method.name()), params)), Jedis3BuilderFactory.BYTE_ARRAY));
    }

    /**
     * TVS.GETINDEX TVS.GETINDEX index_name
     * <p>
     * get index schema info, including: index_name, algorithm, distance_method, data_count, ...
     *
     * @param index index name
     * @return Success: string_map, Fail:  empty
     */
    public Response<Map<String, String>> tvsgetindex(final String index) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSGETINDEX)
            .add(SafeEncoder.encode(index)), BuilderFactory.STRING_MAP));
    }

    public Response<Map<byte[], byte[]>> tvsgetindex(byte[] index) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSGETINDEX)
            .add(index), Jedis3BuilderFactory.BYTE_ARRAY_MAP));
    }

    /**
     * TVS.DELINDEX TVS.DELINDEX index_name
     * <p>
     * delete index
     *
     * @param index index name
     * @return Success: 1; Fail: 0
     */
    public Response<Long> tvsdelindex(final String index) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSDELINDEX)
            .add(SafeEncoder.encode(index)), BuilderFactory.LONG));
    }

    public Response<Long> tvsdelindex(byte[] index) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSDELINDEX)
            .add(index), BuilderFactory.LONG));
    }

    /**
     * TVS.SCANINDEX TVS.SCANINDEX index_name
     * <p>
     * scan index
     *
     * @param cursor start offset
     * @param params the params: [MATCH pattern] [COUNT count]
     *               `MATCH` - Set the pattern which is used to filter the results
     *               `COUNT` - Set the number of fields in a single scan (default is 10)
     *               `NOVAL` - The return result contains no data portion, only cursor information
     * @return A ScanResult. {@link HashBuilderFactory#EXHSCAN_RESULT_STRING}
     */
    public Response<ScanResult<String>> tvsscanindex(Long cursor, HscanParams params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(toByteArray(cursor));
        args.addAll(params.getParams());
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSSCANINDEX)
            .addObjects(args), VectorBuilderFactory.SCAN_CURSOR_STRING));
    }

    /**
     * TVS.HSET TVS.HSET index entityid vector [(attribute_key attribute_value) ...]
     * <p>
     * insert entity into tair-vector module
     *
     * @param index    index name
     * @param entityid entity id
     * @param vector   vector info
     * @param params   scalar attribute key, value
     * @return integer-reply specifically:
     * {@literal k} if success, k is the number of fields that were added..
     * throw error like "(error) Illegal vector dimensions" if error
     */
    public Response<Long> tvshset(final String index, final String entityid, final String vector,
        final String... params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSHSET)
            .addObjects(JoinParameters.joinParameters(SafeEncoder.encode(index), SafeEncoder.encode(entityid),
                SafeEncoder.encode(VectorBuilderFactory.VECTOR_TAG), SafeEncoder.encode(vector),
                SafeEncoder.encodeMany(params))), BuilderFactory.LONG));
    }

    public Response<Long> tvshset(byte[] index, byte[] entityid, byte[] vector, final byte[]... params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSHSET)
            .addObjects(
                JoinParameters.joinParameters(index, entityid, SafeEncoder.encode(VectorBuilderFactory.VECTOR_TAG),
                    vector, params)), BuilderFactory.LONG));
    }

    /**
     * TVS.HGETALL TVS.HGETALL index entityid
     * <p>
     * get entity from tair-vector module
     *
     * @param index    index name
     * @param entityid entity id
     * @return Map, an empty list when {@code entityid} does not exist.
     */
    public Response<Map<String, String>> tvshgetall(final String index, final String entityid) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSHGETALL)
            .add(SafeEncoder.encode(index)).add(SafeEncoder.encode(entityid)), BuilderFactory.STRING_MAP));
    }

    public Response<Map<byte[], byte[]>> tvshgetall(byte[] index, byte[] entityid) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSHGETALL)
            .add(index).add(entityid), Jedis3BuilderFactory.BYTE_ARRAY_MAP));
    }

    /**
     * TVS.HMGETALL TVS.HMGETALL index entityid attribute_key [attribute_key ...]
     * <p>
     * get entity attrs from tair-vector module
     *
     * @param index    index name
     * @param entityid entity id
     * @param attrs    attrs
     * @return List, an empty list when {@code entityid} or {@code attrs} does not exist .
     */
    public Response<List<String>> tvshmget(final String index, final String entityid, final String... attrs) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSHMGET)
            .addObjects(JoinParameters.joinParameters(SafeEncoder.encode(index), SafeEncoder.encode(entityid),
                SafeEncoder.encodeMany(attrs))), BuilderFactory.STRING_LIST));
    }

    public Response<List<byte[]>> tvshmget(byte[] index, byte[] entityid, byte[]... attrs) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSHMGET)
            .addObjects(JoinParameters.joinParameters(index, entityid, attrs)), Jedis3BuilderFactory.BYTE_ARRAY_LIST));
    }

    /**
     * TVS.DEL TVS.DEL index entityid
     * <p>
     * delete entity from tair-vector module
     *
     * @param index    index name
     * @param entityid entity id
     * @return Long integer-reply the number of fields that were removed from the tair-vector
     * not including specified but non existing fields.
     */
    public Response<Long> tvsdel(final String index, final String entityid) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSDEL)
            .add(SafeEncoder.encode(index)).add(SafeEncoder.encode(entityid)), BuilderFactory.LONG));
    }

    public Response<Long> tvsdel(byte[] index, byte[] entityid) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSDEL)
            .add(index).add(entityid), BuilderFactory.LONG));
    }

    /**
     * TVS.DEL TVS.DEL index entityid1 entityi2
     * <p>
     * delete entity from tair-vector module
     *
     * @param index     index name
     * @param entityids entity id
     * @return Long integer-reply the number of fields that were removed from the tair-vector
     * not including specified but non existing fields.
     */
    public Response<Long> tvsdel(final String index, final String... entityids) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSDEL)
            .addObjects(JoinParameters.joinParameters(SafeEncoder.encode(index), SafeEncoder.encodeMany(entityids))),
            BuilderFactory.LONG));
    }

    public Response<Long> tvsdel(byte[] index, byte[]... entityids) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSDEL)
            .addObjects(JoinParameters.joinParameters(index, entityids)), BuilderFactory.LONG));
    }

    /**
     * TVS.HDEL TVS.HDEL index entityid attribute_key [attribute_key ...]
     * <p>
     * delete entity attrs from tair-vector module
     *
     * @param index    index name
     * @param entityid entity id
     * @param attrs    attrs
     * @return Long integer-reply the number of fields that were removed from the tair-vector
     * not including specified but non existing fields.
     */
    public Response<Long> tvshdel(final String index, final String entityid, final String... attrs) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSHDEL)
            .addObjects(JoinParameters.joinParameters(SafeEncoder.encode(index), SafeEncoder.encode(entityid),
                SafeEncoder.encodeMany(attrs))), BuilderFactory.LONG));
    }

    public Response<Long> tvshdel(byte[] index, byte[] entityid, byte[]... attrs) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSHDEL)
            .addObjects(JoinParameters.joinParameters(index, entityid, attrs)), BuilderFactory.LONG));
    }

    /**
     * TVS.SCAN TVS.SCAN index_name cursor [MATCH pattern] [COUNT count]
     * <p>
     * scan entity from tair-vector module
     *
     * @param index  index name
     * @param cursor start offset
     * @param params the params: [MATCH pattern] [COUNT count]
     *               `MATCH` - Set the pattern which is used to filter the results
     *               `COUNT` - Set the number of fields in a single scan (default is 10)
     *               `NOVAL` - The return result contains no data portion, only cursor information
     * @return A ScanResult.
     */
    public Response<ScanResult<String>> tvsscan(final String index, Long cursor, HscanParams params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(SafeEncoder.encode(index));
        args.add(toByteArray(cursor));
        args.addAll(params.getParams());
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSSCAN)
            .addObjects(args), VectorBuilderFactory.SCAN_CURSOR_STRING));
    }

    public Response<ScanResult<byte[]>> tvsscan(byte[] index, Long cursor, HscanParams params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(index);
        args.add(toByteArray(cursor));
        args.addAll(params.getParams());
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSSCAN)
            .addObjects(args), VectorBuilderFactory.SCAN_CURSOR_BYTE));
    }

    /**
     * TVS.KNNSEARCH index_name topn vector
     * <p>
     * query entity by vector
     *
     * @param index  index name
     * @param topn   topn result
     * @param vector query vector
     * @param params for HNSW, params include:
     *               ef_search     range [0, 1000]
     * @return Knn<String>
     */
    public Response<VectorBuilderFactory.Knn<String>> tvsknnsearch(final String index, Long topn, final String vector,
        final String... params) {
        return tvsknnsearchfilter(index, topn, vector, "", params);
    }

    public Response<VectorBuilderFactory.Knn<byte[]>> tvsknnsearch(byte[] index, Long topn, byte[] vector,
        final byte[]... params) {
        return tvsknnsearchfilter(index, topn, vector, SafeEncoder.encode(""), params);
    }

    /**
     * TVS.KNNSEARCH index_name topn vector pattern
     * <p>
     * query entity by vector and scalar pattern
     *
     * @param index   index name
     * @param topn    topn result
     * @param vector  query vector
     * @param pattern support +, -，>, <, !=， ,()，&&, ||, !, ==
     * @param params  for HNSW, params include:
     *                ef_search     range [0, 1000]
     * @return VectorBuilderFactory.Knn<>
     */
    public Response<VectorBuilderFactory.Knn<String>> tvsknnsearchfilter(final String index, Long topn,
        final String vector, final String pattern, final String... params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSKNNSEARCH)
            .addObjects(JoinParameters.joinParameters(SafeEncoder.encode(index), toByteArray(topn), SafeEncoder.encode(vector), SafeEncoder.encode(pattern), SafeEncoder.encodeMany(params))),
            VectorBuilderFactory.STRING_KNN_RESULT));
    }

    public Response<VectorBuilderFactory.Knn<byte[]>> tvsknnsearchfilter(byte[] index, Long topn, byte[] vector,
        byte[] pattern, final byte[]... params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSKNNSEARCH)
            .addObjects(JoinParameters.joinParameters(index, toByteArray(topn), vector, pattern, params)),
            VectorBuilderFactory.BYTE_KNN_RESULT));
    }

    /**
     * TVS.KNNSEARCHFIELD index_name topn vector fields
     * <p>
     * query entity by vector
     *
     * @param index  index name
     * @param topn   topn result
     * @param vector query vector
     * @param fields field list
     * @param params for HNSW, params include:
     *               ef_search     range [0, 1000]
     * @return VectorBuilderFactory.Knn<>
     */
    public Response<VectorBuilderFactory.KnnField<String>> tvsknnsearchfield(final String index, Long topn,
        final String vector, Collection<String> fields, final String... params) {
        return tvsknnsearchfilterfield(index, topn, vector, fields, "", params);
    }

    public Response<VectorBuilderFactory.KnnField<byte[]>> tvsknnsearchfield(byte[] index, Long topn, byte[] vector,
        Collection<byte[]> fields, final byte[]... params) {
        return tvsknnsearchfilterfield(index, topn, vector, fields, SafeEncoder.encode(""), params);
    }

    /**
     * TVS.KNNSEARCHFIELD index_name topn vector pattern
     * <p>
     * query entity by vector and scalar pattern
     *
     * @param index   index name
     * @param topn    topn result
     * @param vector  query vector
     * @param fields  field list
     * @param pattern support +, -，>, <, !=， ,()，&&, ||, !, ==
     * @param params  for HNSW, params include:
     *                ef_search     range [0, 1000]
     * @return VectorBuilderFactory.Knn<>
     */
    public Response<VectorBuilderFactory.KnnField<String>> tvsknnsearchfilterfield(final String index, Long topn,
        final String vector, Collection<String> fields, final String pattern, final String... params) {
        final List<byte[]> args = new ArrayList<>();
        args.add(SafeEncoder.encode(index));
        args.add(toByteArray(topn));
        args.add(SafeEncoder.encode(vector));
        args.add(toByteArray(fields.size()));
        if (!fields.isEmpty()) {
            args.addAll(fields.stream().map(SafeEncoder::encode).collect(Collectors.toList()));
        }
        args.add(SafeEncoder.encode(pattern));
        args.addAll(Arrays.stream(params).map(SafeEncoder::encode).collect(Collectors.toList()));
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSKNNSEARCHFIELD)
            .addObjects(args), VectorBuilderFactory.STRING_KNNFIELD_RESULT));
    }

    public Response<VectorBuilderFactory.KnnField<byte[]>> tvsknnsearchfilterfield(byte[] index, Long topn,
        byte[] vector, Collection<byte[]> fields, byte[] pattern, final byte[]... params) {
        final List<byte[]> args = new ArrayList<>();
        args.add(index);
        args.add(toByteArray(topn));
        args.add(vector);
        args.add(toByteArray(fields.size()));
        if (!fields.isEmpty()) {
            args.addAll(fields);
        }
        args.add(pattern);
        args.addAll(Arrays.stream(params).collect(Collectors.toList()));
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSKNNSEARCHFIELD)
            .addObjects(args), VectorBuilderFactory.BYTE_KNNFIELD_RESULT));
    }

    /**
     * TVS.MKNNSEARCH TVS.MKNNSEARCH index_name topn vector [vector...]
     *
     * @param index   index name
     * @param topn    topn for each vector
     * @param vectors vector list
     * @param params  for HNSW, params include:
     *                ef_search     range [0, 1000]
     * @return Collection<></>
     */
    public Response<Collection<VectorBuilderFactory.Knn<String>>> tvsmknnsearch(final String index, Long topn,
        Collection<String> vectors, final String... params) {
        return tvsmknnsearchfilter(index, topn, vectors, "", params);
    }

    public Response<Collection<VectorBuilderFactory.Knn<byte[]>>> tvsmknnsearch(byte[] index, Long topn,
        Collection<byte[]> vectors, final byte[]... params) {
        return tvsmknnsearchfilter(index, topn, vectors, SafeEncoder.encode(""), params);
    }

    /**
     * TVS.MKNNSEARCH TVS.MKNNSEARCH index_name topn vector [vector...] pattern
     *
     * @param index   index name
     * @param topn    topn for each vector
     * @param vectors vector list
     * @param pattern filter pattern
     * @param params  for HNSW, params include:
     *                ef_search     range [0, 1000]
     * @return Collection<></>
     */
    public Response<Collection<VectorBuilderFactory.Knn<String>>> tvsmknnsearchfilter(final String index, Long topn,
        Collection<String> vectors, final String pattern, final String... params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(SafeEncoder.encode(index));
        args.add(toByteArray(topn));
        args.add(toByteArray(vectors.size()));
        args.addAll(vectors.stream().map(vector -> SafeEncoder.encode(vector)).collect(Collectors.toList()));
        args.add(SafeEncoder.encode(pattern));
        args.addAll(Arrays.stream(params).map(str -> SafeEncoder.encode(str)).collect(Collectors.toList()));
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSMKNNSEARCH)
            .addObjects(args), VectorBuilderFactory.STRING_KNN_BATCH_RESULT));
    }

    public Response<Collection<VectorBuilderFactory.Knn<byte[]>>> tvsmknnsearchfilter(byte[] index, Long topn,
        Collection<byte[]> vectors, byte[] pattern, final byte[]... params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(index);
        args.add(toByteArray(topn));
        args.add(toByteArray(vectors.size()));
        args.addAll(vectors);
        args.add(pattern);
        args.addAll(Arrays.stream(params).collect(Collectors.toList()));
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSMKNNSEARCH)
            .addObjects(args), VectorBuilderFactory.BYTE_KNN_BATCH_RESULT));
    }

    public Response<VectorBuilderFactory.Knn<String>> tvsmindexknnsearch(Collection<String> indexs, Long topn,
        String vector, String... params) {
        return tvsmindexknnsearchfilter(indexs, topn, vector, "", params);
    }

    public Response<VectorBuilderFactory.Knn<byte[]>> tvsmindexknnsearch(Collection<byte[]> indexs, Long topn,
        byte[] vector, byte[]... params) {
        return tvsmindexknnsearchfilter(indexs, topn, vector, SafeEncoder.encode(""), params);
    }

    public Response<VectorBuilderFactory.Knn<String>> tvsmindexknnsearchfilter(Collection<String> indexs, Long topn,
        String vector, String pattern, String... params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(toByteArray(indexs.size()));
        args.addAll(indexs.stream().map(index -> SafeEncoder.encode(index)).collect(Collectors.toList()));
        args.add(toByteArray(topn));
        args.add(SafeEncoder.encode(vector));
        args.add(SafeEncoder.encode(pattern));
        args.addAll(Arrays.stream(params).map(str -> SafeEncoder.encode(str)).collect(Collectors.toList()));
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSMINDEXKNNSEARCH)
            .addObjects(args), VectorBuilderFactory.STRING_KNN_RESULT));
    }

    public Response<VectorBuilderFactory.Knn<byte[]>> tvsmindexknnsearchfilter(Collection<byte[]> indexs, Long topn,
        byte[] vector, byte[] pattern, final byte[]... params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(toByteArray(indexs.size()));
        args.addAll(indexs);
        args.add(toByteArray(topn));
        args.add(vector);
        args.add(pattern);
        args.addAll(Arrays.stream(params).collect(Collectors.toList()));
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSMINDEXKNNSEARCH)
            .addObjects(args), VectorBuilderFactory.BYTE_KNN_RESULT));
    }

    public Response<VectorBuilderFactory.KnnField<String>> tvsmindexknnsearchField(Collection<String> indexs, Long topn,
        String vector, Collection<String> fields, String... params) {
        return tvsmindexknnsearchfilterfield(indexs, topn, vector, fields, "", params);
    }

    public Response<VectorBuilderFactory.KnnField<byte[]>> tvsmindexknnsearchField(Collection<byte[]> indexs, Long topn,
        byte[] vector, Collection<byte[]> fields, byte[]... params) {
        return tvsmindexknnsearchfilterfield(indexs, topn, vector, fields, SafeEncoder.encode(""), params);
    }

    public Response<VectorBuilderFactory.KnnField<String>> tvsmindexknnsearchfilterfield(Collection<String> indexs,
        Long topn, String vector, Collection<String> fields, String pattern, String... params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(toByteArray(indexs.size()));
        args.addAll(indexs.stream().map(SafeEncoder::encode).collect(Collectors.toList()));
        args.add(toByteArray(topn));
        args.add(SafeEncoder.encode(vector));
        args.add(toByteArray(fields.size()));
        if (!fields.isEmpty()) {
            args.addAll(fields.stream().map(SafeEncoder::encode).collect(Collectors.toList()));
        }
        args.add(SafeEncoder.encode(pattern));
        args.addAll(Arrays.stream(params).map(SafeEncoder::encode).collect(Collectors.toList()));
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSMINDEXKNNSEARCHFIELD)
            .addObjects(args), VectorBuilderFactory.STRING_KNNFIELD_RESULT));
    }

    public Response<VectorBuilderFactory.KnnField<byte[]>> tvsmindexknnsearchfilterfield(Collection<byte[]> indexs,
        Long topn, byte[] vector, Collection<byte[]> fields, byte[] pattern, final byte[]... params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(toByteArray(indexs.size()));
        args.addAll(indexs);
        args.add(toByteArray(topn));
        args.add(vector);
        args.add(toByteArray(fields.size()));
        if (!fields.isEmpty()) {
            args.addAll(fields);
        }
        args.add(pattern);
        args.addAll(Arrays.stream(params).collect(Collectors.toList()));
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSMINDEXKNNSEARCHFIELD)
            .addObjects(args), VectorBuilderFactory.BYTE_KNNFIELD_RESULT));
    }

    public Response<Collection<VectorBuilderFactory.Knn<String>>> tvsmindexmknnsearch(Collection<String> indexs,
        Long topn, Collection<String> vectors, String... params) {
        return tvsmindexmknnsearchfilter(indexs, topn, vectors, "", params);
    }

    public Response<Collection<VectorBuilderFactory.Knn<byte[]>>> tvsmindexmknnsearch(Collection<byte[]> indexs,
        Long topn, Collection<byte[]> vectors, byte[]... params) {
        return tvsmindexmknnsearchfilter(indexs, topn, vectors, SafeEncoder.encode(""), params);
    }

    public Response<Collection<VectorBuilderFactory.Knn<String>>> tvsmindexmknnsearchfilter(Collection<String> indexs,
        Long topn, Collection<String> vectors, String pattern, String... params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(toByteArray(indexs.size()));
        args.addAll(indexs.stream().map(index -> SafeEncoder.encode(index)).collect(Collectors.toList()));
        args.add(toByteArray(topn));
        args.add(toByteArray(vectors.size()));
        args.addAll(vectors.stream().map(vector -> SafeEncoder.encode(vector)).collect(Collectors.toList()));
        args.add(SafeEncoder.encode(pattern));
        args.addAll(Arrays.stream(params).map(str -> SafeEncoder.encode(str)).collect(Collectors.toList()));
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSMINDEXMKNNSEARCH)
            .addObjects(args), VectorBuilderFactory.STRING_KNN_BATCH_RESULT));
    }

    public Response<Collection<VectorBuilderFactory.Knn<byte[]>>> tvsmindexmknnsearchfilter(Collection<byte[]> indexs,
        Long topn, Collection<byte[]> vectors, byte[] pattern, byte[]... params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(toByteArray(indexs.size()));
        args.addAll(indexs);
        args.add(toByteArray(topn));
        args.add(toByteArray(vectors.size()));
        args.addAll(vectors);
        args.add(pattern);
        args.addAll(Arrays.stream(params).collect(Collectors.toList()));
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSMINDEXMKNNSEARCH)
            .addObjects(args), VectorBuilderFactory.BYTE_KNN_BATCH_RESULT));
    }

    /**
     * TVS.HINCRBY index entityid field value
     * Increment the long value of a tairvector field by the given amount, not support VECTOR
     *
     * @param index    index name
     * @param entityid entity id
     * @param field    the field type: key
     * @param value    the increment type: long
     * @return Long integer-reply the value at {@code field} after the increment operation.
     */
    public Response<Long> tvshincrby(final String index, final String entityid, final String field, final long value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSHINCRBY)
            .add(SafeEncoder.encode(index))
            .add(SafeEncoder.encode(entityid))
            .add(SafeEncoder.encode(field))
            .add(toByteArray(value)), BuilderFactory.LONG));
    }

    public Response<Long> tvshincrby(byte[] index, byte[] entityid, byte[] field, long value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSHINCRBY)
            .add(index)
            .add(entityid)
            .add(field)
            .add(toByteArray(value)), BuilderFactory.LONG));
    }

    /**
     * TVS.HINCRBYFOLAT index entityid field value
     * Increment the float value of a tairvector field by the given amount, not support VECTOR
     *
     * @param index    index name
     * @param entityid entity id
     * @param field    the field type: key
     * @param value    the increment type: double
     * @return Double bulk-string-reply the value of {@code field} after the increment.
     */
    public Response<Double> tvshincrbyfloat(final String index, final String entityid, final String field,
        final double value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSHINCRBYFLOAT)
            .add(SafeEncoder.encode(index))
            .add(SafeEncoder.encode(entityid))
            .add(SafeEncoder.encode(field))
            .add(toByteArray(value)), BuilderFactory.DOUBLE));
    }

    public Response<Double> tvshincrbyfloat(byte[] index, byte[] entityid, byte[] field, double value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSHINCRBYFLOAT)
            .add(index)
            .add(entityid)
            .add(field)
            .add(toByteArray(value)), BuilderFactory.DOUBLE));
    }

    public Response<VectorBuilderFactory.Knn<String>> tvsgetdistance(String index, String vector,
        Collection<String> keys, Long topn, Float max_dist, String filter) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(SafeEncoder.encode(index));
        args.add(SafeEncoder.encode(vector));
        args.add(toByteArray(keys.size()));
        args.addAll(keys.stream().map(key -> SafeEncoder.encode(key)).collect(Collectors.toList()));
        if (topn != null) {
            args.add(SafeEncoder.encode("TOPN"));
            args.add(toByteArray(topn));
        }
        if (max_dist != null) {
            args.add(SafeEncoder.encode("MAX_DIST"));
            args.add(toByteArray(max_dist));
        }
        if (filter != null) {
            args.add(SafeEncoder.encode("FILTER"));
            args.add(SafeEncoder.encode(filter));
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSGETDISTANCE)
            .addObjects(args), VectorBuilderFactory.STRING_KNN_RESULT));
    }

    public Response<VectorBuilderFactory.Knn<byte[]>> tvsgetdistance(byte[] index, byte[] vector,
        Collection<byte[]> keys, Long topn, Float max_dist, byte[] filter) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(index);
        args.add(vector);
        args.add(toByteArray(keys.size()));
        args.addAll(keys);
        if (topn != null) {
            args.add(SafeEncoder.encode("TOPN"));
            args.add(toByteArray(topn));
        }
        if (max_dist != null) {
            args.add(SafeEncoder.encode("MAX_DIST"));
            args.add(toByteArray(max_dist));
        }
        if (filter != null) {
            args.add(SafeEncoder.encode("FILTER"));
            args.add(filter);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSGETDISTANCE)
            .addObjects(args), VectorBuilderFactory.BYTE_KNN_RESULT));
    }

    /**
     * Set expire time (seconds).
     *
     * @param index   index name
     * @param key     the key
     * @param seconds time is seconds
     * @return Success: true, fail: false.
     */
    public Response<Boolean> tvshexpire(final String index, final String key, final int seconds) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSHEXPIRE)
            .add(SafeEncoder.encode(index))
            .add(SafeEncoder.encode(key))
            .add(toByteArray(seconds)), BuilderFactory.BOOLEAN));
    }

    public Response<Boolean> tvshexpire(final byte[] index, final byte[] key, final int seconds) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSHEXPIRE)
            .add(index)
            .add(key)
            .add(toByteArray(seconds)), BuilderFactory.BOOLEAN));
    }

    /**
     * Set expire time (seconds).
     *
     * @param index        index name
     * @param key          the key
     * @param milliseconds time is milliseconds
     * @return Success: true, fail: false.
     */
    public Response<Boolean> tvshpexpire(final String index, final String key, final int milliseconds) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSHPEXPIRE)
            .add(SafeEncoder.encode(index))
            .add(SafeEncoder.encode(key))
            .add(toByteArray(milliseconds)), BuilderFactory.BOOLEAN));
    }

    public Response<Boolean> tvshpexpire(final byte[] index, final byte[] key, final int milliseconds) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSHPEXPIRE)
            .add(index)
            .add(key)
            .add(toByteArray(milliseconds)), BuilderFactory.BOOLEAN));
    }

    /**
     * Set the expiration for a key as a UNIX timestamp (seconds).
     *
     * @param index    the index name
     * @param key      the key
     * @param unixTime timestamp the timestamp type: posix time, time is seconds
     * @return Success: true, fail: false.
     */
    public Response<Boolean> tvshexpireAt(final String index, final String key, final long unixTime) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSHEXPIREAT)
            .add(SafeEncoder.encode(index))
            .add(SafeEncoder.encode(key))
            .add(toByteArray(unixTime)), BuilderFactory.BOOLEAN));
    }

    public Response<Boolean> tvshexpireAt(final byte[] index, final byte[] key, final long unixTime) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSHEXPIREAT)
            .add(index)
            .add(key)
            .add(toByteArray(unixTime)), BuilderFactory.BOOLEAN));
    }

    /**
     * Set the expiration for a key as a UNIX timestamp (milliseconds).
     *
     * @param index    the index name
     * @param key      the key
     * @param unixTime timestamp the timestamp type: posix time, time is milliseconds
     * @return Success: true, fail: false.
     */
    public Response<Boolean> tvshpexpireAt(final String index, final String key, final long unixTime) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSHPEXPIREAT)
            .add(SafeEncoder.encode(index))
            .add(SafeEncoder.encode(key))
            .add(toByteArray(unixTime)), BuilderFactory.BOOLEAN));
    }

    public Response<Boolean> tvshpexpireAt(final byte[] index, final byte[] key, final long unixTime) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSHPEXPIREAT)
            .add(index)
            .add(key)
            .add(toByteArray(unixTime)), BuilderFactory.BOOLEAN));
    }

    /**
     * Get ttl (seconds).
     *
     * @param index index name
     * @param key   the key
     * @return ttl
     */
    public Response<Long> tvshttl(final String index, final String key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSHTTL)
            .add(SafeEncoder.encode(index))
            .add(SafeEncoder.encode(key)), BuilderFactory.LONG));
    }

    public Response<Long> tvshttl(final byte[] index, final byte[] key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSHTTL)
            .add(index)
            .add(key), BuilderFactory.LONG));
    }

    /**
     * Get ttl (milliseconds).
     *
     * @param index index name
     * @param key   the key
     * @return ttl
     */
    public Response<Long> tvshpttl(final String index, final String key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSHPTTL)
            .add(SafeEncoder.encode(index))
            .add(SafeEncoder.encode(key)), BuilderFactory.LONG));
    }

    public Response<Long> tvshpttl(final byte[] index, final byte[] key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSHPTTL)
            .add(index)
            .add(key), BuilderFactory.LONG));
    }

    /**
     * Get abs expire time  (seconds).
     *
     * @param index index name
     * @param key   the key
     * @return abs expire time
     */
    public Response<Long> tvshexpiretime(final String index, final String key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSHEXPIRETIME)
            .add(SafeEncoder.encode(index))
            .add(SafeEncoder.encode(key)), BuilderFactory.LONG));
    }

    public Response<Long> tvshexpiretime(final byte[] index, final byte[] key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSHEXPIRETIME)
            .add(index)
            .add(key), BuilderFactory.LONG));
    }

    /**
     * Get abs expire time  (milliseconds).
     *
     * @param index index name
     * @param key   the key
     * @return abs expire time
     */
    public Response<Long> tvshpexpiretime(final String index, final String key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSHPEXPIRETIME)
            .add(SafeEncoder.encode(index))
            .add(SafeEncoder.encode(key)), BuilderFactory.LONG));
    }

    public Response<Long> tvshpexpiretime(final byte[] index, final byte[] key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TVSHPEXPIRETIME)
            .add(index)
            .add(key), BuilderFactory.LONG));
    }
}
