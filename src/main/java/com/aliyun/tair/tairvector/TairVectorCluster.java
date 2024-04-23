package com.aliyun.tair.tairvector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairhash.factory.HashBuilderFactory;
import com.aliyun.tair.tairvector.factory.VectorBuilderFactory;
import com.aliyun.tair.tairvector.params.DistanceMethod;
import com.aliyun.tair.tairvector.params.HscanParams;
import com.aliyun.tair.tairvector.params.IndexAlgorithm;
import com.aliyun.tair.util.JoinParameters;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.util.SafeEncoder;

import static redis.clients.jedis.Protocol.toByteArray;

public class TairVectorCluster {
    private JedisCluster jc;

    public TairVectorCluster(JedisCluster jc) {
        this.jc = jc;
    }

    public void quit() {
        if (jc != null) {
            jc.close();
        }
    }

    /**
     * TVS.CREATEINDEX  TVS.CREATEINDEX index_name dims algorithm distance_method  [(attribute_key attribute_value) ... ]
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
    public String tvscreateindex(final String index, int dims, IndexAlgorithm algorithm, DistanceMethod method, final String... attrs) {
        Object obj = jc.sendCommand(SafeEncoder.encode(index), ModuleCommand.TVSCREATEINDEX, JoinParameters.joinParameters(SafeEncoder.encode(index), toByteArray(dims), SafeEncoder.encode(algorithm.name()), SafeEncoder.encode(method.name()), SafeEncoder.encodeMany(attrs)));
        return BuilderFactory.STRING.build(obj);
    }

    public byte[] tvscreateindex(byte[] index, int dims, IndexAlgorithm algorithm, DistanceMethod method, final byte[]... params) {
        Object obj = jc.sendCommand(index, ModuleCommand.TVSCREATEINDEX, JoinParameters.joinParameters(index, toByteArray(dims), SafeEncoder.encode(algorithm.name()), SafeEncoder.encode(method.name()), params));
        return BuilderFactory.BYTE_ARRAY.build(obj);
    }

    /**
     * TVS.GETINDEX TVS.GETINDEX index_name
     * <p>
     * get index schema info, including: index_name, algorithm, distance_method, data_count, ...
     *
     * @param index index name
     * @return Success: string_map, Fail:  empty
     */
    public Map<String, String> tvsgetindex(final String index) {
        Object obj = jc.sendCommand(SafeEncoder.encode(index), ModuleCommand.TVSGETINDEX, SafeEncoder.encode(index));
        return BuilderFactory.STRING_MAP.build(obj);
    }

    public Map<byte[], byte[]> tvsgetindex(byte[] index) {
        Object obj = jc.sendCommand(index, ModuleCommand.TVSGETINDEX, index);
        return BuilderFactory.BYTE_ARRAY_MAP.build(obj);
    }

    /**
     * TVS.DELINDEX TVS.DELINDEX index_name
     * <p>
     * delete index
     *
     * @param index index name
     * @return Success: 1; Fail: 0
     */
    public Long tvsdelindex(final String index) {
        Object obj = jc.sendCommand(SafeEncoder.encode(index), ModuleCommand.TVSDELINDEX, SafeEncoder.encode(index));
        return BuilderFactory.LONG.build(obj);
    }

    public Long tvsdelindex(byte[] index) {
        Object obj = jc.sendCommand(index, ModuleCommand.TVSDELINDEX, index);
        return BuilderFactory.LONG.build(obj);
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
    public ScanResult<String> tvsscanindex(Long cursor, HscanParams params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(toByteArray(cursor));
        args.addAll(params.getParams());
        Object obj = jc.sendCommand(toByteArray(cursor), ModuleCommand.TVSSCANINDEX, args.toArray(new byte[args.size()][]));
        return VectorBuilderFactory.SCAN_CURSOR_STRING.build(obj);
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
    public Long tvshset(final String index, final String entityid, final String vector, final String... params) {
        Object obj = jc.sendCommand(SafeEncoder.encode(index), ModuleCommand.TVSHSET, JoinParameters.joinParameters(SafeEncoder.encode(index), SafeEncoder.encode(entityid), SafeEncoder.encode(VectorBuilderFactory.VECTOR_TAG), SafeEncoder.encode(vector), SafeEncoder.encodeMany(params)));
        return BuilderFactory.LONG.build(obj);
    }

    public Long tvshset(byte[] index, byte[] entityid, byte[] vector, final byte[]... params) {
        Object obj = jc.sendCommand(index, ModuleCommand.TVSHSET, JoinParameters.joinParameters(index, entityid, SafeEncoder.encode(VectorBuilderFactory.VECTOR_TAG), vector, params));
        return BuilderFactory.LONG.build(obj);
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
    public Map<String, String> tvshgetall(final String index, final String entityid) {
        Object obj = jc.sendCommand(SafeEncoder.encode(index), ModuleCommand.TVSHGETALL, SafeEncoder.encode(index), SafeEncoder.encode(entityid));
        return BuilderFactory.STRING_MAP.build(obj);
    }

    public Map<byte[], byte[]> tvshgetall(byte[] index, byte[] entityid) {
        Object obj = jc.sendCommand(index, ModuleCommand.TVSHGETALL, index, entityid);
        return BuilderFactory.BYTE_ARRAY_MAP.build(obj);
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
    public List<String> tvshmget(final String index, final String entityid, final String... attrs) {
        Object obj = jc.sendCommand(SafeEncoder.encode(index), ModuleCommand.TVSHMGET, JoinParameters.joinParameters(SafeEncoder.encode(index), SafeEncoder.encode(entityid), SafeEncoder.encodeMany(attrs)));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<byte[]> tvshmget(byte[] index, byte[] entityid, byte[]... attrs) {
        Object obj = jc.sendCommand(index, ModuleCommand.TVSHMGET, JoinParameters.joinParameters(index, entityid, attrs));
        return BuilderFactory.BYTE_ARRAY_LIST.build(obj);
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
    public Long tvsdel(final String index, final String entityid) {
        Object obj = jc.sendCommand(SafeEncoder.encode(index), ModuleCommand.TVSDEL, SafeEncoder.encode(index), SafeEncoder.encode(entityid));
        return BuilderFactory.LONG.build(obj);
    }

    public Long tvsdel(byte[] index, byte[] entityid) {
        Object obj = jc.sendCommand(index, ModuleCommand.TVSDEL, index, entityid);
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * TVS.DEL TVS.DEL index entityid1 entityi2
     * <p>
     * delete entity from tair-vector module
     *
     * @param index    index name
     * @param entityids entity id
     * @return Long integer-reply the number of fields that were removed from the tair-vector
     * not including specified but non existing fields.
     */
    public Long tvsdel(final String index, final String... entityids) {
        Object obj = jc.sendCommand(SafeEncoder.encode(index), ModuleCommand.TVSDEL, JoinParameters.joinParameters(SafeEncoder.encode(index), SafeEncoder.encodeMany(entityids)));
        return BuilderFactory.LONG.build(obj);
    }

    public Long tvsdel(byte[] index, byte[]... entityids) {
        Object obj = jc.sendCommand(index, ModuleCommand.TVSDEL, JoinParameters.joinParameters(index, entityids));
        return BuilderFactory.LONG.build(obj);
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
    public Long tvshdel(final String index, final String entityid, final String... attrs) {
        Object obj = jc.sendCommand(SafeEncoder.encode(index), ModuleCommand.TVSHDEL, JoinParameters.joinParameters(SafeEncoder.encode(index), SafeEncoder.encode(entityid), SafeEncoder.encodeMany(attrs)));
        return BuilderFactory.LONG.build(obj);
    }

    public Long tvshdel(byte[] index, byte[] entityid, byte[]... attrs) {
        Object obj = jc.sendCommand(index, ModuleCommand.TVSHDEL, JoinParameters.joinParameters(index, entityid, attrs));
        return BuilderFactory.LONG.build(obj);
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
    public ScanResult<String> tvsscan(final String index, Long cursor, HscanParams params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(SafeEncoder.encode(index));
        args.add(toByteArray(cursor));
        args.addAll(params.getParams());
        Object obj = jc.sendCommand(SafeEncoder.encode(index), ModuleCommand.TVSSCAN, args.toArray(new byte[args.size()][]));
        return VectorBuilderFactory.SCAN_CURSOR_STRING.build(obj);
    }

    public ScanResult<byte[]> tvsscan(byte[] index, Long cursor, HscanParams params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(index);
        args.add(toByteArray(cursor));
        args.addAll(params.getParams());
        Object obj = jc.sendCommand(index, ModuleCommand.TVSSCAN, args.toArray(new byte[args.size()][]));
        return VectorBuilderFactory.SCAN_CURSOR_BYTE.build(obj);
    }

    /**
     * TVS.KNNSEARCH TVS.KNNSEARCH index_name topn vector
     * <p>
     * query entity by vector
     *
     * @param index  index name
     * @param topn   topn result
     * @param vector query vector
     * @param params for HNSW, params include:
     *               ef_search     range [0, 1000]
     * @return VectorBuilderFactory.Knn<>
     */
    public VectorBuilderFactory.Knn<String> tvsknnsearch(final String index, Long topn, final String vector, final String... params) {
        return tvsknnsearchfilter(index, topn, vector, "", params);
    }

    public VectorBuilderFactory.Knn<byte[]> tvsknnsearch(byte[] index, Long topn, byte[] vector, final byte[]... params) {
        return tvsknnsearchfilter(index, topn, vector, SafeEncoder.encode(""), params);
    }

    /**
     * TVS.KNNSEARCH TVS.KNNSEARCH index_name topn vector pattern
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
    public VectorBuilderFactory.Knn<String> tvsknnsearchfilter(final String index, Long topn, final String vector, final String pattern, final String... params) {
        Object obj = jc.sendCommand(SafeEncoder.encode(index), ModuleCommand.TVSKNNSEARCH, JoinParameters.joinParameters(SafeEncoder.encode(index), toByteArray(topn),
                SafeEncoder.encode(vector), SafeEncoder.encode(pattern), SafeEncoder.encodeMany(params)));
        return VectorBuilderFactory.STRING_KNN_RESULT.build(obj);
    }

    public VectorBuilderFactory.Knn<byte[]> tvsknnsearchfilter(byte[] index, Long topn, byte[] vector, byte[] pattern, final byte[]... params) {
        Object obj = jc.sendCommand(index, ModuleCommand.TVSKNNSEARCH, JoinParameters.joinParameters(index, toByteArray(topn), vector, pattern, params));
        return VectorBuilderFactory.BYTE_KNN_RESULT.build(obj);
    }

    /**
     * TVS.KNNSEARCHFIELD index_name topn vector
     * <p>
     * query entity by vector
     *
     * @param index  index name
     * @param topn   topn result
     * @param vector query vector
     * @param fields field list
     * @param params for HNSW, params include:
     *               ef_search     range [0, 1000]
     * @return VectorBuilderFactory.KnnField<>
     */
    public VectorBuilderFactory.KnnField<String> tvsknnsearchfield(final String index, Long topn, final String vector, Collection<String> fields, final String... params) {
        return tvsknnsearchfilterfield(index, topn, vector, fields, "", params);
    }

    public VectorBuilderFactory.KnnField<byte[]> tvsknnsearchfield(byte[] index, Long topn, byte[] vector, Collection<byte[]> fields, final byte[]... params) {
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
     * @param fields field list
     * @param pattern support +, -，>, <, !=， ,()，&&, ||, !, ==
     * @param params  for HNSW, params include:
     *                ef_search     range [0, 1000]
     * @return VectorBuilderFactory.KnnField<>
     */
    public VectorBuilderFactory.KnnField<String> tvsknnsearchfilterfield(final String index, Long topn, final String vector, Collection<String> fields, final String pattern, final String... params) {
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
        Object obj = jc.sendCommand(SafeEncoder.encode(index), ModuleCommand.TVSKNNSEARCHFIELD, args.toArray(new byte[args.size()][]));
        return VectorBuilderFactory.STRING_KNNFIELD_RESULT.build(obj);
    }

    public VectorBuilderFactory.KnnField<byte[]> tvsknnsearchfilterfield(byte[] index, Long topn, byte[] vector,  Collection<byte[]> fields, byte[] pattern, final byte[]... params) {
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
        Object obj = jc.sendCommand(index, ModuleCommand.TVSKNNSEARCHFIELD, args.toArray(new byte[args.size()][]));
        return VectorBuilderFactory.BYTE_KNNFIELD_RESULT.build(obj);
    }

    /**
     * TVS.MKNNSEARCH TVS.MKNNSEARCH index_name topn vector [vector...]
     *
     * @param index   index name
     * @param topn    topn for each vector
     * @param vectors vector list
     * @param params  for HNSW, params include:
     *                ef_search     range [0, 1000]
     * @return Collection<>
     */
    public Collection<VectorBuilderFactory.Knn<String>> tvsmknnsearch(final String index, Long topn, Collection<String> vectors, final String... params) {
        return tvsmknnsearchfilter(index, topn, vectors, "", params);
    }

    public Collection<VectorBuilderFactory.Knn<byte[]>> tvsmknnsearch(byte[] index, Long topn, Collection<byte[]> vectors, final byte[]... params) {
        return tvsmknnsearchfilter(index, topn, vectors, SafeEncoder.encode(""), params);
    }

    /**
     * TVS.MKNNSEARCH TVS.MKNNSEARCH index_name topn vector [vector...] pattern
     *
     * @param index   index name
     * @param topn    topn for each vector
     * @param vectors vector list
     * @param pattern support +, -，>, <, !=， ,()，&&, ||, !, ==
     * @param params  for HNSW, params include:
     *                ef_search     range [0, 1000]
     * @return Collection<>
     */
    public Collection<VectorBuilderFactory.Knn<String>> tvsmknnsearchfilter(final String index, Long topn, Collection<String> vectors, final String pattern, final String... params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(SafeEncoder.encode(index));
        args.add(toByteArray(topn));
        args.add(toByteArray(vectors.size()));
        args.addAll(vectors.stream().map(vector -> SafeEncoder.encode(vector)).collect(Collectors.toList()));
        args.add(SafeEncoder.encode(pattern));
        args.addAll(Arrays.stream(params).map(str -> SafeEncoder.encode(str)).collect(Collectors.toList()));
        Object obj = jc.sendCommand(SafeEncoder.encode(index), ModuleCommand.TVSMKNNSEARCH, args.toArray(new byte[args.size()][]));
        return VectorBuilderFactory.STRING_KNN_BATCH_RESULT.build(obj);
    }

    public Collection<VectorBuilderFactory.Knn<byte[]>> tvsmknnsearchfilter(byte[] index, Long topn, Collection<byte[]> vectors, byte[] pattern, final byte[]... params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(index);
        args.add(toByteArray(topn));
        args.add(toByteArray(vectors.size()));
        args.addAll(vectors);
        args.add(pattern);
        args.addAll(Arrays.stream(params).collect(Collectors.toList()));
        Object obj = jc.sendCommand(index, ModuleCommand.TVSMKNNSEARCH, args.toArray(new byte[args.size()][]));
        return VectorBuilderFactory.BYTE_KNN_BATCH_RESULT.build(obj);
    }

    public VectorBuilderFactory.Knn<String> tvsmindexknnsearch(Collection<String> indexs, Long topn, String vector, String... params) {
        return null;
    }

    public VectorBuilderFactory.Knn<byte[]> tvsmindexknnsearch(Collection<byte[]> indexs, Long topn, byte[] vector, byte[]... params) {
        return null;
    }

    public Collection<VectorBuilderFactory.Knn<String>> tvsmindexmknnsearch(Collection<String> indexs, Long topn, Collection<String> vectors, String... params) {
        return null;
    }

    public Collection<VectorBuilderFactory.Knn<byte[]>> tvsmindexmknnsearch(Collection<byte[]> indexs, Long topn, Collection<byte[]> vectors, byte[]... params) {
        return null;
    }

    public VectorBuilderFactory.Knn<String> tvsmindexknnsearchfilter(Collection<String> indexs, Long topn, String vector, String pattern, String... params) {
        return null;
    }

    public VectorBuilderFactory.Knn<byte[]> tvsmindexknnsearchfilter(Collection<byte[]> indexs, Long topn, byte[] vector, byte[] pattern, byte[]... params) {
        return null;
    }

    public Collection<VectorBuilderFactory.Knn<String>> tvsmindexmknnsearchfilter(Collection<String> indexs, Long topn, Collection<String> vectors, String pattern, String... params) {
        return null;
    }

    public Collection<VectorBuilderFactory.Knn<byte[]>> tvsmindexmknnsearchfilter(Collection<byte[]> indexs, Long topn, Collection<byte[]> vectors, byte[] pattern, byte[]... params) {
        return null;
    }

    public VectorBuilderFactory.KnnField<String> tvsmindexknnsearchField(Collection<String> indexs, Long topn, String vector, Collection<String> fields, String... params) {
        return null;
    }

    public VectorBuilderFactory.KnnField<byte[]> tvsmindexknnsearchField(Collection<byte[]> indexs, Long topn, byte[] vector, Collection<byte[]> fields, byte[]... params) {
        return null;
    }

    public VectorBuilderFactory.KnnField<String> tvsmindexknnsearchfilterfield(Collection<String> indexs, Long topn, String vector, Collection<String> fields, String pattern, String... params) {
        return null;
    }

    public VectorBuilderFactory.KnnField<byte[]> tvsmindexknnsearchfilterfield(Collection<byte[]> indexs, Long topn, byte[] vector, Collection<byte[]> fields, byte[] pattern, final byte[]... params) {
        return null;
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
    public Long tvshincrby(final String index, final String entityid, final String field, final long value) {
        Object obj = jc.sendCommand(SafeEncoder.encode(index), ModuleCommand.TVSHINCRBY, SafeEncoder.encode(index), SafeEncoder.encode(entityid), SafeEncoder.encode(field), toByteArray(value));
        return BuilderFactory.LONG.build(obj);
    }

    public Long tvshincrby(byte[] index, byte[] entityid, byte[] field, long value) {
        Object obj = jc.sendCommand(index, ModuleCommand.TVSHINCRBY, index, entityid, field, toByteArray(value));
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * TVS.HINCRBYFOLAT index entityid field value
     * Increment the float value of a tairvector field by the given amount, not support VECTOR
     *
     * @param index    index name
     * @param entityid entity id
     * @param field    the field type: key
     * @param value    the increment type: double
     * @return Double bulk-string-reply the value of {@code field} after the increment
     */
    public Double tvshincrbyfloat(final String index, final String entityid, final String field, final double value) {
        Object obj = jc.sendCommand(SafeEncoder.encode(index), ModuleCommand.TVSHINCRBYFLOAT, SafeEncoder.encode(index), SafeEncoder.encode(entityid), SafeEncoder.encode(field), toByteArray(value));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double tvshincrbyfloat(byte[] index, byte[] entityid, byte[] field, double value) {
        Object obj = jc.sendCommand(index, ModuleCommand.TVSHINCRBYFLOAT, index, entityid, field, toByteArray(value));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public VectorBuilderFactory.Knn<String> tvsgetdistance(String index, String vector, Collection<String> keys, Long topn, Float max_dist, String filter) {
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

        Object obj = jc.sendCommand(SafeEncoder.encode(index), ModuleCommand.TVSGETDISTANCE, args.toArray(new byte[args.size()][]));
        return VectorBuilderFactory.STRING_KNN_RESULT.build(obj);
    }

    public VectorBuilderFactory.Knn<byte[]> tvsgetdistance(byte[] index, byte[] vector, Collection<byte[]> keys, Long topn, Float max_dist, byte[] filter) {
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

        Object obj = jc.sendCommand(index, ModuleCommand.TVSGETDISTANCE, args.toArray(new byte[args.size()][]));
        return VectorBuilderFactory.BYTE_KNN_RESULT.build(obj);
    }

    /**
     * Set expire time (seconds).
     *
     * @param index   index name
     * @param key     the key
     * @param seconds time is seconds
     * @return Success: true, fail: false.
     */
    public Boolean tvshexpire(final String index, final String key, final int seconds) {
        return tvshexpire(SafeEncoder.encode(index), SafeEncoder.encode(key), seconds);
    }

    public Boolean tvshexpire(final byte[] index, final byte[] key, final int seconds) {
        Object obj = jc.sendCommand(index, ModuleCommand.TVSHEXPIRE, index, key, toByteArray(seconds));
        return BuilderFactory.BOOLEAN.build(obj);
    }

    /**
     * Set expire time (seconds).
     *
     * @param index        index name
     * @param key          the key
     * @param milliseconds time is milliseconds
     * @return Success: true, fail: false.
     */
    public Boolean tvshpexpire(final String index, final String key, final int milliseconds) {
        return tvshpexpire(SafeEncoder.encode(index), SafeEncoder.encode(key), milliseconds);
    }

    public Boolean tvshpexpire(final byte[] index, final byte[] key, final int milliseconds) {
        Object obj = jc.sendCommand(index, ModuleCommand.TVSHPEXPIRE, index, key, toByteArray(milliseconds));
        return BuilderFactory.BOOLEAN.build(obj);
    }

    /**
     * Set the expiration for a key as a UNIX timestamp (seconds).
     *
     * @param index    the index name
     * @param key      the key
     * @param unixTime timestamp the timestamp type: posix time, time is seconds
     * @return Success: true, fail: false.
     */
    public Boolean tvshexpireAt(final String index, final String key, final long unixTime) {
        return tvshexpireAt(SafeEncoder.encode(index), SafeEncoder.encode(key), unixTime);
    }

    public Boolean tvshexpireAt(final byte[] index, final byte[] key, final long unixTime) {
        Object obj = jc.sendCommand(index, ModuleCommand.TVSHEXPIREAT, index, key, toByteArray(unixTime));
        return BuilderFactory.BOOLEAN.build(obj);
    }

    /**
     * Set the expiration for a key as a UNIX timestamp (milliseconds).
     *
     * @param index    the index name
     * @param key      the key
     * @param unixTime timestamp the timestamp type: posix time, time is milliseconds
     * @return Success: true, fail: false.
     */
    public Boolean tvshpexpireAt(final String index, final String key, final long unixTime) {
        return tvshpexpireAt(SafeEncoder.encode(index), SafeEncoder.encode(key), unixTime);
    }

    public Boolean tvshpexpireAt(final byte[] index, final byte[] key, final long unixTime) {
        Object obj = jc.sendCommand(index, ModuleCommand.TVSHPEXPIREAT, index, key, toByteArray(unixTime));
        return BuilderFactory.BOOLEAN.build(obj);
    }

    /**
     * Get ttl (seconds).
     *
     * @param index index name
     * @param key   the key
     * @return ttl
     */
    public Long tvshttl(final String index, final String key) {
        return tvshttl(SafeEncoder.encode(index), SafeEncoder.encode(key));
    }

    public Long tvshttl(final byte[] index, final byte[] key) {
        Object obj = jc.sendCommand(index, ModuleCommand.TVSHTTL, index, key);
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * Get ttl (milliseconds).
     *
     * @param index index name
     * @param key   the key
     * @return ttl
     */
    public Long tvshpttl(final String index, final String key) {
        return tvshpttl(SafeEncoder.encode(index), SafeEncoder.encode(key));
    }

    public Long tvshpttl(final byte[] index, final byte[] key) {
        Object obj = jc.sendCommand(index, ModuleCommand.TVSHPTTL, index, key);
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * Get abs expire time  (seconds).
     *
     * @param index index name
     * @param key   the key
     * @return abs expire time
     */
    public Long tvshexpiretime(final String index, final String key) {
        return tvshexpiretime(SafeEncoder.encode(index), SafeEncoder.encode(key));
    }

    public Long tvshexpiretime(final byte[] index, final byte[] key) {
        Object obj = jc.sendCommand(index, ModuleCommand.TVSHEXPIRETIME, index, key);
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * Get abs expire time  (milliseconds).
     *
     * @param index index name
     * @param key   the key
     * @return abs expire time
     */
    public Long tvshpexpiretime(final String index, final String key) {
        return tvshpexpiretime(SafeEncoder.encode(index), SafeEncoder.encode(key));
    }

    public Long tvshpexpiretime(final byte[] index, final byte[] key) {
        Object obj = jc.sendCommand(index, ModuleCommand.TVSHPEXPIRETIME, index, key);
        return BuilderFactory.LONG.build(obj);
    }
}

