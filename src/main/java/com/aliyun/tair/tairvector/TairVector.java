package com.aliyun.tair.tairvector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairvector.factory.VectorBuilderFactory;
import com.aliyun.tair.tairvector.params.DistanceMethod;
import com.aliyun.tair.tairvector.params.HscanParams;
import com.aliyun.tair.tairvector.params.IndexAlgorithm;
import com.aliyun.tair.util.JoinParameters;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.util.SafeEncoder;

import static redis.clients.jedis.Protocol.toByteArray;

public class TairVector {
    private Jedis jedis;
    private JedisPool jedisPool;

    public TairVector(Jedis jedis) {
        this.jedis = jedis;
    }

    public TairVector(JedisPool jedisPool) {
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

    public void quit() {
        if (jedis != null) {
            jedis.quit();
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
     * @param params    other columns, optional
     *                  for HNSW, args include:
     *                  ef_construct     default 100
     *                  M			     default 16
     * @return Success: +OK; Fail: error
     */
    public String tvscreateindex(final String index, int dims, IndexAlgorithm algorithm, DistanceMethod method, final String... params) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSCREATEINDEX, JoinParameters.joinParameters(SafeEncoder.encode(index), toByteArray(dims), SafeEncoder.encode(algorithm.name()), SafeEncoder.encode(method.name()), SafeEncoder.encodeMany(params)));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public byte[] tvscreateindex(byte[] index, int dims, IndexAlgorithm algorithm, DistanceMethod method, final byte[]... params) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSCREATEINDEX, JoinParameters.joinParameters(index, toByteArray(dims), SafeEncoder.encode(algorithm.name()), SafeEncoder.encode(method.name()), params));
            return BuilderFactory.BYTE_ARRAY.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSGETINDEX, SafeEncoder.encode(index));
            return BuilderFactory.STRING_MAP.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Map<byte[], byte[]> tvsgetindex(byte[] index) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSGETINDEX, index);
            return BuilderFactory.BYTE_ARRAY_MAP.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSDELINDEX, SafeEncoder.encode(index));
            return BuilderFactory.LONG.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Long tvsdelindex(byte[] index) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSDELINDEX, index);
            return BuilderFactory.LONG.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
     * @return A ScanResult. {@link VectorBuilderFactory#SCAN_CURSOR_STRING}
     */
    public ScanResult<String> tvsscanindex(Long cursor, HscanParams params) {
        Jedis jedis = getJedis();
        try {
            final List<byte[]> args = new ArrayList<byte[]>();
            args.add(toByteArray(cursor));
            args.addAll(params.getParams());
            Object obj = jedis.sendCommand(ModuleCommand.TVSSCANINDEX, args.toArray(new byte[args.size()][]));
            return VectorBuilderFactory.SCAN_CURSOR_STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSHSET, JoinParameters.joinParameters(SafeEncoder.encode(index), SafeEncoder.encode(entityid), SafeEncoder.encode(VectorBuilderFactory.VECTOR_TAG), SafeEncoder.encode(vector), SafeEncoder.encodeMany(params)));
            return BuilderFactory.LONG.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Long tvshset(byte[] index, byte[] entityid, byte[] vector, final byte[]... params) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSHSET, JoinParameters.joinParameters(index, entityid, SafeEncoder.encode(VectorBuilderFactory.VECTOR_TAG), vector, params));
            return BuilderFactory.LONG.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSHGETALL, SafeEncoder.encode(index), SafeEncoder.encode(entityid));
            return BuilderFactory.STRING_MAP.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Map<byte[], byte[]> tvshgetall(byte[] index, byte[] entityid) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSHGETALL, index, entityid);
            return BuilderFactory.BYTE_ARRAY_MAP.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSHMGET, JoinParameters.joinParameters(SafeEncoder.encode(index), SafeEncoder.encode(entityid), SafeEncoder.encodeMany(attrs)));
            return BuilderFactory.STRING_LIST.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public List<byte[]> tvshmget(byte[] index, byte[] entityid, byte[]... attrs) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSHMGET, JoinParameters.joinParameters(index, entityid, attrs));
            return BuilderFactory.BYTE_ARRAY_LIST.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }


    /**
     * TVS.DEL TVS.DEL index entityid
     * <p>
     * delete entity from tair-vector module
     *
     * @param index    index name
     * @param entityid entity id
     * @return Long integer-reply the number of fields that were removed from the tair-vector
     * not including specified but no existing fields.
     */
    public Long tvsdel(final String index, final String entityid) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSDEL, SafeEncoder.encode(index), SafeEncoder.encode(entityid));
            return BuilderFactory.LONG.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Long tvsdel(byte[] index, byte[] entityid) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSDEL, index, entityid);
            return BuilderFactory.LONG.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * TVS.DEL TVS.DEL index entityid1 entityid2
     * <p>
     * delete entity from tair-vector module
     *
     * @param index    index name
     * @param entityids entity id
     * @return Long integer-reply the number of fields that were removed from the tair-vector
     * not including specified but no existing fields.
     */
    public Long tvsdel(final String index, final String... entityids) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSDEL, JoinParameters.joinParameters(SafeEncoder.encode(index), SafeEncoder.encodeMany(entityids)));
            return BuilderFactory.LONG.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Long tvsdel(byte[] index, byte[]... entityids) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSDEL, JoinParameters.joinParameters(index, entityids));
            return BuilderFactory.LONG.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }


    /**
     * TVS.HDEL TVS.HDEL index entityid attribute_key [attribute_key ...]
     * <p>
     * delete entity attrs from tair-vector module
     *
     * @param index    index name
     * @param entityid entity id
     * @param attr     attr
     * @param attrs    other attrs
     * @return Long integer-reply the number of fields that were removed from the tair-vector
     * not including specified but no existing fields.
     */
    public Long tvshdel(final String index, final String entityid, final String attr, final String... attrs) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSHDEL, JoinParameters.joinParameters(SafeEncoder.encode(index), SafeEncoder.encode(entityid), SafeEncoder.encode(attr), SafeEncoder.encodeMany(attrs)));
            return BuilderFactory.LONG.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Long tvshdel(byte[] index, byte[] entityid, byte[] attr, byte[]... attrs) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSHDEL, JoinParameters.joinParameters(index, entityid, attr, attrs));
            return BuilderFactory.LONG.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            final List<byte[]> args = new ArrayList<byte[]>();
            args.add(SafeEncoder.encode(index));
            args.add(toByteArray(cursor));
            args.addAll(params.getParams());
            Object obj = jedis.sendCommand(ModuleCommand.TVSSCAN, args.toArray(new byte[args.size()][]));
            return VectorBuilderFactory.SCAN_CURSOR_STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public ScanResult<byte[]> tvsscan(byte[] index, Long cursor, HscanParams params) {
        Jedis jedis = getJedis();
        try {
            final List<byte[]> args = new ArrayList<byte[]>();
            args.add(index);
            args.add(toByteArray(cursor));
            args.addAll(params.getParams());
            Object obj = jedis.sendCommand(ModuleCommand.TVSSCAN, args.toArray(new byte[args.size()][]));
            return VectorBuilderFactory.SCAN_CURSOR_BYTE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSKNNSEARCH, JoinParameters.joinParameters(SafeEncoder.encode(index), toByteArray(topn), SafeEncoder.encode(vector), SafeEncoder.encode(pattern), SafeEncoder.encodeMany(params)));
            return VectorBuilderFactory.STRING_KNN_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public VectorBuilderFactory.Knn<byte[]> tvsknnsearchfilter(byte[] index, Long topn, byte[] vector, byte[] pattern, final byte[]... params) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSKNNSEARCH, JoinParameters.joinParameters(index, toByteArray(topn), vector, pattern, params));
            return VectorBuilderFactory.BYTE_KNN_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
     * @param pattern filter pattern, support +, -，>, <, !=， ,()，&&, ||, !, ==
     * @param params  for HNSW, params include:
     *                ef_search     range [0, 1000]
     * @return Collection<>
     */
    public Collection<VectorBuilderFactory.Knn<String>> tvsmknnsearchfilter(final String index, Long topn, Collection<String> vectors, final String pattern, final String... params) {
        Jedis jedis = getJedis();
        try {
            final List<byte[]> args = new ArrayList<byte[]>();
            args.add(SafeEncoder.encode(index));
            args.add(toByteArray(topn));
            args.add(toByteArray(vectors.size()));
            args.addAll(vectors.stream().map(vector -> SafeEncoder.encode(vector)).collect(Collectors.toList()));
            args.add(SafeEncoder.encode(pattern));
            args.addAll(Arrays.stream(params).map(str -> SafeEncoder.encode(str)).collect(Collectors.toList()));
            Object obj = jedis.sendCommand(ModuleCommand.TVSMKNNSEARCH, args.toArray(new byte[args.size()][]));
            return VectorBuilderFactory.STRING_KNN_BATCH_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Collection<VectorBuilderFactory.Knn<byte[]>> tvsmknnsearchfilter(byte[] index, Long topn, Collection<byte[]> vectors, byte[] pattern, final byte[]... params) {
        Jedis jedis = getJedis();
        try {
            final List<byte[]> args = new ArrayList<byte[]>();
            args.add(index);
            args.add(toByteArray(topn));
            args.add(toByteArray(vectors.size()));
            args.addAll(vectors);
            args.add(pattern);
            args.addAll(Arrays.stream(params).collect(Collectors.toList()));
            Object obj = jedis.sendCommand(ModuleCommand.TVSMKNNSEARCH, args.toArray(new byte[args.size()][]));
            return VectorBuilderFactory.BYTE_KNN_BATCH_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }


    public VectorBuilderFactory.Knn<String> tvsmindexknnsearch(Collection<String> indexs, Long topn, String vector, String... params) {
        return tvsmindexknnsearchfilter(indexs, topn, vector, "", params);
    }

    public VectorBuilderFactory.Knn<byte[]> tvsmindexknnsearch(Collection<byte[]> indexs, Long topn, byte[] vector, byte[]... params) {
        return tvsmindexknnsearchfilter(indexs, topn, vector, SafeEncoder.encode(""), params);
    }

    public VectorBuilderFactory.Knn<String> tvsmindexknnsearchfilter(Collection<String> indexs, Long topn, String vector, String pattern, String... params) {
        Jedis jedis = getJedis();
        try {
            final List<byte[]> args = new ArrayList<byte[]>();
            args.add(toByteArray(indexs.size()));
            args.addAll(indexs.stream().map(index -> SafeEncoder.encode(index)).collect(Collectors.toList()));
            args.add(toByteArray(topn));
            args.add(SafeEncoder.encode(vector));
            args.add(SafeEncoder.encode(pattern));
            args.addAll(Arrays.stream(params).map(str -> SafeEncoder.encode(str)).collect(Collectors.toList()));
            Object obj = jedis.sendCommand(ModuleCommand.TVSMINDEXKNNSEARCH, args.toArray(new byte[args.size()][]));
            return VectorBuilderFactory.STRING_KNN_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public VectorBuilderFactory.Knn<byte[]> tvsmindexknnsearchfilter(Collection<byte[]> indexs, Long topn, byte[] vector, byte[] pattern, final byte[]... params) {
        Jedis jedis = getJedis();
        try {
            final List<byte[]> args = new ArrayList<byte[]>();
            args.add(toByteArray(indexs.size()));
            args.addAll(indexs);
            args.add(toByteArray(topn));
            args.add(vector);
            args.add(pattern);
            args.addAll(Arrays.stream(params).collect(Collectors.toList()));
            Object obj = jedis.sendCommand(ModuleCommand.TVSMINDEXKNNSEARCH, args.toArray(new byte[args.size()][]));
            return VectorBuilderFactory.BYTE_KNN_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Collection<VectorBuilderFactory.Knn<String>> tvsmindexmknnsearch(Collection<String> indexs, Long topn, Collection<String> vectors, String... params) {
        return tvsmindexmknnsearchfilter(indexs, topn, vectors, "", params);
    }

    public Collection<VectorBuilderFactory.Knn<byte[]>> tvsmindexmknnsearch(Collection<byte[]> indexs, Long topn, Collection<byte[]> vectors, byte[]... params) {
        return tvsmindexmknnsearchfilter(indexs, topn, vectors, SafeEncoder.encode(""), params);
    }

    public Collection<VectorBuilderFactory.Knn<String>> tvsmindexmknnsearchfilter(Collection<String> indexs, Long topn, Collection<String> vectors, String pattern, String... params) {
        Jedis jedis = getJedis();
        try {
            final List<byte[]> args = new ArrayList<byte[]>();
            args.add(toByteArray(indexs.size()));
            args.addAll(indexs.stream().map(index -> SafeEncoder.encode(index)).collect(Collectors.toList()));
            args.add(toByteArray(topn));
            args.add(toByteArray(vectors.size()));
            args.addAll(vectors.stream().map(vector -> SafeEncoder.encode(vector)).collect(Collectors.toList()));
            args.add(SafeEncoder.encode(pattern));
            args.addAll(Arrays.stream(params).map(str -> SafeEncoder.encode(str)).collect(Collectors.toList()));
            Object obj = jedis.sendCommand(ModuleCommand.TVSMINDEXMKNNSEARCH, args.toArray(new byte[args.size()][]));
            return VectorBuilderFactory.STRING_KNN_BATCH_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Collection<VectorBuilderFactory.Knn<byte[]>> tvsmindexmknnsearchfilter(Collection<byte[]> indexs, Long topn, Collection<byte[]> vectors, byte[] pattern, byte[]... params) {
        Jedis jedis = getJedis();
        try {
            final List<byte[]> args = new ArrayList<byte[]>();
            args.add(toByteArray(indexs.size()));
            args.addAll(indexs);
            args.add(toByteArray(topn));
            args.add(toByteArray(vectors.size()));
            args.addAll(vectors);
            args.add(pattern);
            args.addAll(Arrays.stream(params).collect(Collectors.toList()));
            Object obj = jedis.sendCommand(ModuleCommand.TVSMINDEXMKNNSEARCH, args.toArray(new byte[args.size()][]));
            return VectorBuilderFactory.BYTE_KNN_BATCH_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public VectorBuilderFactory.Knn<String> tvsgetdistance(String index, String vector, Collection<String> keys, Long topn, Float max_dist, String filter) {
        Jedis jedis = getJedis();
        try {
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

            Object obj = jedis.sendCommand(ModuleCommand.TVSGETDISTANCE, args.toArray(new byte[args.size()][]));
            return VectorBuilderFactory.STRING_KNN_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public VectorBuilderFactory.Knn<byte[]> tvsgetdistance(byte[] index, byte[] vector, Collection<byte[]> keys, Long topn, Float max_dist, byte[] filter) {
        Jedis jedis = getJedis();
        try {
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

            Object obj = getJedis().sendCommand(ModuleCommand.TVSGETDISTANCE, args.toArray(new byte[args.size()][]));
            return VectorBuilderFactory.BYTE_KNN_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * TVS.HINCRBY index entityid field value
     * Increment the long value of a tairvector field by the given amount， not support VECTOR
     *
     * @param index    index name
     * @param entityid entity id
     * @param field    the field type: key
     * @param value    the increment type: long
     * @return Long integer-reply the value at {@code field} after the increment operation.
     */
    public Long tvshincrby(final String index, final String entityid, final String field, final long value) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSHINCRBY, SafeEncoder.encode(index), SafeEncoder.encode(entityid), SafeEncoder.encode(field), toByteArray(value));
            return BuilderFactory.LONG.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Long tvshincrby(byte[] index, byte[] entityid, byte[] field, long value) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSHINCRBY, index, entityid, field, toByteArray(value));
            return BuilderFactory.LONG.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * TVS.HINCRBYFOLAT index entityid field value
     * Increment the float value of a tairvector field by the given amount.
     *
     * @param index    index name
     * @param entityid entity id
     * @param field    the field type: key
     * @param value    the increment type: double
     * @return Double bulk-string-reply the value of {@code field} after the increment, not support VECTOR
     */
    public Double tvshincrbyfloat(final String index, final String entityid, final String field, final double value) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSHINCRBYFLOAT, SafeEncoder.encode(index), SafeEncoder.encode(entityid), SafeEncoder.encode(field), toByteArray(value));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double tvshincrbyfloat(byte[] index, byte[] entityid, byte[] field, double value) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSHINCRBYFLOAT, index, entityid, field, toByteArray(value));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSHEXPIRE, index, key, toByteArray(seconds));
            return BuilderFactory.BOOLEAN.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSHPEXPIRE, index, key, toByteArray(milliseconds));
            return BuilderFactory.BOOLEAN.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSHEXPIREAT, index, key, toByteArray(unixTime));
            return BuilderFactory.BOOLEAN.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSHPEXPIREAT, index, key, toByteArray(unixTime));
            return BuilderFactory.BOOLEAN.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSHTTL, index, key);
            return BuilderFactory.LONG.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSHPTTL, index, key);
            return BuilderFactory.LONG.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSHEXPIRETIME, index, key);
            return BuilderFactory.LONG.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSHPEXPIRETIME, index, key);
            return BuilderFactory.LONG.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }
}
