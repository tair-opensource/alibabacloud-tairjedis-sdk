package com.aliyun.tair.tairvector;

import java.util.ArrayList;
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

    /**
     * TVS.CREATEINDEX  TVS.CREATEINDEX index_name dims algorithm distance_method  [(attribute_key attribute_value) ... ]
     * <p>
     * create tair-vector index
     *
     * @param index  index name
     * @param dims   vector dims
     * @param algorithm index algorithm
     * @param method vector distance method
     * @param attrs other columns, optional
     *  for HNSW, args include:
     *      ef_construct     default 100
     *      M			     default 16
     *  for FLAT, args include:
     * @return Success: +OK; Fail: error
     */
    public String tvscreateindex(final String index, int dims, IndexAlgorithm algorithm, DistanceMethod method, final String... attrs) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSCREATEINDEX, JoinParameters.joinParameters(SafeEncoder.encode(index), toByteArray(dims), SafeEncoder.encode(algorithm.name()), SafeEncoder.encode(method.name()), SafeEncoder.encodeMany(attrs)));
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
     * @param params  the params: [MATCH pattern] [COUNT count]
     *  `MATCH` - Set the pattern which is used to filter the results
     *  `COUNT` - Set the number of fields in a single scan (default is 10)
     *  `NOVAL` - The return result contains no data portion, only cursor information
     * @return A ScanResult. {@link VectorBuilderFactory#SCAN_CURSOR_STRING}
     */
    public ScanResult<String> tvsscanindex(Long cursor, HscanParams params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(toByteArray(cursor));
        args.addAll(params.getParams());
        Jedis jedis = getJedis();
        try {
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
     * @param index index name
     * @param entityid entity id
     * @param vector vector info
     * @param params scalar attribute key, value
     *
     * @return integer-reply specifically:
     *  {@literal k} if success, k is the number of fields that were added..
     *  throw error like "(error) Illegal vector dimensions" if error
     */
    public Long tvshset(final String index, final String entityid, final String vector, final String...params) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSHSET, JoinParameters.joinParameters(SafeEncoder.encode(index), SafeEncoder.encode(entityid), SafeEncoder.encode(VectorBuilderFactory.VECTOR_TAG), SafeEncoder.encode(vector), SafeEncoder.encodeMany(params)));
            return BuilderFactory.LONG.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }
    
    public Long tvshset(byte[] index, byte[] entityid, byte[] vector, final byte[]...params) {
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
     * @param index index name
     * @param entityid entity id
     *
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
     * @param index index name
     * @param entityid entity id
     * @param attrs attrs
     *
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
     * @param index index name
     * @param entityid entity id
     *
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
     * TVS.HDEL TVS.HDEL index entityid attribute_key [attribute_key ...]
     * <p>
     * delete entity attrs from tair-vector module
     *
     * @param index index name
     * @param entityid entity id
     * @param attr attr
     * @param attrs other attrs
     *
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
     * @param index index name
     * @param cursor start offset
     * @param params  the params: [MATCH pattern] [COUNT count]
     *  `MATCH` - Set the pattern which is used to filter the results
     *  `COUNT` - Set the number of fields in a single scan (default is 10)
     *  `NOVAL` - The return result contains no data portion, only cursor information
     * @return A ScanResult.
     */
    public ScanResult<String> tvsscan(final String index, Long cursor, HscanParams params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(SafeEncoder.encode(index));
        args.add(toByteArray(cursor));
        args.addAll(params.getParams());
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSSCAN, args.toArray(new byte[args.size()][]));
            return VectorBuilderFactory.SCAN_CURSOR_STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }
    
    public ScanResult<byte[]> tvsscan(byte[] index, Long cursor, HscanParams params) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(index);
        args.add(toByteArray(cursor));
        args.addAll(params.getParams());
        Jedis jedis = getJedis();
        try {
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
     * @param index index name
     * @param topn  topn result
     * @param vector query vector
     * @return VectorBuilderFactory.Knn<>
     */
    public VectorBuilderFactory.Knn<String> tvsknnsearch(final String index, Long topn, final String vector) {
        return tvsknnsearchfilter(index, topn, vector, "");
    }
    public VectorBuilderFactory.Knn<byte[]> tvsknnsearch(byte[] index, Long topn, byte[] vector) {
        return tvsknnsearchfilter(index, topn, vector, SafeEncoder.encode(""));
    }

    /**
     * TVS.KNNSEARCH TVS.KNNSEARCH index_name topn vector pattern
     * <p>
     * query entity by vector and scalar pattern
     *
     * @param index index name
     * @param topn  topn result
     * @param vector query vector
     * @param pattern support +, -，>, <, !=， ,()，&&, ||, !, ==
     * @return VectorBuilderFactory.Knn<>
     */
    public VectorBuilderFactory.Knn<String> tvsknnsearchfilter(final String index, Long topn, final String vector, final String pattern) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSKNNSEARCH, SafeEncoder.encode(index), toByteArray(topn), SafeEncoder.encode(vector), SafeEncoder.encode(pattern));
            return VectorBuilderFactory.STRING_KNN_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public VectorBuilderFactory.Knn<byte[]> tvsknnsearchfilter(byte[] index, Long topn, byte[] vector, byte[] pattern) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSKNNSEARCH, index, toByteArray(topn), vector, pattern);
            return VectorBuilderFactory.BYTE_KNN_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * TVS.MKNNSEARCH TVS.MKNNSEARCH index_name topn vector [vector...]
     *
     * @param index index name
     * @param topn topn for each vector
     * @param vectors vector list
     * @return Collection<>
     */
    public Collection<VectorBuilderFactory.Knn<String>> tvsmknnsearch(final String index, Long topn, Collection<String> vectors) {
        return tvsmknnsearchfilter(index, topn, vectors, "");
    }
    public Collection<VectorBuilderFactory.Knn<byte[]>> tvsmknnsearch(byte[] index, Long topn, Collection<byte[]> vectors) {
       return tvsmknnsearchfilter(index, topn, vectors, SafeEncoder.encode(""));
    }

    /**
     * TVS.MKNNSEARCH TVS.MKNNSEARCH index_name topn vector [vector...] pattern
     *
     * @param index index name
     * @param topn topn for each vector
     * @param vectors vector list
     * @param pattern filter pattern, support +, -，>, <, !=， ,()，&&, ||, !, ==
     * @return Collection<>
     */
    public Collection<VectorBuilderFactory.Knn<String>> tvsmknnsearchfilter(final String index, Long topn, Collection<String> vectors, final String pattern) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(SafeEncoder.encode(index));
        args.add(toByteArray(topn));
        args.add(toByteArray(vectors.size()));
        args.addAll(vectors.stream().map(vector -> SafeEncoder.encode(vector)).collect(Collectors.toList()));
        args.add(SafeEncoder.encode(pattern));
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSMKNNSEARCH, args.toArray(new byte[args.size()][]));
            return VectorBuilderFactory.STRING_KNN_BATCH_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Collection<VectorBuilderFactory.Knn<byte[]>> tvsmknnsearchfilter(byte[] index, Long topn, Collection<byte[]> vectors, byte[] pattern) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(index);
        args.add(toByteArray(topn));
        args.add(toByteArray(vectors.size()));
        args.addAll(vectors);
        args.add(pattern);
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.TVSMKNNSEARCH, args.toArray(new byte[args.size()][]));
            return VectorBuilderFactory.BYTE_KNN_BATCH_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }
}
