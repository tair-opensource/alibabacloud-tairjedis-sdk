package com.aliyun.tair.tairvector;

import com.aliyun.tair.tairvector.factory.VectorBuilderFactory;
import com.aliyun.tair.tairvector.params.DistanceMethod;
import com.aliyun.tair.tairvector.params.HscanParams;
import com.aliyun.tair.tairvector.params.IndexAlgorithm;
import com.aliyun.tair.tairvector.params.MIndexKnnsearchParams;
import redis.clients.jedis.ScanResult;

import java.util.*;

public interface VectorShard {
    public void quit();
    public String tvscreateindex(final String index, int dims, IndexAlgorithm algorithm, DistanceMethod method, final String... params);
    public byte[] tvscreateindex(byte[] index, int dims, IndexAlgorithm algorithm, DistanceMethod method, final byte[]... params);

    public Map<String, String> tvsgetindex(final String index);
    public Map<byte[], byte[]> tvsgetindex(final byte[] index);

    public Long tvsdelindex(final String index);
    public Long tvsdelindex(final byte[] index);

    // public ScanResult<String> tvsscanindex(Long cursor, final HscanParams params);

    public Long tvshset(final String index,final String key, final String vector, final String... params);
    public Long tvshset(final byte[] index,final byte[] key, final byte[] vector, final byte[]... params);

    public Map<String, String> tvshgetall(final String index,  final String key);
    public Map<byte[], byte[]> tvshgetall(final byte[] index,  final byte[] key);

    public List<String> tvshmget(final String index,  final String key, final String... attrs);
    public List<byte[]> tvshmget(final byte[] index, final byte[] key, final byte[]... attrs);

    public Long tvsdel(final String index, final String key);
    public Long tvsdel(final byte[] index, final byte[] key);

    public Long tvshdel(final String index, final String key, final String... attrs);
    public Long tvshdel(final byte[] index, final byte[] key, final byte[]... attrs);

    public ScanResult<String>tvsscan(final String index, Long cursor, final HscanParams params);
    public ScanResult<byte[]>tvsscan(final byte[] index, Long cursor, final HscanParams params);

    public VectorBuilderFactory.Knn<String> tvsknnsearch(final String index, final Long topn, final String vector, final String... params);
    public VectorBuilderFactory.Knn<byte[]> tvsknnsearch(final byte[] index, final Long topn, final byte[] vector, final byte[]... params);

    public VectorBuilderFactory.Knn<String> tvsknnsearchfilter(final String index, Long topn, final String vector, final String pattern, final String... params);
    public VectorBuilderFactory.Knn<byte[]> tvsknnsearchfilter(byte[] index, Long topn, byte[] vector, byte[] pattern, final byte[]... params);

    public Collection<VectorBuilderFactory.Knn<String>> tvsmknnsearch(final String index, final Long topn, final Collection<String> vectors, final String... params);
    public Collection<VectorBuilderFactory.Knn<byte[]>> tvsmknnsearch(final byte[] index, final Long topn, final Collection<byte[]> vectors, final byte[]... params);

    public Collection<VectorBuilderFactory.Knn<String>> tvsmknnsearchfilter(final String index, Long topn, Collection<String> vectors, final String pattern, final String... params);
    public Collection<VectorBuilderFactory.Knn<byte[]>> tvsmknnsearchfilter(byte[] index, Long topn, Collection<byte[]> vectors, byte[] pattern, final byte[]... params);

    public VectorBuilderFactory.Knn<String> tvsmindexknnsearch(final Long topn, final String vector, MIndexKnnsearchParams params,final String... indexs);
    public VectorBuilderFactory.Knn<byte[]> tvsmindexknnsearch(final Long topn, final byte[] vector, MIndexKnnsearchParams params,final byte[]... indexs);

}
