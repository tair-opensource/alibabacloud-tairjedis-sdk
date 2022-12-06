package com.aliyun.tair.tairvector;

import com.aliyun.tair.tairvector.factory.VectorBuilderFactory;
import com.aliyun.tair.tairvector.factory.VectorBuilderFactory.Knn;
import com.aliyun.tair.tairvector.params.DistanceMethod;
import com.aliyun.tair.tairvector.params.HscanParams;
import com.aliyun.tair.tairvector.params.IndexAlgorithm;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.util.JedisClusterCRC16;
import redis.clients.jedis.util.SafeEncoder;

import java.util.*;

public class TairVectorShard {
    private VectorShard vectirInstance;
    private int shardCount;

    public TairVectorShard(VectorShard vectirInstance, int shardCount) {
        this.vectirInstance = vectirInstance;
        if (shardCount < 1) {
            throw new IllegalArgumentException("shards should not be less than 1");
        }
        this.shardCount = shardCount;
    }

    public void quit() {
        this.vectirInstance.quit();
    }

    public String tvscreateindex(String index, int dims, IndexAlgorithm algorithm, DistanceMethod method, String... params) {
        List<String> indexNames = null;
        indexNames = defaultindexsplit(index, shardCount);

        String result = null;
        for (String indexName : indexNames) {
            result = this.vectirInstance.tvscreateindex(indexName, dims, algorithm, method, params);
            if (!result.equals("OK")) {
                //TODO delete pre index
                return result;
            }
        }
        return result;
    }

    public byte[] tvscreateindex(byte[] index, int dims, IndexAlgorithm algorithm, DistanceMethod method, byte[]... params) {
        List<String> indexNames = null;
        indexNames = defaultindexsplit(SafeEncoder.encode(index), shardCount);

        byte[] result = null;
        for (String indexName : indexNames) {
            result = this.vectirInstance.tvscreateindex(SafeEncoder.encode(indexName), dims, algorithm, method, params);
            if (!SafeEncoder.encode(result).equals("OK")) {
                //TODO delete pre index
                return result;
            }
        }
        return result;
    }

    public List<Map<String, String>> tvsgetindex(String index) {
        List<String> indexNames = null;
        indexNames = defaultindexsplit(index, shardCount);

        List<Map<String, String>> results = new ArrayList<>();
        for (String indexName : indexNames) {
            Map<String, String> result = this.vectirInstance.tvsgetindex(indexName);
            if (result == null || result.isEmpty())
                continue;
            else
                results.add(result);
        }
        return results;
    }

    public List<Map<byte[], byte[]>> tvsgetindex(byte[] index) {
        List<String> indexNames = null;
        indexNames = defaultindexsplit(SafeEncoder.encode(index), shardCount);

        List<Map<byte[], byte[]>> results = new ArrayList<>();
        for (String indexName : indexNames) {
            Map<byte[], byte[]> result = this.vectirInstance.tvsgetindex(SafeEncoder.encode(indexName));
            if (result == null || result.isEmpty())
                continue;
            else
                results.add(result);
        }
        return results;
    }

    public Long tvsdelindex(String index) {
        List<String> indexNames = null;
        indexNames = defaultindexsplit(index, shardCount);

        Long result = new Long(0);
        for (String indexName : indexNames) {
            result += this.vectirInstance.tvsdelindex(indexName);
        }
        return result;
    }

    public Long tvsdelindex(byte[] index) {
        List<String> indexNames = null;
        indexNames = defaultindexsplit(SafeEncoder.encode(index), shardCount);

        Long result = new Long(0);
        for (String indexName : indexNames) {
            result += this.vectirInstance.tvsdelindex(SafeEncoder.encode(indexName));
        }
        return result;
    }

    public Long tvshset(String index, String key, String vector, String... params) {
        List<String> indexNames = null;
        indexNames = defaultindexsplit(index, shardCount);

        int slotId = JedisClusterCRC16.getSlot(key);
        String indexName = indexNames.get(slotId % indexNames.size());
        return this.vectirInstance.tvshset(indexName, key, vector, params);
    }

    public Long tvshset(byte[] index, byte[] key, byte[] vector, byte[]... params) {
        List<String> indexNames = null;
        indexNames = defaultindexsplit(SafeEncoder.encode(index), shardCount);

        int slotId = JedisClusterCRC16.getSlot(key);
        String indexName = indexNames.get(slotId % indexNames.size());
        return this.vectirInstance.tvshset(SafeEncoder.encode(indexName), key, vector, params);
    }

    public Map<String, String> tvshgetall(String index, String key) {
        List<String> indexNames = null;
        indexNames = defaultindexsplit(index, shardCount);

        int slotId = JedisClusterCRC16.getSlot(key);
        String indexName = indexNames.get(slotId % indexNames.size());
        return this.vectirInstance.tvshgetall(indexName, key);
    }

    public Map<byte[], byte[]> tvshgetall(byte[] index, byte[] key) {
        List<String> indexNames = null;
        indexNames = defaultindexsplit(SafeEncoder.encode(index), shardCount);

        int slotId = JedisClusterCRC16.getSlot(key);
        String indexName = indexNames.get(slotId % indexNames.size());
        return this.vectirInstance.tvshgetall(SafeEncoder.encode(indexName), key);
    }

    public List<String> tvshmget(String index, String key, String... attrs) {
        List<String> indexNames = null;
        indexNames = defaultindexsplit(index, shardCount);

        int slotId = JedisClusterCRC16.getSlot(key);
        String indexName = indexNames.get(slotId % indexNames.size());
        return this.vectirInstance.tvshmget(indexName, key, attrs);
    }

    public List<byte[]> tvshmget(byte[] index, byte[] key, byte[]... attrs) {
        List<String> indexNames = null;
        indexNames = defaultindexsplit(SafeEncoder.encode(index), shardCount);

        int slotId = JedisClusterCRC16.getSlot(key);
        String indexName = indexNames.get(slotId % indexNames.size());
        return this.vectirInstance.tvshmget(SafeEncoder.encode(indexName), key, attrs);
    }

    public Long tvsdel(String index, String key) {
        List<String> indexNames = null;
        indexNames = defaultindexsplit(index, shardCount);

        int slotId = JedisClusterCRC16.getSlot(key);
        String indexName = indexNames.get(slotId % indexNames.size());
        return this.vectirInstance.tvsdel(indexName, key);
    }

    public Long tvsdel(byte[] index, byte[] key) {
        List<String> indexNames = null;
        indexNames = defaultindexsplit(SafeEncoder.encode(index), shardCount);

        int slotId = JedisClusterCRC16.getSlot(key);
        String indexName = indexNames.get(slotId % indexNames.size());
        return this.vectirInstance.tvsdel(SafeEncoder.encode(indexName), key);
    }


    public Long tvshdel(String index, String key, String... attrs) {
        List<String> indexNames = null;
        indexNames = defaultindexsplit(index, shardCount);

        int slotId = JedisClusterCRC16.getSlot(key);
        String indexName = indexNames.get(slotId % indexNames.size());
        return this.vectirInstance.tvshdel(indexName, key, attrs);
    }

    public Long tvshdel(byte[] index, byte[] key, byte[]... attrs) {
        List<String> indexNames = null;
        indexNames = defaultindexsplit(SafeEncoder.encode(index), shardCount);

        int slotId = JedisClusterCRC16.getSlot(key);
        String indexName = indexNames.get(slotId % indexNames.size());
        return this.vectirInstance.tvshdel(SafeEncoder.encode(indexName), key, attrs);
    }

    public List<ScanResult<String>> tvsscan(String index, Long cursor, HscanParams params) {
        List<String> indexNames = null;
        indexNames = defaultindexsplit(index, shardCount);

        List<ScanResult<String>> results = new ArrayList<>();
        for (String indexName : indexNames) {
            ScanResult<String> result = this.vectirInstance.tvsscan(indexName, cursor, params);
            if (result == null)
                continue;
            else
                results.add(result);
        }
        return results;
    }


    public List<ScanResult<byte[]>> tvsscan(byte[] index, Long cursor, HscanParams params) {
        List<String> indexNames = null;
        indexNames = defaultindexsplit(SafeEncoder.encode(index), shardCount);

        List<ScanResult<byte[]>> results = new ArrayList<>();
        for (String indexName : indexNames) {
            ScanResult<byte[]> result = this.vectirInstance.tvsscan(SafeEncoder.encode(indexName), cursor, params);
            if (result == null)
                continue;
            else
                results.add(result);
        }
        return results;
    }

    public Knn<String> tvsknnsearch(String index, Long topn, String vector, String... params) {
        return tvsknnsearchfilter(index, topn, vector, "", params);
    }

    public Knn<byte[]> tvsknnsearch(byte[] index, Long topn, byte[] vector, byte[]... params) {
        return tvsknnsearchfilter(index, topn, vector, SafeEncoder.encode(""), params);
    }

    public Collection<Knn<String>> tvsmknnsearch(String index, Long topn, Collection<String> vectors, String... params) {
        return tvsmknnsearchfilter(index, topn, vectors, "", params);
    }

    public Collection<Knn<byte[]>> tvsmknnsearch(byte[] index, Long topn, Collection<byte[]> vectors, byte[]... params) {
        return tvsmknnsearchfilter(index, topn, vectors, SafeEncoder.encode(""), params);
    }

    public Knn<String> tvsknnsearchfilter(final String index, Long topn, final String vector, final String pattern, final String... params) {
        List<String> indexNames = null;
        indexNames = defaultindexsplit(index, shardCount);
        Long shardTopN = topnforshard(topn, shardCount);

        List<Knn<String>> rets = new ArrayList<>();
        for (int i = 0; i < indexNames.size(); ++i) {
            rets.add(this.vectirInstance.tvsknnsearchfilter(indexNames.get(i), shardTopN, vector, pattern, params));
        }
        return mergeSearchResult(rets, topn);
    }

    public Knn<byte[]> tvsknnsearchfilter(byte[] index, Long topn, byte[] vector, byte[] pattern, final byte[]... params) {
        List<String> indexNames = null;
        indexNames = defaultindexsplit(SafeEncoder.encode(index), shardCount);
        Long shardTopN = topnforshard(topn, shardCount);

        List<Knn<byte[]>> rets = new ArrayList<>();
        for (int i = 0; i < indexNames.size(); ++i) {
            rets.add(this.vectirInstance.tvsknnsearchfilter(SafeEncoder.encode(indexNames.get(i)), shardTopN, vector, pattern, params));
        }
        return mergeSearchResult(rets, topn);
    }

    public Collection<Knn<String>> tvsmknnsearchfilter(final String index, Long topn, Collection<String> vectors, final String pattern, final String... params) {
        List<String> indexNames = null;
        indexNames = defaultindexsplit(index, shardCount);
        Long shardTopN = topnforshard(topn, shardCount);

        List<List<Knn<String>>> rets = new ArrayList<>();
        for (int i = 0; i < vectors.size(); ++i) { rets.add(new ArrayList<>()); }

        for (int i = 0; i < indexNames.size(); ++i) {
            Collection<Knn<String>> shardRet = this.vectirInstance.tvsmknnsearchfilter(indexNames.get(i), shardTopN, vectors, pattern, params);
            int vectorIdx = 0;
            for (Knn<String> vectorRet : shardRet) {
                rets.get(vectorIdx).add(vectorRet);
                vectorIdx++;
            }
        }
        Collection<Knn<String>> result = new ArrayList<>();
        for (List<Knn<String>> ret : rets) {
            result.add(mergeSearchResult(ret, topn));
        }
        return result;
    }

    public Collection<Knn<byte[]>> tvsmknnsearchfilter(byte[] index, Long topn, Collection<byte[]> vectors, byte[] pattern, final byte[]... params) {
        List<String> indexNames = null;
        indexNames = defaultindexsplit(SafeEncoder.encode(index), shardCount);
        Long shardTopN = topnforshard(topn, shardCount);

        List<List<Knn<byte[]>>> rets = new ArrayList<>();
        for (int i = 0; i < vectors.size(); ++i) { rets.add(new ArrayList<>()); }

        for (int i = 0; i < indexNames.size(); ++i) {
            Collection<Knn<byte[]>> shardRet = this.vectirInstance.tvsmknnsearchfilter(SafeEncoder.encode(indexNames.get(i)), shardTopN, vectors, pattern, params);
            int vectorIdx = 0;
            for (Knn<byte[]> vectorRet : shardRet) {
                rets.get(vectorIdx).add(vectorRet);
                vectorIdx++;
            }
        }
        Collection<Knn<byte[]>> result = new ArrayList<>();
        for (List<Knn<byte[]>> ret : rets) {
            result.add(mergeSearchResult(ret, topn));
        }

        return result;
    }

    static public List<String> defaultindexsplit(final String index, final int shards) {
        List<String> nameList = new ArrayList<>();
        for (int i = 0; i < shards; ++i) {
            nameList.add(String.join("_", index, String.valueOf(i)));
        }
        return nameList;
    }

    static public Long topnforshard(final Long topn, final int shards) {
        Long shardTopN = (long)Math.ceil(topn / shards * 1.1);
        return shardTopN;
    }

    static public <T> Knn<T> mergeSearchResult(List<Knn<T>> rets, Long topn) {
        Queue<VectorBuilderFactory.KnnItem<T>> queue = new PriorityQueue<>();
        for (Knn<T> ret : rets) {
            for (VectorBuilderFactory.KnnItem<T> item : ret.getKnnResults()) {
                queue.add(item);
            }
        }

        Knn<T> mergeRets = new Knn<>();
        int count = queue.size();
        for (int i = 0; i < topn && i < count; ++i) {
            mergeRets.add(queue.poll());
        }

        return mergeRets;
    }
}
