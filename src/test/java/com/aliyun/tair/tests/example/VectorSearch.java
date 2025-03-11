package com.aliyun.tair.tests.example;

import com.aliyun.tair.tairvector.TairVector;
import com.aliyun.tair.tairvector.factory.VectorBuilderFactory;
import com.aliyun.tair.tairvector.params.DistanceMethod;
import com.aliyun.tair.tairvector.params.IndexAlgorithm;
import io.valkey.JedisPool;
import io.valkey.JedisPoolConfig;

public class VectorSearch {
    // init timeout
    private static final int DEFAULT_CONNECTION_TIMEOUT = 5000;
    // api timeout
    private static final int DEFAULT_SO_TIMEOUT = 2000;
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 6379;
    private static final String PASSWORD = null;
    private static JedisPool jedisPool = null;
    private static TairVector tairVector = null;
    private static final JedisPoolConfig config = new JedisPoolConfig();

    static {
        // JedisPool config: https://help.aliyun.com/document_detail/98726.html
        config.setMaxTotal(32);
        config.setMaxIdle(32);
        config.setMaxIdle(20);

        jedisPool = new JedisPool(config, HOST, PORT, DEFAULT_CONNECTION_TIMEOUT,
            DEFAULT_SO_TIMEOUT, PASSWORD, 0, null);
        tairVector = new TairVector(jedisPool);
    }

    public static boolean createIndex(final String index, int dims, final String... attrs) {
        try {
            String ret = tairVector.tvscreateindex(index, dims, IndexAlgorithm.HNSW, DistanceMethod.IP, attrs);
            return "OK".equals(ret);
        } catch (Exception e) {
            // logger.error(e);
            return false;
        }
    }

    /**
     * insert entity into tair vector
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
    public static boolean addVector(final String index, final String entityid, final String vector, final String...params) {
        try {
            tairVector.tvshset(index, entityid, vector, params);
            return true;
        } catch (Exception e) {
            // logger.error(e);
            return false;
        }
    }

    /**
     * query entity by vector
     *
     * @param index index name
     * @param topn  topn result
     * @param vector query vector
     * @return VectorBuilderFactory.Knn<>
     */
    public static VectorBuilderFactory.Knn<String> knnSearch(final String index, Long topn, final String vector) {
        try {
            return tairVector.tvsknnsearch(index, topn, vector);
        } catch (Exception e) {
            // logger.error(e);
            return null;
        }
    }

    public static void main(String[] args) {
        String index = "VectorSearch";
        createIndex(index, 8);

        addVector(index, "first_entity_knn", "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name", "sammy");
        addVector(index, "second_entity_knn", "[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]", "name", "tiddy");

        System.out.println(knnSearch(index, 2L, "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]"));
    }
}
