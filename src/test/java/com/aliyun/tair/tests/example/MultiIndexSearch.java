package com.aliyun.tair.tests.example;

import com.aliyun.tair.tairsearch.TairSearch;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class MultiIndexSearch {
    // init timeout
    private static final int DEFAULT_CONNECTION_TIMEOUT = 5000;
    // api timeout
    private static final int DEFAULT_SO_TIMEOUT = 2000;
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 6379;
    private static final String PASSWORD = null;
    private static JedisPool jedisPool = null;
    private static TairSearch tairSearch = null;
    private static final JedisPoolConfig config = new JedisPoolConfig();

    static {
        // JedisPool config: https://help.aliyun.com/document_detail/98726.html
        config.setMaxTotal(32);
        config.setMaxIdle(32);
        config.setMaxIdle(20);

        jedisPool = new JedisPool(config, HOST, PORT, DEFAULT_CONNECTION_TIMEOUT,
            DEFAULT_SO_TIMEOUT, PASSWORD, 0, null);
        tairSearch = new TairSearch(jedisPool);
    }

    /**
     * create index, The field of index is parsed according to the field corresponding to the text
     * @param index the index
     * @param schema the index schema
     * @return success: true, fail: false.
     */
    public static boolean createIndex(final String index, final String schema) {
        try {
            tairSearch.tftcreateindex(index, schema);
            return true;
        } catch (Exception e) {
            // logger.error(e);
            return false;
        }
    }

    /**
     * Add doc to index, doc is JSON format.
     * @param index the index
     * @param doc the doc content
     * @return unique doc id
     */
    public static String addDoc(final String index, final String doc) {
        try {
            return tairSearch.tftadddoc(index, doc);
        } catch (Exception e) {
            // logger.error(e)
            return null;
        }
    }

    /**
     * search index by request
     * @param index the index
     * @param request the request
     * @return
     */
    public static String searchIndex(final String index, final String request) {
        try {
            return tairSearch.tftsearch(index, request);
        } catch (Exception e) {
            // logger.error(e);
            return null;
        }
    }

    public static void main(String[] args) {
        String key = "MultiIndexSearch";
        // create index
        createIndex(key, "{\"mappings\":{\"properties\":{\"departure\":{\"type\":\"keyword\"},"
            + "\"destination\":{\"type\":\"keyword\"},\"date\":{\"type\":\"keyword\"},"
            + "\"seat\":{\"type\":\"keyword\"},\"with\":{\"type\":\"keyword\"},\"flight_id\":{\"type\":\"keyword\"},"
            + "\"price\":{\"type\":\"double\"},\"departure_time\":{\"type\":\"long\"},"
            + "\"destination_time\":{\"type\":\"long\"}}}}");
        // add doc
        addDoc(key, "{\"departure\":\"zhuhai\",\"destination\":\"hangzhou\",\"date\":\"2022-09-01\","
            + "\"seat\":\"first\",\"with\":\"baby\",\"flight_id\":\"CZ1000\",\"price\":986.1,"
            + "\"departure_time\":1661991010,\"destination_time\":1661998210}");

        // search index
        String request = "{\"sort\":[\"departure_time\"],\"query\":{\"bool\":{\"must\":[{\"term\":{\"date\":\"2022-09"
            + "-01\"}},{\"term\":{\"seat\":\"first\"}}]}}}";
        System.out.println(searchIndex(key, request));
    }
}
