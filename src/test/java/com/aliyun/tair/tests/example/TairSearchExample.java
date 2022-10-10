package com.aliyun.tair.tests.example;

import com.aliyun.tair.tairsearch.TairSearch;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class TairSearchExample {
    // init timeout
    private static final int DEFAULT_CONNECTION_TIMEOUT = 5000;
    // api timeout
    private static final int DEFAULT_SO_TIMEOUT = 2000;
    private static final String HOST = "r-xxx.redis.rds.aliyuncs.com";
    private static final int PORT = 6379;
    private static final String PASSWORD = null;
    private static JedisPool jedisPool = null;
    private static TairSearch tairSearch = null;
    private static final JedisPoolConfig config = new JedisPoolConfig();

    static {
        // 参数设置最佳实践可参考：https://help.aliyun.com/document_detail/98726.html
        config.setMaxTotal(32);
        config.setMaxIdle(32);
        config.setMaxIdle(20);

        jedisPool = new JedisPool(config, HOST, PORT, DEFAULT_CONNECTION_TIMEOUT,
            DEFAULT_SO_TIMEOUT, PASSWORD, 0, null);
        tairSearch = new TairSearch(jedisPool);
    }

    public static boolean createIndex(String index, String request) {
        try {
            String ret = tairSearch.tftcreateindex(index, request);
            if ("OK".equals(ret)) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public static String adddoc(String index, String doc) {
        try {
            return tairSearch.tftadddoc(index, doc);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String search(String index, String request) {
        try {
            return tairSearch.tftsearch(index, request);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static long del(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.del(key);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }

    public static void main(String[] args) throws Exception {
        String index = "index";
        String request = "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}";
        String doc = "{\"f0\":\"v0\",\"f1\":\"3\"}";
        String searchRequest = "{\"query\":{\"match\":{\"f1\":\"3\"}}}";
        System.out.println(del(index));
        System.out.println(createIndex(index, request));
        System.out.println(adddoc(index, doc));
        System.out.println(search(index, searchRequest));
    }
}
