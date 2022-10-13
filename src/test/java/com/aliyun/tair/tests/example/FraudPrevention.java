package com.aliyun.tair.tests.example;

import com.aliyun.tair.taircpc.TairCpc;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class FraudPrevention {
    // init timeout
    private static final int DEFAULT_CONNECTION_TIMEOUT = 5000;
    // api timeout
    private static final int DEFAULT_SO_TIMEOUT = 2000;
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 6379;
    private static final String PASSWORD = null;
    private static JedisPool jedisPool = null;
    private static TairCpc tairCpc = null;
    private static final JedisPoolConfig config = new JedisPoolConfig();

    static {
        // JedisPool config: https://help.aliyun.com/document_detail/98726.html
        config.setMaxTotal(32);
        config.setMaxIdle(32);
        config.setMaxIdle(20);

        jedisPool = new JedisPool(config, HOST, PORT, DEFAULT_CONNECTION_TIMEOUT,
            DEFAULT_SO_TIMEOUT, PASSWORD, 0, null);
        tairCpc = new TairCpc(jedisPool);
    }

    /**
     * update item to key
     * @param key the key
     * @param item the item
     * @return success: true, fail: false
     */
    public static boolean cpcAdd(final String key, final String item) {
        try {
            String ret = tairCpc.cpcUpdate(key, item);
            return "OK".equals(ret);
        } catch (Exception e) {
            // logger.error(e)
            return false;
        }
    }

    /**
     * Estimate all quantities in cpc.
     * @param key the key
     * @return the number
     */
    public static Double cpcEstimate(final String key) {
        try {
            return tairCpc.cpcEstimate(key);
        } catch (Exception e) {
            // logger.error(e)
            return null;
        }
    }

    public static void main(String[] args) {
        String key = "FraudPrevention";
        cpcAdd(key, "a");
        cpcAdd(key, "b");
        cpcAdd(key, "c");
        System.out.println(cpcEstimate(key));
        cpcAdd(key, "d");
        System.out.println(cpcEstimate(key));
    }
}
