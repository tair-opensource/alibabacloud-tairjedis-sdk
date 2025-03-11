package com.aliyun.tair.tests.example;

import com.aliyun.tair.tairbloom.TairBloom;
import io.valkey.JedisPool;
import io.valkey.JedisPoolConfig;

public class CrawlerSystem {
    // init timeout
    private static final int DEFAULT_CONNECTION_TIMEOUT = 5000;
    // api timeout
    private static final int DEFAULT_SO_TIMEOUT = 2000;
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 6379;
    private static final String PASSWORD = null;
    private static JedisPool jedisPool = null;
    private static TairBloom tairBloom = null;
    private static final JedisPoolConfig config = new JedisPoolConfig();

    static {
        // JedisPool config: https://help.aliyun.com/document_detail/98726.html
        config.setMaxTotal(32);
        config.setMaxIdle(32);
        config.setMaxIdle(20);

        jedisPool = new JedisPool(config, HOST, PORT, DEFAULT_CONNECTION_TIMEOUT,
            DEFAULT_SO_TIMEOUT, PASSWORD, 0, null);
        tairBloom = new TairBloom(jedisPool);
    }

    /**
     * Determine if the URL has been crawled
     * @param key key
     * @param urls the urls
     */
    public static Boolean[] bfMexists(final String key, final String... urls) {
        try {
            return tairBloom.bfmexists(key, urls);
        } catch (Exception e) {
            // logger.error(e);
            return null;
        }
    }

    public static void main(String[] args) {
        String key = "CrawlerSystem";
        tairBloom.bfadd(key, "abc");
        tairBloom.bfadd(key, "def");
        tairBloom.bfadd(key, "ghi");
        System.out.println(bfMexists(key, "abc", "def", "xxx"));
    }
}
