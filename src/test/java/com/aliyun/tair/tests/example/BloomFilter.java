package com.aliyun.tair.tests.example;

import java.util.UUID;

import com.aliyun.tair.tairbloom.TairBloom;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class BloomFilter {
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
     * Recommend the doc to the user, ignore it if it has been recommended, otherwise recommend it and mark it.
     * @param userid the user id
     * @param docid the doc id
     */
    public static void recommendedSystem(final String userid, final String docid) {
        if (tairBloom.bfexists(userid, docid)) {
            // do nothing
        } else {
            // recommend to user sendRecommendMsg(docid);
            // add userid with docid
            tairBloom.bfadd(userid, docid);
        }
    }

    public static void main(String[] args) {
        String key = "BloomFilter";
        recommendedSystem(key, UUID.randomUUID().toString());
        recommendedSystem(key, UUID.randomUUID().toString());
        recommendedSystem(key, UUID.randomUUID().toString());
    }
}
