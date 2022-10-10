package com.aliyun.tair.tests.example;

import com.aliyun.tair.tairzset.TairZset;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class TairZsetExample {
    // init timeout
    private static final int DEFAULT_CONNECTION_TIMEOUT = 5000;
    // api timeout
    private static final int DEFAULT_SO_TIMEOUT = 2000;
    private static final String HOST = "r-xxx.redis.rds.aliyuncs.com";
    private static final int PORT = 6379;
    private static final String PASSWORD = null;
    private static JedisPool jedisPool = null;
    private static TairZset tairZset = null;
    private static final JedisPoolConfig config = new JedisPoolConfig();

    static {
        // JedisPool config: https://help.aliyun.com/document_detail/98726.html
        config.setMaxTotal(32);
        config.setMaxIdle(32);
        config.setMaxIdle(20);

        jedisPool = new JedisPool(config, HOST, PORT, DEFAULT_CONNECTION_TIMEOUT,
            DEFAULT_SO_TIMEOUT, PASSWORD, 0, null);
        tairZset = new TairZset(jedisPool);
    }

    public static long exzadd(String key, String score, String field) {
        try {
            return tairZset.exzadd(key, score, field);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static long exzrank(String key, String field) {
        try {
            return tairZset.exzrank(key, field);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
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
        String key = "key";
        String field = "item";
        String score = "1#1#2";
        System.out.println(del(key));
        System.out.println(exzadd(key, score, field));
        System.out.println(exzrank(key, field));
    }
}
