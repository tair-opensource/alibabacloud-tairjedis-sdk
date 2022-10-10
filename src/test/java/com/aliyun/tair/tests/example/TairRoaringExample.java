package com.aliyun.tair.tests.example;

import com.aliyun.tair.tairroaring.TairRoaring;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class TairRoaringExample {
    // init timeout
    private static final int DEFAULT_CONNECTION_TIMEOUT = 5000;
    // api timeout
    private static final int DEFAULT_SO_TIMEOUT = 2000;
    private static final String HOST = "r-xxx.redis.rds.aliyuncs.com";
    private static final int PORT = 6379;
    private static final String PASSWORD = null;
    private static JedisPool jedisPool = null;
    private static TairRoaring tairRoaring = null;
    private static final JedisPoolConfig config = new JedisPoolConfig();

    static {
        // JedisPool config: https://help.aliyun.com/document_detail/98726.html
        config.setMaxTotal(32);
        config.setMaxIdle(32);
        config.setMaxIdle(20);

        jedisPool = new JedisPool(config, HOST, PORT, DEFAULT_CONNECTION_TIMEOUT,
            DEFAULT_SO_TIMEOUT, PASSWORD, 0, null);
        tairRoaring = new TairRoaring(jedisPool);
    }

    public static long setbit(String key, long offset, String value) {
        try {
            return tairRoaring.trsetbit(key, offset, value);
        } catch (Exception e) {
            e.printStackTrace(); // handle exception
        }
        return -1;
    }

    public static long getbit(String key, long offset) {
        try {
            return tairRoaring.trgetbit(key, offset);
        } catch (Exception e) {
            e.printStackTrace(); // handle exception
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

    public static void main(String[] args) {
        del("key");
        setbit("key", 10, "1");
        System.out.println(getbit("key", 10));
    }
}
