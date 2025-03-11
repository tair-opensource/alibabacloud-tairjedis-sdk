package com.aliyun.tair.tests.example;

import com.aliyun.tair.tairbloom.TairBloom;
import io.valkey.Jedis;
import io.valkey.JedisPool;
import io.valkey.JedisPoolConfig;

public class TairBloomExample {
    // init timeout
    private static final int DEFAULT_CONNECTION_TIMEOUT = 5000;
    // api timeout
    private static final int DEFAULT_SO_TIMEOUT = 2000;
    private static final String HOST = "r-xxx.redis.rds.aliyuncs.com";
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

    public static boolean bfadd(String key, String item) {
        try {
            return tairBloom.bfadd(key, item);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public static boolean bfexists(String key, String item) {
        try {
            return tairBloom.bfexists(key, item);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
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
        String item = "item";
        System.out.println(del(key));
        System.out.println(bfadd(key, item));
        System.out.println(bfexists(key, item));
    }
}
