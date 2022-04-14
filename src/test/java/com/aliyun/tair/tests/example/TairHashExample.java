package com.aliyun.tair.tests.example;

import com.aliyun.tair.tairhash.TairHash;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class TairHashExample {
    // init timeout
    private static final int DEFAULT_CONNECTION_TIMEOUT = 5000;
    // api timeout
    private static final int DEFAULT_SO_TIMEOUT = 2000;
    private static final String HOST = "r-xxx.redis.rds.aliyuncs.com";
    private static final int PORT = 6379;
    private static final String PASSWORD = null;
    private static JedisPool jedisPool = null;
    private static final JedisPoolConfig config = new JedisPoolConfig();

    static {
        // 参数设置最佳实践可参考：https://help.aliyun.com/document_detail/98726.html
        config.setMaxTotal(32);
        config.setMaxIdle(32);
        config.setMaxIdle(20);

        jedisPool = new JedisPool(config, HOST, PORT, DEFAULT_CONNECTION_TIMEOUT,
            DEFAULT_SO_TIMEOUT, PASSWORD, 0, null);
    }

    public static long exhset(String key, String field, String value) {
        try (Jedis jedis = jedisPool.getResource()) {
            TairHash hash = new TairHash(jedis);
            return hash.exhset(key, field, value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static String exhget(String key, String field) {
        try (Jedis jedis = jedisPool.getResource()) {
            TairHash hash = new TairHash(jedis);
            return hash.exhget(key, field);
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
        String key = "key";
        String field = "item";
        String value = "value";
        System.out.println(del(key));
        System.out.println(exhset(key, field, value));
        System.out.println(exhget(key, field));
    }
}
