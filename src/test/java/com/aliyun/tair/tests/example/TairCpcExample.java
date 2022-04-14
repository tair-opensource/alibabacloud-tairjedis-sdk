package com.aliyun.tair.tests.example;

import com.aliyun.tair.taircpc.TairCpc;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class TairCpcExample {
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

    public static boolean cpcUpdate(String key, String field) {
        try (Jedis jedis = jedisPool.getResource()) {
            TairCpc cpc = new TairCpc(jedis);
            String result = cpc.cpcUpdate(key, field);
            if ("OK".equals(result)) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public static double cpcEstimate(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            TairCpc cpc = new TairCpc(jedis);
            return cpc.cpcEstimate(key);
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
        System.out.println(del(key));
        System.out.println(cpcUpdate(key, field));
        System.out.println(cpcEstimate(key));
    }
}
