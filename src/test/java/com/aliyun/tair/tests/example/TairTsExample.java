package com.aliyun.tair.tests.example;

import com.aliyun.tair.tairts.TairTs;
import com.aliyun.tair.tairts.results.ExtsDataPointResult;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class TairTsExample {
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

    public static boolean tsadd(String key, String field, String ts, double value) {
        try (Jedis jedis = jedisPool.getResource()) {
            TairTs tairts = new TairTs(jedis);
            String ret = tairts.extsadd(key, field, ts, value);
            if ("OK".equals(ret)) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public static ExtsDataPointResult tsget(String key, String field) {
        try (Jedis jedis = jedisPool.getResource()) {
            TairTs tairts = new TairTs(jedis);
            return tairts.extsget(key, field);
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
        String key = "cpu_load";
        String field = "ip_127.0.0.1";
        String ts = "1587350023000";
        double value = 3.0;
        System.out.println(del(key));
        System.out.println(tsadd(key, field, ts, value));
        System.out.println(tsget(key, field).getTs());
    }
}
