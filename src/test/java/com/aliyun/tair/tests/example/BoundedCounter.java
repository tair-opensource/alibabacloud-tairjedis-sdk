package com.aliyun.tair.tests.example;

import java.util.ArrayList;
import java.util.Arrays;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author bodong.ybd
 * @date 2022/9/1
 */
public class BoundedCounter {
    // init timeout
    private static final int DEFAULT_CONNECTION_TIMEOUT = 5000;
    // api timeout
    private static final int DEFAULT_SO_TIMEOUT = 2000;
    private static final String HOST = "127.0.0.1";
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

    /**
     * tryAcquire is thread-safe and will increment the key from 0 to the upper bound within an interval of time,
     * and return failure once it exceeds
     * @param key the key
     * @param upperBound the max value
     * @param interval the time interval
     * @return acquire success: true; fail: false
     */
    public static boolean tryAcquire(String key, int upperBound, int interval) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.eval("if redis.call('exists', KEYS[1]) == 1 "
                    + "then return redis.call('EXINCRBY', KEYS[1], '1', 'MAX', ARGV[1], 'KEEPTTL') "
                    + "else return redis.call('EXSET', KEYS[1], 0, 'EX', ARGV[2]) end",
                Arrays.asList(key), Arrays.asList(String.valueOf(upperBound), String.valueOf(interval)));
            return true;
        } catch (Exception e) {
            // logger.error(e);
            return false;
        }
    }

    public static void main(String[] args) {
        String key = "rateLimiter";
        for (int i = 0; i < 10; i++) {
            System.out.printf("attempt %d, result: %s\n", i, tryAcquire(key, 8, 10));
        }
    }
}
