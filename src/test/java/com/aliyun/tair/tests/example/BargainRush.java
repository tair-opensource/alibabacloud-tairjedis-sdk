package com.aliyun.tair.tests.example;

import java.util.Arrays;

import com.aliyun.tair.tairstring.TairString;
import com.aliyun.tair.tairstring.params.ExincrbyParams;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author bodong.ybd
 * @date 2022/9/1
 */
public class BargainRush {
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
     * bargainRush decrements the value of key from upperBound by 1 until lowerBound
     * @param key the key
     * @param upperBound the max value
     * @param lowerBound the min value
     * @return acquire success: true; fail: false
     */
    public static boolean bargainRush(String key, int upperBound, int lowerBound) {
        try (Jedis jedis = jedisPool.getResource()) {
            TairString tairString = new TairString(jedis);
            tairString.exincrBy(key, -1, ExincrbyParams.ExincrbyParams().def(upperBound).min(lowerBound));
            return true;
        } catch (Exception e) {
            // logger.error(e);
            return false;
        }
    }

    public static void main(String[] args) {
        String key = "rateLimiter";
        for (int i = 0; i < 20; i++) {
            System.out.printf("attempt %d, result: %s\n", i, bargainRush(key, 10, 0));
        }
    }
}
