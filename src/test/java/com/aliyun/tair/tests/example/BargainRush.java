package com.aliyun.tair.tests.example;

import com.aliyun.tair.tairstring.TairString;
import com.aliyun.tair.tairstring.params.ExincrbyParams;
import io.valkey.JedisPool;
import io.valkey.JedisPoolConfig;

public class BargainRush {
    // init timeout
    private static final int DEFAULT_CONNECTION_TIMEOUT = 5000;
    // api timeout
    private static final int DEFAULT_SO_TIMEOUT = 2000;
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 6379;
    private static final String PASSWORD = null;
    private static JedisPool jedisPool = null;
    private static TairString tairString = null;
    private static final JedisPoolConfig config = new JedisPoolConfig();

    static {
        // JedisPool config: https://help.aliyun.com/document_detail/98726.html
        config.setMaxTotal(32);
        config.setMaxIdle(32);
        config.setMaxIdle(20);

        jedisPool = new JedisPool(config, HOST, PORT, DEFAULT_CONNECTION_TIMEOUT,
            DEFAULT_SO_TIMEOUT, PASSWORD, 0, null);
        tairString = new TairString(jedisPool);
    }

    /**
     * bargainRush decrements the value of key from upperBound by 1 until lowerBound
     * @param key the key
     * @param upperBound the max value
     * @param lowerBound the min value
     * @return acquire success: true; fail: false
     */
    public static boolean bargainRush(final String key, final int upperBound, final int lowerBound) {
        try {
            tairString.exincrBy(key, -1, ExincrbyParams.ExincrbyParams().def(upperBound).min(lowerBound));
            return true;
        } catch (Exception e) {
            // logger.error(e);
            return false;
        }
    }

    public static void main(String[] args) {
        String key = "bargainRush";
        for (int i = 0; i < 20; i++) {
            System.out.printf("attempt %d, result: %s\n", i, bargainRush(key, 10, 0));
        }
    }
}
