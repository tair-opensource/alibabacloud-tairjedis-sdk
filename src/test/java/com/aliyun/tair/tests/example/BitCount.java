package com.aliyun.tair.tests.example;

import com.aliyun.tair.tairroaring.TairRoaring;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class BitCount {
    // init timeout
    private static final int DEFAULT_CONNECTION_TIMEOUT = 5000;
    // api timeout
    private static final int DEFAULT_SO_TIMEOUT = 2000;
    private static final String HOST = "127.0.0.1";
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

    /**
     * Set key offset value, value can be 0 or 1.
     * @param key the key
     * @param offset the offset
     * @param value the new value
     * @return success: true, fail: false
     */
    public static boolean setBit(final String key, final long offset, final long value) {
        try {
            tairRoaring.trsetbit(key, offset, value);
            return true;
        } catch (Exception e) {
            // logger.error(e)
            return false;
        }
    }

    /**
     * Count the number of elements in the bitmap.
     * @param key the key
     * @return the number of elements
     */
    public static long bitCount(final String key) {
        try {
            return tairRoaring.trbitcount(key);
        } catch (Exception e) {
            // logger.error(e);
            return -1;
        }
    }

    public static void main(String[] args) {
        String key1 = "BitCount";
        setBit(key1, 0, 1);
        setBit(key1, 1, 1);
        setBit(key1, 2, 1);
        System.out.println(bitCount(key1));
    }
}
