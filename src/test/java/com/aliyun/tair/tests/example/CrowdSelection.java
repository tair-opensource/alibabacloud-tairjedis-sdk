package com.aliyun.tair.tests.example;

import com.aliyun.tair.tairroaring.TairRoaring;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class CrowdSelection {
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
     * Get key offset value.
     * @param key the key
     * @param offset the offset
     * @return the offset value, if not exists, return 0
     */
    public static long getBit(final String key, final long offset) {
        try {
            return tairRoaring.trgetbit(key, offset);
        } catch (Exception e) {
            // logger.error(e);
            return -1;
        }
    }

    /**
     * AND the two bitmaps and store the result in a new destkey.
     * @param destkey the dest key
     * @param keys the source key
     * @return success: true, fail: false
     */
    public static boolean bitAnd(final String destkey, final String... keys) {
        try {
            tairRoaring.trbitop(destkey, "AND", keys);
            return true;
        } catch (Exception e) {
            // logger.error(e);
            return false;
        }
    }

    public static void main(String[] args) {
        String key1 = "CrowdSelection-1";
        String key2 = "CrowdSelection-2";
        String key3 = "CrowdSelection-destKey";
        setBit(key1, 0, 1);
        setBit(key1, 1, 1);
        setBit(key2, 1, 1);
        System.out.println(getBit(key1, 0));
        bitAnd(key3, key1, key2);
        System.out.println(getBit(key3, 0));
        System.out.println(getBit(key3, 1));
    }
}
