package com.aliyun.tair.tests.example;

import java.util.Map;

import com.aliyun.tair.tairgis.TairGis;
import io.valkey.JedisPool;
import io.valkey.JedisPoolConfig;

public class CarTrack {
    // init timeout
    private static final int DEFAULT_CONNECTION_TIMEOUT = 5000;
    // api timeout
    private static final int DEFAULT_SO_TIMEOUT = 2000;
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 6379;
    private static final String PASSWORD = null;
    private static JedisPool jedisPool = null;
    private static TairGis tairGis = null;
    private static final JedisPoolConfig config = new JedisPoolConfig();

    static {
        // JedisPool config: https://help.aliyun.com/document_detail/98726.html
        config.setMaxTotal(32);
        config.setMaxIdle(32);
        config.setMaxIdle(20);

        jedisPool = new JedisPool(config, HOST, PORT, DEFAULT_CONNECTION_TIMEOUT,
            DEFAULT_SO_TIMEOUT, PASSWORD, 0, null);
        tairGis = new TairGis(jedisPool);
    }

    /**
     * add longitude/latitude to key, timestamp represents the current moment.
     * @param key the key
     * @param ts the timestamp
     * @param longitude the longitude
     * @param latitude the latitude
     * @return success: true, fail: false
     */
    public static boolean addCoordinate(final String key, final String ts, final double longitude, final double latitude) {
        try {
            Long ret = tairGis.gisadd(key, ts, "POINT (" + longitude + " " + latitude + ")");
            if (ret == 1) {
                return true;
            }
        } catch (Exception e) {
            // logger.error(e);
        }
        return false;
    }

    /**
     * Get all points under a key.
     * @param key the key
     * @return A map, the key is the time, and the value is the coordinate
     */
    public static Map<String, String> getAllCoordinate(final String key) {
        try {
            return tairGis.gisgetall(key);
        } catch (Exception e) {
            // logger.error(e);
            return null;
        }
    }

    public static void main(String[] args) throws Exception {
        String key = "CarTrack";
        addCoordinate(key, String.valueOf(System.currentTimeMillis()), 120.036188, 30.287922);
        Thread.sleep(1);
        addCoordinate(key, String.valueOf(System.currentTimeMillis()), 120.037625, 30.292225);
        Thread.sleep(1);
        addCoordinate(key, String.valueOf(System.currentTimeMillis()), 120.034435, 30.303303);

        System.out.println(getAllCoordinate(key));
    }
}
