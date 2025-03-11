package com.aliyun.tair.tests.example;

import java.util.Map;

import com.aliyun.tair.tairgis.TairGis;
import io.valkey.JedisPool;
import io.valkey.JedisPoolConfig;

public class LbsBuy {
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
     * Add a service store geographical scope.
     * @param key the key
     * @param storeName the store name
     * @param storeWkt the store wkt
     * @return success: true, fail: false
     */
    public static boolean addPolygon(final String key, final String storeName, final String storeWkt) {
        try {
            Long ret = tairGis.gisadd(key, storeName, storeWkt);
            if (ret == 1) {
                return true;
            }
        } catch (Exception e) {
            // logger.error(e);
        }
        return false;
    }

    /**
     * Determine whether the user's location is within the service range of the store.
     * @param key the key
     * @param userLocation the user location
     * @return Stores that can serve users
     */
    public static Map<String, String> getServiceStore(final String key, final String userLocation) {
        try {
            return tairGis.giscontains(key, userLocation);
        } catch (Exception e) {
            // logger.error(e);
            return null;
        }
    }

    public static void main(String[] args) throws Exception {
        String key = "LbsBuy";
        addPolygon(key, "store-1", "POLYGON ((120.058897 30.283681, 120.093033 30.286363, 120.097632 30.269147, 120.050705 30.252863))");
        addPolygon(key, "store-2", "POLYGON ((120.026343 30.285739, 120.029289 30.280749, 120.0382 30.281997, 120.037051 30.288109))");

        System.out.println((getServiceStore(key, "POINT(120.072264 30.27501)")));
    }
}
