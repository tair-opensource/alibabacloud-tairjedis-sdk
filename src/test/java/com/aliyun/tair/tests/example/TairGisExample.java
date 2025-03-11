package com.aliyun.tair.tests.example;

import java.util.Map;

import com.aliyun.tair.tairgis.TairGis;
import io.valkey.Jedis;
import io.valkey.JedisPool;
import io.valkey.JedisPoolConfig;

public class TairGisExample {
    // init timeout
    private static final int DEFAULT_CONNECTION_TIMEOUT = 5000;
    // api timeout
    private static final int DEFAULT_SO_TIMEOUT = 2000;
    private static final String HOST = "r-xxx.redis.rds.aliyuncs.com";
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

    public static long gisadd(String key, String polygonName, String polygonWkt) {
        try {
            return tairGis.gisadd(key, polygonName, polygonWkt);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }

    public static Map<String, String> giscontains(String key, String searchWkt) {
        try {
            return tairGis.giscontains(key, searchWkt);
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
        String polygonName = "campus";
        String polygonWkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))";
        String searchWkt = "POINT (30 11)";
        System.out.println(del(key));
        System.out.println(gisadd(key, polygonName, polygonWkt));
        System.out.println(giscontains(key, searchWkt));
    }
}
