package com.aliyun.tair.tests.example;

import com.aliyun.tair.tairdoc.TairDoc;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class GamePackage {
    // init timeout
    private static final int DEFAULT_CONNECTION_TIMEOUT = 5000;
    // api timeout
    private static final int DEFAULT_SO_TIMEOUT = 2000;
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 6379;
    private static final String PASSWORD = null;
    private static JedisPool jedisPool = null;
    private static TairDoc tairDoc = null;
    private static final JedisPoolConfig config = new JedisPoolConfig();

    static {
        // JedisPool config: https://help.aliyun.com/document_detail/98726.html
        config.setMaxTotal(32);
        config.setMaxIdle(32);
        config.setMaxIdle(20);

        jedisPool = new JedisPool(config, HOST, PORT, DEFAULT_CONNECTION_TIMEOUT,
            DEFAULT_SO_TIMEOUT, PASSWORD, 0, null);
        tairDoc = new TairDoc(jedisPool);
    }

    /**
     * Add equipment to package
     * @param key the key
     * @param packagePath the package path
     * @param equipment the new equipment
     * @return total number of equipment
     */
    public static Long addEquipment(final String key, final String packagePath, final String equipment) {
        try {
            return tairDoc.jsonarrAppend(key, packagePath, equipment);
        } catch (Exception e) {
            // logger.error(e);
            return null;
        }
    }

    public static void main(String[] args) {
        String key = "GamePackage";
        tairDoc.jsonset(key, ".", "[]");
        System.out.println(addEquipment(key, ".", "\"lightsaber\""));
        System.out.println(addEquipment(key, ".", "\"howitzer\""));
        System.out.println(addEquipment(key, ".", "\"gun\""));

    }

}
