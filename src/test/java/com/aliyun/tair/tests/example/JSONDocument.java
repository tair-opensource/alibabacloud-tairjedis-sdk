package com.aliyun.tair.tests.example;

import com.aliyun.tair.tairdoc.TairDoc;
import io.valkey.JedisPool;
import io.valkey.JedisPoolConfig;

public class JSONDocument {
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
     * Save JSON in key at path.
     * @param key the key
     * @param path the path, can be JSONPath or JSONPointer
     * @param json the json content
     * @return success: true, fail: false
     */
    public static boolean jsonSave(final String key, final String path, final String json) {
        try {
            String result = tairDoc.jsonset(key, path, json);
            if ("OK".equals(result)) {
                return true;
            }
        } catch (Exception e) {
            // logger.error(e);
        }
        return false;
    }

    /**
     * Get JSON elements from path
     * @param key the key
     * @param path the path, can be JSONPath or JSONPointer
     * @return the JSON elements
     */
    public static String jsonGet(final String key, final String path) {
        try {
            return tairDoc.jsonget(key, path);
        } catch (Exception e) {
            // logger.error(e)
            return null;
        }
    }

    public static void main(String[] args) {
        String key = "JSONDocument";
        jsonSave(key, ".", "{\"name\":\"tom\",\"age\":22,\"description\":\"A man with a blue lightsaber\","
            + "\"friends\":[]}");
        System.out.println(jsonGet(key, ".description"));

    }
}
