package com.aliyun.tair.tests.example;

import com.aliyun.tair.tairsearch.TairSearch;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class FullTextSearch {
    // init timeout
    private static final int DEFAULT_CONNECTION_TIMEOUT = 5000;
    // api timeout
    private static final int DEFAULT_SO_TIMEOUT = 2000;
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 6379;
    private static final String PASSWORD = null;
    private static JedisPool jedisPool = null;
    private static TairSearch tairSearch = null;
    private static final JedisPoolConfig config = new JedisPoolConfig();

    static {
        // 参数设置最佳实践可参考：https://help.aliyun.com/document_detail/98726.html
        config.setMaxTotal(32);
        config.setMaxIdle(32);
        config.setMaxIdle(20);

        jedisPool = new JedisPool(config, HOST, PORT, DEFAULT_CONNECTION_TIMEOUT,
            DEFAULT_SO_TIMEOUT, PASSWORD, 0, null);
        tairSearch = new TairSearch(jedisPool);
    }

    /**
     * create index, The field of index is parsed according to the field corresponding to the text
     * @param index the index
     * @param schema the index schema
     * @return success: true, fail: false.
     */
    public static boolean createIndex(final String index, final String schema) {
        try {
            tairSearch.tftcreateindex(index, schema);
            return true;
        } catch (Exception e) {
            // logger.error(e);
            return false;
        }
    }

    /**
     * Add doc to index, doc is JSON format.
     * @param index the index
     * @param doc the doc content
     * @return unique doc id
     */
    public static String addDoc(final String index, final String doc) {
        try {
            return tairSearch.tftadddoc(index, doc);
        } catch (Exception e) {
            // logger.error(e)
            return null;
        }
    }

    /**
     * search index by request
     * @param index the index
     * @param request the request
     * @return
     */
    public static String searchIndex(final String index, final String request) {
        try {
            return tairSearch.tftsearch(index, request);
        } catch (Exception e) {
            // logger.error(e);
            return null;
        }
    }

    public static void main(String[] args) {
        String key = "FullTextSearch";
        // create index
        createIndex(key, "{\"mappings\":{\"properties\":{\"title\":{\"type\":\"keyword\"},"
            + "\"content\":{\"type\":\"text\",\"analyzer\":\"jieba\"},\"time\":{\"type\":\"long\"},"
            + "\"author\":{\"type\":\"keyword\"},\"heat\":{\"type\":\"integer\"}}}}");
        // add doc
        addDoc(key, "{\"title\":\"Does not work\",\"content\":\"It was removed from the beta a while ago. You should "
            + "have expected it was going to be removed from the stable client as well at some point.\","
            + "\"time\":1541713787,\"author\":\"cSg|mc\",\"heat\":10}");
        addDoc(key, "{\"title\":\"paypal no longer launches to purchase\",\"content\":\"Since the last update, I "
            + "cannot purchase anything via the app. I just keep getting a screen that says\",\"time\":1551476987,"
            + "\"author\":\"disasterpeac\",\"heat\":2}");
        addDoc(key, "{\"title\":\"cat not login\",\"content\":\"Hey! I am trying to login to steam beta client via qr"
            + " code / steam guard code but both methods does not work for me\",\"time\":1664488187,"
            + "\"author\":\"7xx\",\"heat\":100}");
        // search index
        String request = "{\"sort\":[{\"heat\":{\"order\":\"desc\"}}],\"query\":{\"match\":{\"content\":\"paypal work"
            + " code\"}}}";
        System.out.println(searchIndex(key, request));
    }
}
