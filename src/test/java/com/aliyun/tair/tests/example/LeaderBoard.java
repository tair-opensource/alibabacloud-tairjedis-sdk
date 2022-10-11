package com.aliyun.tair.tests.example;

import java.util.ArrayList;
import java.util.List;

import com.aliyun.tair.tairzset.TairZset;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author bodong.ybd
 * @date 2022/10/10
 */
public class LeaderBoard {
    // init timeout
    private static final int DEFAULT_CONNECTION_TIMEOUT = 5000;
    // api timeout
    private static final int DEFAULT_SO_TIMEOUT = 2000;
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 6379;
    private static final String PASSWORD = null;
    private static JedisPool jedisPool = null;
    private static TairZset tairZset = null;
    private static final JedisPoolConfig config = new JedisPoolConfig();

    static {
        // JedisPool config: https://help.aliyun.com/document_detail/98726.html
        config.setMaxTotal(32);
        config.setMaxIdle(32);
        config.setMaxIdle(20);

        jedisPool = new JedisPool(config, HOST, PORT, DEFAULT_CONNECTION_TIMEOUT,
            DEFAULT_SO_TIMEOUT, PASSWORD, 0, null);
        tairZset = new TairZset(jedisPool);
    }

    /**
     * Add User with Multi scores.
     * @param key the key
     * @param member the member
     * @param scores the multi dimensional score
     * @return success: true; fail: false
     */
    public static boolean addUser(final String key, final String member, final double... scores) {
        try {
            tairZset.exzadd(key, member, scores);
            return true;
        } catch (Exception e) {
            // logger.error(e);
            return false;
        }
    }

    /**
     * Get the top element of the leaderboard.
     * @param key the key
     * @param startOffset start offset
     * @param endOffset end offset
     * @return the top elements.
     */
    public static List<String> top(final String key, final long startOffset, final long endOffset) {
        try {
            return tairZset.exzrevrange(key, startOffset, endOffset);
        } catch (Exception e) {
            // logger.error(e);
            return new ArrayList<>();
        }
    }

    public static void main(String[] args) {
        String key = "LeaderBoard";
        // add three user
        addUser(key, "user1", 20, 10, 30);
        addUser(key, "user2", 20, 15, 10);
        addUser(key, "user3", 30, 10, 10);
        // get top 2
        System.out.println(top(key, 0, 1));

    }
}
