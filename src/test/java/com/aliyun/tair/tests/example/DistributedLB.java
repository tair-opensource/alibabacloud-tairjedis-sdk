package com.aliyun.tair.tests.example;

import com.aliyun.tair.tairzset.DistributedLeaderBoard;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class DistributedLB {
    // init timeout
    private static final int DEFAULT_CONNECTION_TIMEOUT = 5000;
    // api timeout
    private static final int DEFAULT_SO_TIMEOUT = 2000;
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 6379;
    private static final String PASSWORD = null;
    private static JedisPool jedisPool = null;
    private static DistributedLeaderBoard dlb = null;
    private static final JedisPoolConfig config = new JedisPoolConfig();

    private static final int shardKeySize = 10;  // sharded key size
    private static final int pageSize = 10;      // page size
    private static final boolean reverse = true; // the order
    private static final boolean useZeroIndexForRank = false;

    static {
        // JedisPool config: https://help.aliyun.com/document_detail/98726.html
        config.setMaxTotal(32);
        config.setMaxIdle(32);
        config.setMaxIdle(20);

        jedisPool = new JedisPool(config, HOST, PORT, DEFAULT_CONNECTION_TIMEOUT,
            DEFAULT_SO_TIMEOUT, PASSWORD, 0, null);
        dlb = new DistributedLeaderBoard("distributed_leaderboard", jedisPool,
            shardKeySize, pageSize, reverse, useZeroIndexForRank);
    }

    public static void main(String[] args) {
        JedisPool jedisPool = new JedisPool();
        // Create distribute leaderboard
        //                    Gold Silver Bronze
        dlb.addMember("A",     32,  21, 16);
        dlb.addMember("D",     14,  4,  16);
        dlb.addMember("C",     20,  7,  12);
        dlb.addMember("B",     25,  29, 21);
        dlb.addMember("E",     13,  21, 18);
        dlb.addMember("F",     13,  17,  14);

        // Get A rank
        dlb.rankFor("A"); // 1
        System.out.println(dlb.rankFor("A"));

        // Get top3
        dlb.top(3);
        System.out.println(dlb.top(3));
        // [{"member":"A","score":"32#21#16","rank":1}, 
        // {"member":"B","score":"25#29#21","rank":2}, 
        // {"member":"C","score":"20#7#12","rank":3}]
    }
}
