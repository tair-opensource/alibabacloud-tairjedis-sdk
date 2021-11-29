package com.aliyun.tair.tests.tairzset;

import com.aliyun.tair.tairzset.DistributedLeaderBoard;
import redis.clients.jedis.JedisPool;

public class DistributedLeaderBoradExample {
    private static final int shardKeySize = 10;  // 底层子排行榜的数量
    private static final int pageSize = 10;      // 排行榜每页包含的个数
    private static final boolean reverse = true; // 是否按照从小到大
    private static final boolean useZeroIndexForRank = false; // 是否用0作为排名起点

    public static void main(String[] args) {
        JedisPool jedisPool = new JedisPool();
        // 创建分布式排行榜排行榜
        DistributedLeaderBoard dlb = new DistributedLeaderBoard("distributed_leaderboard", jedisPool,
            shardKeySize, pageSize, reverse, useZeroIndexForRank);

        // 如果金牌数相同，按照银牌数排序，否则继续按照铜牌
        //                    金牌 银牌 铜牌
        dlb.addMember("A",     32,  21, 16);
        dlb.addMember("D",     14,  4,  16);
        dlb.addMember("C",     20,  7,  12);
        dlb.addMember("B",     25,  29, 21);
        dlb.addMember("E",     13,  21, 18);
        dlb.addMember("F",     13,  17,  14);

        // 获取 A 的排名
        dlb.rankFor("A"); // 1
        System.out.println(dlb.rankFor("A"));

        // 获取top3
        dlb.top(3);
        System.out.println(dlb.top(3));
        // [{"member":"A","score":"32#21#16","rank":1}, 
        // {"member":"B","score":"25#29#21","rank":2}, 
        // {"member":"C","score":"20#7#12","rank":3}]
    }
}
