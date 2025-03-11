package com.aliyun.tair.tests.tairzset;

import com.aliyun.tair.tairzset.LeaderBoard;
import io.valkey.JedisPool;

public class LeaderBoradExample {
    public static void main(String[] args) {
        JedisPool jedisPool = new JedisPool();
        // 创建排行榜
        LeaderBoard lb = new LeaderBoard("leaderboard", jedisPool, 10, true, false);

        // 如果金牌数相同，按照银牌数排序，否则继续按照铜牌
        //                    金牌 银牌 铜牌
        lb.addMember("A",     32,  21, 16);
        lb.addMember("D",     14,  4,  16);
        lb.addMember("C",     20,  7,  12);
        lb.addMember("B",     25,  29, 21);
        lb.addMember("E",     13,  21, 18);
        lb.addMember("F",     13,  17,  14);

        // 获取 A 的排名
        lb.rankFor("A"); // 1
        System.out.println(lb.rankFor("A"));

        // 获取top3
        lb.top(3);
        System.out.println(lb.top(3));
        // [{"member":"A","score":"32#21#16","rank":1}, 
        // {"member":"B","score":"25#29#21","rank":2}, 
        // {"member":"C","score":"20#7#12","rank":3}]

        // 获取整个排行榜
        lb.allLeaders();
        System.out.println(lb.allLeaders());
        // [{"member":"A","score":"32#21#16","rank":1}, 
        // {"member":"B","score":"25#29#21","rank":2}, 
        // {"member":"C","score":"20#7#12","rank":3}, 
        // {"member":"D","score":"14#4#16","rank":4}, 
        // {"member":"E","score":"13#21#18","rank":5}, 
        // {"member":"F","score":"13#17#14","rank":6}]
    }
}
