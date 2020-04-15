package com.aliyun.tair.tests.eagleeye;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.aliyun.tair.tairhash.TairHashCluster;
import com.taobao.eagleeye.EagleEye;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisClusterEagleTest {
    private static final int DEFAULT_TIMEOUT = 2000;
    private static final int DEFAULT_REDIRECTIONS = 5;
    private static final JedisPoolConfig DEFAULT_CONFIG = new JedisPoolConfig();
    private static final String SINGLE_ADDR  = "r-8vbe30c186f51e44.redis.zhangbei.rds.aliyuncs.com";
    private static final String CLUSTER_ADDR = "r-8vbe30c186f51e44pd.redis.zhangbei.rds.aliyuncs.com";
    private static final String PASSWORD = "adNzYmKQjM2fK1p";

    static JedisPool jedisPool;
    static JedisCluster jc;
    static TairHashCluster tairHashCluster;
    static {
        DEFAULT_CONFIG.setTestOnBorrow(true);
        DEFAULT_CONFIG.setTestOnCreate(true);

        jedisPool = new JedisPool(DEFAULT_CONFIG, SINGLE_ADDR, 6379, DEFAULT_TIMEOUT, PASSWORD);

        Set<HostAndPort> jedisClusterNode = new HashSet<HostAndPort>();
        jedisClusterNode.add(new HostAndPort(CLUSTER_ADDR, 6379));
        jc = new JedisCluster(jedisClusterNode, DEFAULT_TIMEOUT, DEFAULT_TIMEOUT,
            DEFAULT_REDIRECTIONS, PASSWORD, "clientName", DEFAULT_CONFIG);
        tairHashCluster = new TairHashCluster(jc);
    }

    private static void singleTest() {
        for (int i = 0; i < 10; i++) {
            String key = UUID.randomUUID().toString();
            try (Jedis jedis = jedisPool.getResource()) {
                System.out.println(jedis.set(key, key));
                System.out.println(jedis.get(key));
            }
        }
    }

    private static void clusterTest() throws Exception {
        for (int i = 0; i < 10; i++) {
            String key = UUID.randomUUID().toString();
            System.out.println(jc.set(key, key));
            System.out.println(jc.get(key));
        }

        Thread.sleep(60 * 1000);
    }

    private static void tairHashClusterTest() throws Exception {
        for (int i = 0; i < 10; i++) {
            String key = UUID.randomUUID().toString();
            System.out.println(tairHashCluster.exhset(key, key, key));
            System.out.println(tairHashCluster.exhset(key, key, key));
            System.out.println(tairHashCluster.exhmget(key, key, key));
        }

        Thread.sleep(60 * 1000);
    }

    public static void main(String args[]) throws Exception {
        EagleEye.startTrace(null, "test123", 0);
        tairHashClusterTest();
        EagleEye.endTrace("00");

        Thread.sleep(70 * 1000);
    }
}
