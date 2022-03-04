package com.aliyun.tair.tests;

import java.util.HashSet;
import java.util.Set;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

public class TestBase {
    protected static final String HOST = "127.0.0.1";
    protected static final int PORT = 6379;
    protected static final int CLUSTER_PORT = 30001;

    protected static Jedis jedis;
    protected static JedisPool jedisPool;
    protected static JedisCluster jedisCluster;

    static {
        try {
            jedis = new Jedis(HOST, PORT, 60 * 1000); // timeout 60s
            jedisPool = new JedisPool(HOST, PORT);
            Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
            jedisClusterNodes.add(new HostAndPort(HOST, CLUSTER_PORT));
            jedisCluster = new JedisCluster(jedisClusterNodes);
        } catch (Exception e) {

        }
    }
}
