package com.aliyun.tair.tests;

import java.util.HashSet;
import java.util.Set;

import com.aliyun.tair.mcommamd.TairCluster;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

public class TestBase {
    protected static final String HOST = "127.0.0.1";
    protected static final int PORT = 6378;
    protected static final int CLUSTER_PORT = 6379;
    protected static final int CLUSTER_PORT2 = 26379;
    protected static final int CLUSTER_PORT3 = 46379;

    protected static Jedis jedis;
    protected static JedisCluster jedisCluster;
    protected static TairCluster tairCluster;

    static {
        try {
            jedis = new Jedis(HOST, PORT, 2000 * 100);
            if (!"PONG".equals(jedis.ping())) {
                System.exit(-1);
            }

            Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
            jedisClusterNodes.add(new HostAndPort(HOST, CLUSTER_PORT));
            jedisClusterNodes.add(new HostAndPort(HOST, CLUSTER_PORT2));
            jedisClusterNodes.add(new HostAndPort(HOST, CLUSTER_PORT3));
            jedisCluster = new JedisCluster(jedisClusterNodes);
            tairCluster = new TairCluster(jedisClusterNodes);
        } catch (Exception e) {

        }
    }
}
