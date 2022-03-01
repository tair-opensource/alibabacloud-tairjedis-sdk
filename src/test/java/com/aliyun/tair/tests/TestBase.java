package com.aliyun.tair.tests;

import java.util.HashSet;
import java.util.Set;

import com.aliyun.tair.mcommamd.TairCluster;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

public class TestBase {
    protected static final String HOST = "127.0.0.1";
    protected static final int PORT = 6379;
    protected static final int CLUSTER_PORT = 30001;
    protected static final int CLUSTER_PORT2 = 30002;
    protected static final int CLUSTER_PORT3 = 30003;

    protected static Jedis jedis;
    protected static JedisPool jedisPool;
    protected static JedisCluster jedisCluster;
    protected static TairCluster tairCluster;

    static {
        try {
            jedis = new Jedis(HOST, PORT, 2000 * 100);
            jedisPool = new JedisPool(HOST, PORT);
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
