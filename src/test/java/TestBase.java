import java.util.HashSet;
import java.util.Set;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

/**
 * @author bodong.ybd
 * @date 2019/12/17
 */
public class TestBase {
    protected static final String HOST = "127.0.0.1";
    protected static final int PORT = 6379;
    protected static final int CLUSTER_PORT = 30001;

    protected static Jedis jedis;
    protected static JedisCluster jedisCluster;

    static {
        jedis = new Jedis(HOST, PORT);
        if (!"PONG".equals(jedis.ping())) {
            System.exit(-1);
        }

        Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
        jedisClusterNodes.add(new HostAndPort(HOST, CLUSTER_PORT));
        jedisCluster = new JedisCluster(jedisClusterNodes);
    }
}
