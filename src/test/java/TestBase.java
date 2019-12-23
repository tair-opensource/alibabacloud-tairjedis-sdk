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
}
