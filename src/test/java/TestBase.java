import java.util.HashSet;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

/**
 * @author bodong.ybd
 * @date 2019/12/17
 */
public class TestBase {
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 6379;
    private static final int CLUSTER_PORT = 30001;

    private static Jedis jedis;
    private static JedisCluster jedisCluster;

    public static TairDoc tairDoc;
    public static TairDocPipeline tairDocPipeline;
    public static TairDocCluster tairDocCluster;

    @BeforeClass
    public static void setUp() {
        if (jedis == null) {
            jedis = new Jedis(HOST, PORT);
            if (!"PONG".equals(jedis.ping())) {
                System.exit(-1);
            }

            Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
            jedisClusterNodes.add(new HostAndPort(HOST, CLUSTER_PORT));
            jedisCluster = new JedisCluster(jedisClusterNodes);

            tairDoc = new TairDoc(jedis);
            tairDocPipeline = new TairDocPipeline();
            tairDocPipeline.setClient(jedis.getClient());
            tairDocCluster = new TairDocCluster(jedisCluster);
        }
    }

    @AfterClass
    public static void tearDown() {
        if (jedis != null) {
            jedis.close();
        }
    }
}
