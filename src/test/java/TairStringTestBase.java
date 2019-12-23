import com.kvstore.jedis.TairString;
import com.kvstore.jedis.TairStringCluster;
import com.kvstore.jedis.TairStringPipeline;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Set;

public class TairStringTestBase extends TestBase {

    public static TairString tairString;
    public static TairStringPipeline tairStringPipeline;
    public static TairStringCluster tairStringCluster;

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


            tairString = new TairString(jedis);
            tairStringPipeline = new TairStringPipeline();
            tairStringPipeline.setClient(jedis.getClient());
        }
    }

    @AfterClass
    public static void tearDown() {
        if (jedis != null) {
            jedis.close();
        }
    }
}
