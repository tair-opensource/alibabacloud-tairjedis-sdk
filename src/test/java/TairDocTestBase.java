import org.junit.AfterClass;
import org.junit.BeforeClass;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import com.kvstore.jedis.tairdoc.TairDoc;
import com.kvstore.jedis.tairdoc.TairDocCluster;
import com.kvstore.jedis.tairdoc.TairDocPipeline;

import java.util.HashSet;
import java.util.Set;

public class TairDocTestBase extends TestBase {

    public static TairDoc tairDoc;
    public static TairDocPipeline tairDocPipeline;
    public static TairDocCluster tairDocCluster;

    @BeforeClass
    public static void setUp() {
        tairDoc = new TairDoc(jedis);
        tairDocPipeline = new TairDocPipeline();
        tairDocPipeline.setClient(jedis.getClient());
        tairDocCluster = new TairDocCluster(jedisCluster);
    }
}
