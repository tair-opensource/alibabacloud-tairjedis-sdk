import com.kvstore.jedis.tairbloom.TairBloom;
import com.kvstore.jedis.tairbloom.TairBloomCluster;
import com.kvstore.jedis.tairbloom.TairBloomPipeline;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Set;

public class TairBloomTestBase extends TestBase {
    public static TairBloom tairBloom;
    public static TairBloomPipeline tairBloomPipeline;
    public static TairBloomCluster tairBloomCluster;

    @BeforeClass
    public static void setUp() {
        tairBloom = new TairBloom(jedis);
        tairBloomPipeline = new TairBloomPipeline();
        tairBloomPipeline.setClient(jedis.getClient());
        tairBloomCluster = new TairBloomCluster(jedisCluster);
    }
}
