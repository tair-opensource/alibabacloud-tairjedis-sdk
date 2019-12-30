import com.kvstore.jedis.tairstring.TairString;
import com.kvstore.jedis.tairstring.TairStringCluster;
import com.kvstore.jedis.tairstring.TairStringPipeline;
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
        tairString = new TairString(jedis);
        tairStringPipeline = new TairStringPipeline();
        tairStringPipeline.setClient(jedis.getClient());
        tairStringCluster = new TairStringCluster(jedisCluster);
    }
}
