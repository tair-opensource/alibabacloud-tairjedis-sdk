import com.kvstore.jedis.tairhash.TairHash;
import com.kvstore.jedis.tairhash.TairHashCluster;
import com.kvstore.jedis.tairhash.TairHashPipeline;
import com.kvstore.jedis.tairstring.TairString;
import com.kvstore.jedis.tairstring.TairStringCluster;
import com.kvstore.jedis.tairstring.TairStringPipeline;
import org.junit.BeforeClass;

public class TairHashTestBase extends TestBase {

    public static TairHash tairHash;
    public static TairHashPipeline tairHashPipeline;
    public static TairHashCluster tairHashCluster;

    @BeforeClass
    public static void setUp() {
        tairHash = new TairHash(jedis);
        tairHashPipeline = new TairHashPipeline();
        tairHashPipeline.setClient(jedis.getClient());
        tairHashCluster = new TairHashCluster(jedisCluster);
    }
}
