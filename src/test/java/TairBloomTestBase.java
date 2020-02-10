import com.aliyun.tair.tairbloom.TairBloom;
import com.aliyun.tair.tairbloom.TairBloomCluster;
import com.aliyun.tair.tairbloom.TairBloomPipeline;
import org.junit.BeforeClass;

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
