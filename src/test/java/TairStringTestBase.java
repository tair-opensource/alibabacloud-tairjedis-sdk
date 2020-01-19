import com.aliyun.tairjedis.tairstring.TairString;
import com.aliyun.tairjedis.tairstring.TairStringCluster;
import com.aliyun.tairjedis.tairstring.TairStringPipeline;
import org.junit.BeforeClass;

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
