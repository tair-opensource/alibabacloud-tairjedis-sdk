package com.aliyun.tair.tests.tairbloom;

import com.aliyun.tair.tairbloom.TairBloom;
import com.aliyun.tair.tairbloom.TairBloomCluster;
import com.aliyun.tair.tairbloom.TairBloomPipeline;
import com.aliyun.tair.tests.TestBase;
import org.junit.BeforeClass;

public class TairBloomTestBase extends TestBase {
    public static TairBloom tairBloom;
    public static TairBloomPipeline tairBloomPipeline;
    public static TairBloomCluster tairBloomCluster;

    @BeforeClass
    public static void setUp() {
        tairBloom = new TairBloom(jedisPool);
        tairBloomPipeline = new TairBloomPipeline();
        tairBloomPipeline.setClient(jedis.getClient());
        tairBloomCluster = new TairBloomCluster(jedisCluster);
    }
}
