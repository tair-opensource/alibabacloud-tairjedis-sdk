package com.aliyun.tair.tests.tairhash;

import com.aliyun.tair.tairhash.TairHash;
import com.aliyun.tair.tairhash.TairHashCluster;
import com.aliyun.tair.tairhash.TairHashPipeline;
import com.aliyun.tair.tests.TestBase;
import org.junit.BeforeClass;

public class TairHashTestBase extends TestBase {

    public static TairHash tairHash;
    public static TairHashPipeline tairHashPipeline;
    public static TairHashCluster tairHashCluster;

    @BeforeClass
    public static void setUp() {
        tairHash = new TairHash(jedisPool);
        tairHashPipeline = new TairHashPipeline();
        tairHashPipeline.setClient(jedis.getClient());
        tairHashCluster = new TairHashCluster(jedisCluster);
    }
}
