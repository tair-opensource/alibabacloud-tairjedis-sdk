package com.aliyun.tair.tests.tairzset;

import com.aliyun.tair.tairzset.TairZset;
import com.aliyun.tair.tairzset.TairZsetCluster;
import com.aliyun.tair.tairzset.TairZsetPipeline;
import com.aliyun.tair.tests.TestBase;
import org.junit.BeforeClass;

public class TairZsetTestBase extends TestBase {
    public static TairZset tairZset;
    public static TairZsetPipeline tairZsetPipeline;
    public static TairZsetCluster tairZsetCluster;

    @BeforeClass
    public static void setUp() {
        tairZset = new TairZset(jedis);
        tairZsetPipeline = new TairZsetPipeline();
        tairZsetPipeline.setClient(jedis.getClient());
        tairZsetCluster = new TairZsetCluster(jedisCluster);
    }
}
