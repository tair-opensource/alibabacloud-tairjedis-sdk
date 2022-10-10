package com.aliyun.tair.tests.taircpc;

import com.aliyun.tair.taircpc.*;
import com.aliyun.tair.tests.TestBase;
import org.junit.BeforeClass;

public class TairCpcTestBase extends TestBase {

    public static TairCpc tairCpcNew;
    public static TairCpcPipeline tairCpcPipeline;
    public static TairCpcCluster tairCpcClusterNew;

    @BeforeClass
    public static void setUp() {
        tairCpcNew = new TairCpc(jedisPool);
        tairCpcPipeline = new TairCpcPipeline();
        tairCpcPipeline.setClient(jedis.getClient());
        tairCpcClusterNew = new TairCpcCluster(jedisCluster);
    }
}
