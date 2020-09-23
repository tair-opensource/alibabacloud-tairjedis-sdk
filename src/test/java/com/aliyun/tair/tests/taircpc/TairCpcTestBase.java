package com.aliyun.tair.tests.taircpc;

import com.aliyun.tair.taircpc.*;
import com.aliyun.tair.tairts.TairTs;
import com.aliyun.tair.tests.TestBase;
import org.junit.BeforeClass;

public class TairCpcTestBase extends TestBase {

    public static TairCpcOld tairCpc;
    public static TairCpc tairCpcNew;
    public static TairTs tairTs;
    public static TairCpcPipeline tairCpcPipeline;
    public static TairCpcClusterOld tairCpcCluster;
    public static TairCpcCluster tairCpcClusterNew;

    @BeforeClass
    public static void setUp() {
        tairCpc = new TairCpcOld(jedis);
        tairCpcNew = new TairCpc(jedis);
        tairTs = new TairTs(jedis);
        tairCpcPipeline = new TairCpcPipeline();
        tairCpcPipeline.setClient(jedis.getClient());
        tairCpcCluster = new TairCpcClusterOld(jedisCluster);
        tairCpcClusterNew = new TairCpcCluster(jedisCluster);
    }
}
