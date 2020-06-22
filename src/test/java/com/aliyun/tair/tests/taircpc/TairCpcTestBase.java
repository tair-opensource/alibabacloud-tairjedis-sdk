package com.aliyun.tair.tests.taircpc;

import com.aliyun.tair.taircpc.TairCpc;
import com.aliyun.tair.taircpc.TairCpcCluster;
import com.aliyun.tair.taircpc.TairCpcPipeline;
import com.aliyun.tair.tairts.TairTs;
import com.aliyun.tair.tests.TestBase;
import org.junit.BeforeClass;

public class TairCpcTestBase extends TestBase {

    public static TairCpc tairCpc;
    public static TairTs tairTs;
    public static TairCpcPipeline tairCpcPipeline;
    public static TairCpcCluster tairCpcCluster;

    @BeforeClass
    public static void setUp() {
        tairCpc = new TairCpc(jedis);
        tairTs = new TairTs(jedis);
        tairCpcPipeline = new TairCpcPipeline();
        tairCpcPipeline.setClient(jedis.getClient());
        tairCpcCluster = new TairCpcCluster(jedisCluster);
    }
}
