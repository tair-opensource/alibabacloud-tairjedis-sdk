package com.aliyun.tair.tests.tairvector;

import java.util.List;

import com.aliyun.tair.tairvector.TairVector;
import com.aliyun.tair.tairvector.TairVectorCluster;
import com.aliyun.tair.tairvector.TairVectorPipeline;
import com.aliyun.tair.tests.TestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import redis.clients.jedis.ScanResult;

import static com.aliyun.tair.tests.AssertUtil.assertEquals;

public class TairVectorTestBase extends TestBase {
    public static TairVector tairVector;
    public static TairVectorPipeline tairVectorPipeline;
    public static TairVectorCluster tairVectorCluster;

    @BeforeClass
    public static void setUp() {
        tairVector = new TairVector(jedisPool);
        tairVectorPipeline = new TairVectorPipeline();
        tairVectorPipeline.setClient(jedis.getClient());
        tairVectorCluster = new TairVectorCluster(jedisCluster);
    }
}
