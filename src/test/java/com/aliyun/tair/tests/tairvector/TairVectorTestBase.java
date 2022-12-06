package com.aliyun.tair.tests.tairvector;

import com.aliyun.tair.tairvector.TairVector;
import com.aliyun.tair.tairvector.TairVectorCluster;
import com.aliyun.tair.tairvector.TairVectorPipeline;
import com.aliyun.tair.tairvector.TairVectorShard;
import com.aliyun.tair.tests.TestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import redis.clients.jedis.ScanResult;

import java.util.List;

import static com.aliyun.tair.tests.AssertUtil.assertEquals;

public class TairVectorTestBase extends TestBase {
    public static TairVector tairVector;
    public static TairVectorPipeline tairVectorPipeline;
    public static TairVectorCluster tairVectorCluster;
    public static TairVectorShard tairVectorDistribute;

    @BeforeClass
    public static void setUp() {
        tairVector = new TairVector(jedis);
        tairVectorPipeline = new TairVectorPipeline();
        tairVectorPipeline.setClient(jedis.getClient());
        tairVectorCluster = new TairVectorCluster(jedisCluster);
        tairVectorDistribute = new TairVectorShard(tairVectorCluster,2);

    }

    @AfterClass
    public static void closeDown() {
        tairVector.quit();
        tairVectorCluster.quit();
        tairVectorDistribute.quit();
    }

    public static void assertLongListEquals(List<Long> expected, List<Long> actual) {
        assertEquals(expected.size(), actual.size());
        for (int n = 0; n < expected.size(); n++) {
            assertEquals(expected.get(n), actual.get(n));
        }
    }

    public static void assertScanResultEquals(List<Long> expected, ScanResult<Long> actual) {
        for (int n = 0; n < expected.size(); n++) {
            assertEquals(expected.get(n), actual.getResult().get(n));
        }
    }
}
