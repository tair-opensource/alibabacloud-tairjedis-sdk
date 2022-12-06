package com.aliyun.tair.tests.tairroaring;

import com.aliyun.tair.tairroaring.TairRoaring;
import com.aliyun.tair.tairroaring.TairRoaringCluster;
import com.aliyun.tair.tairroaring.TairRoaringPipeline;
import com.aliyun.tair.tests.TestBase;
import org.junit.Assert;
import org.junit.BeforeClass;
import java.util.Map.Entry;
import com.aliyun.tair.tairroaring.factory.RoaringBuilderFactory;
import redis.clients.jedis.ScanResult;


import java.util.AbstractMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.aliyun.tair.tests.AssertUtil.assertEquals;
import static org.junit.Assert.assertArrayEquals;

public class TairRoaringTestBase extends TestBase {
    public static TairRoaring tairRoaring;
    public static TairRoaringPipeline tairRoaringPipeline;
    public static TairRoaringCluster tairRoaringCluster;

    @BeforeClass
    public static void setUp() {
        tairRoaring = new TairRoaring(jedisPool);
        tairRoaringPipeline = new TairRoaringPipeline();
        tairRoaringPipeline.setClient(jedis.getClient());
        tairRoaringCluster = new TairRoaringCluster(jedisCluster);
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
