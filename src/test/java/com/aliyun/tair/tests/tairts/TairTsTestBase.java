package com.aliyun.tair.tests.tairts;

import com.aliyun.tair.tairts.TairTs;
import com.aliyun.tair.tairts.TairTsCluster;
import com.aliyun.tair.tairts.TairTsPipeline;
import com.aliyun.tair.tests.TestBase;
import org.junit.BeforeClass;

public class TairTsTestBase extends TestBase {

    public static TairTs tairTs;
    public static TairTsPipeline tairTsPipeline;
    public static TairTsCluster tairTsCluster;

    @BeforeClass
    public static void setUp() {
        tairTs = new TairTs(jedisPool);
        tairTsPipeline = new TairTsPipeline(getJedis());
        tairTsCluster = new TairTsCluster(jedisCluster);
    }
}