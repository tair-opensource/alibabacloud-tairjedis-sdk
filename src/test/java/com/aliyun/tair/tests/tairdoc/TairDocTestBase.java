package com.aliyun.tair.tests.tairdoc;

import com.aliyun.tair.tests.TestBase;
import org.junit.BeforeClass;

import com.aliyun.tair.tairdoc.TairDoc;
import com.aliyun.tair.tairdoc.TairDocCluster;
import com.aliyun.tair.tairdoc.TairDocPipeline;

public class TairDocTestBase extends TestBase {

    public static TairDoc tairDoc;
    public static TairDocPipeline tairDocPipeline;
    public static TairDocCluster tairDocCluster;

    @BeforeClass
    public static void setUp() {
        tairDoc = new TairDoc(jedisPool);
        tairDocPipeline = new TairDocPipeline(getJedis());
        tairDocCluster = new TairDocCluster(jedisCluster);
    }
}
