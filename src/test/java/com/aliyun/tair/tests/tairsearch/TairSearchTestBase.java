package com.aliyun.tair.tests.tairsearch;

import com.aliyun.tair.tairsearch.TairSearch;
import com.aliyun.tair.tairsearch.TairSearchCluster;
import com.aliyun.tair.tairsearch.TairSearchPipeline;
import com.aliyun.tair.tests.TestBase;
import org.junit.BeforeClass;

public class TairSearchTestBase extends TestBase {
    public static TairSearch tairSearch;
    public static TairSearchPipeline tairSearchPipeline;
    public static TairSearchCluster tairSearchCluster;

    @BeforeClass
    public static void setUp() {
        tairSearch = new TairSearch(jedis);
        tairSearchPipeline = new TairSearchPipeline();
        tairSearchPipeline.setClient(jedis.getClient());
        tairSearchCluster = new TairSearchCluster(jedisCluster);
    }
}
