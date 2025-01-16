package com.aliyun.tair.tests.tairgis;

import com.aliyun.tair.tairgis.TairGis;
import com.aliyun.tair.tairgis.TairGisCluster;
import com.aliyun.tair.tairgis.TairGisPipeline;
import com.aliyun.tair.tests.TestBase;
import org.junit.BeforeClass;

public class TairGisTestBase extends TestBase {

    public static TairGis tairGis;
    public static TairGisPipeline tairGisPipeline;
    public static TairGisCluster tairGisCluster;

    @BeforeClass
    public static void setUp() {
        tairGis = new TairGis(jedisPool);
        tairGisPipeline = new TairGisPipeline(getJedis());
        tairGisCluster = new TairGisCluster(jedisCluster);

    }
}
