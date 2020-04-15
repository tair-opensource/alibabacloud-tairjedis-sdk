package com.aliyun.tair.tests.tairstring;

import com.aliyun.tair.tairstring.TairString;
import com.aliyun.tair.tairstring.TairStringCluster;
import com.aliyun.tair.tairstring.TairStringPipeline;
import com.aliyun.tair.tests.TestBase;
import org.junit.BeforeClass;

public class TairStringTestBase extends TestBase {

    public static TairString tairString;
    public static TairStringPipeline tairStringPipeline;
    public static TairStringCluster tairStringCluster;

    @BeforeClass
    public static void setUp() {
        tairString = new TairString(jedis);
        tairStringPipeline = new TairStringPipeline();
        tairStringPipeline.setClient(jedis.getClient());
        tairStringCluster = new TairStringCluster(jedisCluster);
    }
}
