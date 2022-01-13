package com.aliyun.tair.tests.tairzset;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TairZsetPipelineTest extends TairZsetTestBase {

    @Before
    public void before() {
        jedis.flushAll();
    }

    @Test
    public void exzbasic() {
        tairZsetPipeline.exzadd("foo", "1", "a");
        tairZsetPipeline.exzadd("foo", "10", "b");
        tairZsetPipeline.exzadd("foo", "0.1", "c");
        List<Object> objects = tairZsetPipeline.syncAndReturnAll();
        for (Object obj : objects) {
            assertEquals(Long.valueOf(1), obj);
        }
    }
}
