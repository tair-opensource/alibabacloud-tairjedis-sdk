package com.aliyun.tair.tests.tairroaring;

import java.util.List;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TairRoaringPipelineTest extends TairRoaringTestBase {

    @Test
    public void tairroaringtest() {
        jedis.del("foo");
        int i = 0;

        tairRoaringPipeline.trsetbit("foo", 10, 1);
        tairRoaringPipeline.trsetbit("foo", 20, "1");
        tairRoaringPipeline.trsetbit("foo", 30, 1);
        tairRoaringPipeline.trsetbit("foo", 30, 0);
        tairRoaringPipeline.trbitcount("foo");
        tairRoaringPipeline.trgetbit("foo", 10);
        tairRoaringPipeline.trgetbit("foo", "20");
        tairRoaringPipeline.trmin("foo");
        tairRoaringPipeline.trmax("foo");


        List<Object> objs = tairRoaringPipeline.syncAndReturnAll();
        i = 0;
        assertEquals("0", objs.get(i++).toString());
        assertEquals("0", objs.get(i++).toString());
        assertEquals("0", objs.get(i++).toString());
        assertEquals("1", objs.get(i++).toString());
        assertEquals("2", objs.get(i++).toString());
        assertEquals("1", objs.get(i++).toString());
        assertEquals("1", objs.get(i++).toString());
        assertEquals("10", objs.get(i++).toString());
        assertEquals("20", objs.get(i).toString());

        jedis.del("foo");
    }

    @Test
    public  void trbitoptest() throws Exception {
        jedis.del("foo");
        int i = 0;

        tairRoaringPipeline.trappendintarray("foo", 1, 3, 5, 7, 9);
        tairRoaringPipeline.trappendintarray("bar", 2, 4, 6, 8, 10);
        tairRoaringPipeline.trbitop("dest", "OR", "foo", "bar");
        tairRoaringPipeline.trbitcount("dest");

        List<Object> objs = tairRoaringPipeline.syncAndReturnAll();
        i = 0;
        assertEquals("OK", objs.get(i++));
        assertEquals("OK", objs.get(i++));
        assertEquals("10", objs.get(i++).toString());
        assertEquals("10", objs.get(i).toString());

        jedis.del("foo");
    }
}
