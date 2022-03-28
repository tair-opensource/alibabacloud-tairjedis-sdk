package com.aliyun.tair.tests.tairroaring;

import com.aliyun.tair.tairstring.results.ExgetResult;
import org.junit.Test;
import redis.clients.jedis.ScanResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TairRoaringPipelineTest extends TairRoaringTestBase {

    @Test
    public void tairroaringtest() throws Exception {
        jedis.del("foo");

        tairRoaringPipeline.trsetbit("foo", 10, 1);
        tairRoaringPipeline.trsetbit("foo", 20, "1");
        tairRoaringPipeline.trsetbit("foo", 30, 1);
        tairRoaringPipeline.trsetbit("foo", 30, 0);
        tairRoaringPipeline.trbitcount("foo");
        tairRoaringPipeline.trgetbit("foo", 10);
        tairRoaringPipeline.trgetbit("foo", 20);
        tairRoaringPipeline.trmin("foo");
        tairRoaringPipeline.trmax("foo");

        int i = 0;
        List<Object> objs = tairRoaringPipeline.syncAndReturnAll();
        assertEquals((long)0, objs.get(i++));
        assertEquals((long)0, objs.get(i++));
        assertEquals((long)0, objs.get(i++));
        assertEquals((long)1, objs.get(i++));
        assertEquals((long)2, objs.get(i++));
        assertEquals((long)1, objs.get(i++));
        assertEquals((long)1, objs.get(i++));
        assertEquals((long)10, objs.get(i++));
        assertEquals((long)20, objs.get(i++));

        jedis.del("foo");
    }

    @Test
    public void trbitsrangemixedtest() throws Exception {
        jedis.del("foo");

        tairRoaringPipeline.trsetbits("foo", 1, 3, 5, 7, 9);
        tairRoaringPipeline.trbitcount("foo", 0, 3);
        tairRoaringPipeline.trclearbits("foo", 2, 3, 4); // 1, 5, 7, 9
        tairRoaringPipeline.trgetbits("foo", 1, 2, 3, 4, 5);
        tairRoaringPipeline.trrange("foo", 0, 10);
        tairRoaringPipeline.trmin("foo");
        tairRoaringPipeline.trmax("foo");
        tairRoaringPipeline.trscan("foo", 5, 10);
        tairRoaringPipeline.trappendbitarray("foo", 3, "1010101"); // 1, 4, 6, 8, 10
        tairRoaringPipeline.trrangebitarray("foo", 0, 10);
        tairRoaringPipeline.trbitcount("foo", 0, 10);

        int i = 0;
        List<Object> objs = tairRoaringPipeline.syncAndReturnAll();
        assertEquals((long)5, objs.get(i++));
        assertEquals((long)2, objs.get(i++));
        assertEquals((long)1, objs.get(i++));

        List<Long> expect = new ArrayList<Long>();
        expect.add((long) 1);
        expect.add((long) 0);
        expect.add((long) 0);
        expect.add((long) 0);
        expect.add((long) 1);
        assertLongListEquals(expect, (List<Long>)objs.get(i++));

        expect = new ArrayList<Long>();
        expect.add((long) 1);
        expect.add((long) 5);
        expect.add((long) 7);
        expect.add((long) 9);
        assertLongListEquals(expect, (List<Long>)objs.get(i++));

        assertEquals((long)1, objs.get(i++));
        assertEquals((long)9, objs.get(i++));

        expect = new ArrayList<Long>();
        expect.add((long) 6);
        expect.add((long) 8);
        expect.add((long) 10);
        assertLongListEquals(expect, (List<Long>)objs.get(i++));

        assertEquals((long)5, (long)objs.get(i++));
        assertEquals("01001010101", objs.get(i++).toString());
        assertEquals((long)5, (long)objs.get(i++));

        jedis.del("foo");
    }

    @Test
    public void trbitoptest() throws Exception {
        jedis.del("foo");

        tairRoaringPipeline.trappendintarray("foo", 1, 3, 5, 7, 9);
        tairRoaringPipeline.trappendintarray("bar", 2, 4, 6, 8, 10);
        tairRoaringPipeline.trbitop("dest", "OR", "foo", "bar");
        tairRoaringPipeline.trbitopcard("OR", "foo", "bar");
        tairRoaringPipeline.trbitcount("dest");

        List<Object> objs = tairRoaringPipeline.syncAndReturnAll();
        int i = 0;
        assertEquals("OK", objs.get(i++));
        assertEquals("OK", objs.get(i++));
        assertEquals((long)10, objs.get(i++));
        assertEquals((long)10, objs.get(i++));
        assertEquals((long)10, objs.get(i++));

        jedis.del("foo");
    }
}
