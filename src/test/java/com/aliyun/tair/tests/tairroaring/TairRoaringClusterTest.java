package com.aliyun.tair.tests.tairroaring;

import org.junit.Test;
import com.aliyun.tair.jedis3.ScanResult;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TairRoaringClusterTest extends TairRoaringTestBase {
    @Test
    public void trbitstests() {
        jedisCluster.del("foo");

        assertEquals(0, tairRoaringCluster.trsetbit("foo", 1, 1));
        assertEquals(0, tairRoaringCluster.trsetbit("foo", 2, "1"));
        assertEquals(5, tairRoaringCluster.trsetbits("foo", 3, 4, 5));

        assertEquals(1, tairRoaringCluster.trgetbit("foo", 1));
        assertEquals(0, tairRoaringCluster.trgetbit("foo", 11));

        assertEquals(1, tairRoaringCluster.trclearbits("foo", 5, 6, 7));

        List<Long> result = tairRoaringCluster.trgetbits("foo", 1, 2, 3, 4, 5);
        List<Long> expect = new ArrayList<Long>();
        expect.add((long) 1);
        expect.add((long) 1);
        expect.add((long) 1);
        expect.add((long) 1);
        expect.add((long) 0);
        assertLongListEquals(expect, result);

        assertEquals(3, tairRoaringCluster.trbitcount("foo", 0, 3));
        assertEquals(1, tairRoaringCluster.trmin("foo"));
        assertEquals(4, tairRoaringCluster.trmax("foo"));

        jedisCluster.del("foo");
    }

    @Test
    public void trbitrangeingtest() {
        jedisCluster.del("foo");

        assertEquals(5, tairRoaringCluster.trsetbits("foo", 1, 3, 5, 7, 9));

        List<Long> result = tairRoaringCluster.trrange("foo", 1, 5);
        List<Long> expect = new ArrayList<Long>();
        expect.add((long) 1);
        expect.add((long) 3);
        expect.add((long) 5);
        assertLongListEquals(expect, result);

        result = tairRoaringCluster.trrange("foo", 0, 4);
        expect = new ArrayList<Long>();
        expect.add((long) 1);
        expect.add((long) 3);
        assertLongListEquals(expect, result);

        ScanResult<Long> rawresult = tairRoaringCluster.trscan("foo", 0);
        assertEquals("0", rawresult.getCursor());
        result = rawresult.getResult();
        expect = new ArrayList<Long>();
        expect.add((long) 1);
        expect.add((long) 3);
        expect.add((long) 5);
        expect.add((long) 7);
        expect.add((long) 9);
        assertLongListEquals(expect, result);

        rawresult = tairRoaringCluster.trscan("foo", 4, 2);
        assertEquals("9", rawresult.getCursor());
        result = rawresult.getResult();
        expect = new ArrayList<Long>();
        expect.add((long) 5);
        expect.add((long) 7);
        assertLongListEquals(expect, result);

        jedisCluster.del("foo");
        assertEquals(5, tairRoaringCluster.trappendbitarray("foo", 0, "101010101"));
        result = tairRoaringCluster.trrange("foo", 0, 10);
        expect = new ArrayList<Long>();
        expect.add((long) 1);
        expect.add((long) 3);
        expect.add((long) 5);
        expect.add((long) 7);
        expect.add((long) 9);
        assertLongListEquals(expect, result);

        jedisCluster.del("foo");
        assertEquals(5, tairRoaringCluster.trappendbitarray("foo", -1, "101010101"));
        result = tairRoaringCluster.trrange("foo", 0, 10);
        expect = new ArrayList<Long>();
        expect.add((long) 0);
        expect.add((long) 2);
        expect.add((long) 4);
        expect.add((long) 6);
        expect.add((long) 8);
        assertLongListEquals(expect, result);

        jedisCluster.del("foo");
    }

    @Test
    public  void trbitoptest() throws Exception {
        jedisCluster.del("foo{hashtag}");
        jedisCluster.del("bar{hashtag}");
        jedisCluster.del("dest{hashtag}");

        assertEquals("OK", tairRoaringCluster.trappendintarray("foo{hashtag}", 1, 3, 5, 7, 9));
        assertEquals("OK", tairRoaringCluster.trappendintarray("bar{hashtag}", 2, 4, 6, 8, 10));
        assertEquals(10, tairRoaringCluster.trbitop("dest{hashtag}", "OR", "foo{hashtag}", "bar{hashtag}"));
        assertEquals(10, tairRoaringCluster.trbitcount("dest{hashtag}"));
        assertEquals(0, tairRoaringCluster.trbitopcard("AND", "foo{hashtag}", "bar{hashtag}"));

        jedisCluster.del("foo{hashtag}");
        jedisCluster.del("bar{hashtag}");
        jedisCluster.del("dest{hashtag}");
    }


    @Test
    public  void trmultikeytest() throws Exception {
        getJedis().del("foo{tairroaring}");
        getJedis().del("bar{tairroaring}");
        getJedis().del("baz{tairroaring}");

        assertEquals(5, tairRoaring.trsetbits("foo{tairroaring}", 1, 3, 5, 7, 9));
        assertEquals(5, tairRoaring.trsetbits("bar{tairroaring}", 2, 4, 6, 8, 10));
        assertEquals(10, tairRoaring.trsetrange("baz{tairroaring}", 1, 10));

        assertEquals(false, tairRoaring.trcontains("foo{tairroaring}", "bar{tairroaring}"));
        assertEquals(true, tairRoaring.trcontains("foo{tairroaring}", "baz{tairroaring}"));

        assertEquals(new Double(0.5), tairRoaring.trjaccard("foo{tairroaring}", "baz{tairroaring}"));

        assertEquals("OK", tairRoaring.trdiff("result{tairroaring","foo{tairroaring}", "bar{tairroaring}"));

        getJedis().del("foo{tairroaring}");
        getJedis().del("bar{tairroaring}");
        getJedis().del("baz{tairroaring}");
    }
}
