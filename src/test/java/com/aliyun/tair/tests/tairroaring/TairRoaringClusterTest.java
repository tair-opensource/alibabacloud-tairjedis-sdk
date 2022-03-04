package com.aliyun.tair.tests.tairroaring;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TairRoaringClusterTest extends TairRoaringTestBase {
    @Test
    public void tairroaringtest() {
        jedisCluster.del("foo");
        long result = tairRoaringCluster.trsetbit("foo", 10, 1);
        assertEquals(0, result);

        result = tairRoaringCluster.trsetbit("foo", 20, "1");
        assertEquals(0, result);

        result = tairRoaringCluster.trsetbit("foo", 30, 1);
        assertEquals(0, result);

        result = tairRoaringCluster.trsetbit("foo", 30, "0");
        assertEquals(1, result);

        result = tairRoaringCluster.trbitcount("foo");
        assertEquals(2, result);

        assertEquals(1, tairRoaringCluster.trgetbit("foo", 10));
        assertEquals(1, tairRoaringCluster.trgetbit("foo", "20"));

        assertEquals(10, tairRoaringCluster.trmin("foo"));
        assertEquals(20, tairRoaringCluster.trmax("foo"));

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

        jedisCluster.del("foo");
        jedisCluster.del("bar");
        jedisCluster.del("dest");
    }
}
