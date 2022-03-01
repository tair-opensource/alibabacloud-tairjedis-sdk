package com.aliyun.tair.tests.tairroaring;

import org.junit.Test;

import java.util.List;

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
        assertEquals(3, result);

        assertEquals(1, tairRoaringCluster.trgetbit("foo", 10));
        assertEquals(1, tairRoaringCluster.trgetbit("foo", "20"));

        assertEquals(10, tairRoaringCluster.trmin("foo"));
        assertEquals(20, tairRoaringCluster.trmax("foo"));

        jedisCluster.del("foo");
    }

    @Test
    public  void trbitoptest() throws Exception {
        jedisCluster.del("foo");
        jedisCluster.del("bar");
        jedisCluster.del("dest");

        assertEquals("OK", tairRoaringCluster.trappendintarray("foo", 1, 3, 5, 7, 9));
        assertEquals("OK", tairRoaringCluster.trappendintarray("bar", 2, 4, 6, 8, 10));
        assertEquals(10, tairRoaringCluster.trbitop("dest", "OR", "foo", "bar"));
        assertEquals(10, tairRoaringCluster.trbitcount("dest"));

        jedisCluster.del("foo");
        jedisCluster.del("bar");
        jedisCluster.del("dest");
    }
}
