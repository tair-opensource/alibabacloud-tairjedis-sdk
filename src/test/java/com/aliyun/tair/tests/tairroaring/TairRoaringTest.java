package com.aliyun.tair.tests.tairroaring;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TairRoaringTest extends TairRoaringTestBase {
    @Test
    public void trbittest() {
        jedis.del("foo");

        assertEquals(0, tairRoaring.trsetbit("foo", 10, 1));
        assertEquals(0, tairRoaring.trsetbit("foo", 20, 1));
        assertEquals(0, tairRoaring.trsetbit("foo", 30, 1));
        assertEquals(1, tairRoaring.trsetbit("foo", 30, 0));
        assertEquals(2, tairRoaring.trbitcount("foo"));

        assertEquals(1, tairRoaring.trgetbit("foo", 10));
        assertEquals(1, tairRoaring.trgetbit("foo", "20"));

        assertEquals(10, tairRoaring.trmin("foo"));
        assertEquals(20, tairRoaring.trmax("foo"));

        jedis.del("foo");
    }

    @Test
    public void trsettest() {
        jedis.del("foo");

        //assertEquals("OK", tairRoaring.trsetrange("foo", 1, 10));
        assertEquals("OK", tairRoaring.trsetintarray("foo", 1, 2, 3, 4, 5, 6, 7, 8, 9));

        // TODO
        assertEquals(9, tairRoaring.trbitcount("foo"));

        assertEquals("OK", tairRoaring.troptimize("foo"));

        // TODO
        assertEquals("cardinality: 9\r\n" +
                "number of containers: 1\r\n" +
                "max value: 9\r\n" +
                "min value: 1\r\n" +
                "number of array containers: 0\r\n" +
                "\tarray container values: 0\r\n" +
                "\tarray container bytes: 0\r\n" +
                "number of bitset containers: 0\r\n" +
                "\tbitset container values: 0\r\n" +
                "\tbitset container bytes: 0\r\n" +
                "number of run containers: 1\r\n" +
                "\trun container values: 9\r\n" +
                "\trun container bytes: 6\r\n", tairRoaring.trstat("foo"));

        jedis.del("foo");
    }

    @Test
    public  void trbitoptest() throws Exception {
        jedis.del("foo");
        jedis.del("bar");
        jedis.del("dest");

        assertEquals("OK", tairRoaring.trappendintarray("foo", 1, 3, 5, 7, 9));
        assertEquals("OK", tairRoaring.trappendintarray("bar", 2, 4, 6, 8, 10));
        assertEquals(10, tairRoaring.trbitop("dest", "OR", "foo", "bar"));
        assertEquals(10, tairRoaring.trbitcount("dest"));

        jedis.del("foo");
        jedis.del("bar");
        jedis.del("dest");
    }

    @Test
    public void trgetmultitest() {
        jedis.del("foo");
        assertEquals("OK", tairRoaring.trappendintarray("foo", 1, 3, 5, 7, 9, 11, 13, 15, 17, 19));

        List<Long> result = tairRoaring.trrangeintarray("foo", 0, 4);
        List<Long> expect = new ArrayList<Long>();
        expect.add((long) 1);
        expect.add((long) 3);
        expect.add((long) 5);
        expect.add((long) 7);
        expect.add((long) 9);
        assertLongListEquals(expect, result);
        jedis.del("foo");
    }
}
