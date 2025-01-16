package com.aliyun.tair.tests.tairroaring;

import org.junit.Test;
import com.aliyun.tair.jedis3.ScanResult;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TairRoaringTest extends TairRoaringTestBase {
    @Test
    public void trbit_mixed_test() {
        getJedis().del("foo");

        assertEquals(0, tairRoaring.trsetbit("foo", 10, 1));
        assertEquals(0, tairRoaring.trsetbit("foo", 20, 1));
        assertEquals(0, tairRoaring.trsetbit("foo", 30, 1));
        assertEquals(1, tairRoaring.trsetbit("foo", 30, 0));
        assertEquals(2, tairRoaring.trbitcount("foo"));

        assertEquals(1, tairRoaring.trgetbit("foo", 10));
        assertEquals(0, tairRoaring.trgetbit("foo", 11));

        assertEquals(10, tairRoaring.trmin("foo"));
        assertEquals(20, tairRoaring.trmax("foo"));

        getJedis().del("foo");
    }

    @Test
    public void trbits_mixed_test() {
        getJedis().del("foo");

        assertEquals(5, tairRoaring.trsetbits("foo", 1, 3, 5, 7, 9));
        assertEquals(5, tairRoaring.trbitcount("foo"));

        assertEquals(7, tairRoaring.trsetbits("foo", 5, 7, 9, 11, 13));
        assertEquals(7, tairRoaring.trbitcount("foo"));

        assertEquals(3, tairRoaring.trclearbits("foo", 5, 6, 7, 8, 9));
        assertEquals(4, tairRoaring.trbitcount("foo"));

        List<Long> result = tairRoaring.trgetbits("foo", 1, 2, 3, 4, 5);
        List<Long> expect = new ArrayList<Long>();
        expect.add((long) 1);
        expect.add((long) 0);
        expect.add((long) 1);
        expect.add((long) 0);
        expect.add((long) 0);
        assertLongListEquals(expect, result);

        assertEquals("OK", tairRoaring.trappendintarray("foo", 1, 2, 3));
        result = tairRoaring.trgetbits("foo", 1, 2, 3, 4, 5);
        expect = new ArrayList<Long>();
        expect.add((long) 1);
        expect.add((long) 1);
        expect.add((long) 1);
        expect.add((long) 0);
        expect.add((long) 0);
        assertLongListEquals(expect, result);

        assertEquals("OK", tairRoaring.trsetintarray("foo", 2, 3));
        result = tairRoaring.trgetbits("foo", 1, 2, 3, 4, 5);
        expect = new ArrayList<Long>();
        expect.add((long) 0);
        expect.add((long) 1);
        expect.add((long) 1);
        expect.add((long) 0);
        expect.add((long) 0);
        assertLongListEquals(expect, result);

        result = tairRoaring.trgetbits("foo", 1, 1, 2, 3, 3);
        expect = new ArrayList<Long>();
        expect.add((long) 0);
        expect.add((long) 0);
        expect.add((long) 1);
        expect.add((long) 1);
        expect.add((long) 1);
        assertLongListEquals(expect, result);

        assertEquals(2, tairRoaring.trmin("foo"));
        assertEquals(3, tairRoaring.trmax("foo"));

        getJedis().del("foo");
    }

    @Test
    public void trbitrangeing_mixed_test() {
        getJedis().del("foo");

        assertEquals(5, tairRoaring.trsetbits("foo", 1, 3, 5, 7, 9));

        List<Long> result = tairRoaring.trrange("foo", 1, 5);
        List<Long> expect = new ArrayList<Long>();
        expect.add((long) 1);
        expect.add((long) 3);
        expect.add((long) 5);
        assertLongListEquals(expect, result);

        result = tairRoaring.trrange("foo", 0, 4);
        expect = new ArrayList<Long>();
        expect.add((long) 1);
        expect.add((long) 3);
        assertLongListEquals(expect, result);
        getJedis().del("foo");
    }

    @Test
    public void trscantest() {
        ScanResult<Long> rawresult = tairRoaring.trscan("no-key", 0);
        assertEquals("0", rawresult.getCursor());
        List<Long> result = rawresult.getResult();
        List<Long> expect = new ArrayList<Long>();
        assertLongListEquals(expect, result);

        getJedis().del("foo");
        assertEquals(5, tairRoaring.trsetbits("foo", 1, 3, 5, 7, 9));

        rawresult = tairRoaring.trscan("foo", 0);
        assertEquals("0", rawresult.getCursor());
        result = rawresult.getResult();
        expect = new ArrayList<Long>();
        expect.add((long) 1);
        expect.add((long) 3);
        expect.add((long) 5);
        expect.add((long) 7);
        expect.add((long) 9);
        assertLongListEquals(expect, result);

        rawresult = tairRoaring.trscan("foo", 4, 2);
        assertEquals("9", rawresult.getCursor());
        result = rawresult.getResult();
        expect = new ArrayList<Long>();
        expect.add((long) 5);
        expect.add((long) 7);
        assertLongListEquals(expect, result);

        getJedis().del("foo");
    }

    @Test
    public void trappendbitarryatest() {
        getJedis().del("foo");
        assertEquals(5, tairRoaring.trappendbitarray("foo", 0, "101010101"));
        List<Long> result = tairRoaring.trrange("foo", 0, 10);
        List<Long> expect = new ArrayList<Long>();
        expect.add((long) 1);
        expect.add((long) 3);
        expect.add((long) 5);
        expect.add((long) 7);
        expect.add((long) 9);
        assertLongListEquals(expect, result);

        getJedis().del("foo");
        assertEquals(5, tairRoaring.trappendbitarray("foo", -1, "101010101"));
        result = tairRoaring.trrange("foo", 0, 10);
        expect = new ArrayList<Long>();
        expect.add((long) 0);
        expect.add((long) 2);
        expect.add((long) 4);
        expect.add((long) 6);
        expect.add((long) 8);
        assertLongListEquals(expect, result);

        getJedis().del("foo");
    }

    @Test
    public void trstatus_mixed_test() {
        getJedis().del("foo");

        assertEquals(9, tairRoaring.trsetbits("foo", 1, 2, 3, 4, 5, 6, 7, 8, 9));

        assertEquals("OK", tairRoaring.troptimize("foo"));

        assertEquals(9, tairRoaring.trbitcount("foo"));

        assertEquals(5, tairRoaring.trbitcount("foo", 0, 5));

        assertEquals(1, tairRoaring.trbitcount("foo", 9, 20));

        assertEquals(1, tairRoaring.trbitpos("foo", 1));

        assertEquals(0, tairRoaring.trbitpos("foo", 0));

        assertEquals(2, tairRoaring.trbitpos("foo", 1, 2));
        assertEquals(6, tairRoaring.trbitpos("foo", 1, -4));
        assertEquals(0, tairRoaring.trbitpos("foo", 0, 1));

        assertEquals("cardinality: 9\r\n" +
                "number of containers: 1\r\n" +
                "max value: 9\r\n" +
                "min value: 1\r\n" +
                "sum value: 45\r\n" +
                "number of array containers: 0\r\n" +
                "\tarray container values: 0\r\n" +
                "\tarray container bytes: 0\r\n" +
                "number of bitset containers: 0\r\n" +
                "\tbitset container values: 0\r\n" +
                "\tbitset container bytes: 0\r\n" +
                "number of run containers: 1\r\n" +
                "\trun container values: 9\r\n" +
                "\trun container bytes: 6\r\n", tairRoaring.trstat("foo", false));

        getJedis().del("foo");
    }

    @Test
    public  void trloadstringtest() throws Exception {
        getJedis().del("foo");
        getJedis().set("strkey", "101010101");

        assertEquals(5, tairRoaring.trloadstring("foo", "strkey"));
        getJedis().del("foo");
    }

    @Test
    public  void tremptytest() throws Exception {
        getJedis().del("foo");
        List<Long> expect = new ArrayList<Long>();

        List<Long> result = tairRoaring.trrange("foo", 0, 4);
        assertLongListEquals(expect, result);

        result = tairRoaring.trgetbits("foo", 0, 4);
        assertLongListEquals(expect, result);

        assertEquals(-1, tairRoaring.trmin("foo"));
        assertEquals(-1, tairRoaring.trmax("foo"));
        assertEquals(-1, tairRoaring.trbitpos("foo", "1", 1));
        assertEquals(-1, tairRoaring.trrank("foo", 1));
        assertEquals(null, tairRoaring.trstat("foo", false));
        assertEquals(null, tairRoaring.troptimize("foo"));
        assertEquals(0, tairRoaring.trbitcount("foo"));
        assertEquals(0, tairRoaring.trclearbits("foo", 1, 3, 5));
    }

    @Test
    public  void trbitoptest() throws Exception {
        getJedis().del("foo");
        getJedis().del("bar");
        getJedis().del("dest");

        assertEquals("OK", tairRoaring.trappendintarray("foo", 1, 3, 5, 7, 9));
        assertEquals("OK", tairRoaring.trappendintarray("bar", 2, 4, 6, 8, 10));

        assertEquals(10, tairRoaring.trbitop("dest", "OR", "foo", "bar"));
        assertEquals(0, tairRoaring.trbitopcard("AND", "foo", "bar"));

        getJedis().del("foo");
        getJedis().del("bar");
        getJedis().del("dest");
    }

    @Test
    public void trgetmultitest() {
        getJedis().del("foo");
        assertEquals("OK", tairRoaring.trappendintarray("foo", 1, 3, 5, 7, 9, 11, 13, 15, 17, 19));

        List<Long> result = tairRoaring.trrange("foo", 0, 4);
        List<Long> expect = new ArrayList<Long>();
        expect.add((long) 1);
        expect.add((long) 3);
        assertLongListEquals(expect, result);
        getJedis().del("foo");
    }

    @Test
    public  void trmultikeytest() throws Exception {
        getJedis().del("foo");
        getJedis().del("bar");
        getJedis().del("baz");

        assertEquals(5, tairRoaring.trsetbits("foo", 1, 3, 5, 7, 9));
        assertEquals(5, tairRoaring.trsetbits("bar", 2, 4, 6, 8, 10));
        assertEquals(10, tairRoaring.trsetrange("baz", 1, 10));

        assertEquals(false, tairRoaring.trcontains("foo", "bar"));
        assertEquals(true, tairRoaring.trcontains("foo", "baz"));

        assertEquals(new Double(0.5), tairRoaring.trjaccard("foo", "baz"));

        assertEquals("OK", tairRoaring.trdiff("result","foo", "bar"));

        getJedis().del("foo");
        getJedis().del("bar");
        getJedis().del("baz");
    }

}
