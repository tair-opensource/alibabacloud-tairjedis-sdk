package com.aliyun.tair.tests.tairzset;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.tair.tairzset.params.ExzaddParams;
import com.aliyun.tair.tairzset.params.ExzrangeParams;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.util.SafeEncoder;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TairZsetTest extends TairZsetTestBase {
    final byte[] bfoo = { 0x01, 0x02, 0x03, 0x04 };
    final byte[] bbar = { 0x05, 0x06, 0x07, 0x08 };
    final byte[] bcar = { 0x09, 0x0A, 0x0B, 0x0C };
    final byte[] ba = { 0x0A };
    final byte[] bb = { 0x0B };
    final byte[] bc = { 0x0C };
    final byte[] bInclusiveB = { 0x5B, 0x0B };
    final byte[] bExclusiveC = { 0x28, 0x0C };
    final byte[] bLexMinusInf = { 0x2D };
    final byte[] bLexPlusInf = { 0x2B };

    @Before
    public void before() {
        jedis.flushAll();
    }

    @Test
    public void zadd() {
        long status = tairZset.exzadd("foo", "a", 1d);
        assertEquals(1, status);

        status = tairZset.exzadd("foo", "b", 10d);
        assertEquals(1, status);

        status = tairZset.exzadd("foo", "c", 0.1d);
        assertEquals(1, status);

        status = tairZset.exzadd("foo", "a", 2d);
        assertEquals(0, status);

        // Binary
        long bstatus = tairZset.exzadd(bfoo, ba, 1d);
        assertEquals(1, bstatus);

        bstatus = tairZset.exzadd(bfoo, bb, 10d);
        assertEquals(1, bstatus);

        bstatus = tairZset.exzadd(bfoo, bc, 0.1d);
        assertEquals(1, bstatus);

        bstatus = tairZset.exzadd(bfoo, ba, 2d);
        assertEquals(0, bstatus);

    }

    @Test
    public void zaddWithParams() {
        jedis.del("foo");

        // xx: never add new member
        long status = tairZset.exzadd("foo", "1", "a", ExzaddParams.ExzaddParams().xx());
        assertEquals(0L, status);

        tairZset.exzadd("foo", "1", "a");
        // nx: never update current member
        status = tairZset.exzadd("foo", "2", "a", ExzaddParams.ExzaddParams().nx());
        assertEquals(0L, status);
        assertEquals("1", tairZset.exzscore("foo", "a"));

        Map<String, String> scoreMembers = new HashMap<String, String>();
        scoreMembers.put("2", "a");
        scoreMembers.put("1", "b");
        // ch: return count of members not only added, but also updated
        status = tairZset.exzadd("foo", scoreMembers, ExzaddParams.ExzaddParams().ch());
        assertEquals(2L, status);

        // binary
        jedis.del(bfoo);

        // xx: never add new member
        status = tairZset.exzadd(bfoo, "1".getBytes(), ba, ExzaddParams.ExzaddParams().xx());
        assertEquals(0L, status);

        tairZset.exzadd(bfoo, "1".getBytes(), ba);
        // nx: never update current member
        status = tairZset.exzadd(bfoo, "2".getBytes(), ba, ExzaddParams.ExzaddParams().nx());
        assertEquals(0L, status);
        assertArrayEquals("1".getBytes(), tairZset.exzscore(bfoo, ba));

        Map<byte[], byte[]> binaryScoreMembers = new HashMap<byte[], byte[]>();
        binaryScoreMembers.put("2".getBytes(), ba);
        binaryScoreMembers.put("1".getBytes(), bb);
        // ch: return count of members not only added, but also updated
        status = tairZset.exzadd(bfoo, binaryScoreMembers, ExzaddParams.ExzaddParams().ch());
        assertEquals(2L, status);
    }

    public static void assertByteArrayListEquals(List<byte[]> expected, List<byte[]> actual) {
        assertEquals(expected.size(), actual.size());
        for (int n = 0; n < expected.size(); n++) {
            assertArrayEquals(expected.get(n), actual.get(n));
        }
    }

    @Test
    public void zrange() {
        tairZset.exzadd("foo", "1", "a");
        tairZset.exzadd("foo", "10", "b");
        tairZset.exzadd("foo", "0.1", "c");
        tairZset.exzadd("foo", "2", "a");

        List<String> expected = new ArrayList<>();
        expected.add("c");
        expected.add("a");

        List<String> range = tairZset.exzrange("foo", 0, 1);
        assertEquals(expected, range);

        expected.add("b");
        range = tairZset.exzrange("foo", 0, 100);
        assertEquals(expected, range);

        // Binary
        tairZset.exzadd(bfoo, ba, 1d);
        tairZset.exzadd(bfoo, bb, 10d);
        tairZset.exzadd(bfoo, bc, 0.1d);
        tairZset.exzadd(bfoo, ba, 2d);

        List<byte[]> bexpected = new ArrayList<>();
        bexpected.add(bc);
        bexpected.add(ba);

        List<byte[]> brange = tairZset.exzrange(bfoo, 0, 1);
        assertByteArrayListEquals(bexpected, brange);

        bexpected.add(bb);
        brange = tairZset.exzrange(bfoo, 0, 100);
        assertByteArrayListEquals(bexpected, brange);

    }

    @Test
    public void zrangeByLex() {
        tairZset.exzadd("foo", "1", "aa");
        tairZset.exzadd("foo", "1", "c");
        tairZset.exzadd("foo", "1", "bb");
        tairZset.exzadd("foo", "1", "d");

        List<String> expected = new ArrayList<>();
        expected.add("bb");
        expected.add("c");

        // exclusive aa ~ inclusive c
        assertEquals(expected, tairZset.exzrangeByLex("foo", "(aa", "[c"));

        expected.clear();
        expected.add("bb");
        expected.add("c");

        // with LIMIT
        assertEquals(expected, tairZset.exzrangeByLex("foo", "-", "+", ExzrangeParams.ZRangeParams().limit(1, 2)));
    }

    @Test
    public void zrangeByLexBinary() {
        // binary
        tairZset.exzadd(bfoo, ba, 1);
        tairZset.exzadd(bfoo, bc, 1);
        tairZset.exzadd(bfoo, bb, 1);

        List<byte[]> bExpected = new ArrayList<>();
        bExpected.add(bb);

        assertByteArrayListEquals(bExpected, tairZset.exzrangeByLex(bfoo, bInclusiveB, bExclusiveC));

        bExpected.clear();
        bExpected.add(ba);
        bExpected.add(bb);

        // with LIMIT
        assertByteArrayListEquals(bExpected,
            tairZset.exzrangeByLex(bfoo, bLexMinusInf, bLexPlusInf, ExzrangeParams.ZRangeParams().limit(0, 2)));
    }

    @Test
    public void zrevrange() {
        tairZset.exzadd("foo", "1", "a");
        tairZset.exzadd("foo", "10", "b");
        tairZset.exzadd("foo", "0.1", "c");
        tairZset.exzadd("foo", "2", "a");

        List<String> expected = new ArrayList<>();
        expected.add("b");
        expected.add("a");

        List<String> range = tairZset.exzrevrange("foo", 0, 1);
        assertEquals(expected, range);

        expected.add("c");
        range = tairZset.exzrevrange("foo", 0, 100);
        assertEquals(expected, range);

        // Binary
        tairZset.exzadd(bfoo, ba, 1d);
        tairZset.exzadd(bfoo, bb, 10d);
        tairZset.exzadd(bfoo, bc, 0.1d);
        tairZset.exzadd(bfoo, ba, 2d);

        List<byte[]> bexpected = new ArrayList<>();
        bexpected.add(bb);
        bexpected.add(ba);

        List<byte[]> brange = tairZset.exzrevrange(bfoo, 0, 1);
        assertByteArrayListEquals(bexpected, brange);

        bexpected.add(bc);
        brange = tairZset.exzrevrange(bfoo, 0, 100);
        assertByteArrayListEquals(bexpected, brange);

    }

    @Test
    public void zrem() {
        tairZset.exzadd("foo", "1", "a");
        tairZset.exzadd("foo", "2", "b");

        long status = tairZset.exzrem("foo", "a");

        List<String> expected = new ArrayList<>();
        expected.add("b");

        assertEquals(1, status);
        assertEquals(expected, tairZset.exzrange("foo", 0, 100));

        status = tairZset.exzrem("foo", "bar");

        assertEquals(0, status);

        // Binary
        tairZset.exzadd(bfoo, ba, 1d);
        tairZset.exzadd(bfoo, bb, 2d);

        long bstatus = tairZset.exzrem(bfoo, ba);

        List<byte[]> bexpected = new ArrayList<>();
        bexpected.add(bb);

        assertEquals(1, bstatus);
        assertByteArrayListEquals(bexpected, tairZset.exzrange(bfoo, 0, 100));

        bstatus = tairZset.exzrem(bfoo, bbar);
        assertEquals(0, bstatus);
    }

    @Test
    public void zincrby() {
        tairZset.exzadd("foo", "1", "a");
        tairZset.exzadd("foo", "2", "b");

        String score = tairZset.exzincrBy("foo", "2", "a");

        List<String> expected = new ArrayList<>();
        expected.add("b");
        expected.add("a");

        assertEquals("3", score);
        assertEquals(expected, tairZset.exzrange("foo", 0, 100));

        // Binary
        tairZset.exzadd(bfoo, ba, 1d);
        tairZset.exzadd(bfoo, bb, 2d);

        byte[] bscore = tairZset.exzincrBy(bfoo, ba, 2d);

        List<byte[]> bexpected = new ArrayList<>();
        bexpected.add(bb);
        bexpected.add(ba);

        assertArrayEquals("3".getBytes(), bscore);
        assertByteArrayListEquals(bexpected, tairZset.exzrange(bfoo, 0, 100));

    }

    @Test
    public void zrank() {
        tairZset.exzadd("foo", "1", "a");
        tairZset.exzadd("foo", "2", "b");

        long rank = tairZset.exzrank("foo", "a");
        assertEquals(0, rank);

        rank = tairZset.exzrank("foo", "b");
        assertEquals(1, rank);

        assertNull(tairZset.exzrank("car", "b"));

        // Binary
        tairZset.exzadd(bfoo, ba, 1d);
        tairZset.exzadd(bfoo, bb, 2d);

        long brank = tairZset.exzrank(bfoo, ba);
        assertEquals(0, brank);

        brank = tairZset.exzrank(bfoo, bb);
        assertEquals(1, brank);

        assertNull(tairZset.exzrank(bcar, bb));

    }

    @Test
    public void zrevrank() {
        tairZset.exzadd("foo", "a", 1d);
        tairZset.exzadd("foo", "b", 2d);

        long rank = tairZset.exzrevrank("foo", "a");
        assertEquals(1, rank);

        rank = tairZset.exzrevrank("foo", "b");
        assertEquals(0, rank);

        // Binary
        tairZset.exzadd(bfoo, ba, 1d);
        tairZset.exzadd(bfoo, bb, 2d);

        long brank = tairZset.exzrevrank(bfoo, ba);
        assertEquals(1, brank);

        brank = tairZset.exzrevrank(bfoo, bb);
        assertEquals(0, brank);

    }

    @Test
    public void zrangeWithScores() {
        tairZset.exzadd("foo", "a", 1);
        tairZset.exzadd("foo", "b", 10);
        tairZset.exzadd("foo", "c", 0.1);
        tairZset.exzadd("foo", "a", 2);

        List<String> expected = new ArrayList<>();
        expected.add("c");
        expected.add("0.10000000000000001");
        expected.add("a");
        expected.add("2");

        List<String> range = tairZset.exzrangeWithScores("foo", 0, 1);
        assertEquals(expected, range);

        expected.add("b");
        expected.add("10");
        range = tairZset.exzrangeWithScores("foo", 0, 100);
        assertEquals(expected, range);
    }

    @Test
    public void zcard() {
        tairZset.exzadd("foo", "a", 1d);
        tairZset.exzadd("foo", "b", 10d);
        tairZset.exzadd("foo", "c", 0.1d);
        tairZset.exzadd("foo", "a", 2d);

        long size = tairZset.exzcard("foo");
        assertEquals(3, size);

        // Binary
        tairZset.exzadd(bfoo, ba, 1d);
        tairZset.exzadd(bfoo, bb, 10d);
        tairZset.exzadd(bfoo, bc, 0.1d);
        tairZset.exzadd(bfoo, ba, 2d);

        long bsize = tairZset.exzcard(bfoo);
        assertEquals(3, bsize);
    }

    @Test
    public void zscore() {
        tairZset.exzadd("foo", "a", 1d);
        tairZset.exzadd("foo", "b", 10d);
        tairZset.exzadd("foo", "c", 0.1d);
        tairZset.exzadd("foo", "a", 2d);

        String score = tairZset.exzscore("foo", "b");
        assertEquals("10", score);

        score = tairZset.exzscore("foo", "c");
        assertEquals("0.10000000000000001", score);

        score = tairZset.exzscore("foo", "s");
        assertNull(score);

        // Binary
        tairZset.exzadd(bfoo, ba, 1d);
        tairZset.exzadd(bfoo, bb, 10d);
        tairZset.exzadd(bfoo, bc, 0.1d);
        tairZset.exzadd(bfoo, ba, 2d);

        byte[] bscore = tairZset.exzscore(bfoo, bb);
        assertArrayEquals("10".getBytes(), bscore);

        bscore = tairZset.exzscore(bfoo, bc);
        assertArrayEquals("0.10000000000000001".getBytes(), bscore);

        bscore = tairZset.exzscore(bfoo, SafeEncoder.encode("s"));
        assertNull(bscore);

    }

    @Test
    public void zcount() {
        tairZset.exzadd("foo", "a", 1d);
        tairZset.exzadd("foo", "b", 10d);
        tairZset.exzadd("foo", "c", 0.1d);
        tairZset.exzadd("foo", "a", 2d);

        long result = tairZset.exzcount("foo", "0.01", "2.1");

        assertEquals(2, result);

        result = tairZset.exzcount("foo", "(0.01", "+inf");

        assertEquals(3, result);

        // Binary
        tairZset.exzadd(bfoo, ba, 1d);
        tairZset.exzadd(bfoo, bb, 10d);
        tairZset.exzadd(bfoo, bc, 0.1d);
        tairZset.exzadd(bfoo, ba, 2d);

        long bresult = tairZset.exzcount(bfoo, "0.01".getBytes(), "2.1".getBytes());

        assertEquals(2, bresult);

        bresult = tairZset.exzcount(bfoo, SafeEncoder.encode("(0.01"), SafeEncoder.encode("+inf"));

        assertEquals(3, bresult);
    }

    @Test
    public void zlexcount() {
        tairZset.exzadd("foo", "a", 1);
        tairZset.exzadd("foo", "b", 1);
        tairZset.exzadd("foo", "c", 1);
        tairZset.exzadd("foo", "aa", 1);

        long result = tairZset.exzlexcount("foo", "[aa", "(c");
        assertEquals(2, result);

        result = tairZset.exzlexcount("foo", "-", "+");
        assertEquals(4, result);

        result = tairZset.exzlexcount("foo", "-", "(c");
        assertEquals(3, result);

        result = tairZset.exzlexcount("foo", "[aa", "+");
        assertEquals(3, result);
    }

    @Test
    public void zrangebyscore() {
        tairZset.exzadd("foo", "a", 1d);
        tairZset.exzadd("foo", "b", 10d);
        tairZset.exzadd("foo", "c", 0.1d);
        tairZset.exzadd("foo", "a", 2d);

        List<String> range = tairZset.exzrangeByScore("foo", "0", "2");

        List<String> expected = new ArrayList<>();
        expected.add("c");
        expected.add("a");

        assertEquals(expected, range);

        range = tairZset.exzrangeByScore("foo", "0", "2", ExzrangeParams.ZRangeParams().limit(0, 1));

        expected = new ArrayList<>();
        expected.add("c");

        assertEquals(expected, range);

        range = tairZset.exzrangeByScore("foo", "0", "2", ExzrangeParams.ZRangeParams().limit(1, 1));
        List<String> range2 = tairZset.exzrangeByScore("foo", "-inf", "(2");
        assertEquals(expected, range2);

        expected = new ArrayList<>();
        expected.add("a");

        assertEquals(expected, range);

        // Binary
        tairZset.exzadd(bfoo, ba, 1d);
        tairZset.exzadd(bfoo, bb, 10d);
        tairZset.exzadd(bfoo, bc, 0.1d);
        tairZset.exzadd(bfoo, ba, 2d);

        List<byte[]> brange = tairZset.exzrangeByScore(bfoo, "0".getBytes(), "2".getBytes());

        List<byte[]> bexpected = new ArrayList<>();
        bexpected.add(bc);
        bexpected.add(ba);

        assertByteArrayListEquals(bexpected, brange);

        brange = tairZset.exzrangeByScore(bfoo, "0".getBytes(), "2".getBytes(), ExzrangeParams.ZRangeParams().limit(0, 1));

        bexpected = new ArrayList<>();
        bexpected.add(bc);

        assertByteArrayListEquals(bexpected, brange);

        brange = tairZset.exzrangeByScore(bfoo, "0".getBytes(), "2".getBytes(), ExzrangeParams.ZRangeParams().limit(1, 1));
        List<byte[]> brange2 = tairZset.exzrangeByScore(bfoo, SafeEncoder.encode("-inf"),
            SafeEncoder.encode("(2"));
        assertByteArrayListEquals(bexpected, brange2);

        bexpected = new ArrayList<>();
        bexpected.add(ba);

        assertByteArrayListEquals(bexpected, brange);

    }

    @Test
    public void zremrangeByRank() {
        tairZset.exzadd("foo", "a", 1d);
        tairZset.exzadd("foo", "b", 10d);
        tairZset.exzadd("foo", "c", 0.1d);
        tairZset.exzadd("foo", "a", 2d);

        long result = tairZset.exzremrangeByRank("foo", 0, 0);

        assertEquals(1, result);

        List<String> expected = new ArrayList<>();
        expected.add("a");
        expected.add("b");

        assertEquals(expected, tairZset.exzrange("foo", 0, 100));

        // Binary
        tairZset.exzadd(bfoo, ba, 1d);
        tairZset.exzadd(bfoo, bb, 10d);
        tairZset.exzadd(bfoo, bc, 0.1d);
        tairZset.exzadd(bfoo, ba, 2d);

        long bresult = tairZset.exzremrangeByRank(bfoo, 0, 0);

        assertEquals(1, bresult);

        List<byte[]> bexpected = new ArrayList<>();
        bexpected.add(ba);
        bexpected.add(bb);

        assertByteArrayListEquals(bexpected, tairZset.exzrange(bfoo, 0, 100));
    }

    @Test
    public void zremrangeByScore() {
        tairZset.exzadd("foo", "a", 1d);
        tairZset.exzadd("foo", "b", 10d);
        tairZset.exzadd("foo", "c", 0.1d);
        tairZset.exzadd("foo", "a", 2d);

        long result = tairZset.exzremrangeByScore("foo", "0", "2");

        assertEquals(2, result);

        List<String> expected = new ArrayList<>();
        expected.add("b");

        assertEquals(expected, tairZset.exzrange("foo", 0, 100));

        // Binary
        tairZset.exzadd(bfoo, ba, 1d);
        tairZset.exzadd(bfoo, bb, 10d);
        tairZset.exzadd(bfoo, bc, 0.1d);
        tairZset.exzadd(bfoo, ba, 2d);

        long bresult = tairZset.exzremrangeByScore(bfoo, "0".getBytes(), "2".getBytes());

        assertEquals(2, bresult);

        List<byte[]> bexpected = new ArrayList<>();
        bexpected.add(bb);

        assertByteArrayListEquals(bexpected, tairZset.exzrange(bfoo, 0, 100));
    }

    @Test
    public void zremrangeByLex() {
        tairZset.exzadd("foo", "a", 1);
        tairZset.exzadd("foo", "b", 1);
        tairZset.exzadd("foo", "c", 1);
        tairZset.exzadd("foo", "aa", 1);

        long result = tairZset.exzremrangeByLex("foo", "[aa", "(c");

        assertEquals(2, result);

        List<String> expected = new ArrayList<>();
        expected.add("a");
        expected.add("c");

        assertEquals(expected, tairZset.exzrangeByLex("foo", "-", "+"));
    }
}
