package com.aliyun.tair.tests.tairdoc;

import java.util.List;
import java.util.UUID;

import com.aliyun.tair.tairdoc.params.JsongetParams;
import com.aliyun.tair.tairdoc.params.JsonsetParams;
import org.junit.Assert;
import org.junit.Test;

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TairDocClusterTest extends TairDocTestBase {
    private String jsonKey;
    private static final String JSON_STRING_EXAMPLE = "{\"foo\":\"bar\",\"baz\":42}";
    private static final String JSON_ARRAY_EXAMPLE = "{\"id\":[1,2,3]}";
    private String randomkey_;
    private byte[] randomKeyBinary_;

    public TairDocClusterTest() {
        randomkey_ = "randomkey_" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        randomKeyBinary_ = ("randomkey_" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        jsonKey = "jsonkey" + "-" + Thread.currentThread().getName() + "-" + UUID.randomUUID().toString();
    }

    @Test
    public void jsonSetTest() {
        String ret = tairDocCluster.jsonset(jsonKey, ".", JSON_STRING_EXAMPLE);
        assertEquals("OK", ret);

        ret = tairDocCluster.jsonget(jsonKey, ".");
        assertEquals(JSON_STRING_EXAMPLE, ret);

        ret = tairDocCluster.jsonget(jsonKey, ".foo");
        assertEquals("\"bar\"", ret);

        ret = tairDocCluster.jsonget(jsonKey, ".baz");
        assertEquals("42", ret);
    }

    @Test
    public void jsonSetTestBinary() {
        String ret = tairDocCluster.jsonset(jsonKey.getBytes(), ".".getBytes(), JSON_STRING_EXAMPLE.getBytes());
        assertEquals("OK", ret);

        byte[] bret = tairDocCluster.jsonget(jsonKey.getBytes(), ".".getBytes());
        assertArrayEquals(bret, JSON_STRING_EXAMPLE.getBytes());

        bret = tairDocCluster.jsonget(jsonKey.getBytes(), ".foo".getBytes());
        assertArrayEquals(bret, "\"bar\"".getBytes());

        bret = tairDocCluster.jsonget(jsonKey.getBytes(), ".baz".getBytes());
        assertArrayEquals(bret, "42".getBytes());
    }

    @Test
    public void jsonSetWithNXXX() {
        String ret = tairDocCluster.jsonset(jsonKey, ".", JSON_STRING_EXAMPLE, JsonsetParams.JsonsetParams().xx());
        assertNull(ret);

        ret = tairDocCluster.jsonset(jsonKey, ".", JSON_STRING_EXAMPLE, JsonsetParams.JsonsetParams().nx());
        assertEquals("OK", ret);

        ret = tairDocCluster.jsonset(jsonKey, ".", JSON_STRING_EXAMPLE, JsonsetParams.JsonsetParams().xx());
        assertEquals("OK", ret);
    }

    @Test
    public void jsonSetWithNXXXBinary() {
        String ret = tairDocCluster.jsonset(jsonKey.getBytes(), ".".getBytes(), JSON_STRING_EXAMPLE.getBytes(), JsonsetParams.JsonsetParams().xx());
        assertNull(ret);

        ret = tairDocCluster.jsonset(jsonKey.getBytes(), ".".getBytes(), JSON_STRING_EXAMPLE.getBytes(), JsonsetParams.JsonsetParams().nx());
        assertEquals("OK", ret);

        ret = tairDocCluster.jsonset(jsonKey.getBytes(), ".".getBytes(), JSON_STRING_EXAMPLE.getBytes(), JsonsetParams.JsonsetParams().xx());
        assertEquals("OK", ret);
    }

    @Test
    public void jsonMgetTest() {
        String jsonkey1 = jsonKey + "{1}1";
        String jsonkey2 = jsonKey + "{1}2";
        String jsonkey3 = jsonKey + "{1}3";

        assertEquals("OK", tairDocCluster.jsonset(jsonkey1, ".", JSON_STRING_EXAMPLE));
        assertEquals("OK", tairDocCluster.jsonset(jsonkey2, ".", JSON_STRING_EXAMPLE));
        assertEquals("OK", tairDocCluster.jsonset(jsonkey3, ".", JSON_STRING_EXAMPLE));

        List<String> mgetRet = tairDocCluster.jsonmget(jsonkey1, jsonkey2, jsonkey3, ".baz");
        for (int i = 0; i < mgetRet.size(); i++) {
            Assert.assertEquals("42", mgetRet.get(i));
        }
    }

    @Test
    public void jsonMgetTestBinary() {
        String jsonkey1 = jsonKey + "{1}1";
        String jsonkey2 = jsonKey + "{1}2";
        String jsonkey3 = jsonKey + "{1}3";

        assertEquals("OK", tairDocCluster.jsonset(jsonkey1.getBytes(), ".".getBytes(), JSON_STRING_EXAMPLE.getBytes()));
        assertEquals("OK", tairDocCluster.jsonset(jsonkey2.getBytes(), ".".getBytes(), JSON_STRING_EXAMPLE.getBytes()));
        assertEquals("OK", tairDocCluster.jsonset(jsonkey3.getBytes(), ".".getBytes(), JSON_STRING_EXAMPLE.getBytes()));

        List<byte[]> mgetRet = tairDocCluster.jsonmget(jsonkey1.getBytes(), jsonkey2.getBytes(), jsonkey3.getBytes(), ".baz".getBytes());
        for (int i = 0; i < mgetRet.size(); i++) {
            Assert.assertEquals("42", new String(mgetRet.get(i)));
        }
    }

    @Test
    public void jsonSetWithException() {
        try {
            tairDocCluster.jsonset(jsonKey, "/abc", JSON_STRING_EXAMPLE);
        } catch (Exception e) {
            if (e.getMessage().contains("ERR new objects must be created at the root")) {
                assertTrue(true);
            } else {
                assertTrue(false);
            }
        }
    }

    @Test
    public void jsonGetTest() {
        String ret = tairDocCluster.jsonset(jsonKey, ".", JSON_STRING_EXAMPLE);
        assertEquals("OK", ret);

        ret = tairDocCluster.jsonget(jsonKey, ".");
        assertEquals(JSON_STRING_EXAMPLE, ret);

        ret = tairDocCluster.jsonget(jsonKey, ".foo");
        assertEquals("\"bar\"", ret);

        ret = tairDocCluster.jsonget(jsonKey, ".baz");
        assertEquals("42", ret);

        try {
            tairDocCluster.jsonget(jsonKey, ".not-exists");
        } catch (Exception e) {
            if (e.getMessage().contains("ERR pointer illegal")) {
                assertTrue(true);
            } else {
                Assert.assertFalse(true);
            }
        }
    }

    @Test
    public void jsonGetWithXmlAndYaml() {
        String ret = tairDocCluster.jsonset(jsonKey, ".", JSON_STRING_EXAMPLE);
        assertEquals("OK", ret);

        ret = tairDocCluster.jsonget(jsonKey, ".", JsongetParams.JsongetParams().format("xml"));
        assertEquals("<?xml version=\"1.0\" encoding=\"UTF-8\"?><root><foo>bar</foo><baz>42</baz></root>", ret);

        ret = tairDocCluster.jsonget(jsonKey, ".", JsongetParams.JsongetParams().format("yaml"));
        assertEquals("\nfoo: bar\nbaz: 42\n", ret);

        byte[] bret = tairDocCluster.jsonget(jsonKey.getBytes(), ".".getBytes(), JsongetParams.JsongetParams().format("yaml"));
        assertEquals("\nfoo: bar\nbaz: 42\n", new String(bret));
    }

    @Test
    public void jsonDelTest() {
        String ret = tairDocCluster.jsonset(jsonKey, ".", JSON_STRING_EXAMPLE);
        assertEquals("OK", ret);

        long lret = tairDocCluster.jsondel(jsonKey.getBytes(), ".foo".getBytes());
        assertEquals(1, lret);

        try {
            tairDocCluster.jsondel(jsonKey, ".not-exists");
        } catch (Exception e) {
            if (e.getMessage().contains("ERR old item is null")) {
                assertTrue(true);
            } else {
                assertTrue(false);
            }
        }

        lret = tairDocCluster.jsondel(jsonKey);
        assertEquals(1, lret);

        ret = tairDocCluster.jsonget(jsonKey);
        assertNull(ret);
    }

    @Test
    public void jsonTypeTest() {
        String ret = tairDocCluster.jsonset(jsonKey, ".", JSON_STRING_EXAMPLE);
        assertEquals("OK", ret);

        ret = tairDocCluster.jsontype(jsonKey);
        assertEquals("object", ret);

        byte[] bret = tairDocCluster.jsontype(jsonKey.getBytes(), ".foo".getBytes());
        assertEquals("string", new String(bret));

        ret = tairDocCluster.jsontype(jsonKey, ".baz");
        assertEquals("number", ret);

        ret = tairDocCluster.jsontype(jsonKey, ".not-exists");
        assertNull(ret);
    }

    @Test
    public void jsonNumincrbyTest() {
        String ret = tairDocCluster.jsonset(jsonKey, ".", JSON_STRING_EXAMPLE);
        assertEquals("OK", ret);

        double dret = tairDocCluster.jsonnumincrBy(jsonKey, ".baz", 1D);
        assertEquals(43, dret, 0.1);

        dret = tairDocCluster.jsonnumincrBy(jsonKey.getBytes(), ".baz".getBytes(), 1.5);
        assertEquals(44.5, dret, 0.1);

        try {
            tairDocCluster.jsonnumincrBy(jsonKey, ".foo", 1D);
        } catch (Exception e) {
            if (e.getMessage().contains("ERR node not exists")) {
                assertTrue(true);
            } else {
                assertTrue(false);
            }
        }
    }

    @Test
    public void jsonStrappendTest() {
        String ret = tairDocCluster.jsonset(jsonKey, ".", JSON_STRING_EXAMPLE);
        assertEquals("OK", ret);

        long lret = tairDocCluster.jsonstrAppend(jsonKey, ".foo", "rrrr");
        assertEquals(7, lret);

        lret = tairDocCluster.jsonstrAppend(jsonKey.getBytes(), ".foo".getBytes(), "r".getBytes());
        assertEquals(8, lret);

        ret = tairDocCluster.jsonget(jsonKey, ".foo");
        assertEquals("\"barrrrrr\"", ret);

        try {
            tairDocCluster.jsonstrAppend(jsonKey.getBytes(), ".not-exists".getBytes());
        } catch (Exception e) {
            if (e.getMessage().contains("ERR node not exists")) {
                assertTrue(true);
            } else {
                assertTrue(false);
            }
        }

        lret = tairDocCluster.jsonstrAppend("not-exists", ".foo");
        assertEquals(-1, lret);
    }

    @Test
    public void jsonStrlenTest() {
        String ret = tairDocCluster.jsonset(jsonKey, ".", JSON_STRING_EXAMPLE);
        assertEquals("OK", ret);

        long lret = tairDocCluster.jsonstrAppend(jsonKey, ".foo", "rrrrr");
        assertEquals(lret, 8);

        ret = tairDocCluster.jsonget(jsonKey, ".foo");
        assertEquals(ret, "\"barrrrrr\"");

        lret = tairDocCluster.jsonstrlen(jsonKey, ".foo");
        assertEquals(8, lret);

        lret = tairDocCluster.jsonstrlen(jsonKey.getBytes(), ".foo".getBytes());
        assertEquals(8, lret);
    }

    @Test
    public void jsonArrappendTest() {
        String ret = tairDocCluster.jsonset(jsonKey, ".", JSON_ARRAY_EXAMPLE);
        assertEquals("OK", ret);

        long lret = tairDocCluster.jsonarrAppend(jsonKey.getBytes(), ".id".getBytes(),
            "null".getBytes(), "false".getBytes(), "true".getBytes());
        assertEquals(6, lret);

        ret = tairDocCluster.jsonget(jsonKey, ".id.2");
        assertEquals("3", ret);
    }

    @Test
    public void jsonArrpopTest() {
        String ret = tairDocCluster.jsonset(jsonKey, ".", JSON_ARRAY_EXAMPLE);
        assertEquals("OK", ret);

        ret = tairDocCluster.jsonarrPop(jsonKey, ".id", 1);
        assertEquals("2", ret);

        byte[] bret = tairDocCluster.jsonarrPop(jsonKey.getBytes(), ".id".getBytes(), -1);
        assertEquals("3", new String(bret));

        try {
            tairDocCluster.jsonarrPop(jsonKey, ".id", 10);
        } catch (Exception e) {
            if (e.getMessage().contains("ERR array index outflow")) {
                assertTrue(true);
            } else {
                assertTrue(false);
            }
        }

        ret = tairDocCluster.jsonarrPop(jsonKey, ".id");
        assertEquals("1", ret);

        try {
            tairDocCluster.jsonarrPop(jsonKey.getBytes(), ".id".getBytes());
        } catch (Exception e) {
            if (e.getMessage().contains("ERR array index outflow")) {
                assertTrue(true);
            } else {
                assertTrue(false);
            }
        }
    }

    @Test
    public void jsonArrinsertTest() {
        String ret = tairDocCluster.jsonset(jsonKey, ".", JSON_ARRAY_EXAMPLE);
        assertEquals("OK", ret);

        long lret = tairDocCluster.jsonarrInsert(jsonKey, ".id", "3", "5", "6");
        assertEquals(lret, 5);

        ret = tairDocCluster.jsonget(jsonKey, ".id");
        assertEquals("[1,2,3,5,6]", ret);

        lret = tairDocCluster.jsonarrInsert(jsonKey.getBytes(), ".id".getBytes(), "3".getBytes(), "4".getBytes());
        assertEquals(6, lret);

        ret = tairDocCluster.jsonget(jsonKey, ".id");
        assertEquals("[1,2,3,4,5,6]", ret);
    }

    @Test
    public void jsonArrlenTest() {
        String ret = tairDocCluster.jsonset(jsonKey, ".", JSON_ARRAY_EXAMPLE);
        assertEquals("OK", ret);

        long lret = tairDocCluster.jsonArrlen(jsonKey, ".id");
        assertEquals(3, lret);

        lret = tairDocCluster.jsonArrLen(jsonKey.getBytes(), ".id".getBytes());
        assertEquals(3, lret);
    }

    @Test
    public void jsonArrlenTest2() {
        String ret = tairDocCluster.jsonset(jsonKey, ".", "[1, 2, 3]");
        assertEquals("OK", ret);

        long lret = tairDocCluster.jsonArrLen(jsonKey);
        assertEquals(3, lret);

        lret = tairDocCluster.jsonArrLen(jsonKey.getBytes());
        assertEquals(3, lret);
    }

    @Test
    public void jsonArrtrimTest() {
        String ret = tairDocCluster.jsonset(jsonKey, ".", "{\"id\":[1,2,3,4,5,6]}");
        assertEquals("OK", ret);

        long lret = tairDocCluster.jsonarrTrim(jsonKey, ".id", 3, 4);
        assertEquals(2, lret);

        byte[] bret = tairDocCluster.jsonget(jsonKey.getBytes(), ".id".getBytes());
        assertEquals("[4,5]", new String(bret));


        lret = tairDocCluster.jsonarrTrim(jsonKey.getBytes(), ".id".getBytes(), 0, 0);
        assertEquals(1, lret);

        try {
            tairDocCluster.jsonarrTrim(jsonKey, ".id", 3, 4);
        } catch (Exception e) {
            if (e.getMessage().contains("ERR array index outflow")) {
                assertTrue(true);
            } else {
                assertTrue(false);
            }
        }
    }


    @Test
    public void jsondelException() {
        tairDocCluster.jsondel(randomkey_);
        tairDocCluster.jsondel(randomKeyBinary_);

        try {
            jedisCluster.set(randomkey_, "bar");
            tairDocCluster.jsondel(randomkey_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
        try {
            jedisCluster.set(randomKeyBinary_, "bar".getBytes());
            tairDocCluster.jsondel(randomKeyBinary_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void jsongetException() {
        tairDocCluster.jsonget(randomkey_);
        tairDocCluster.jsonget(randomKeyBinary_);

        try {
            jedisCluster.set(randomkey_, "bar");
            tairDocCluster.jsonget(randomkey_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
        try {
            jedisCluster.set(randomKeyBinary_, "bar".getBytes());
            tairDocCluster.jsonget(randomKeyBinary_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void jsonmgetException() {
        try {
            jedisCluster.set(randomkey_, "bar");
            tairDocCluster.jsonmget(randomkey_, "", "");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void jsonsetException() {
        try {
            jedisCluster.set(randomkey_, JSON_STRING_EXAMPLE);
            tairDocCluster.jsonset(randomkey_, ".", JSON_STRING_EXAMPLE);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
        try {
            jedisCluster.set(randomKeyBinary_, "bar".getBytes());
            tairDocCluster.jsonset(randomKeyBinary_, "".getBytes(), "".getBytes());
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void jsontypeException() {
        tairDocCluster.jsontype(randomkey_);
        tairDocCluster.jsontype(randomKeyBinary_);

        try {
            jedisCluster.set(randomkey_, "bar");
            tairDocCluster.jsontype(randomkey_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
        try {
            jedisCluster.set(randomKeyBinary_, "bar".getBytes());
            tairDocCluster.jsontype(randomKeyBinary_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void jsonnumincrbyException() {
        try {
            jedisCluster.set(randomkey_, "bar");
            tairDocCluster.jsonnumincrBy(randomkey_, 1.0);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
        try {
            jedisCluster.set(randomKeyBinary_, "bar".getBytes());
            tairDocCluster.jsonnumincrBy(randomKeyBinary_, 1.0);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void jsonstrappendException() {
        tairDocCluster.jsonstrAppend(randomkey_, JSON_ARRAY_EXAMPLE);
        tairDocCluster.jsonstrAppend(randomKeyBinary_, JSON_ARRAY_EXAMPLE.getBytes());

        try {
            jedisCluster.set(randomkey_, "bar");
            tairDocCluster.jsonstrAppend(randomkey_, JSON_ARRAY_EXAMPLE);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
        try {
            jedisCluster.set(randomKeyBinary_, "bar".getBytes());
            tairDocCluster.jsonstrAppend(randomKeyBinary_, JSON_ARRAY_EXAMPLE.getBytes());
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void jsonstrlenException() {
        tairDocCluster.jsonstrlen(randomkey_);
        tairDocCluster.jsonstrlen(randomKeyBinary_);

        try {
            jedisCluster.set(randomkey_, "bar");
            tairDocCluster.jsonstrlen(randomkey_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
        try {
            jedisCluster.set(randomKeyBinary_, "bar".getBytes());
            tairDocCluster.jsonstrlen(randomKeyBinary_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void jsonarrappendException() {
        try {
            jedisCluster.set(randomkey_, "bar");
            tairDocCluster.jsonarrAppend(randomkey_, "", "", "", "");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void jsonarrpopException() {
        try {
            tairDocCluster.jsonarrPop(randomkey_, "");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("no such key"));
        }

        try {
            jedisCluster.set(randomkey_, "bar");
            tairDocCluster.jsonarrPop(randomkey_, "");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void jsonarrlenException() {
        tairDocCluster.jsonArrLen(randomkey_);

        try {
            jedisCluster.set(randomkey_, "bar");
            tairDocCluster.jsonArrLen(randomkey_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void jsonarrtrimException() {
        tairDocCluster.jsonarrTrim(randomkey_, "", 0, -1);

        try {
            jedisCluster.set(randomkey_, "bar");
            tairDocCluster.jsonarrTrim(randomkey_, "", 0, -1);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }
}
