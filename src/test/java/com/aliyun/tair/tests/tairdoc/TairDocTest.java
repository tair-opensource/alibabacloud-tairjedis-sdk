package com.aliyun.tair.tests.tairdoc;

import java.util.UUID;

import com.aliyun.tair.tairdoc.params.JsongetParams;
import com.aliyun.tair.tairdoc.params.JsonsetParams;
import org.junit.Assert;
import org.junit.Test;

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TairDocTest extends TairDocTestBase {
    private String jsonKey;
    private static final String JSON_STRING_EXAMPLE = "{\"foo\":\"bar\",\"baz\":42}";
    private static final String JSON_ARRAY_EXAMPLE = "{\"id\":[1,2,3]}";
    private String randomkey_;
    private byte[] randomKeyBinary_;

    public TairDocTest() {
        randomkey_ = "randomkey_" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        randomKeyBinary_ = ("randomkey_" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        jsonKey = "jsonkey" + "-" + Thread.currentThread().getName() + "-" + UUID.randomUUID().toString();
    }

    @Test
    public void jsonSetTest() {
        String ret = tairDoc.jsonset(jsonKey, ".", JSON_STRING_EXAMPLE);
        assertEquals("OK", ret);

        ret = tairDoc.jsonget(jsonKey, ".");
        assertEquals(JSON_STRING_EXAMPLE, ret);

        ret = tairDoc.jsonget(jsonKey, ".foo");
        assertEquals("\"bar\"", ret);

        ret = tairDoc.jsonget(jsonKey, ".baz");
        assertEquals("42", ret);
    }

    @Test
    public void jsonSetWithNXXX() {
        String ret = tairDoc.jsonset(jsonKey, ".", JSON_STRING_EXAMPLE, JsonsetParams.JsonsetParams().xx());
        assertNull(ret);

        ret = tairDoc.jsonset(jsonKey, ".", JSON_STRING_EXAMPLE, JsonsetParams.JsonsetParams().nx());
        assertEquals("OK", ret);

        ret = tairDoc.jsonset(jsonKey, ".", JSON_STRING_EXAMPLE, JsonsetParams.JsonsetParams().xx());
        assertEquals("OK", ret);
    }

    @Test
    public void jsonSetWithException() {
        try {
            tairDoc.jsonset(jsonKey, "/abc", JSON_STRING_EXAMPLE);
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
        String ret = tairDoc.jsonset(jsonKey, ".", JSON_STRING_EXAMPLE);
        assertEquals("OK", ret);

        ret = tairDoc.jsonget(jsonKey, ".");
        assertEquals(JSON_STRING_EXAMPLE, ret);

        ret = tairDoc.jsonget(jsonKey, ".foo");
        assertEquals("\"bar\"", ret);

        ret = tairDoc.jsonget(jsonKey, ".baz");
        assertEquals("42", ret);

        try {
            tairDoc.jsonget(jsonKey, ".not-exists");
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
        String ret = tairDoc.jsonset(jsonKey, ".", JSON_STRING_EXAMPLE);
        assertEquals("OK", ret);

        ret = tairDoc.jsonget(jsonKey, ".", JsongetParams.JsongetParams().format("xml"));
        assertEquals("<?xml version=\"1.0\" encoding=\"UTF-8\"?><root><foo>bar</foo><baz>42</baz></root>", ret);

        ret = tairDoc.jsonget(jsonKey, ".", JsongetParams.JsongetParams().format("yaml"));
        assertEquals("\nfoo: bar\nbaz: 42\n", ret);
    }

    @Test
    public void jsonDelTest() {
        String ret = tairDoc.jsonset(jsonKey, ".", JSON_STRING_EXAMPLE);
        assertEquals("OK", ret);

        long lret = tairDoc.jsondel(jsonKey, ".foo");
        assertEquals(1, lret);

        try {
            tairDoc.jsondel(jsonKey, ".not-exists");
        } catch (Exception e) {
            if (e.getMessage().contains("ERR old item is null")) {
                assertTrue(true);
            } else {
                assertTrue(false);
            }
        }

        lret = tairDoc.jsondel(jsonKey);
        assertEquals(1, lret);

        ret = tairDoc.jsonget(jsonKey);
        assertNull(ret);
    }

    @Test
    public void jsonTypeTest() {
        String ret = tairDoc.jsonset(jsonKey, ".", JSON_STRING_EXAMPLE);
        assertEquals("OK", ret);

        ret = tairDoc.jsontype(jsonKey);
        assertEquals("object", ret);

        ret = tairDoc.jsontype(jsonKey, ".foo");
        assertEquals("string", ret);

        ret = tairDoc.jsontype(jsonKey, ".baz");
        assertEquals("number", ret);

        ret = tairDoc.jsontype(jsonKey, ".not-exists");
        assertNull(ret);
    }

    @Test
    public void jsonNumincrbyTest() {
        String ret = tairDoc.jsonset(jsonKey, ".", JSON_STRING_EXAMPLE);
        assertEquals("OK", ret);

        double dret = tairDoc.jsonnumincrBy(jsonKey, ".baz", 1D);
        assertEquals(43, dret, 0.1);

        dret = tairDoc.jsonnumincrBy(jsonKey, ".baz", 1.5);
        assertEquals(44.5, dret, 0.1);

        try {
            tairDoc.jsonnumincrBy(jsonKey, ".foo", 1D);
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
        String ret = tairDoc.jsonset(jsonKey, ".", JSON_STRING_EXAMPLE);
        assertEquals("OK", ret);

        long lret = tairDoc.jsonstrAppend(jsonKey, ".foo", "rrrrr");
        assertEquals(8, lret);

        ret = tairDoc.jsonget(jsonKey, ".foo");
        assertEquals("\"barrrrrr\"", ret);

        try {
            tairDoc.jsonstrAppend(jsonKey, ".not-exists");
        } catch (Exception e) {
            if (e.getMessage().contains("ERR node not exists")) {
                assertTrue(true);
            } else {
                assertTrue(false);
            }
        }

        lret = tairDoc.jsonstrAppend("not-exists", ".foo");
        assertEquals(-1, lret);
    }

    @Test
    public void jsonStrlenTest() {
        String ret = tairDoc.jsonset(jsonKey, ".", JSON_STRING_EXAMPLE);
        assertEquals("OK", ret);

        long lret = tairDoc.jsonstrAppend(jsonKey, ".foo", "rrrrr");
        assertEquals(lret, 8);

        ret = tairDoc.jsonget(jsonKey, ".foo");
        assertEquals(ret, "\"barrrrrr\"");

        lret = tairDoc.jsonstrlen(jsonKey, ".foo");
        assertEquals(8, lret);
    }

    @Test
    public void jsonArrappendTest() {
        String ret = tairDoc.jsonset(jsonKey, ".", JSON_ARRAY_EXAMPLE);
        assertEquals("OK", ret);

        long lret = tairDoc.jsonarrAppend(jsonKey, ".id", "null", "false", "true");
        assertEquals(6, lret);

        ret = tairDoc.jsonget(jsonKey, ".id.2");
        assertEquals("3", ret);
    }

    @Test
    public void jsonArrpopTest() {
        String ret = tairDoc.jsonset(jsonKey, ".", JSON_ARRAY_EXAMPLE);
        assertEquals("OK", ret);

        ret = tairDoc.jsonarrPop(jsonKey, ".id", 1);
        assertEquals("2", ret);

        ret = tairDoc.jsonarrPop(jsonKey, ".id", -1);
        assertEquals("3", ret);

        try {
            tairDoc.jsonarrPop(jsonKey, ".id", 10);
        } catch (Exception e) {
            if (e.getMessage().contains("ERR array index outflow")) {
                assertTrue(true);
            } else {
                assertTrue(false);
            }
        }

        ret = tairDoc.jsonarrPop(jsonKey, ".id");
        assertEquals("1", ret);

        try {
            tairDoc.jsonarrPop(jsonKey, ".id");
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
        String ret = tairDoc.jsonset(jsonKey, ".", JSON_ARRAY_EXAMPLE);
        assertEquals("OK", ret);

        long lret = tairDoc.jsonarrInsert(jsonKey, ".id", "3", "5", "6");
        assertEquals(lret, 5);

        ret = tairDoc.jsonget(jsonKey, ".id");
        assertEquals("[1,2,3,5,6]", ret);

        lret = tairDoc.jsonarrInsert(jsonKey, ".id", "3", "4");
        assertEquals(6, lret);

        ret = tairDoc.jsonget(jsonKey, ".id");
        assertEquals("[1,2,3,4,5,6]", ret);
    }

    @Test
    public void jsonArrlenTest() {
        String ret = tairDoc.jsonset(jsonKey, ".", JSON_ARRAY_EXAMPLE);
        assertEquals("OK", ret);

        long lret = tairDoc.jsonArrlen(jsonKey, ".id");
        assertEquals(3, lret);
    }

    @Test
    public void jsonArrtrimTest() {
        String ret = tairDoc.jsonset(jsonKey, ".", "{\"id\":[1,2,3,4,5,6]}");
        assertEquals("OK", ret);

        long lret = tairDoc.jsonarrTrim(jsonKey, ".id", 3, 4);
        assertEquals(2, lret);

        ret = tairDoc.jsonget(jsonKey, ".id");
        assertEquals("[4,5]", ret);

        try {
            tairDoc.jsonarrTrim(jsonKey, ".id", 3, 4);
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
        tairDoc.jsondel(randomkey_);
        tairDoc.jsondel(randomKeyBinary_);

        try {
            jedis.set(randomkey_, "bar");
            tairDoc.jsondel(randomkey_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
        try {
            jedis.set(randomKeyBinary_, "bar".getBytes());
            tairDoc.jsondel(randomKeyBinary_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void jsongetException() {
        tairDoc.jsonget(randomkey_);
        tairDoc.jsonget(randomKeyBinary_);

        try {
            jedis.set(randomkey_, "bar");
            tairDoc.jsonget(randomkey_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
        try {
            jedis.set(randomKeyBinary_, "bar".getBytes());
            tairDoc.jsonget(randomKeyBinary_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void jsonmgetException() {
        try {
            jedis.set(randomkey_, "bar");
            tairDoc.jsonmget(randomkey_, "", "");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void jsonsetException() {
        try {
            jedis.set(randomkey_, JSON_STRING_EXAMPLE);
            tairDoc.jsonset(randomkey_, ".", JSON_STRING_EXAMPLE);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
        try {
            jedis.set(randomKeyBinary_, "bar".getBytes());
            tairDoc.jsonset(randomKeyBinary_, "".getBytes(), "".getBytes());
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void jsontypeException() {
        tairDoc.jsontype(randomkey_);
        tairDoc.jsontype(randomKeyBinary_);

        try {
            jedis.set(randomkey_, "bar");
            tairDoc.jsontype(randomkey_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
        try {
            jedis.set(randomKeyBinary_, "bar".getBytes());
            tairDoc.jsontype(randomKeyBinary_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void jsonnumincrbyException() {
        try {
            jedis.set(randomkey_, "bar");
            tairDoc.jsonnumincrBy(randomkey_, 1.0);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
        try {
            jedis.set(randomKeyBinary_, "bar".getBytes());
            tairDoc.jsonnumincrBy(randomKeyBinary_, 1.0);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void jsonstrappendException() {
        tairDoc.jsonstrAppend(randomkey_, JSON_ARRAY_EXAMPLE);
        tairDoc.jsonstrAppend(randomKeyBinary_, JSON_ARRAY_EXAMPLE.getBytes());

        try {
            jedis.set(randomkey_, "bar");
            tairDoc.jsonstrAppend(randomkey_, JSON_ARRAY_EXAMPLE);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
        try {
            jedis.set(randomKeyBinary_, "bar".getBytes());
            tairDoc.jsonstrAppend(randomKeyBinary_, JSON_ARRAY_EXAMPLE.getBytes());
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void jsonstrlenException() {
        tairDoc.jsonstrlen(randomkey_);
        tairDoc.jsonstrlen(randomKeyBinary_);

        try {
            jedis.set(randomkey_, "bar");
            tairDoc.jsonstrlen(randomkey_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
        try {
            jedis.set(randomKeyBinary_, "bar".getBytes());
            tairDoc.jsonstrlen(randomKeyBinary_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void jsonarrappendException() {
        try {
            jedis.set(randomkey_, "bar");
            tairDoc.jsonarrAppend(randomkey_, "", "", "", "");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void jsonarrpopException() {
        try {
            tairDoc.jsonarrPop(randomkey_, "");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("no such key"));
        }

        try {
            jedis.set(randomkey_, "bar");
            tairDoc.jsonarrPop(randomkey_, "");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void jsonarrinsertException() {
        try {
            jedis.set(randomkey_, "bar");
            tairDoc.jsonarrInsert(randomkey_, "", "", "", "");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void jsonarrlenException() {
        tairDoc.jsonArrlen(randomkey_);

        try {
            jedis.set(randomkey_, "bar");
            tairDoc.jsonArrlen(randomkey_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void jsonarrtrimException() {
        tairDoc.jsonarrTrim(randomkey_, "", 0, -1);

        try {
            jedis.set(randomkey_, "bar");
            tairDoc.jsonarrTrim(randomkey_, "", 0, -1);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }
}
