package com.aliyun.tair.tests.tairdoc;

import java.util.UUID;

import com.aliyun.tair.tairdoc.params.JsongetParams;
import com.aliyun.tair.tairdoc.params.JsonsetParams;
import org.junit.Assert;
import org.junit.Test;

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;

public class TairDocTest extends TairDocTestBase {
    private String jsonKey;
    private static final String JSON_STRING_EXAMPLE = "{\"foo\":\"bar\",\"baz\":42}";
    private static final String JSON_ARRAY_EXAMPLE = "{\"id\":[1,2,3]}";

    public TairDocTest() {
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
                Assert.assertTrue(true);
            } else {
                Assert.assertTrue(false);
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
                Assert.assertTrue(true);
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
                Assert.assertTrue(true);
            } else {
                Assert.assertTrue(false);
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
                Assert.assertTrue(true);
            } else {
                Assert.assertTrue(false);
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
        assertEquals( "\"barrrrrr\"", ret);

        try {
            tairDoc.jsonstrAppend(jsonKey, ".not-exists");
        } catch (Exception e) {
            if (e.getMessage().contains("ERR node not exists")) {
                Assert.assertTrue(true);
            } else {
                Assert.assertTrue(false);
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
                Assert.assertTrue(true);
            } else {
                Assert.assertTrue(false);
            }
        }

        ret = tairDoc.jsonarrPop(jsonKey, ".id");
        assertEquals("1", ret);

        try {
            tairDoc.jsonarrPop(jsonKey, ".id");
        } catch (Exception e) {
            if (e.getMessage().contains("ERR array index outflow")) {
                Assert.assertTrue(true);
            } else {
                Assert.assertTrue(false);
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
                Assert.assertTrue(true);
            } else {
                Assert.assertTrue(false);
            }
        }
    }
}
