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
    public void jsonSetTestBinary() {
        String ret = tairDoc.jsonset(jsonKey.getBytes(), ".".getBytes(), JSON_STRING_EXAMPLE.getBytes());
        assertEquals("OK", ret);

        byte[] bret = tairDoc.jsonget(jsonKey.getBytes(), ".".getBytes());
        assertArrayEquals(bret, JSON_STRING_EXAMPLE.getBytes());

        bret = tairDoc.jsonget(jsonKey.getBytes(), ".foo".getBytes());
        assertArrayEquals(bret, "\"bar\"".getBytes());

        bret = tairDoc.jsonget(jsonKey.getBytes(), ".baz".getBytes());
        assertArrayEquals(bret, "42".getBytes());
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
    public void jsonSetWithNXXXBinary() {
        String ret = tairDoc.jsonset(jsonKey.getBytes(), ".".getBytes(), JSON_STRING_EXAMPLE.getBytes(), JsonsetParams.JsonsetParams().xx());
        assertNull(ret);

        ret = tairDoc.jsonset(jsonKey.getBytes(), ".".getBytes(), JSON_STRING_EXAMPLE.getBytes(), JsonsetParams.JsonsetParams().nx());
        assertEquals("OK", ret);

        ret = tairDoc.jsonset(jsonKey.getBytes(), ".".getBytes(), JSON_STRING_EXAMPLE.getBytes(), JsonsetParams.JsonsetParams().xx());
        assertEquals("OK", ret);
    }

    @Test
    public void jsonMgetTest() {
        String jsonkey1 = jsonKey + "1";
        String jsonkey2 = jsonKey + "2";
        String jsonkey3 = jsonKey + "3";

        assertEquals("OK", tairDoc.jsonset(jsonkey1, ".", JSON_STRING_EXAMPLE));
        assertEquals("OK", tairDoc.jsonset(jsonkey2, ".", JSON_STRING_EXAMPLE));
        assertEquals("OK", tairDoc.jsonset(jsonkey3, ".", JSON_STRING_EXAMPLE));

        List<String> mgetRet = tairDoc.jsonmget(jsonkey1, jsonkey2, jsonkey3, ".baz");
        for (int i = 0; i < mgetRet.size(); i++) {
            Assert.assertEquals("42", mgetRet.get(i));
        }
    }

    @Test
    public void jsonMgetTestBinary() {
        String jsonkey1 = jsonKey + "1";
        String jsonkey2 = jsonKey + "2";
        String jsonkey3 = jsonKey + "3";

        assertEquals("OK", tairDoc.jsonset(jsonkey1.getBytes(), ".".getBytes(), JSON_STRING_EXAMPLE.getBytes()));
        assertEquals("OK", tairDoc.jsonset(jsonkey2.getBytes(), ".".getBytes(), JSON_STRING_EXAMPLE.getBytes()));
        assertEquals("OK", tairDoc.jsonset(jsonkey3.getBytes(), ".".getBytes(), JSON_STRING_EXAMPLE.getBytes()));

        List<byte[]> mgetRet = tairDoc.jsonmget(jsonkey1.getBytes(), jsonkey2.getBytes(), jsonkey3.getBytes(), ".baz".getBytes());
        for (int i = 0; i < mgetRet.size(); i++) {
            Assert.assertEquals("42", new String(mgetRet.get(i)));
        }
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

        byte[] bret = tairDoc.jsonget(jsonKey.getBytes(), ".".getBytes(), JsongetParams.JsongetParams().format("yaml"));
        assertEquals("\nfoo: bar\nbaz: 42\n", new String(bret));
    }

    @Test
    public void jsonDelTest() {
        String ret = tairDoc.jsonset(jsonKey, ".", JSON_STRING_EXAMPLE);
        assertEquals("OK", ret);

        long lret = tairDoc.jsondel(jsonKey.getBytes(), ".foo".getBytes());
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

        byte[] bret = tairDoc.jsontype(jsonKey.getBytes(), ".foo".getBytes());
        assertEquals("string", new String(bret));

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

        dret = tairDoc.jsonnumincrBy(jsonKey.getBytes(), ".baz".getBytes(), 1.5);
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

        long lret = tairDoc.jsonstrAppend(jsonKey, ".foo", "rrrr");
        assertEquals(7, lret);

        lret = tairDoc.jsonstrAppend(jsonKey.getBytes(), ".foo".getBytes(), "r".getBytes());
        assertEquals(8, lret);

        ret = tairDoc.jsonget(jsonKey, ".foo");
        assertEquals("\"barrrrrr\"", ret);

        try {
            tairDoc.jsonstrAppend(jsonKey.getBytes(), ".not-exists".getBytes());
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

        lret = tairDoc.jsonstrlen(jsonKey.getBytes(), ".foo".getBytes());
        assertEquals(8, lret);
    }

    @Test
    public void jsonArrappendTest() {
        String ret = tairDoc.jsonset(jsonKey, ".", JSON_ARRAY_EXAMPLE);
        assertEquals("OK", ret);

        long lret = tairDoc.jsonarrAppend(jsonKey.getBytes(), ".id".getBytes(),
            "null".getBytes(), "false".getBytes(), "true".getBytes());
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

        byte[] bret = tairDoc.jsonarrPop(jsonKey.getBytes(), ".id".getBytes(), -1);
        assertEquals("3", new String(bret));

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
            tairDoc.jsonarrPop(jsonKey.getBytes(), ".id".getBytes());
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

        lret = tairDoc.jsonarrInsert(jsonKey.getBytes(), ".id".getBytes(), "3".getBytes(), "4".getBytes());
        assertEquals(6, lret);

        ret = tairDoc.jsonget(jsonKey, ".id");
        assertEquals("[1,2,3,4,5,6]", ret);
    }

    @Test
    public void jsonArrlenTest() {
        String ret = tairDoc.jsonset(jsonKey, ".", JSON_ARRAY_EXAMPLE);
        assertEquals("OK", ret);

        long lret = tairDoc.jsonArrLen(jsonKey, ".id");
        assertEquals(3, lret);

        lret = tairDoc.jsonArrLen(jsonKey.getBytes(), ".id".getBytes());
        assertEquals(3, lret);
    }

    @Test
    public void jsonArrlenTest2() {
        String ret = tairDoc.jsonset(jsonKey, ".", "[1, 2, 3]");
        assertEquals("OK", ret);

        long lret = tairDoc.jsonArrLen(jsonKey);
        assertEquals(3, lret);

        lret = tairDoc.jsonArrLen(jsonKey.getBytes());
        assertEquals(3, lret);
    }

    @Test
    public void jsonArrtrimTest() {
        String ret = tairDoc.jsonset(jsonKey, ".", "{\"id\":[1,2,3,4,5,6]}");
        assertEquals("OK", ret);

        long lret = tairDoc.jsonarrTrim(jsonKey, ".id", 3, 4);
        assertEquals(2, lret);

        byte[] bret = tairDoc.jsonget(jsonKey.getBytes(), ".id".getBytes());
        assertEquals("[4,5]", new String(bret));


        lret = tairDoc.jsonarrTrim(jsonKey.getBytes(), ".id".getBytes(), 0, 0);
        assertEquals(1, lret);

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
        tairDoc.jsonArrLen(randomkey_);

        try {
            jedis.set(randomkey_, "bar");
            tairDoc.jsonArrLen(randomkey_);
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

    @Test
    public void jsonMergeCreatePathTest() {
        String ret = tairDoc.jsonset(jsonKey, ".", "{\"a\":2}");
        assertEquals("OK", ret);
        
        ret = tairDoc.jsonMerge(jsonKey, ".b", "8");
        assertEquals("OK", ret);
        
        ret = tairDoc.jsonget(jsonKey, ".");
        assertEquals("{\"a\":2,\"b\":8}", ret);
    }

    @Test
    public void jsonMergeReplaceValueTest() {
        String ret = tairDoc.jsonset(jsonKey, ".", "{\"a\":2}");
        assertEquals("OK", ret);
        
        ret = tairDoc.jsonMerge(jsonKey, ".a", "3");
        assertEquals("OK", ret);
        
        ret = tairDoc.jsonget(jsonKey, ".");
        assertEquals("{\"a\":3}", ret);
    }

    @Test
    public void jsonMergeDeleteValueTest() {
        String ret = tairDoc.jsonset(jsonKey, ".", "{\"a\":2}");
        assertEquals("OK", ret);
        
        ret = tairDoc.jsonMerge(jsonKey, ".", "{\"a\":null}");
        assertEquals("OK", ret);
        
        ret = tairDoc.jsonget(jsonKey, ".");
        assertEquals("{}", ret);
    }

    @Test
    public void jsonMergeReplaceArrayTest() {
        String ret = tairDoc.jsonset(jsonKey, ".", "{\"a\":[2,4,6,8]}");
        assertEquals("OK", ret);
        
        ret = tairDoc.jsonMerge(jsonKey, ".a", "[10,12]");
        assertEquals("OK", ret);
        
        ret = tairDoc.jsonget(jsonKey, ".");
        assertEquals("{\"a\":[10,12]}", ret);
    }

    @Test
    public void jsonMergeMultiPathsTest() {
        String ret = tairDoc.jsonset(jsonKey, ".", "{\"f1\":{\"a\":1},\"f2\":{\"a\":2}}");
        assertEquals("OK", ret);
        
        ret = tairDoc.jsonMerge(jsonKey, ".", "{\"f1\":null,\"f2\":{\"a\":3,\"b\":4},\"f3\":[2,4,6]}");
        assertEquals("OK", ret);
        
        ret = tairDoc.jsonget(jsonKey, ".");
        assertEquals("{\"f2\":{\"a\":3,\"b\":4},\"f3\":[2,4,6]}", ret);
    }

    @Test
    public void jsonMergeCreatePathTestBinary() {
        String ret = tairDoc.jsonset(jsonKey.getBytes(), ".".getBytes(), "{\"a\":2}".getBytes());
        assertEquals("OK", ret);
        
        ret = tairDoc.jsonMerge(jsonKey.getBytes(), ".b".getBytes(), "8".getBytes());
        assertEquals("OK", ret);
        
        byte[] bret = tairDoc.jsonget(jsonKey.getBytes(), ".".getBytes());
        assertEquals("{\"a\":2,\"b\":8}", new String(bret));
    }

    @Test
    public void jsonMergeReplaceValueTestBinary() {
        String ret = tairDoc.jsonset(jsonKey.getBytes(), ".".getBytes(), "{\"a\":2}".getBytes());
        assertEquals("OK", ret);
        
        ret = tairDoc.jsonMerge(jsonKey.getBytes(), ".a".getBytes(), "3".getBytes());
        assertEquals("OK", ret);
        
        byte[] bret = tairDoc.jsonget(jsonKey.getBytes(), ".".getBytes());
        assertEquals("{\"a\":3}", new String(bret));
    }

    @Test
    public void jsonMergeDeleteValueTestBinary() {
        String ret = tairDoc.jsonset(jsonKey.getBytes(), ".".getBytes(), "{\"a\":2}".getBytes());
        assertEquals("OK", ret);
        
        ret = tairDoc.jsonMerge(jsonKey.getBytes(), ".".getBytes(), "{\"a\":null}".getBytes());
        assertEquals("OK", ret);
        
        byte[] bret = tairDoc.jsonget(jsonKey.getBytes(), ".".getBytes());
        assertEquals("{}", new String(bret));
    }

    @Test
    public void jsonMergeReplaceArrayTestBinary() {
        String ret = tairDoc.jsonset(jsonKey.getBytes(), ".".getBytes(), "{\"a\":[2,4,6,8]}".getBytes());
        assertEquals("OK", ret);
        
        ret = tairDoc.jsonMerge(jsonKey.getBytes(), ".a".getBytes(), "[10,12]".getBytes());
        assertEquals("OK", ret);
        
        byte[] bret = tairDoc.jsonget(jsonKey.getBytes(), ".".getBytes());
        assertEquals("{\"a\":[10,12]}", new String(bret));
    }

    @Test
    public void jsonMergeMultiPathsTestBinary() {
        String ret = tairDoc.jsonset(jsonKey.getBytes(), ".".getBytes(), "{\"f1\":{\"a\":1},\"f2\":{\"a\":2}}".getBytes());
        assertEquals("OK", ret);
        
        ret = tairDoc.jsonMerge(jsonKey.getBytes(), ".".getBytes(), "{\"f1\":null,\"f2\":{\"a\":3,\"b\":4},\"f3\":[2,4,6]}".getBytes());
        assertEquals("OK", ret);
        
        byte[] bret = tairDoc.jsonget(jsonKey.getBytes(), ".".getBytes());
        assertEquals("{\"f2\":{\"a\":3,\"b\":4},\"f3\":[2,4,6]}", new String(bret));
    }
}
