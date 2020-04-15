package com.aliyun.tair.tests.tairdoc;

import java.util.List;
import java.util.UUID;

import com.aliyun.tair.tairdoc.params.JsonsetParams;
import org.junit.Test;
import redis.clients.jedis.exceptions.JedisDataException;

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TairDocPipelineTest extends TairDocTestBase {
    private String jsonKey;
    private static final String JSON_STRING_EXAMPLE = "{\"foo\":\"bar\",\"baz\":42}";
    private static final String JSON_ARRAY_EXAMPLE = "{\"id\":[1,2,3]}";

    public TairDocPipelineTest() {
        jsonKey = "jsonkey" + "-" + Thread.currentThread().getName() + "-" + UUID.randomUUID().toString();
    }

    @Test
    public void jsonSetTest() {
        tairDocPipeline.jsonset(jsonKey, ".", JSON_STRING_EXAMPLE);
        tairDocPipeline.jsonget(jsonKey, ".");
        tairDocPipeline.jsonget(jsonKey, ".foo");
        tairDocPipeline.jsonget(jsonKey, ".baz");

        List<Object> objs = tairDocPipeline.syncAndReturnAll();

        assertEquals("OK", objs.get(0));
        assertEquals(JSON_STRING_EXAMPLE, objs.get(1));
        assertEquals("\"bar\"", objs.get(2));
        assertEquals("42", objs.get(3));
    }

    @Test
    public void jsonSetWithNXXX() {
        tairDocPipeline.jsonset(jsonKey, ".", JSON_STRING_EXAMPLE, JsonsetParams.JsonsetParams().xx());
        tairDocPipeline.jsonset(jsonKey, ".", JSON_STRING_EXAMPLE, JsonsetParams.JsonsetParams().nx());
        tairDocPipeline.jsonset(jsonKey, ".", JSON_STRING_EXAMPLE, JsonsetParams.JsonsetParams().xx());

        List<Object> objs = tairDocPipeline.syncAndReturnAll();
        assertNull(objs.get(0));
        assertEquals("OK", objs.get(1));
        assertEquals("OK", objs.get(2));
    }

    @Test
    public void jsonGetTest() {
        tairDocPipeline.jsonset(jsonKey, ".", JSON_STRING_EXAMPLE);
        tairDocPipeline.jsonget(jsonKey, ".");
        tairDocPipeline.jsonget(jsonKey, ".foo");
        tairDocPipeline.jsonget(jsonKey, ".not-exists");
        tairDocPipeline.jsonget(jsonKey, ".baz");

        List<Object> objs = tairDocPipeline.syncAndReturnAll();

        assertEquals("OK", objs.get(0));
        assertEquals(JSON_STRING_EXAMPLE, objs.get(1));
        assertEquals("\"bar\"", objs.get(2));
        assertTrue(((JedisDataException)objs.get(3)).getMessage().contains("ERR pointer illegal"));
        assertEquals("42", objs.get(4));
    }
}
