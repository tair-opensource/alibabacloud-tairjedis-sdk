package com.aliyun.tair.tests.tairdoc;

import java.util.List;
import java.util.UUID;

import com.aliyun.tair.tairdoc.params.JsongetParams;
import com.aliyun.tair.tairdoc.params.JsonsetParams;
import org.junit.Test;
import io.valkey.Response;

import static org.junit.Assert.assertEquals;

public class TairDocPipelineTest extends TairDocTestBase {
    private String jsonKey;
    private static final String JSON_STRING_EXAMPLE = "{\"foo\":\"bar\",\"baz\":42, \"id\":[1,2,3]}";

    public TairDocPipelineTest() {
        jsonKey = "jsonkey" + "-" + Thread.currentThread().getName() + "-" + UUID.randomUUID().toString();
    }

    @Test
    public void jsonSetTest() {
        Response<String> r1 = tairDocPipeline.jsonset(jsonKey, ".", JSON_STRING_EXAMPLE);
        Response<String> r2 = tairDocPipeline.jsonget(jsonKey, ".foo");
        Response<String> r3 = tairDocPipeline.jsontype(jsonKey, ".foo");
        Response<Long> r4 = tairDocPipeline.jsonstrAppend(jsonKey, ".foo", "r");
        Response<Long> r5 = tairDocPipeline.jsonstrlen(jsonKey, ".foo");
        Response<Double> r6 = tairDocPipeline.jsonnumincrBy(jsonKey, ".baz", 10.0);
        Response<Long> r7 = tairDocPipeline.jsonarrAppend(jsonKey, ".id", "4", "5");
        Response<String> r8 = tairDocPipeline.jsonarrPop(jsonKey, ".id");
        Response<Long> r9 = tairDocPipeline.jsonarrInsert(jsonKey, ".id", "3", "5", "6");
        Response<Long> r10 = tairDocPipeline.jsonarrTrim(jsonKey, ".id", 4, 5);
        Response<Long> r11 = tairDocPipeline.jsonArrLen(jsonKey, ".id");
        Response<String> r12 = tairDocPipeline.jsonarrPop(jsonKey, ".id", 0);

        tairDocPipeline.sync();

        assertEquals("OK", r1.get());
        assertEquals("\"bar\"", r2.get());
        assertEquals("string", r3.get());
        assertEquals(4, (long)r4.get());
        assertEquals(4, (long)r5.get());
        assertEquals(52.0, r6.get(), 0.0001);
        assertEquals(5, (long)r7.get());
        assertEquals("5", r8.get());
        assertEquals(6, (long)r9.get());
        assertEquals(2, (long)r10.get());
        assertEquals(2, (long)r11.get());
        assertEquals("6", r12.get());
    }

    @Test
    public void jsonSetTestBinary() {
        Response<String> r1 = tairDocPipeline.jsonset(jsonKey.getBytes(), ".".getBytes(),
            JSON_STRING_EXAMPLE.getBytes());
        Response<byte[]> r2 = tairDocPipeline.jsonget(jsonKey.getBytes(), ".foo".getBytes());
        Response<byte[]> r3 = tairDocPipeline.jsontype(jsonKey.getBytes(), ".foo".getBytes());
        Response<Long> r4 = tairDocPipeline.jsonstrAppend(jsonKey.getBytes(), ".foo".getBytes(), "r".getBytes());
        Response<Long> r5 = tairDocPipeline.jsonstrlen(jsonKey.getBytes(), ".foo".getBytes());
        Response<Double> r6 = tairDocPipeline.jsonnumincrBy(jsonKey.getBytes(), ".baz".getBytes(), 10.0);
        Response<Long> r7 = tairDocPipeline.jsonarrAppend(jsonKey.getBytes(), ".id".getBytes(), "4".getBytes(),
            "5".getBytes());
        Response<byte[]> r8 = tairDocPipeline.jsonarrPop(jsonKey.getBytes(), ".id".getBytes());
        Response<Long> r9 = tairDocPipeline.jsonarrInsert(jsonKey.getBytes(), ".id".getBytes(), "3".getBytes(),
            "5".getBytes(), "6".getBytes());
        Response<Long> r10 = tairDocPipeline.jsonarrTrim(jsonKey.getBytes(), ".id".getBytes(), 4, 5);
        Response<Long> r11 = tairDocPipeline.jsonArrLen(jsonKey.getBytes(), ".id".getBytes());
        Response<byte[]> r12 = tairDocPipeline.jsonarrPop(jsonKey.getBytes(), ".id".getBytes(), 0);

        tairDocPipeline.sync();

        assertEquals("OK", r1.get());
        assertEquals("\"bar\"", new String(r2.get()));
        assertEquals("string", new String(r3.get()));
        assertEquals(4, (long)r4.get());
        assertEquals(4, (long)r5.get());
        assertEquals(52.0, r6.get(), 0.0001);
        assertEquals(5, (long)r7.get());
        assertEquals("5", new String(r8.get()));
        assertEquals(6, (long)r9.get());
        assertEquals(2, (long)r10.get());
        assertEquals(2, (long)r11.get());
        assertEquals("6", new String(r12.get()));
    }

    @Test
    public void pipelineTest2() {
        Response<String> r1 = tairDocPipeline.jsonset(jsonKey, ".", JSON_STRING_EXAMPLE, new JsonsetParams().nx());
        Response<String> r2 = tairDocPipeline.jsonget(jsonKey);
        Response<String> r3 = tairDocPipeline.jsonget(jsonKey, ".id", new JsongetParams().format("yaml"));
        Response<Long> r4 = tairDocPipeline.jsondel(jsonKey, ".id");
        Response<Long> r5 = tairDocPipeline.jsondel(jsonKey);
        Response<String> r6 = tairDocPipeline.jsonset(jsonKey, ".", "42", new JsonsetParams().nx());
        Response<String> r7 = tairDocPipeline.jsontype(jsonKey);
        Response<Double> r8 = tairDocPipeline.jsonnumincrBy(jsonKey, 10.0);
        Response<String> r9 = tairDocPipeline.jsonset(jsonKey, ".", "\"bar\"");
        Response<Long> r10 = tairDocPipeline.jsonstrAppend(jsonKey, "r");
        Response<Long> r11 = tairDocPipeline.jsonstrlen(jsonKey);
        Response<String> r12 = tairDocPipeline.jsonset(jsonKey, ".", "[1, 2, 3]");
        Response<Long> r13 = tairDocPipeline.jsonArrLen(jsonKey);
        Response<List<String>> r14 = tairDocPipeline.jsonmget(jsonKey, ".");

        tairDocPipeline.sync();

        assertEquals("OK", r1.get());
        assertEquals("{\"foo\":\"bar\",\"baz\":42,\"id\":[1,2,3]}", r2.get());
        assertEquals("\n- 1\n- 2\n- 3", r3.get());
        assertEquals(1, (long)r4.get());
        assertEquals(1, (long)r5.get());
        assertEquals("OK", r6.get());
        assertEquals("number", r7.get());
        assertEquals(52.0, r8.get(), 0.0001);
        assertEquals("OK", r9.get());
        assertEquals(4, (long)r10.get());
        assertEquals(4, (long)r11.get());
        assertEquals("OK", r12.get());
        assertEquals(3, (long)r13.get());
        assertEquals(1, (long)r14.get().size());
    }

    @Test
    public void pipelineTest2Binary() {
        Response<String> r1 = tairDocPipeline.jsonset(jsonKey.getBytes(), ".".getBytes(),
            JSON_STRING_EXAMPLE.getBytes(), new JsonsetParams().nx());
        Response<byte[]> r2 = tairDocPipeline.jsonget(jsonKey.getBytes());
        Response<byte[]> r3 = tairDocPipeline.jsonget(jsonKey.getBytes(), ".id".getBytes(), new JsongetParams().format("yaml"));
        Response<Long> r4 = tairDocPipeline.jsondel(jsonKey.getBytes(), ".id".getBytes());
        Response<Long> r5 = tairDocPipeline.jsondel(jsonKey.getBytes());
        Response<String> r6 = tairDocPipeline.jsonset(jsonKey.getBytes(), ".".getBytes(), "42".getBytes(), new JsonsetParams().nx());
        Response<byte[]> r7 = tairDocPipeline.jsontype(jsonKey.getBytes());
        Response<Double> r8 = tairDocPipeline.jsonnumincrBy(jsonKey.getBytes(), 10.0);
        Response<String> r9 = tairDocPipeline.jsonset(jsonKey.getBytes(), ".".getBytes(), "\"bar\"".getBytes());
        Response<Long> r10 = tairDocPipeline.jsonstrAppend(jsonKey.getBytes(), "r".getBytes());
        Response<Long> r11 = tairDocPipeline.jsonstrlen(jsonKey.getBytes());
        Response<String> r12 = tairDocPipeline.jsonset(jsonKey.getBytes(), ".".getBytes(), "[1, 2, 3]".getBytes());
        Response<Long> r13 = tairDocPipeline.jsonArrLen(jsonKey.getBytes());
        Response<List<byte[]>> r14 = tairDocPipeline.jsonmget(jsonKey.getBytes(), ".".getBytes());

        tairDocPipeline.sync();

        assertEquals("OK", r1.get());
        assertEquals("{\"foo\":\"bar\",\"baz\":42,\"id\":[1,2,3]}", new String(r2.get()));
        assertEquals("\n- 1\n- 2\n- 3", new String(r3.get()));
        assertEquals(1, (long)r4.get());
        assertEquals(1, (long)r5.get());
        assertEquals("OK", r6.get());
        assertEquals("number", new String(r7.get()));
        assertEquals(52.0, r8.get(), 0.0001);
        assertEquals("OK", r9.get());
        assertEquals(4, (long)r10.get());
        assertEquals(4, (long)r11.get());
        assertEquals(1, (long)r14.get().size());
    }
}
