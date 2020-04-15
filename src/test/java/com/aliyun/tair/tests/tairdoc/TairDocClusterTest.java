package com.aliyun.tair.tests.tairdoc;

import java.util.UUID;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

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
}
