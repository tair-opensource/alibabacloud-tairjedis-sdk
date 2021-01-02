package com.aliyun.tair.tests.taircpc;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class TairCpcPipelineTest extends TairCpcTestBase {
    private String key;
    private String key2;
    private String item;
    private String item2;
    private String item3;
    private String item4;
    private byte[] bkey;
    private byte[] bitem;
    private byte[] bitem2;
    private byte[] bitem3;
    private String randomkey_;
    private byte[] randomKeyBinary_;

    public TairCpcPipelineTest() {
        randomkey_ = "randomkey_" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        randomKeyBinary_ = ("randomkey_" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        key = "key" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        key2 = "key2" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        item = "item" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        item2 = "item2" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        item3 = "item3" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        item4 = "item4" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        bkey = ("bkey" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        bitem = ("bitem" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        bitem2 = ("bitem2" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        bitem3 = ("bitem3" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
    }


    @Test
    public void cpcupdateTest() throws Exception {

        int i = 0;
        tairCpcPipeline.cpcUpdate(key, item);

        tairCpcPipeline.cpcUpdate(bkey, bitem);

        List<Object> objs = tairCpcPipeline.syncAndReturnAll();
        i = 0;
        Assert.assertEquals("OK", objs.get(i++));
        Assert.assertEquals("OK", objs.get(i++));
    }
}
