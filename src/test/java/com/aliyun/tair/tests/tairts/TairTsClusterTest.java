package com.aliyun.tair.tests.tairts;

import com.aliyun.tair.tairts.params.ExtsAttributesParams;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.UUID;

import static redis.clients.jedis.Protocol.toByteArray;

public class TairTsClusterTest extends TairTsTestBase {
    private String randomSkey;
    private String randomSkey2;
    private byte[] bSkey;
    private byte[] bSkey2;
    private String randomPkey;
    private byte[] randomPKeyBinary;

    public TairTsClusterTest() {
        randomPkey = "randomPkey_" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        randomPKeyBinary = ("randomPkey_" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        randomSkey = "key" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        randomSkey2 = "key2" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        bSkey = ("bkey" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        bSkey2 = ("bkey2" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
    }

    @Test
    public void extsaddTest() throws Exception {
        long startTs = 1588812501110L;
        long endTs = 1589812501110L;

        for (int i = 0; i < 1; i++) {
            double val = i;
            long ts = startTs + i*1;
            String tsStr = String.valueOf(ts);
            ExtsAttributesParams params = new ExtsAttributesParams();
            params.dataEt(1000000000);
            params.chunkSize(1024);
            params.uncompressed();
            ArrayList<String> labels = new ArrayList<String>();
            labels.add("label1");
            labels.add("1");
            labels.add("label2");
            labels.add("2");
            params.labels(labels);

            String addRet = tairTsCluster.extsadd(randomPkey, randomSkey, tsStr, val);
            Assert.assertEquals("OK", addRet);
            ts = ts + 1;
            tsStr = String.valueOf(ts);
            addRet = tairTsCluster.extsadd(randomPkey, randomSkey, tsStr, val, params);
            Assert.assertEquals("OK", addRet);
        }

        for (int i = 0; i < 1; i++) {
            long val = i;
            long ts = startTs + i*1;
            byte[] tsStr = toByteArray(ts);
            ExtsAttributesParams params = new ExtsAttributesParams();
            params.dataEt(1000000000);
            params.chunkSize(1024);
            params.uncompressed();
            ArrayList<String> labels = new ArrayList<String>();
            labels.add("label1");
            labels.add("1");
            labels.add("label2");
            labels.add("2");
            params.labels(labels);

            String addRet = tairTsCluster.extsadd(randomPKeyBinary, bSkey, tsStr, val);
            Assert.assertEquals("OK", addRet);
            ts = ts + 1;
            tsStr = toByteArray(ts);
            addRet = tairTsCluster.extsadd(randomPKeyBinary, bSkey, tsStr, val, params);
            Assert.assertEquals("OK", addRet);
        }
    }
}
