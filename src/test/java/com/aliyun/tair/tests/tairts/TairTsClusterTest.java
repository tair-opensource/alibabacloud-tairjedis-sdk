package com.aliyun.tair.tests.tairts;

import com.aliyun.tair.tairts.params.ExtsAttributesParams;
import com.aliyun.tair.tairts.params.ExtsStringAggregationParams;
import com.aliyun.tair.tairts.results.ExtsStringDataPointResult;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static io.valkey.Protocol.toByteArray;

public class TairTsClusterTest extends TairTsTestBase {
    private String randomSkey;
    private String randomSkey2;
    private byte[] bSkey;
    private byte[] bSkey2;
    private String randomPkey;
    private byte[] randomPKeyBinary;
    private long startTs;
    private long endTs;
    private String value;

    public TairTsClusterTest() {
        randomPkey = "randomPkey_" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        randomPKeyBinary = ("randomPkey_" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        randomSkey = "key" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        randomSkey2 = "key2" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        bSkey = ("bkey" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        bSkey2 = ("bkey2" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        startTs = (System.currentTimeMillis() - 100000) / 1000 * 1000;
        endTs = System.currentTimeMillis() / 1000 * 1000;
        value = "This is a #TsString * value*";
    }

    @Test
    public void extsaddTest() throws Exception {

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
