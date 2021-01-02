package com.aliyun.tair.tests.tairts;

import com.aliyun.tair.tairts.params.ExtsAttributesParams;
import com.aliyun.tair.tairts.params.ExtsStringAggregationParams;
import com.aliyun.tair.tairts.results.ExtsStringDataPointResult;
import com.aliyun.tair.tairts.results.ExtsStringSkeyResult;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static redis.clients.jedis.Protocol.toByteArray;

public class TairTsStringClusterTest extends TairTsTestBase {
    private String randomSkey;
    private String randomSkey2;
    private byte[] bSkey;
    private byte[] bSkey2;
    private String randomPkey;
    private byte[] randomPKeyBinary;
    private long startTs;
    private long endTs;
    private String value;

    public TairTsStringClusterTest() {
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
    public void extsStringAddTest() throws Exception {

        for (int i = 0; i < 1; i++) {
            String val = value + String.valueOf(i);
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

            String addRet = tairTsCluster.extsaddstr(randomPkey, randomSkey, tsStr, val);
            Assert.assertEquals("OK", addRet);
            ts = ts + 1;
            tsStr = String.valueOf(ts);
            addRet = tairTsCluster.extsaddstr(randomPkey, randomSkey, tsStr, val, params);
            Assert.assertEquals("OK", addRet);
        }

        for (int i = 0; i < 1; i++) {
            String valstr = value + String.valueOf(i);
            byte[] val = valstr.getBytes();
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

            String addRet = tairTsCluster.extsaddstr(randomPKeyBinary, bSkey, tsStr, val);
            Assert.assertEquals("OK", addRet);
            ts = ts + 1;
            tsStr = toByteArray(ts);
            addRet = tairTsCluster.extsaddstr(randomPKeyBinary, bSkey, tsStr, val, params);
            Assert.assertEquals("OK", addRet);
        }
    }

    @Test
    public void extsStringRangeTest() throws Exception {

        long num = 3;
        String startTsStr = String.valueOf(startTs);
        String endTsStr = String.valueOf(endTs);

        for (int i = 0; i < num; i++) {
            String val = value + String.valueOf(i);
            long ts = startTs + i*1000;
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

            String addRet = tairTsCluster.extsaddstr(randomPkey, randomSkey, tsStr, val, params);
            Assert.assertEquals("OK", addRet);
        }

        ExtsStringAggregationParams paramsAgg = new ExtsStringAggregationParams();
        paramsAgg.maxCountSize(10);

        ExtsStringSkeyResult rangeByteRet = tairTsCluster.extsrangestr(randomPkey, randomSkey, startTsStr, endTsStr, paramsAgg);
        List<ExtsStringDataPointResult> dataPointRet = rangeByteRet.getDataPoints();
        assertEquals(num, dataPointRet.size());
        for (int i = 0; i < num; i++) {
            String val = value + String.valueOf(i);
            long ts = startTs + i*1000;
            assertEquals(ts, dataPointRet.get(i).getTs());
            assertEquals(true, val.equals(dataPointRet.get(i).getValue()));
        }

        for (int i = 0; i < num; i++) {
            String valstr = value + String.valueOf(i);
            byte[] val = valstr.getBytes();
            long ts = startTs + i*1000;
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

            String addRet = tairTsCluster.extsaddstr(randomPKeyBinary, bSkey, tsStr.getBytes(), val, params);
            Assert.assertEquals("OK", addRet);
        }

        paramsAgg = new ExtsStringAggregationParams();
        paramsAgg.maxCountSize(10);

        rangeByteRet = tairTsCluster.extsrangestr(randomPKeyBinary, bSkey, startTsStr.getBytes(), endTsStr.getBytes(), paramsAgg);
        dataPointRet = rangeByteRet.getDataPoints();
        assertEquals(num, dataPointRet.size());
        for (int i = 0; i < num; i++) {
            String val = value + String.valueOf(i);
            long ts = startTs + i*1000;
            assertEquals(ts, dataPointRet.get(i).getTs());
            assertEquals(true, val.equals(dataPointRet.get(i).getValue()));
        }
    }
}
