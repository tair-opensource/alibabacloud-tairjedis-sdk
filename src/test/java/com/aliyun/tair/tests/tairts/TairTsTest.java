package com.aliyun.tair.tests.tairts;

import com.aliyun.tair.tairts.params.ExtsAggregationParams;
import com.aliyun.tair.tairts.params.ExtsAttributesParams;
import com.aliyun.tair.tairts.params.ExtsDataPoint;
import com.aliyun.tair.tairts.params.ExtsFilter;
import com.aliyun.tair.tairts.results.ExtsDataPointResult;
import com.aliyun.tair.tairts.results.ExtsLabelResult;
import com.aliyun.tair.tairts.results.ExtsSkeyResult;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static redis.clients.jedis.Protocol.toByteArray;

public class TairTsTest extends TairTsTestBase {
    private String randomSkey;
    private String randomSkey2;
    private byte[] bSkey;
    private byte[] bSkey2;
    private String randomPkey;
    private byte[] randomPKeyBinary;
    private long startTs;
    private long endTs;

    public TairTsTest() {
        randomPkey = "randomPkey_" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        randomPKeyBinary = ("randomPkey_" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        randomSkey = "key" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        randomSkey2 = "key2" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        bSkey = ("bkey" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        bSkey2 = ("bkey2" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        startTs = (System.currentTimeMillis() - 100000) / 1000 * 1000;
        endTs = System.currentTimeMillis() / 1000 * 1000;
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

            String addRet = tairTs.extsadd(randomPkey, randomSkey, tsStr, val);
            Assert.assertEquals("OK", addRet);
            ts = ts + 1;
            tsStr = String.valueOf(ts);
            addRet = tairTs.extsadd(randomPkey, randomSkey, tsStr, val, params);
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

            String addRet = tairTs.extsadd(randomPKeyBinary, bSkey, tsStr, val);
            Assert.assertEquals("OK", addRet);
            ts = ts + 1;
            tsStr = toByteArray(ts);
            addRet = tairTs.extsadd(randomPKeyBinary, bSkey, tsStr, val, params);
            Assert.assertEquals("OK", addRet);
        }
    }

    @Test
    public void extsRawModifyTest() throws Exception {

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

            String addRet = tairTs.extsrawmodify(randomPkey, randomSkey, tsStr, val);
            Assert.assertEquals("OK", addRet);
            addRet = tairTs.extsrawmodify(randomPkey, randomSkey, tsStr, val);
            Assert.assertEquals("OK", addRet);
            ExtsDataPointResult getRet = tairTs.extsget(randomPkey, randomSkey);
            assertEquals((long)ts, getRet.getTs());
            assertEquals(i, getRet.getDoubleValue(), 0.0);
            ts = ts + 1;
            tsStr = String.valueOf(ts);
            addRet = tairTs.extsrawmodify(randomPkey, randomSkey, tsStr, val, params);
            Assert.assertEquals("OK", addRet);
            addRet = tairTs.extsrawmodify(randomPkey, randomSkey, tsStr, val, params);
            Assert.assertEquals("OK", addRet);
            getRet = tairTs.extsget(randomPkey, randomSkey);
            assertEquals((long)ts, getRet.getTs());
            assertEquals(i, getRet.getDoubleValue(), 0.0);
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

            String addRet = tairTs.extsrawmodify(randomPKeyBinary, bSkey, tsStr, val);
            Assert.assertEquals("OK", addRet);
            addRet = tairTs.extsrawmodify(randomPKeyBinary, bSkey, tsStr, val);
            Assert.assertEquals("OK", addRet);
            ExtsDataPointResult getRet = tairTs.extsget(randomPKeyBinary, bSkey);
            assertEquals((long)ts, getRet.getTs());
            assertEquals(i, getRet.getDoubleValue(), 0.0);
            ts = ts + 1;
            tsStr = toByteArray(ts);
            addRet = tairTs.extsrawmodify(randomPKeyBinary, bSkey, tsStr, val, params);
            Assert.assertEquals("OK", addRet);
            tairTs.extsrawmodify(randomPKeyBinary, bSkey, tsStr, val, params);
            Assert.assertEquals("OK", addRet);
            getRet = tairTs.extsget(randomPKeyBinary, bSkey);
            assertEquals((long)ts, getRet.getTs());
            assertEquals(i, getRet.getDoubleValue(), 0.0);
        }
    }

//    @Test
//    public void extsEvictTest() throws Exception {
//
//        long num = 100;
//        String startTsStr = String.valueOf(0);
//        String endTsStr = String.valueOf(endTs);
//
//        for (int i = 0; i < num; i++) {
//            double val = i;
//            long ts = startTs + i*1000;
//            String tsStr = String.valueOf(ts);
//            ExtsAttributesParams params = new ExtsAttributesParams();
//            params.dataEt(10000);
//            params.chunkSize(10);
//            params.uncompressed();
//            ArrayList<String> labels = new ArrayList<String>();
//            labels.add("label1");
//            labels.add("1");
//            labels.add("label2");
//            labels.add("2");
//            params.labels(labels);
//
//            String addRet = tairTs.extsadd(randomPkey, randomSkey, tsStr, val, params);
//            Assert.assertEquals("OK", addRet);
//        }
//
//        ExtsAggregationParams paramsAgg = new ExtsAggregationParams();
//        paramsAgg.maxCountSize(1000);
//        paramsAgg.aggAvg(1000);
//
//        List<ExtsDataPointResult> rangeByteRet = tairTs.extsrange(randomPkey, randomSkey, startTsStr, endTsStr, paramsAgg);
////        System.out.print(rangeByteRet.get(0).getTs());
//        System.out.print(rangeByteRet.get(0).getDoubleValue());
//        assertEquals(num, rangeByteRet.size());
//        for (int i = 0; i < num; i++) {
//            double val = i;
//            long ts = startTs + i*1000;
//            assertEquals(ts, rangeByteRet.get(i).getTs());
//            assertEquals(val, rangeByteRet.get(i).getDoubleValue(), 0.0);
//        }
//    }

    @Test
    public void extsmaddTest() throws Exception {

        for (int i = 0; i < 1; i++) {
            long val = i;
            long ts = startTs + i*1;
            String tsStr = String.valueOf(ts);
            ExtsAttributesParams params = new ExtsAttributesParams();
            params.dataEt(1000000000);
            params.chunkSize(1024);
            params.uncompressed();
            ArrayList<String> labels = new ArrayList<String>();
            labels.add("label1");
            labels.add("1");
            params.labels(labels);

            ArrayList<ExtsDataPoint<String>> addList = new ArrayList<ExtsDataPoint<String>>();
            ExtsDataPoint<String> add1 = new ExtsDataPoint<String>(randomSkey, tsStr, val);
            ExtsDataPoint<String> add2 = new ExtsDataPoint<String>(randomSkey2, tsStr, val);
            addList.add(add1);
            addList.add(add2);
            List<String> maddRet = tairTs.extsmadd(randomPkey, addList);
            for (int j = 0; j < maddRet.size(); j++) {
                Assert.assertEquals("OK", maddRet.get(j));
            }

            String delRet = tairTs.extsdel(randomPkey, randomSkey);
            Assert.assertEquals("OK", delRet);
            delRet = tairTs.extsdel(randomPkey, randomSkey2);
            Assert.assertEquals("OK", delRet);

            maddRet = tairTs.extsmadd(randomPkey, addList, params);
            for (int j = 0; j < maddRet.size(); j++) {
                Assert.assertEquals("OK", maddRet.get(j));
            }

            delRet = tairTs.extsdel(randomPkey, randomSkey);
            Assert.assertEquals("OK", delRet);
            delRet = tairTs.extsdel(randomPkey, randomSkey2);
            Assert.assertEquals("OK", delRet);
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
            params.labels(labels);

            ArrayList<ExtsDataPoint<byte[]>> addList = new ArrayList<ExtsDataPoint<byte[]>>();
            ExtsDataPoint<byte[]> add1 = new ExtsDataPoint<byte[]>(bSkey, tsStr, val);
            ExtsDataPoint<byte[]> add2 = new ExtsDataPoint<byte[]>(bSkey2, tsStr, val);
            addList.add(add1);
            addList.add(add2);
            List<String> maddRet = tairTs.extsmadd(randomPKeyBinary, addList);
            for (int j = 0; j < maddRet.size(); j++) {
                Assert.assertEquals("OK", maddRet.get(j));
            }

            String delRet = tairTs.extsdel(randomPKeyBinary, bSkey);
            Assert.assertEquals("OK", delRet);
            delRet = tairTs.extsdel(randomPKeyBinary, bSkey2);
            Assert.assertEquals("OK", delRet);

            maddRet = tairTs.extsmadd(randomPKeyBinary, addList,params);
            for (int j = 0; j < maddRet.size(); j++) {
                Assert.assertEquals("OK", maddRet.get(j));
            }

            delRet = tairTs.extsdel(randomPKeyBinary, bSkey);
            Assert.assertEquals("OK", delRet);
            delRet = tairTs.extsdel(randomPKeyBinary, bSkey2);
            Assert.assertEquals("OK", delRet);
        }
    }

    @Test
    public void extsRawMModifyTest() throws Exception {

        for (int i = 0; i < 1; i++) {
            long val = i;
            long ts = startTs + i*1;
            String tsStr = String.valueOf(ts);
            ExtsAttributesParams params = new ExtsAttributesParams();
            params.dataEt(1000000000);
            params.chunkSize(1024);
            params.uncompressed();
            ArrayList<String> labels = new ArrayList<String>();
            labels.add("label1");
            labels.add("1");
            params.labels(labels);

            ArrayList<ExtsDataPoint<String>> addList = new ArrayList<ExtsDataPoint<String>>();
            ExtsDataPoint<String> add1 = new ExtsDataPoint<String>(randomSkey, tsStr, val);
            ExtsDataPoint<String> add2 = new ExtsDataPoint<String>(randomSkey2, tsStr, val);
            addList.add(add1);
            addList.add(add2);
            List<String> maddRet = tairTs.extsmrawmodify(randomPkey, addList);
            for (int j = 0; j < maddRet.size(); j++) {
                Assert.assertEquals("OK", maddRet.get(j));
            }

            ExtsDataPointResult getRet = tairTs.extsget(randomPkey, randomSkey);
            assertEquals((long)ts, getRet.getTs());
            assertEquals(i, getRet.getDoubleValue(), 0.0);

            getRet = tairTs.extsget(randomPkey, randomSkey2);
            assertEquals((long)ts, getRet.getTs());
            assertEquals(i, getRet.getDoubleValue(), 0.0);

            ts = ts + 1;
            tsStr = String.valueOf(ts);
            ArrayList<ExtsDataPoint<String>> addList2 = new ArrayList<ExtsDataPoint<String>>();
            add1 = new ExtsDataPoint<String>(randomSkey, tsStr, val);
            add2 = new ExtsDataPoint<String>(randomSkey2, tsStr, val);
            addList2.add(add1);
            addList2.add(add2);

            maddRet = tairTs.extsmrawmodify(randomPkey, addList2,params);
            for (int j = 0; j < maddRet.size(); j++) {
                Assert.assertEquals("OK", maddRet.get(j));
            }

            getRet = tairTs.extsget(randomPkey, randomSkey);
            assertEquals((long)ts, getRet.getTs());
            assertEquals(i, getRet.getDoubleValue(), 0.0);

            getRet = tairTs.extsget(randomPkey, randomSkey2);
            assertEquals((long)ts, getRet.getTs());
            assertEquals(i, getRet.getDoubleValue(), 0.0);

            String delRet = tairTs.extsdel(randomPkey, randomSkey);
            Assert.assertEquals("OK", delRet);
            delRet = tairTs.extsdel(randomPkey, randomSkey2);
            Assert.assertEquals("OK", delRet);
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
            params.labels(labels);

            ArrayList<ExtsDataPoint<byte[]>> addList = new ArrayList<ExtsDataPoint<byte[]>>();
            ExtsDataPoint<byte[]> add1 = new ExtsDataPoint<byte[]>(bSkey, tsStr, val);
            ExtsDataPoint<byte[]> add2 = new ExtsDataPoint<byte[]>(bSkey2, tsStr, val);
            addList.add(add1);
            addList.add(add2);
            List<String> maddRet = tairTs.extsmrawmodify(randomPKeyBinary, addList);
            for (int j = 0; j < maddRet.size(); j++) {
                Assert.assertEquals("OK", maddRet.get(j));
            }

            ExtsDataPointResult getRet = tairTs.extsget(randomPKeyBinary, bSkey);
            assertEquals((long)ts, getRet.getTs());
            assertEquals(i, getRet.getDoubleValue(), 0.0);

            getRet = tairTs.extsget(randomPKeyBinary, bSkey2);
            assertEquals((long)ts, getRet.getTs());
            assertEquals(i, getRet.getDoubleValue(), 0.0);

            ts = ts + 1;
            tsStr = toByteArray(ts);
            ArrayList<ExtsDataPoint<byte[]>> addList2 = new ArrayList<ExtsDataPoint<byte[]>>();
            add1 = new ExtsDataPoint<byte[]>(bSkey, tsStr, val);
            add2 = new ExtsDataPoint<byte[]>(bSkey2, tsStr, val);
            addList2.add(add1);
            addList2.add(add2);

            maddRet = tairTs.extsmrawmodify(randomPKeyBinary, addList2,params);
            for (int j = 0; j < maddRet.size(); j++) {
                Assert.assertEquals("OK", maddRet.get(j));
            }

            getRet = tairTs.extsget(randomPKeyBinary, bSkey);
            assertEquals((long)ts, getRet.getTs());
            assertEquals(i, getRet.getDoubleValue(), 0.0);

            getRet = tairTs.extsget(randomPKeyBinary, bSkey2);
            assertEquals((long)ts, getRet.getTs());
            assertEquals(i, getRet.getDoubleValue(), 0.0);

            String delRet = tairTs.extsdel(randomPKeyBinary, bSkey);
            Assert.assertEquals("OK", delRet);
            delRet = tairTs.extsdel(randomPKeyBinary, bSkey2);
            Assert.assertEquals("OK", delRet);
        }
    }

    @Test
    public void extsRawMIncrTest() throws Exception {

        for (int i = 0; i < 1; i++) {
            long val = i;
            long ts = startTs + i*1;
            String tsStr = String.valueOf(ts);
            ExtsAttributesParams params = new ExtsAttributesParams();
            params.dataEt(1000000000);
            params.chunkSize(1024);
            params.uncompressed();
            ArrayList<String> labels = new ArrayList<String>();
            labels.add("label1");
            labels.add("1");
            params.labels(labels);

            ArrayList<ExtsDataPoint<String>> addList = new ArrayList<ExtsDataPoint<String>>();
            ExtsDataPoint<String> add1 = new ExtsDataPoint<String>(randomSkey, tsStr, val);
            ExtsDataPoint<String> add2 = new ExtsDataPoint<String>(randomSkey2, tsStr, val);
            addList.add(add1);
            addList.add(add2);
            List<String> maddRet = tairTs.extsmrawincr(randomPkey, addList);
            for (int j = 0; j < maddRet.size(); j++) {
                Assert.assertEquals("OK", maddRet.get(j));
            }

            ExtsDataPointResult getRet = tairTs.extsget(randomPkey, randomSkey);
            assertEquals((long)ts, getRet.getTs());
            assertEquals(val, getRet.getDoubleValue(), 0.0);

            getRet = tairTs.extsget(randomPkey, randomSkey2);
            assertEquals((long)ts, getRet.getTs());
            assertEquals(val, getRet.getDoubleValue(), 0.0);

            ts = ts + 1;
            val = val + 1;
            tsStr = String.valueOf(ts);
            ArrayList<ExtsDataPoint<String>> addList2 = new ArrayList<ExtsDataPoint<String>>();
            add1 = new ExtsDataPoint<String>(randomSkey, tsStr, val);
            add2 = new ExtsDataPoint<String>(randomSkey2, tsStr, val);
            addList2.add(add1);
            addList2.add(add2);

            maddRet = tairTs.extsmrawincr(randomPkey, addList2,params);
            for (int j = 0; j < maddRet.size(); j++) {
                Assert.assertEquals("OK", maddRet.get(j));
            }

            getRet = tairTs.extsget(randomPkey, randomSkey);
            assertEquals((long)ts, getRet.getTs());
            assertEquals(val, getRet.getDoubleValue(), 0.0);

            getRet = tairTs.extsget(randomPkey, randomSkey2);
            assertEquals((long)ts, getRet.getTs());
            assertEquals(val, getRet.getDoubleValue(), 0.0);

            String delRet = tairTs.extsdel(randomPkey, randomSkey);
            Assert.assertEquals("OK", delRet);
            delRet = tairTs.extsdel(randomPkey, randomSkey2);
            Assert.assertEquals("OK", delRet);
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
            params.labels(labels);

            ArrayList<ExtsDataPoint<byte[]>> addList = new ArrayList<ExtsDataPoint<byte[]>>();
            ExtsDataPoint<byte[]> add1 = new ExtsDataPoint<byte[]>(bSkey, tsStr, val);
            ExtsDataPoint<byte[]> add2 = new ExtsDataPoint<byte[]>(bSkey2, tsStr, val);
            addList.add(add1);
            addList.add(add2);
            List<String> maddRet = tairTs.extsmrawincr(randomPKeyBinary, addList);
            for (int j = 0; j < maddRet.size(); j++) {
                Assert.assertEquals("OK", maddRet.get(j));
            }

            ExtsDataPointResult getRet = tairTs.extsget(randomPKeyBinary, bSkey);
            assertEquals((long)ts, getRet.getTs());
            assertEquals(val, getRet.getDoubleValue(), 0.0);

            getRet = tairTs.extsget(randomPKeyBinary, bSkey2);
            assertEquals((long)ts, getRet.getTs());
            assertEquals(val, getRet.getDoubleValue(), 0.0);

            ts = ts + 1;
            tsStr = toByteArray(ts);
            ArrayList<ExtsDataPoint<byte[]>> addList2 = new ArrayList<ExtsDataPoint<byte[]>>();
            add1 = new ExtsDataPoint<byte[]>(bSkey, tsStr, val);
            add2 = new ExtsDataPoint<byte[]>(bSkey2, tsStr, val);
            addList2.add(add1);
            addList2.add(add2);

            maddRet = tairTs.extsmrawincr(randomPKeyBinary, addList2,params);
            for (int j = 0; j < maddRet.size(); j++) {
                Assert.assertEquals("OK", maddRet.get(j));
            }

            getRet = tairTs.extsget(randomPKeyBinary, bSkey);
            assertEquals((long)ts, getRet.getTs());
            assertEquals(val, getRet.getDoubleValue(), 0.0);

            getRet = tairTs.extsget(randomPKeyBinary, bSkey2);
            assertEquals((long)ts, getRet.getTs());
            assertEquals(val, getRet.getDoubleValue(), 0.0);

            String delRet = tairTs.extsdel(randomPKeyBinary, bSkey);
            Assert.assertEquals("OK", delRet);
            delRet = tairTs.extsdel(randomPKeyBinary, bSkey2);
            Assert.assertEquals("OK", delRet);
        }
    }

    @Test
    public void extsgetTest() throws Exception {

        for (int i = 0; i < 1; i++) {
            long val = i;
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

            String addRet = tairTs.extsadd(randomPkey, randomSkey, tsStr, val, params);
            Assert.assertEquals("OK", addRet);

            ExtsDataPointResult getRet = tairTs.extsget(randomPkey, randomSkey);
            assertEquals((long)ts, getRet.getTs());
            assertEquals(i, getRet.getDoubleValue(), 0.0);
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

            String addRet = tairTs.extsadd(randomPKeyBinary, bSkey, tsStr, val);
            Assert.assertEquals("OK", addRet);

            ExtsDataPointResult getRet = tairTs.extsget(randomPKeyBinary, bSkey);
            assertEquals((long)ts, getRet.getTs());
            assertEquals(i, getRet.getDoubleValue(), 0.0);
        }
    }

    @Test
    public void extsqueryTest() throws Exception {

        for (int i = 0; i < 1; i++) {
            long val = i;
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

            ExtsAttributesParams params2 = new ExtsAttributesParams();
            params2.dataEt(1000000000);
            params2.chunkSize(1024);
            params2.uncompressed();
            ArrayList<String> labels2 = new ArrayList<String>();
            labels2.add("label1");
            labels2.add("1");
            labels2.add("label3");
            labels2.add("3");
            params2.labels(labels2);

            String addRet = tairTs.extsadd(randomPkey, randomSkey, tsStr, val, params);
            Assert.assertEquals("OK", addRet);
            addRet = tairTs.extsadd(randomPkey, randomSkey2, tsStr, val, params2);
            Assert.assertEquals("OK", addRet);

            ExtsFilter<String> filter1 = new ExtsFilter<String>("label1=1");
            ExtsFilter<String> filter2 = new ExtsFilter<String>("label2=2");
            ExtsFilter<String> filter3 = new ExtsFilter<String>("label3=3");
            ExtsFilter<String> filter4 = new ExtsFilter<String>("label2=3");
            ArrayList<ExtsFilter<String>> filterList = new ArrayList<ExtsFilter<String>>();
            filterList.add(filter1);
            filterList.add(filter2);

            List<String> queryRet = tairTs.extsquery(randomPkey, filterList);
            assertEquals(1, queryRet.size());
            assertEquals(randomSkey, queryRet.get(0));


            ArrayList<ExtsFilter<String>> filterList2 = new ArrayList<ExtsFilter<String>>();
            filterList2.add(filter1);
            filterList2.add(filter3);

            queryRet = tairTs.extsquery(randomPkey, filterList2);
            assertEquals(1, queryRet.size());
            assertEquals(randomSkey2, queryRet.get(0));

            ArrayList<ExtsFilter<String>> filterList3 = new ArrayList<ExtsFilter<String>>();
            filterList3.add(filter1);

            queryRet = tairTs.extsquery(randomPkey, filterList3);
            assertEquals(2, queryRet.size());

            ArrayList<ExtsFilter<String>> filterList4 = new ArrayList<ExtsFilter<String>>();
            filterList4.add(filter4);

            queryRet = tairTs.extsquery(randomPkey, filterList4);
            assertEquals(0, queryRet.size());
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

            ExtsAttributesParams params2 = new ExtsAttributesParams();
            params2.dataEt(1000000000);
            params2.chunkSize(1024);
            params2.uncompressed();
            ArrayList<String> labels2 = new ArrayList<String>();
            labels2.add("label1");
            labels2.add("1");
            labels2.add("label3");
            labels2.add("3");
            params2.labels(labels2);

            String addRet = tairTs.extsadd(randomPKeyBinary, bSkey, tsStr, val, params);
            Assert.assertEquals("OK", addRet);
            addRet = tairTs.extsadd(randomPKeyBinary, bSkey2, tsStr, val, params2);
            Assert.assertEquals("OK", addRet);

            ExtsFilter<byte[]> filter1 = new ExtsFilter<byte[]>("label1=1".getBytes());
            ExtsFilter<byte[]> filter2 = new ExtsFilter<byte[]>("label2=2".getBytes());
            ExtsFilter<byte[]> filter3 = new ExtsFilter<byte[]>("label3=3".getBytes());
            ExtsFilter<byte[]> filter4 = new ExtsFilter<byte[]>("label2=3".getBytes());
            ArrayList<ExtsFilter<byte[]>> filterList = new ArrayList<ExtsFilter<byte[]>>();
            filterList.add(filter1);
            filterList.add(filter2);

            List<byte[]> queryRet = tairTs.extsquery(randomPKeyBinary, filterList);
            assertEquals(1, queryRet.size());
            assertEquals(new String(bSkey), new String(queryRet.get(0)));


            ArrayList<ExtsFilter<byte[]>> filterList2 = new ArrayList<ExtsFilter<byte[]>>();
            filterList2.add(filter1);
            filterList2.add(filter3);

            queryRet = tairTs.extsquery(randomPKeyBinary, filterList2);
            assertEquals(1, queryRet.size());
            assertEquals(new String(bSkey2), new String(queryRet.get(0)));

            ArrayList<ExtsFilter<byte[]>> filterList3 = new ArrayList<ExtsFilter<byte[]>>();
            filterList3.add(filter1);

            queryRet = tairTs.extsquery(randomPKeyBinary, filterList3);
            assertEquals(2, queryRet.size());

            ArrayList<ExtsFilter<byte[]>> filterList4 = new ArrayList<ExtsFilter<byte[]>>();
            filterList4.add(filter4);

            queryRet = tairTs.extsquery(randomPKeyBinary, filterList4);
            assertEquals(0, queryRet.size());
        }
    }

    @Test
    public void extsrangeTest() throws Exception {
        long num = 3;
        String startTsStr = String.valueOf(startTs);
        String endTsStr = String.valueOf(endTs);

        for (int i = 0; i < num; i++) {
            double val = i;
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

            String addRet = tairTs.extsadd(randomPkey, randomSkey, tsStr, val, params);
            Assert.assertEquals("OK", addRet);
        }

        ExtsAggregationParams paramsAgg = new ExtsAggregationParams();
        paramsAgg.maxCountSize(10);
        paramsAgg.aggAvg(1000);

        ExtsSkeyResult rangeByteRet = tairTs.extsrange(randomPkey, randomSkey, startTsStr, endTsStr, paramsAgg);
        List<ExtsDataPointResult> dataPointRet = rangeByteRet.getDataPoints();
        assertEquals(num, dataPointRet.size());
        for (int i = 0; i < num; i++) {
            double val = i;
            long ts = startTs + i*1000;
            assertEquals(ts, dataPointRet.get(i).getTs());
            assertEquals(val, dataPointRet.get(i).getDoubleValue(), 0.0);
        }

        for (int i = 0; i < num; i++) {
            double val = i;
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

            String addRet = tairTs.extsadd(randomPKeyBinary, bSkey, tsStr.getBytes(), val, params);
            Assert.assertEquals("OK", addRet);
        }

        paramsAgg = new ExtsAggregationParams();
        paramsAgg.maxCountSize(10);
        paramsAgg.aggAvg(1000);

        rangeByteRet = tairTs.extsrange(randomPKeyBinary, bSkey, startTsStr.getBytes(), endTsStr.getBytes(), paramsAgg);
        dataPointRet = rangeByteRet.getDataPoints();
        assertEquals(num, dataPointRet.size());
        for (int i = 0; i < num; i++) {
            double val = i;
            long ts = startTs + i*1000;
            assertEquals(ts, dataPointRet.get(i).getTs());
            assertEquals(val, dataPointRet.get(i).getDoubleValue(), 0.0);
        }
    }

    @Test
    public void extsmrangeKeysTest() throws Exception {
        long num = 3;
        long labelNum = 0;
        String startTsStr = String.valueOf(startTs);
        String endTsStr = String.valueOf(endTs);

        for (int i = 0; i < num; i++) {
            double val = i;
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
            labelNum = labels.size()/2;

            String addRet = tairTs.extsadd(randomPkey, randomSkey, tsStr, val, params);
            Assert.assertEquals("OK", addRet);
            addRet = tairTs.extsadd(randomPkey, randomSkey2, tsStr, val, params);
            Assert.assertEquals("OK", addRet);
        }

        ExtsAggregationParams paramsAgg = new ExtsAggregationParams();
        paramsAgg.maxCountSize(10);
        paramsAgg.aggAvg(1000);

        ArrayList<String> keys = new ArrayList<String>();
        keys.add(randomSkey);
        keys.add(randomSkey2);

        List<ExtsSkeyResult> rangeByteRet = tairTs.extsmrange(randomPkey, keys, startTsStr, endTsStr);
        assertEquals(2, rangeByteRet.size());
        assertEquals(randomSkey, rangeByteRet.get(0).getSkey());
        List<ExtsLabelResult> labelRet = rangeByteRet.get(0).getLabels();
        assertEquals(0, labelRet.size());

        List<ExtsDataPointResult> dataPointRet = rangeByteRet.get(0).getDataPoints();
        for (int i = 0; i < num; i++) {
            double val = i;
            long ts = startTs + i*1000;
            assertEquals(ts, dataPointRet.get(i).getTs());
            assertEquals(val, dataPointRet.get(i).getDoubleValue(), 0.0);
        }
        assertEquals(randomSkey2, rangeByteRet.get(1).getSkey());
        labelRet = rangeByteRet.get(1).getLabels();
        assertEquals(0, labelRet.size());

        dataPointRet = rangeByteRet.get(1).getDataPoints();
        for (int i = 0; i < num; i++) {
            double val = i;
            long ts = startTs + i*1000;
            assertEquals(ts, dataPointRet.get(i).getTs());
            assertEquals(val, dataPointRet.get(i).getDoubleValue(), 0.0);
        }
    }

    @Test
    public void extsmrangeKeysByteTest() throws Exception {
        long num = 3;
        long labelNum = 0;
        String startTsStr = String.valueOf(startTs);
        String endTsStr = String.valueOf(endTs);

        for (int i = 0; i < num; i++) {
            double val = i;
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
            labelNum = labels.size()/2;

            String addRet = tairTs.extsadd(randomPKeyBinary, bSkey, tsStr.getBytes(), val, params);
            addRet = tairTs.extsadd(randomPKeyBinary, bSkey2, tsStr.getBytes(), val, params);
            Assert.assertEquals("OK", addRet);
        }

        ExtsAggregationParams paramsAgg = new ExtsAggregationParams();
        paramsAgg.maxCountSize(10);
        paramsAgg.aggAvg(1000);

        ArrayList<byte[]> keys = new ArrayList<byte[]>();
        keys.add(bSkey);
        keys.add(bSkey2);

        List<ExtsSkeyResult> rangeByteRet = tairTs.extsmrange(randomPKeyBinary, keys, startTsStr.getBytes(), endTsStr.getBytes());
        assertEquals(2, rangeByteRet.size());
        assertEquals(new String(bSkey), rangeByteRet.get(0).getSkey());
        List<ExtsLabelResult> labelRet = rangeByteRet.get(0).getLabels();
        assertEquals(0, labelRet.size());

        List<ExtsDataPointResult> dataPointRet = rangeByteRet.get(0).getDataPoints();
        for (int i = 0; i < num; i++) {
            double val = i;
            long ts = startTs + i*1000;
            assertEquals(ts, dataPointRet.get(i).getTs());
            assertEquals(val, dataPointRet.get(i).getDoubleValue(), 0.0);
        }
        assertEquals(new String(bSkey2), rangeByteRet.get(1).getSkey());
        labelRet = rangeByteRet.get(1).getLabels();
        assertEquals(0, labelRet.size());

        dataPointRet = rangeByteRet.get(1).getDataPoints();
        for (int i = 0; i < num; i++) {
            double val = i;
            long ts = startTs + i*1000;
            assertEquals(ts, dataPointRet.get(i).getTs());
            assertEquals(val, dataPointRet.get(i).getDoubleValue(), 0.0);
        }
    }

    @Test
    public void extsmrangeKeysLabelsTest() throws Exception {
        long num = 3;
        long labelNum = 0;
        String startTsStr = String.valueOf(startTs);
        String endTsStr = String.valueOf(endTs);

        for (int i = 0; i < num; i++) {
            double val = i;
            long ts = startTs + i * 1000;
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
            labelNum = labels.size() / 2;

            String addRet = tairTs.extsadd(randomPkey, randomSkey, tsStr, val, params);
            Assert.assertEquals("OK", addRet);
            addRet = tairTs.extsadd(randomPkey, randomSkey2, tsStr, val, params);
            Assert.assertEquals("OK", addRet);
        }

        ExtsAggregationParams paramsAgg = new ExtsAggregationParams();
        paramsAgg.maxCountSize(10);
        paramsAgg.aggAvg(1000);
        paramsAgg.withLabels();

        ArrayList<String> keys = new ArrayList<String>();
        keys.add(randomSkey);
        keys.add(randomSkey2);

        List<ExtsSkeyResult> rangeByteRet = tairTs.extsmrange(randomPkey, keys, startTsStr, endTsStr, paramsAgg);
        assertEquals(2, rangeByteRet.size());
        assertEquals(randomSkey, rangeByteRet.get(0).getSkey());
        List<ExtsLabelResult> labelRet = rangeByteRet.get(0).getLabels();

        assertEquals("label1", labelRet.get(0).getName());
        assertEquals("1", labelRet.get(0).getValue());
        assertEquals("label2", labelRet.get(1).getName());
        assertEquals("2", labelRet.get(1).getValue());

        List<ExtsDataPointResult> dataPointRet = rangeByteRet.get(0).getDataPoints();
        for (int i = 0; i < num; i++) {
            double val = i;
            long ts = startTs + i * 1000;
            assertEquals(ts, dataPointRet.get(i).getTs());
            assertEquals(val, dataPointRet.get(i).getDoubleValue(), 0.0);
        }

        assertEquals(randomSkey2, rangeByteRet.get(1).getSkey());
        labelRet = rangeByteRet.get(1).getLabels();

        assertEquals("label1", labelRet.get(0).getName());
        assertEquals("1", labelRet.get(0).getValue());
        assertEquals("label2", labelRet.get(1).getName());
        assertEquals("2", labelRet.get(1).getValue());

        dataPointRet = rangeByteRet.get(1).getDataPoints();
        for (int i = 0; i < num; i++) {
            double val = i;
            long ts = startTs + i * 1000;
            assertEquals(ts, dataPointRet.get(i).getTs());
            assertEquals(val, dataPointRet.get(i).getDoubleValue(), 0.0);
        }
    }

    @Test
    public void extsmrangeSkeysLabelsByteTest() throws Exception {
        long num = 3;
        long labelNum = 0;
        String startTsStr = String.valueOf(startTs);
        String endTsStr = String.valueOf(endTs);

        for (int i = 0; i < num; i++) {
            double val = i;
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
            labelNum = labels.size()/2;

            String addRet = tairTs.extsadd(randomPKeyBinary, bSkey, tsStr.getBytes(), val, params);
            Assert.assertEquals("OK", addRet);
            addRet = tairTs.extsadd(randomPKeyBinary, bSkey2, tsStr.getBytes(), val, params);
            Assert.assertEquals("OK", addRet);
        }

        ExtsAggregationParams paramsAgg = new ExtsAggregationParams();
        paramsAgg.maxCountSize(10);
        paramsAgg.aggAvg(1000);
        paramsAgg.withLabels();

        ArrayList<byte[]> keys = new ArrayList<byte[]>();
        keys.add(bSkey);
        keys.add(bSkey2);

        List<ExtsSkeyResult> rangeByteRet = tairTs.extsmrange(randomPKeyBinary, keys, startTsStr.getBytes(), endTsStr.getBytes(), paramsAgg);
        assertEquals(2, rangeByteRet.size());
        assertEquals(new String(bSkey), rangeByteRet.get(0).getSkey());
        List<ExtsLabelResult> labelRet = rangeByteRet.get(0).getLabels();

        assertEquals("label1", labelRet.get(0).getName());
        assertEquals("1", labelRet.get(0).getValue());
        assertEquals("label2", labelRet.get(1).getName());
        assertEquals("2", labelRet.get(1).getValue());

        List<ExtsDataPointResult> dataPointRet = rangeByteRet.get(0).getDataPoints();
        for (int i = 0; i < num; i++) {
            double val = i;
            long ts = startTs + i*1000;
            assertEquals(ts, dataPointRet.get(i).getTs());
            assertEquals(val, dataPointRet.get(i).getDoubleValue(), 0.0);
        }

        assertEquals(new String(bSkey2), rangeByteRet.get(1).getSkey());
        labelRet = rangeByteRet.get(1).getLabels();

        assertEquals("label1", labelRet.get(0).getName());
        assertEquals("1", labelRet.get(0).getValue());
        assertEquals("label2", labelRet.get(1).getName());
        assertEquals("2", labelRet.get(1).getValue());

        dataPointRet = rangeByteRet.get(1).getDataPoints();
        for (int i = 0; i < num; i++) {
            double val = i;
            long ts = startTs + i*1000;
            assertEquals(ts, dataPointRet.get(i).getTs());
            assertEquals(val, dataPointRet.get(i).getDoubleValue(), 0.0);
        }
    }

    @Test
    public void extsmrangeTest() throws Exception {
        long num = 3;
        long labelNum = 0;
        String startTsStr = String.valueOf(startTs);
        String endTsStr = String.valueOf(endTs);

        for (int i = 0; i < num; i++) {
            double val = i;
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
            labelNum = labels.size()/2;

            String addRet = tairTs.extsadd(randomPkey, randomSkey, tsStr, val, params);
            Assert.assertEquals("OK", addRet);
        }

        ExtsAggregationParams paramsAgg = new ExtsAggregationParams();
        paramsAgg.maxCountSize(10);
        paramsAgg.aggAvg(1000);

        ExtsFilter<String> filter1 = new ExtsFilter<String>("label1=1");
        ExtsFilter<String> filter2 = new ExtsFilter<String>("label2=2");
        ExtsFilter<String> filter3 = new ExtsFilter<String>("label3=3");
        ExtsFilter<String> filter4 = new ExtsFilter<String>("label2=3");
        ArrayList<ExtsFilter<String>> filterList = new ArrayList<ExtsFilter<String>>();
        filterList.add(filter1);
        filterList.add(filter2);

        List<ExtsSkeyResult> rangeByteRet = tairTs.extsmrange(randomPkey, startTsStr, endTsStr, filterList);
        assertEquals(1, rangeByteRet.size());
        assertEquals(randomSkey, rangeByteRet.get(0).getSkey());
        List<ExtsLabelResult> labelRet = rangeByteRet.get(0).getLabels();
        assertEquals(0, labelRet.size());

        List<ExtsDataPointResult> dataPointRet = rangeByteRet.get(0).getDataPoints();
        for (int i = 0; i < num; i++) {
            double val = i;
            long ts = startTs + i*1000;
            assertEquals(ts, dataPointRet.get(i).getTs());
            assertEquals(val, dataPointRet.get(i).getDoubleValue(), 0.0);
        }
    }

    @Test
    public void extsmrangeByteTest() throws Exception {
        long num = 3;
        long labelNum = 0;
        String startTsStr = String.valueOf(startTs);
        String endTsStr = String.valueOf(endTs);

        for (int i = 0; i < num; i++) {
            double val = i;
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
            labelNum = labels.size()/2;

            String addRet = tairTs.extsadd(randomPKeyBinary, bSkey, tsStr.getBytes(), val, params);
            Assert.assertEquals("OK", addRet);
        }

        ExtsAggregationParams paramsAgg = new ExtsAggregationParams();
        paramsAgg.maxCountSize(10);
        paramsAgg.aggAvg(1000);

        ExtsFilter<byte[]> filter1 = new ExtsFilter<byte[]>("label1=1".getBytes());
        ExtsFilter<byte[]> filter2 = new ExtsFilter<byte[]>("label2=2".getBytes());
        ExtsFilter<byte[]> filter3 = new ExtsFilter<byte[]>("label3=3".getBytes());
        ExtsFilter<byte[]> filter4 = new ExtsFilter<byte[]>("label2=3".getBytes());
        ArrayList<ExtsFilter<byte[]>> filterList = new ArrayList<ExtsFilter<byte[]>>();
        filterList.add(filter1);
        filterList.add(filter2);

        List<ExtsSkeyResult> rangeByteRet = tairTs.extsmrange(randomPKeyBinary, startTsStr.getBytes(), endTsStr.getBytes(), filterList);
        assertEquals(1, rangeByteRet.size());
        assertEquals(new String(bSkey), rangeByteRet.get(0).getSkey());
        List<ExtsLabelResult> labelRet = rangeByteRet.get(0).getLabels();
        assertEquals(0, labelRet.size());

        List<ExtsDataPointResult> dataPointRet = rangeByteRet.get(0).getDataPoints();
        for (int i = 0; i < num; i++) {
            double val = i;
            long ts = startTs + i*1000;
            assertEquals(ts, dataPointRet.get(i).getTs());
            assertEquals(val, dataPointRet.get(i).getDoubleValue(), 0.0);
        }
    }

    @Test
    public void extsmrangeLabelsTest() throws Exception {
        long num = 3;
        long labelNum = 0;
        String startTsStr = String.valueOf(startTs);
        String endTsStr = String.valueOf(endTs);

        for (int i = 0; i < num; i++) {
            double val = i;
            long ts = startTs + i * 1000;
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
            labelNum = labels.size() / 2;

            String addRet = tairTs.extsadd(randomPkey, randomSkey, tsStr, val, params);
            addRet = tairTs.extsadd(randomPkey, randomSkey2, tsStr, val, params);
            Assert.assertEquals("OK", addRet);
        }

        ExtsAggregationParams paramsAgg = new ExtsAggregationParams();
        paramsAgg.maxCountSize(10);
        paramsAgg.aggAvg(1000);
        paramsAgg.withLabels();

        ExtsFilter<String> filter1 = new ExtsFilter<String>("label1=1");
        ExtsFilter<String> filter2 = new ExtsFilter<String>("label2=2");
        ExtsFilter<String> filter3 = new ExtsFilter<String>("label3=3");
        ExtsFilter<String> filter4 = new ExtsFilter<String>("label2=3");
        ArrayList<ExtsFilter<String>> filterList = new ArrayList<ExtsFilter<String>>();
        filterList.add(filter1);
        filterList.add(filter2);

        List<ExtsSkeyResult> rangeByteRet = tairTs.extsmrange(randomPkey, startTsStr, endTsStr, paramsAgg, filterList);
        assertEquals(2, rangeByteRet.size());
        assertEquals(randomSkey, rangeByteRet.get(0).getSkey());
        List<ExtsLabelResult> labelRet = rangeByteRet.get(0).getLabels();

        assertEquals("label1", labelRet.get(0).getName());
        assertEquals("1", labelRet.get(0).getValue());
        assertEquals("label2", labelRet.get(1).getName());
        assertEquals("2", labelRet.get(1).getValue());

        List<ExtsDataPointResult> dataPointRet = rangeByteRet.get(0).getDataPoints();
        for (int i = 0; i < num; i++) {
            double val = i;
            long ts = startTs + i * 1000;
            assertEquals(ts, dataPointRet.get(i).getTs());
            assertEquals(val, dataPointRet.get(i).getDoubleValue(), 0.0);
        }

        assertEquals(randomSkey2, rangeByteRet.get(1).getSkey());
        labelRet = rangeByteRet.get(1).getLabels();

        assertEquals("label1", labelRet.get(0).getName());
        assertEquals("1", labelRet.get(0).getValue());
        assertEquals("label2", labelRet.get(1).getName());
        assertEquals("2", labelRet.get(1).getValue());

        dataPointRet = rangeByteRet.get(1).getDataPoints();
        for (int i = 0; i < num; i++) {
            double val = i;
            long ts = startTs + i * 1000;
            assertEquals(ts, dataPointRet.get(i).getTs());
            assertEquals(val, dataPointRet.get(i).getDoubleValue(), 0.0);
        }
    }

    @Test
    public void extsmrangeLabelsByteTest() throws Exception {
        long num = 3;
        long labelNum = 0;
        String startTsStr = String.valueOf(startTs);
        String endTsStr = String.valueOf(endTs);

        for (int i = 0; i < num; i++) {
            double val = i;
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
            labelNum = labels.size()/2;

            String addRet = tairTs.extsadd(randomPKeyBinary, bSkey, tsStr.getBytes(), val, params);
            Assert.assertEquals("OK", addRet);
        }

        ExtsAggregationParams paramsAgg = new ExtsAggregationParams();
        paramsAgg.maxCountSize(10);
        paramsAgg.aggAvg(1000);

        ExtsFilter<byte[]> filter1 = new ExtsFilter<byte[]>("label1=1".getBytes());
        ExtsFilter<byte[]> filter2 = new ExtsFilter<byte[]>("label2=2".getBytes());
        ExtsFilter<byte[]> filter3 = new ExtsFilter<byte[]>("label3=3".getBytes());
        ExtsFilter<byte[]> filter4 = new ExtsFilter<byte[]>("label2=3".getBytes());
        ArrayList<ExtsFilter<byte[]>> filterList = new ArrayList<ExtsFilter<byte[]>>();
        filterList.add(filter1);
        filterList.add(filter2);
        paramsAgg.withLabels();

        List<ExtsSkeyResult> rangeByteRet = tairTs.extsmrange(randomPKeyBinary, startTsStr.getBytes(), endTsStr.getBytes(), paramsAgg, filterList);
        assertEquals(1, rangeByteRet.size());
        assertEquals(new String(bSkey), rangeByteRet.get(0).getSkey());
        List<ExtsLabelResult> labelRet = rangeByteRet.get(0).getLabels();

        assertEquals("label1", labelRet.get(0).getName());
        assertEquals("1", labelRet.get(0).getValue());
        assertEquals("label2", labelRet.get(1).getName());
        assertEquals("2", labelRet.get(1).getValue());

        List<ExtsDataPointResult> dataPointRet = rangeByteRet.get(0).getDataPoints();
        for (int i = 0; i < num; i++) {
            double val = i;
            long ts = startTs + i*1000;
            assertEquals(ts, dataPointRet.get(i).getTs());
            assertEquals(val, dataPointRet.get(i).getDoubleValue(), 0.0);
        }
    }

    @Test
    public void extsprangeTest() throws Exception {
        long num = 3;
        long labelNum = 0;
        String startTsStr = String.valueOf(startTs);
        String endTsStr = String.valueOf(endTs);

        for (int i = 0; i < num; i++) {
            double val = i;
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
            labelNum = labels.size()/2;

            String addRet = tairTs.extsadd(randomPkey, randomSkey, tsStr, val, params);
            Assert.assertEquals("OK", addRet);
        }

        ExtsAggregationParams paramsAgg = new ExtsAggregationParams();
        paramsAgg.maxCountSize(10);
        paramsAgg.aggAvg(1000);

        ExtsFilter<String> filter1 = new ExtsFilter<String>("label1=1");
        ExtsFilter<String> filter2 = new ExtsFilter<String>("label2=2");
        ExtsFilter<String> filter3 = new ExtsFilter<String>("label3=3");
        ExtsFilter<String> filter4 = new ExtsFilter<String>("label2=3");
        ArrayList<ExtsFilter<String>> filterList = new ArrayList<ExtsFilter<String>>();
        filterList.add(filter1);
        filterList.add(filter2);

        ExtsSkeyResult rangeByteRet = tairTs.extsprange(randomPkey, startTsStr, endTsStr, "sum", 1000, filterList);
        List<ExtsDataPointResult> dataPointRet = rangeByteRet.getDataPoints();
        assertEquals(num, dataPointRet.size());
        for (int i = 0; i < num; i++) {
            double val = i;
            long ts = startTs + i*1000;
            assertEquals(ts, dataPointRet.get(i).getTs());
            assertEquals(val, dataPointRet.get(i).getDoubleValue(), 0.0);
        }
    }

    @Test
    public void extsprangeByteTest() throws Exception {
        long num = 3;
        long labelNum = 0;
        String startTsStr = String.valueOf(startTs);
        String endTsStr = String.valueOf(endTs);

        for (int i = 0; i < num; i++) {
            double val = i;
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
            labelNum = labels.size()/2;

            String addRet = tairTs.extsadd(randomPKeyBinary, bSkey, tsStr.getBytes(), val, params);
            Assert.assertEquals("OK", addRet);
        }

        ExtsAggregationParams paramsAgg = new ExtsAggregationParams();
        paramsAgg.maxCountSize(10);
        paramsAgg.aggAvg(1000);

        ExtsFilter<byte[]> filter1 = new ExtsFilter<byte[]>("label1=1".getBytes());
        ExtsFilter<byte[]> filter2 = new ExtsFilter<byte[]>("label2=2".getBytes());
        ExtsFilter<byte[]> filter3 = new ExtsFilter<byte[]>("label3=3".getBytes());
        ExtsFilter<byte[]> filter4 = new ExtsFilter<byte[]>("label2=3".getBytes());
        ArrayList<ExtsFilter<byte[]>> filterList = new ArrayList<ExtsFilter<byte[]>>();
        filterList.add(filter1);
        filterList.add(filter2);

        ExtsSkeyResult rangeByteRet = tairTs.extsprange(randomPKeyBinary, startTsStr.getBytes(), endTsStr.getBytes(), "sum".getBytes(), 1000, filterList);
        List<ExtsDataPointResult> dataPointRet = rangeByteRet.getDataPoints();
        assertEquals(num, dataPointRet.size());
        for (int i = 0; i < num; i++) {
            double val = i;
            long ts = startTs + i*1000;
            assertEquals(ts, dataPointRet.get(i).getTs());
            assertEquals(val, dataPointRet.get(i).getDoubleValue(), 0.0);
        }
    }


    @Test
    public void extsprangeAggregationSkeyTest() throws Exception {
        long num = 3;
        long labelNum = 0;
        String startTsStr = String.valueOf(startTs);
        String endTsStr = String.valueOf(endTs);

        for (int i = 0; i < num; i++) {
            double val = i;
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
            labelNum = labels.size()/2;

            String addRet = tairTs.extsadd(randomPkey, randomSkey, tsStr, val, params);
            Assert.assertEquals("OK", addRet);
        }

        ExtsAggregationParams paramsAgg = new ExtsAggregationParams();
        paramsAgg.maxCountSize(10);
        paramsAgg.aggAvg(1000);

        ExtsFilter<String> filter1 = new ExtsFilter<String>("label1=1");
        ExtsFilter<String> filter2 = new ExtsFilter<String>("label2=2");
        ExtsFilter<String> filter3 = new ExtsFilter<String>("label3=3");
        ExtsFilter<String> filter4 = new ExtsFilter<String>("label2=3");
        ArrayList<ExtsFilter<String>> filterList = new ArrayList<ExtsFilter<String>>();
        filterList.add(filter1);
        filterList.add(filter2);

        ExtsSkeyResult rangeByteRet = tairTs.extsprange(randomPkey, startTsStr, endTsStr, "sum", 1000, filterList);
        List<ExtsDataPointResult> dataPointRet = rangeByteRet.getDataPoints();
        assertEquals(num, dataPointRet.size());
        for (int i = 0; i < num; i++) {
            double val = i;
            long ts = startTs + i*1000;
            assertEquals(ts, dataPointRet.get(i).getTs());
            assertEquals(val, dataPointRet.get(i).getDoubleValue(), 0.0);
        }
    }

    @Test
    public void extsprangeAggregationSkeyByteTest() throws Exception {
        long num = 3;
        long labelNum = 0;
        String startTsStr = String.valueOf(startTs);
        String endTsStr = String.valueOf(endTs);

        for (int i = 0; i < num; i++) {
            double val = i;
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
            labelNum = labels.size()/2;

            String addRet = tairTs.extsadd(randomPKeyBinary, bSkey, tsStr.getBytes(), val, params);
            Assert.assertEquals("OK", addRet);
        }

        ExtsAggregationParams paramsAgg = new ExtsAggregationParams();
        paramsAgg.maxCountSize(10);
        paramsAgg.aggAvg(1000);

        ExtsFilter<byte[]> filter1 = new ExtsFilter<byte[]>("label1=1".getBytes());
        ExtsFilter<byte[]> filter2 = new ExtsFilter<byte[]>("label2=2".getBytes());
        ExtsFilter<byte[]> filter3 = new ExtsFilter<byte[]>("label3=3".getBytes());
        ExtsFilter<byte[]> filter4 = new ExtsFilter<byte[]>("label2=3".getBytes());
        ArrayList<ExtsFilter<byte[]>> filterList = new ArrayList<ExtsFilter<byte[]>>();
        filterList.add(filter1);
        filterList.add(filter2);

        ExtsSkeyResult rangeByteRet = tairTs.extsprange(randomPKeyBinary, startTsStr.getBytes(), endTsStr.getBytes(), "sum".getBytes(), 1000, filterList);
        List<ExtsDataPointResult> dataPointRet = rangeByteRet.getDataPoints();
        assertEquals(num, dataPointRet.size());
        for (int i = 0; i < num; i++) {
            double val = i;
            long ts = startTs + i*1000;
            assertEquals(ts, dataPointRet.get(i).getTs());
            assertEquals(val, dataPointRet.get(i).getDoubleValue(), 0.0);
        }
    }
}
