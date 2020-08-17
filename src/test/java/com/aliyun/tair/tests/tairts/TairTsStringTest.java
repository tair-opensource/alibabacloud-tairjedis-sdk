package com.aliyun.tair.tests.tairts;

import com.aliyun.tair.tairts.params.*;
import com.aliyun.tair.tairts.results.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static redis.clients.jedis.Protocol.toByteArray;

public class TairTsStringTest extends TairTsTestBase {
    private String randomSkey;
    private String randomSkey2;
    private byte[] bSkey;
    private byte[] bSkey2;
    private String randomPkey;
    private byte[] randomPKeyBinary;
    private long startTs;
    private long endTs;
    private String value;

    public TairTsStringTest() {
        randomPkey = "randomPkey_" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        randomPKeyBinary = ("randomPkey_" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        randomSkey = "key" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        randomSkey2 = "key2" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        bSkey = ("bkey" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        bSkey2 = ("bkey2" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        startTs = (System.currentTimeMillis() - 1000000) / 1000 * 1000;
        endTs = System.currentTimeMillis() / 1000 * 1000;
        value = "This is a #TsString * value*";
    }

    @Test
    public void extsaddTest() throws Exception {

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

            String addRet = tairTs.extsaddstr(randomPkey, randomSkey, tsStr, val);
            Assert.assertEquals("OK", addRet);
            ts = ts + 1;
            tsStr = String.valueOf(ts);
            addRet = tairTs.extsaddstr(randomPkey, randomSkey, tsStr, val, params);
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

            String addRet = tairTs.extsaddstr(randomPKeyBinary, bSkey, tsStr, val);
            Assert.assertEquals("OK", addRet);
            ts = ts + 1;
            tsStr = toByteArray(ts);
            addRet = tairTs.extsaddstr(randomPKeyBinary, bSkey, tsStr, val, params);
            Assert.assertEquals("OK", addRet);
        }
    }

    @Test
    public void extsmaddTest() throws Exception {

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
            params.labels(labels);

            ArrayList<ExtsStringDataPoint<String>> addList = new ArrayList<ExtsStringDataPoint<String>>();
            ExtsStringDataPoint<String> add1 = new ExtsStringDataPoint<String>(randomSkey, tsStr, val);
            ExtsStringDataPoint<String> add2 = new ExtsStringDataPoint<String>(randomSkey2, tsStr, val);
            addList.add(add1);
            addList.add(add2);
            List<String> maddRet = tairTs.extsmaddstr(randomPkey, 2, addList);
            for (int j = 0; j < maddRet.size(); j++) {
                Assert.assertEquals("OK", maddRet.get(j));
            }

            String delRet = tairTs.extsdelstr(randomPkey, randomSkey);
            Assert.assertEquals("OK", delRet);
            delRet = tairTs.extsdelstr(randomPkey, randomSkey2);
            Assert.assertEquals("OK", delRet);

            maddRet = tairTs.extsmaddstr(randomPkey, 2, addList,params);
            for (int j = 0; j < maddRet.size(); j++) {
                Assert.assertEquals("OK", maddRet.get(j));
            }

            delRet = tairTs.extsdelstr(randomPkey, randomSkey);
            Assert.assertEquals("OK", delRet);
            delRet = tairTs.extsdelstr(randomPkey, randomSkey2);
            Assert.assertEquals("OK", delRet);
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
            params.labels(labels);

            ArrayList<ExtsStringDataPoint<byte[]>> addList = new ArrayList<ExtsStringDataPoint<byte[]>>();
            ExtsStringDataPoint<byte[]> add1 = new ExtsStringDataPoint<byte[]>(bSkey, tsStr, val);
            ExtsStringDataPoint<byte[]> add2 = new ExtsStringDataPoint<byte[]>(bSkey2, tsStr, val);
            addList.add(add1);
            addList.add(add2);
            List<String> maddRet = tairTs.extsmaddstr(randomPKeyBinary, 2, addList);
            for (int j = 0; j < maddRet.size(); j++) {
                Assert.assertEquals("OK", maddRet.get(j));
            }

            String delRet = tairTs.extsdelstr(randomPKeyBinary, bSkey);
            Assert.assertEquals("OK", delRet);
            delRet = tairTs.extsdelstr(randomPKeyBinary, bSkey2);
            Assert.assertEquals("OK", delRet);

            maddRet = tairTs.extsmaddstr(randomPKeyBinary, 2, addList,params);
            for (int j = 0; j < maddRet.size(); j++) {
                Assert.assertEquals("OK", maddRet.get(j));
            }

            delRet = tairTs.extsdelstr(randomPKeyBinary, bSkey);
            Assert.assertEquals("OK", delRet);
            delRet = tairTs.extsdelstr(randomPKeyBinary, bSkey2);
            Assert.assertEquals("OK", delRet);
        }
    }

    @Test
    public void extsgetTest() throws Exception {

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

            String addRet = tairTs.extsaddstr(randomPkey, randomSkey, tsStr, val, params);
            Assert.assertEquals("OK", addRet);

            ExtsStringDataPointResult getRet = tairTs.extsgetstr(randomPkey, randomSkey);
            assertEquals((long)ts, getRet.getTs());
            assertEquals(true, val.equals(getRet.getValue()));
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

            String addRet = tairTs.extsaddstr(randomPKeyBinary, bSkey, tsStr, val);
            Assert.assertEquals("OK", addRet);

            ExtsStringDataPointResult getRet = tairTs.extsgetstr(randomPKeyBinary, bSkey);
            assertEquals((long)ts, getRet.getTs());
            assertEquals(true, valstr.equals(getRet.getValue()));
        }
    }

    @Test
    public void extsqueryTest() throws Exception {

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

            String addRet = tairTs.extsaddstr(randomPkey, randomSkey, tsStr, val, params);
            Assert.assertEquals("OK", addRet);
            addRet = tairTs.extsaddstr(randomPkey, randomSkey2, tsStr, val, params2);
            Assert.assertEquals("OK", addRet);

            ExtsFilter<String> filter1 = new ExtsFilter<String>("label1=1");
            ExtsFilter<String> filter2 = new ExtsFilter<String>("label2=2");
            ExtsFilter<String> filter3 = new ExtsFilter<String>("label3=3");
            ExtsFilter<String> filter4 = new ExtsFilter<String>("label2=3");
            ArrayList<ExtsFilter<String>> filterList = new ArrayList<ExtsFilter<String>>();
            filterList.add(filter1);
            filterList.add(filter2);

            List<String> queryRet = tairTs.extsquerystr(randomPkey, filterList);
            assertEquals(1, queryRet.size());
            assertEquals(randomSkey, queryRet.get(0));


            ArrayList<ExtsFilter<String>> filterList2 = new ArrayList<ExtsFilter<String>>();
            filterList2.add(filter1);
            filterList2.add(filter3);

            queryRet = tairTs.extsquerystr(randomPkey, filterList2);
            assertEquals(1, queryRet.size());
            assertEquals(randomSkey2, queryRet.get(0));

            ArrayList<ExtsFilter<String>> filterList3 = new ArrayList<ExtsFilter<String>>();
            filterList3.add(filter1);

            queryRet = tairTs.extsquerystr(randomPkey, filterList3);
            assertEquals(2, queryRet.size());

            ArrayList<ExtsFilter<String>> filterList4 = new ArrayList<ExtsFilter<String>>();
            filterList4.add(filter4);

            queryRet = tairTs.extsquerystr(randomPkey, filterList4);
            assertEquals(0, queryRet.size());
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

            String addRet = tairTs.extsaddstr(randomPKeyBinary, bSkey, tsStr, val, params);
            Assert.assertEquals("OK", addRet);
            addRet = tairTs.extsaddstr(randomPKeyBinary, bSkey2, tsStr, val, params2);
            Assert.assertEquals("OK", addRet);

            ExtsFilter<byte[]> filter1 = new ExtsFilter<byte[]>("label1=1".getBytes());
            ExtsFilter<byte[]> filter2 = new ExtsFilter<byte[]>("label2=2".getBytes());
            ExtsFilter<byte[]> filter3 = new ExtsFilter<byte[]>("label3=3".getBytes());
            ExtsFilter<byte[]> filter4 = new ExtsFilter<byte[]>("label2=3".getBytes());
            ArrayList<ExtsFilter<byte[]>> filterList = new ArrayList<ExtsFilter<byte[]>>();
            filterList.add(filter1);
            filterList.add(filter2);

            List<byte[]> queryRet = tairTs.extsquerystr(randomPKeyBinary, filterList);
            assertEquals(1, queryRet.size());
            assertEquals(new String(bSkey), new String(queryRet.get(0)));


            ArrayList<ExtsFilter<byte[]>> filterList2 = new ArrayList<ExtsFilter<byte[]>>();
            filterList2.add(filter1);
            filterList2.add(filter3);

            queryRet = tairTs.extsquerystr(randomPKeyBinary, filterList2);
            assertEquals(1, queryRet.size());
            assertEquals(new String(bSkey2), new String(queryRet.get(0)));

            ArrayList<ExtsFilter<byte[]>> filterList3 = new ArrayList<ExtsFilter<byte[]>>();
            filterList3.add(filter1);

            queryRet = tairTs.extsquerystr(randomPKeyBinary, filterList3);
            assertEquals(2, queryRet.size());

            ArrayList<ExtsFilter<byte[]>> filterList4 = new ArrayList<ExtsFilter<byte[]>>();
            filterList4.add(filter4);

            queryRet = tairTs.extsquerystr(randomPKeyBinary, filterList4);
            assertEquals(0, queryRet.size());
        }
    }

    @Test
    public void extsrangeTest() throws Exception {
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

            String addRet = tairTs.extsaddstr(randomPkey, randomSkey, tsStr, val, params);
            Assert.assertEquals("OK", addRet);
        }

        ExtsStringAggregationParams paramsAgg = new ExtsStringAggregationParams();
        paramsAgg.maxCountSize(10);

        ExtsStringSkeyResult rangeByteRet = tairTs.extsrangestr(randomPkey, randomSkey, startTsStr, endTsStr, paramsAgg);
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

            String addRet = tairTs.extsaddstr(randomPKeyBinary, bSkey, tsStr.getBytes(), val, params);
            Assert.assertEquals("OK", addRet);
        }

        paramsAgg = new ExtsStringAggregationParams();
        paramsAgg.maxCountSize(10);

        rangeByteRet = tairTs.extsrangestr(randomPKeyBinary, bSkey, startTsStr.getBytes(), endTsStr.getBytes(), paramsAgg);
        dataPointRet = rangeByteRet.getDataPoints();
        assertEquals(num, dataPointRet.size());
        for (int i = 0; i < num; i++) {
            String val = value + String.valueOf(i);
            long ts = startTs + i*1000;
            assertEquals(ts, dataPointRet.get(i).getTs());
            assertEquals(true, val.equals(dataPointRet.get(i).getValue()));
        }
    }

    @Test
    public void extsmrangeTest() throws Exception {
        long num = 3;
        long labelNum = 0;
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
            labelNum = labels.size()/2;

            String addRet = tairTs.extsaddstr(randomPkey, randomSkey, tsStr, val, params);
            Assert.assertEquals("OK", addRet);
        }

        ExtsStringAggregationParams paramsAgg = new ExtsStringAggregationParams();
        paramsAgg.maxCountSize(10);

        ExtsFilter<String> filter1 = new ExtsFilter<String>("label1=1");
        ExtsFilter<String> filter2 = new ExtsFilter<String>("label2=2");
        ExtsFilter<String> filter3 = new ExtsFilter<String>("label3=3");
        ExtsFilter<String> filter4 = new ExtsFilter<String>("label2=3");
        ArrayList<ExtsFilter<String>> filterList = new ArrayList<ExtsFilter<String>>();
        filterList.add(filter1);
        filterList.add(filter2);

        List<ExtsStringSkeyResult> rangeByteRet = tairTs.extsmrangestr(randomPkey, startTsStr, endTsStr, filterList);
        assertEquals(1, rangeByteRet.size());
        assertEquals(randomSkey, rangeByteRet.get(0).getSkey());
        List<ExtsLabelResult> labelRet = rangeByteRet.get(0).getLabels();
        assertEquals(0, labelRet.size());

        List<ExtsStringDataPointResult> dataPointRet = rangeByteRet.get(0).getDataPoints();
        for (int i = 0; i < num; i++) {
            String val = value + String.valueOf(i);
            long ts = startTs + i*1000;
            assertEquals(ts, dataPointRet.get(i).getTs());
            assertEquals(true, val.equals(dataPointRet.get(i).getValue()));
        }
    }

    @Test
    public void extsmrangeByteTest() throws Exception {
        long num = 3;
        long labelNum = 0;
        String startTsStr = String.valueOf(startTs);
        String endTsStr = String.valueOf(endTs);

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
            labelNum = labels.size()/2;

            String addRet = tairTs.extsaddstr(randomPKeyBinary, bSkey, tsStr.getBytes(), val, params);
            Assert.assertEquals("OK", addRet);
        }

        ExtsStringAggregationParams paramsAgg = new ExtsStringAggregationParams();
        paramsAgg.maxCountSize(10);

        ExtsFilter<byte[]> filter1 = new ExtsFilter<byte[]>("label1=1".getBytes());
        ExtsFilter<byte[]> filter2 = new ExtsFilter<byte[]>("label2=2".getBytes());
        ExtsFilter<byte[]> filter3 = new ExtsFilter<byte[]>("label3=3".getBytes());
        ExtsFilter<byte[]> filter4 = new ExtsFilter<byte[]>("label2=3".getBytes());
        ArrayList<ExtsFilter<byte[]>> filterList = new ArrayList<ExtsFilter<byte[]>>();
        filterList.add(filter1);
        filterList.add(filter2);

        List<ExtsStringSkeyResult> rangeByteRet = tairTs.extsmrangestr(randomPKeyBinary, startTsStr.getBytes(), endTsStr.getBytes(), filterList);
        assertEquals(1, rangeByteRet.size());
        assertEquals(new String(bSkey), rangeByteRet.get(0).getSkey());
        List<ExtsLabelResult> labelRet = rangeByteRet.get(0).getLabels();
        assertEquals(0, labelRet.size());

        List<ExtsStringDataPointResult> dataPointRet = rangeByteRet.get(0).getDataPoints();
        for (int i = 0; i < num; i++) {
            String val = value + String.valueOf(i);
            long ts = startTs + i*1000;
            assertEquals(ts, dataPointRet.get(i).getTs());
            assertEquals(true, val.equals(dataPointRet.get(i).getValue()));
        }
    }

    @Test
    public void extsmrangeLabelsTest() throws Exception {
        long num = 3;
        long labelNum = 0;
        String startTsStr = String.valueOf(startTs);
        String endTsStr = String.valueOf(endTs);

        for (int i = 0; i < num; i++) {
            String val = value + String.valueOf(i);
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

            String addRet = tairTs.extsaddstr(randomPkey, randomSkey, tsStr, val, params);
            Assert.assertEquals("OK", addRet);
        }

        ExtsStringAggregationParams paramsAgg = new ExtsStringAggregationParams();
        paramsAgg.maxCountSize(10);
        paramsAgg.withLabels();

        ExtsFilter<String> filter1 = new ExtsFilter<String>("label1=1");
        ExtsFilter<String> filter2 = new ExtsFilter<String>("label2=2");
        ExtsFilter<String> filter3 = new ExtsFilter<String>("label3=3");
        ExtsFilter<String> filter4 = new ExtsFilter<String>("label2=3");
        ArrayList<ExtsFilter<String>> filterList = new ArrayList<ExtsFilter<String>>();
        filterList.add(filter1);
        filterList.add(filter2);

        List<ExtsStringSkeyResult> rangeByteRet = tairTs.extsmrangestr(randomPkey, startTsStr, endTsStr, paramsAgg, filterList);
        assertEquals(1, rangeByteRet.size());
        assertEquals(randomSkey, rangeByteRet.get(0).getSkey());
        List<ExtsLabelResult> labelRet = rangeByteRet.get(0).getLabels();

        assertEquals("label1", labelRet.get(0).getName());
        assertEquals("1", labelRet.get(0).getValue());
        assertEquals("label2", labelRet.get(1).getName());
        assertEquals("2", labelRet.get(1).getValue());

        List<ExtsStringDataPointResult> dataPointRet = rangeByteRet.get(0).getDataPoints();
        for (int i = 0; i < num; i++) {
            String val = value + String.valueOf(i);
            long ts = startTs + i * 1000;
            assertEquals(ts, dataPointRet.get(i).getTs());
            assertEquals(true, val.equals(dataPointRet.get(i).getValue()));
        }
    }

    @Test
    public void extsmrangeLabelsByteTest() throws Exception {
        long num = 1000;
        long labelNum = 0;
        String startTsStr = String.valueOf(startTs);
        String endTsStr = String.valueOf(endTs);

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
            labelNum = labels.size()/2;

            String addRet = tairTs.extsaddstr(randomPKeyBinary, bSkey, tsStr.getBytes(), val, params);
            Assert.assertEquals("OK", addRet);
        }

        ExtsStringAggregationParams paramsAgg = new ExtsStringAggregationParams();
        paramsAgg.maxCountSize(1000);

        ExtsFilter<byte[]> filter1 = new ExtsFilter<byte[]>("label1=1".getBytes());
        ExtsFilter<byte[]> filter2 = new ExtsFilter<byte[]>("label2=2".getBytes());
        ExtsFilter<byte[]> filter3 = new ExtsFilter<byte[]>("label3=3".getBytes());
        ExtsFilter<byte[]> filter4 = new ExtsFilter<byte[]>("label2=3".getBytes());
        ArrayList<ExtsFilter<byte[]>> filterList = new ArrayList<ExtsFilter<byte[]>>();
        filterList.add(filter1);
        filterList.add(filter2);
        paramsAgg.withLabels();

        List<ExtsStringSkeyResult> rangeByteRet = tairTs.extsmrangestr(randomPKeyBinary, startTsStr.getBytes(), endTsStr.getBytes(), paramsAgg, filterList);
        assertEquals(1, rangeByteRet.size());
        assertEquals(new String(bSkey), rangeByteRet.get(0).getSkey());
        List<ExtsLabelResult> labelRet = rangeByteRet.get(0).getLabels();

        assertEquals("label1", labelRet.get(0).getName());
        assertEquals("1", labelRet.get(0).getValue());
        assertEquals("label2", labelRet.get(1).getName());
        assertEquals("2", labelRet.get(1).getValue());

        List<ExtsStringDataPointResult> dataPointRet = rangeByteRet.get(0).getDataPoints();
        for (int i = 0; i < num; i++) {
            String val = value + String.valueOf(i);
            long ts = startTs + i*1000;
            assertEquals(ts, dataPointRet.get(i).getTs());
            assertEquals(true, val.equals(dataPointRet.get(i).getValue()));
        }
    }
}
