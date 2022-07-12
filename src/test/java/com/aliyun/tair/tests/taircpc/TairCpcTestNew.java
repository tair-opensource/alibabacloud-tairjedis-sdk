package com.aliyun.tair.tests.taircpc;

import com.aliyun.tair.taircpc.params.*;
import com.aliyun.tair.taircpc.results.Update2JudResult;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TairCpcTestNew extends TairCpcTestBase {
    private String key;
    private String key2;
    private String key3;
    private String item;
    private String item2;
    private String item3;
    private String item4;
    private byte[] bkey;
    private byte[] bkey2;
    private byte[] bkey3;
    private byte[] bitem;
    private byte[] bitem2;
    private String content1;
    private String content2;
    private byte[] bcontent1;
    private byte[] bcontent2;
    private long count1;
    private long count2;
    private long timestamp;
    private long winsize;

    public TairCpcTestNew() {
        key = "key" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        key2 = "key2" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        key3 = "key3" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        item = "item" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        item2 = "item2" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        item3 = "item3" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        item4 = "item4" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        bkey = ("bkey" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        bkey2 = ("bkey2" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        bkey3 = ("bkey2" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        bitem = ("bitem" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        bitem2 = ("bitem2" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        content1 = "content1" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        content2 = "content2" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        bcontent1 = ("content1" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        bcontent2 = ("content2" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        count1 = 100;
        count2 = 200;
        timestamp = 1000000;
        winsize = 6000;
    }

    @Test
    public void cpcupdateTest() throws Exception {

        String addRet = tairCpcNew.cpcUpdate(key, item);
        Assert.assertEquals("OK", addRet);

        String addRetByte = tairCpcNew.cpcUpdate(bkey, bitem);
        Assert.assertEquals("OK", addRetByte);
    }

    @Test
    public void cpcupdateExpireTest() throws Exception {

        CpcUpdateParams cpcUpdateParams = new CpcUpdateParams();
        cpcUpdateParams.ex(2);
        String addRet = tairCpcNew.cpcUpdate(key, item, cpcUpdateParams);
        Assert.assertEquals("OK", addRet);
        addRet = tairCpcNew.cpcUpdate(key, item2);
        Assert.assertEquals("OK", addRet);

        Double estimateRet = tairCpcNew.cpcEstimate(key);
        assertEquals(2.00, estimateRet, 0.001);

        Thread.sleep(2000);

        estimateRet = tairCpcNew.cpcEstimate(key);
        assertNull(estimateRet);

        addRet = tairCpcNew.cpcUpdate(key, item);
        Assert.assertEquals("OK", addRet);
        cpcUpdateParams.ex(0);
        addRet = tairCpcNew.cpcUpdate(key, item2, cpcUpdateParams);
        Assert.assertEquals("OK", addRet);

        Thread.sleep(1000);
        estimateRet = tairCpcNew.cpcEstimate(key);
        assertEquals(2.00, estimateRet, 0.001);

        cpcUpdateParams.ex(2);
        addRet = tairCpcNew.cpcUpdate(key, item2, cpcUpdateParams);

        cpcUpdateParams.ex(-1);
        addRet = tairCpcNew.cpcUpdate(key, item2, cpcUpdateParams);

        Thread.sleep(2000);
        estimateRet = tairCpcNew.cpcEstimate(key);
        assertEquals(2.00, estimateRet, 0.001);

        cpcUpdateParams.ex(2);
        addRet = tairCpcNew.cpcUpdate(bkey, bitem, cpcUpdateParams);
        Assert.assertEquals("OK", addRet);
        addRet = tairCpcNew.cpcUpdate(bkey, bitem2);
        Assert.assertEquals("OK", addRet);

        estimateRet = tairCpcNew.cpcEstimate(bkey);
        assertEquals(2.00, estimateRet, 0.01);

        Thread.sleep(2000);

        estimateRet = tairCpcNew.cpcEstimate(bkey);
        assertNull(estimateRet);

        addRet = tairCpcNew.cpcUpdate(bkey, bitem);
        Assert.assertEquals("OK", addRet);
        cpcUpdateParams.ex(0);
        addRet = tairCpcNew.cpcUpdate(bkey, bitem2, cpcUpdateParams);
        Assert.assertEquals("OK", addRet);

        Thread.sleep(1000);
        estimateRet = tairCpcNew.cpcEstimate(bkey);
        assertEquals(2.00, estimateRet, 0.001);

        cpcUpdateParams.ex(2);
        addRet = tairCpcNew.cpcUpdate(bkey, bitem2, cpcUpdateParams);

        cpcUpdateParams.ex(-1);
        addRet = tairCpcNew.cpcUpdate(bkey, bitem2, cpcUpdateParams);

        Thread.sleep(2000);
        estimateRet = tairCpcNew.cpcEstimate(bkey);
        assertEquals(2.00, estimateRet, 0.001);
    }

    @Test
    public void cpcupdate2judTest() throws Exception {

        String addRet = tairCpcNew.cpcUpdate(key, item);
        Assert.assertEquals("OK", addRet);

        addRet = tairCpcNew.cpcUpdate(key, item2);
        Assert.assertEquals("OK", addRet);

        addRet = tairCpcNew.cpcUpdate(key, item3);
        Assert.assertEquals("OK", addRet);

        Double estimateRet = tairCpcNew.cpcEstimate(key);
        assertEquals(3.00, estimateRet, 0.001);

        Update2JudResult judRet = tairCpcNew.cpcUpdate2Jud(key, item);
        assertEquals(3.00, judRet.getValue(), 0.001);
        assertEquals(0.00, judRet.getDiffValue(), 0.001);

        judRet = tairCpcNew.cpcUpdate2Jud(key, item4);
        assertEquals(4.00, judRet.getValue(), 0.001);
        assertEquals(1.00, judRet.getDiffValue(), 0.001);
    }

    @Test
    public void cpcAccurateEstimationTest() throws Exception {

        int testnum = 120;
        String addRet = null;
        for (int i = 0; i < testnum; i++) {
            String itemtmp = item + String.valueOf(i);
            addRet = tairCpcNew.cpcUpdate(key, itemtmp);
            Assert.assertEquals("OK", addRet);
        }

        Double estimateRet = tairCpcNew.cpcEstimate(key);
        assertEquals(120.00, estimateRet, 0.1);
    }

    @Test
    public void cpcMupdateTest() throws Exception {

        ArrayList<CpcData> addList = new ArrayList<CpcData>();
        CpcData add1 = new CpcData(key, item);
        add1.ex(4);
        CpcData add2 = new CpcData(key2, item2);
        add2.ex(4);
        addList.add(add1);
        addList.add(add2);

        String addRet = tairCpcNew.cpcMUpdate(addList);
        Assert.assertEquals("OK", addRet);
        Double estimateRet = tairCpcNew.cpcEstimate(key);
        assertEquals(1.00, estimateRet, 0.1);
        Thread.sleep(5 * 1000);
        estimateRet = tairCpcNew.cpcEstimate(key);
        assertNull(estimateRet);
    }

    @Test
    public void cpcMupdate2EstTest() throws Exception {

        ArrayList<CpcData> addList = new ArrayList<CpcData>();
        CpcData add1 = new CpcData(key, item);
        CpcData add2 = new CpcData(key2, item2);
        CpcData add3 = new CpcData(key, item2);
        addList.add(add1);
        addList.add(add2);
        addList.add(add3);

        List<Double> addRet = tairCpcNew.cpcMUpdate2Est(addList);
        for (int j = 0; j < 2; j++) {
            Assert.assertEquals(1.0, addRet.get(j), 0.1);
        }
        Assert.assertEquals(2.0, addRet.get(2), 0.1);
    }

    @Test
    public void cpcMupdate2JudTest() throws Exception {

        ArrayList<CpcData> addList = new ArrayList<CpcData>();
        CpcData add1 = new CpcData(key, item);
        CpcData add2 = new CpcData(key2, item2);
        CpcData add3 = new CpcData(key, item2);
        addList.add(add1);
        addList.add(add2);
        addList.add(add3);

        List<Update2JudResult> addRet = tairCpcNew.cpcMUpdate2Jud(addList);
        for (int j = 0; j < addRet.size() -1; j++) {
            Assert.assertEquals(1.0, addRet.get(j).getValue(), 0.1);
            Assert.assertEquals(1.0, addRet.get(j).getDiffValue(), 0.1);
        }
        Assert.assertEquals(2.0, addRet.get(2).getValue(), 0.1);
        Assert.assertEquals(1.0, addRet.get(2).getDiffValue(), 0.1);
    }

//    @Test
//    public void cpcArrayUpdateTest() throws Exception {
//
//        String addRet = tairCpcNew.cpcArrayUpdate(key, 1, item, 5);
//        Assert.assertEquals("OK", addRet);
//        addRet = tairCpcNew.cpcArrayUpdate(key, 1, item2, 5);
//        Assert.assertEquals("OK", addRet);
//        addRet = tairCpcNew.cpcArrayUpdate(key, 3, item, 5);
//        Assert.assertEquals("OK", addRet);
//        addRet = tairCpcNew.cpcArrayUpdate(key, 5, item, 5);
//        Assert.assertEquals("OK", addRet);
//
//        Double estimateRet = tairCpcNew.cpcArrayEstimate(key, 1);
//        assertEquals(2.00, estimateRet, 0.001);
//        estimateRet = tairCpcNew.cpcArrayEstimate(key, 3);
//        assertEquals(1.00, estimateRet, 0.001);
//        estimateRet = tairCpcNew.cpcArrayEstimate(key, 5);
//        assertEquals(1.00, estimateRet, 0.001);
//    }
//
//    @Test
//    public void cpcArrayUpdate2EstTest() throws Exception {
//
//        Double addRet = tairCpcNew.cpcArrayUpdate2Est(key, 1, item, 5);
//        assertEquals(1.00, addRet, 0.001);
//        addRet = tairCpcNew.cpcArrayUpdate2Est(key, 1, item2, 5);
//        assertEquals(2.00, addRet, 0.001);
//        addRet = tairCpcNew.cpcArrayUpdate2Est(key, 3, item, 5);
//        assertEquals(1.00, addRet, 0.001);
//        addRet = tairCpcNew.cpcArrayUpdate2Est(key, 5, item, 5);
//        assertEquals(1.00, addRet, 0.001);
//
//        addRet = tairCpcNew.cpcArrayUpdate2Est(key, 7, item2, 5);
//        assertEquals(1.00, addRet, 0.001);
//
//        Double estimateRet = tairCpcNew.cpcArrayEstimate(key, 7);
//        assertEquals(1.00, estimateRet, 0.001);
//        estimateRet = tairCpcNew.cpcArrayEstimate(key, 3);
//        assertEquals(1.00, estimateRet, 0.001);
//        estimateRet = tairCpcNew.cpcArrayEstimate(key, 5);
//        assertEquals(1.00, estimateRet, 0.001);
//    }
//
//    @Test
//    public void cpcArrayUpdate2JudTest() throws Exception {
//
//        Update2JudResult addRet = tairCpcNew.cpcArrayUpdate2Jud(key, 1, item, 5);
//        assertEquals(1.00, addRet.getValue(), 0.001);
//        assertEquals(1.00, addRet.getDiffValue(), 0.001);
//        Update2JudResult addRet2 = tairCpcNew.cpcArrayUpdate2Jud(key, 1, item, 5);
//        assertEquals(1.00, addRet2.getValue(), 0.001);
//        assertEquals(0.00, addRet2.getDiffValue(), 0.001);
//        addRet = tairCpcNew.cpcArrayUpdate2Jud(key, 1, item2, 5);
//        assertEquals(2.00, addRet.getValue(), 0.001);
//        assertEquals(1.00, addRet.getDiffValue(), 0.001);
//        addRet = tairCpcNew.cpcArrayUpdate2Jud(key, 3, item, 5);
//        assertEquals(1.00, addRet.getValue(), 0.001);
//        assertEquals(1.00, addRet.getDiffValue(), 0.001);
//        addRet = tairCpcNew.cpcArrayUpdate2Jud(key, 5, item, 5);
//        assertEquals(1.00, addRet.getValue(), 0.001);
//        assertEquals(1.00, addRet.getDiffValue(), 0.001);
//
//        addRet = tairCpcNew.cpcArrayUpdate2Jud(key, 7, item2, 5);
//        assertEquals(1.00, addRet.getValue(), 0.001);
//        assertEquals(1.00, addRet.getDiffValue(), 0.001);
//
//        Double estimateRet = tairCpcNew.cpcArrayEstimate(key, 7);
//        assertEquals(1.00, estimateRet, 0.001);
//        estimateRet = tairCpcNew.cpcArrayEstimate(key, 3);
//        assertEquals(1.00, estimateRet, 0.001);
//        estimateRet = tairCpcNew.cpcArrayEstimate(key, 5);
//        assertEquals(1.00, estimateRet, 0.001);
//    }

//    @Test
//    public void cpcArrayMUpdate2JudWithKeyTest() throws Exception {
//
//        ArrayList<CpcArrayData> addList = new ArrayList<CpcArrayData>();
//        CpcArrayData add1 = new CpcArrayData(key, timestamp, item);
//        CpcArrayData add2 = new CpcArrayData(key, timestamp, item);
//        CpcArrayData add3 = new CpcArrayData(key,timestamp, item2);
//        addList.add(add1);
//        addList.add(add2);
//        addList.add(add3);
//
//        String addRet = tairCpcNew.cpcArrayMUpdate(addList);
//
////        Assert.assertEquals(2.0, addRet.get(key).getValue(), 0.1);
////        Assert.assertEquals(2.0, addRet.get(key).getDiffValue(), 0.1);
//    }

    @Test
    public void cpcArrayEstimateRangeTest() throws Exception {

        String addRet = tairCpcNew.cpcArrayUpdate(key, timestamp, item);
        assertEquals("OK", addRet);

//        addRet = tairCpcNew.cpcArrayUpdate2Jud(key, 6, item2, 5);
//        assertEquals(1.00, addRet.getValue(), 0.001);
//        assertEquals(1.00, addRet.getDiffValue(), 0.001);

        List<Double> estimateRet = tairCpcNew.cpcArrayEstimateRange(key, timestamp-1000, timestamp+1000);
        assertEquals(1.00, estimateRet.get(0), 0.001);

        Double getRet = tairCpcNew.cpcArrayEstimateTimeMerge(key, timestamp-1000, timestamp+1000);
        assertEquals(1.00, getRet, 0.001);
    }
//
//    @Test
//    public void cpcArrayEstimateRangeSumTest() throws Exception {
//
//        Update2JudResult addRet = tairCpcNew.cpcArrayUpdate2Jud(key, 1, item, 5);
//        assertEquals(1.00, addRet.getValue(), 0.001);
//        assertEquals(1.00, addRet.getDiffValue(), 0.001);
//        Update2JudResult addRet2 = tairCpcNew.cpcArrayUpdate2Jud(key, 1, item, 5);
//        assertEquals(1.00, addRet2.getValue(), 0.001);
//        assertEquals(0.00, addRet2.getDiffValue(), 0.001);
//        addRet = tairCpcNew.cpcArrayUpdate2Jud(key, 1, item2, 5);
//        assertEquals(2.00, addRet.getValue(), 0.001);
//        assertEquals(1.00, addRet.getDiffValue(), 0.001);
//        addRet = tairCpcNew.cpcArrayUpdate2Jud(key, 3, item, 5);
//        assertEquals(1.00, addRet.getValue(), 0.001);
//        assertEquals(1.00, addRet.getDiffValue(), 0.001);
//        addRet = tairCpcNew.cpcArrayUpdate2Jud(key, 5, item, 5);
//        assertEquals(1.00, addRet.getValue(), 0.001);
//        assertEquals(1.00, addRet.getDiffValue(), 0.001);
//
//        Double estimateRet = tairCpcNew.cpcArrayEstimateRangeSum(key, 5, 5);
//        assertEquals(4.00, estimateRet, 0.001);
//    }

    @Test
    public void sumTest() throws Exception {

        Double addRet = tairCpcNew.sumAdd(key, 100);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcNew.sumAdd(key, 150);
        assertEquals(250.00, addRet, 0.001);

        Double getRet = tairCpcNew.sumGet(key);
        assertEquals(250.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpcNew.sumAdd(key2, 100, cpcUpdateParams2);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcNew.sumAdd(key2, 150, cpcUpdateParams2);
        assertEquals(250.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcNew.sumGet(key2);
        assertEquals(0.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.px(2000);

        addRet = tairCpcNew.sumAdd(key3, 100, cpcUpdateParams3);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcNew.sumAdd(key3, 150, cpcUpdateParams3);
        assertEquals(250.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcNew.sumGet(key3);
        assertEquals(0.00, getRet, 0.001);

        Double setRet = tairCpcNew.sumSet(key, 100);
        assertEquals(100.00, setRet, 0.001);
    }

    @Test
    public void sumArrayTest() throws Exception {

        Double addRet = tairCpcNew.sumArrayAdd(key, timestamp, 100);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcNew.sumArrayAdd(key, timestamp, 300);
        assertEquals(400.00, addRet, 0.001);

        Double getRet = tairCpcNew.sumArrayGet(key, timestamp);
        assertEquals(400.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);
        cpcUpdateParams2.size(7);
        cpcUpdateParams2.win(winsize);

        addRet = tairCpcNew.sumArrayAdd(key2, timestamp, 100, cpcUpdateParams2);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcNew.sumArrayAdd(key2, timestamp, 300, cpcUpdateParams2);
        assertEquals(400.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcNew.sumArrayGet(key2, timestamp);
        assertNull(getRet);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.size(7);
        cpcUpdateParams3.win(winsize);

        addRet = tairCpcNew.sumArrayAdd(key3, timestamp, 100, cpcUpdateParams3);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcNew.sumArrayAdd(key3, timestamp, 300, cpcUpdateParams3);
        assertEquals(400.00, addRet, 0.001);

        addRet = tairCpcNew.sumArrayAdd(key3, timestamp-winsize, 300, cpcUpdateParams3);
        assertEquals(300.00, addRet, 0.001);

        getRet = tairCpcNew.sumArrayGetRangeTimeMerge(key3, timestamp-winsize, timestamp);
        assertEquals(700.00, getRet, 0.001);

        getRet = tairCpcNew.sumArrayGetRangeMerge(key3, timestamp, 2);
        assertEquals(700.00, getRet, 0.001);
    }

    @Test
    public void maxTest() throws Exception {

        Double addRet = tairCpcNew.maxAdd(key, 150);
        assertEquals(150.00, addRet, 0.001);

        addRet = tairCpcNew.maxAdd(key, 100);
        assertEquals(150.00, addRet, 0.001);

        Double getRet = tairCpcNew.maxGet(key);
        assertEquals(150.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpcNew.maxAdd(key2, 100, cpcUpdateParams2);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcNew.maxAdd(key2, 150, cpcUpdateParams2);
        assertEquals(150.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcNew.maxGet(key2);
        assertEquals(0.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.px(2000);

        addRet = tairCpcNew.maxAdd(key3, 100, cpcUpdateParams3);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcNew.maxAdd(key3, 150, cpcUpdateParams3);
        assertEquals(150.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcNew.maxGet(key3);
        assertEquals(0.00, getRet, 0.001);

        Double setRet = tairCpcNew.maxSet(key, 100);
        assertEquals(100.00, setRet, 0.001);
    }

    @Test
    public void maxArrayTest() throws Exception {

        Double addRet = tairCpcNew.maxArrayAdd(key, timestamp, 100);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcNew.maxArrayAdd(key, timestamp, 300);
        assertEquals(300.00, addRet, 0.001);

        Double getRet = tairCpcNew.maxArrayGet(key, timestamp);
        assertEquals(300.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);
        cpcUpdateParams2.size(7);
        cpcUpdateParams2.win(winsize);

        addRet = tairCpcNew.maxArrayAdd(key2, timestamp, 100, cpcUpdateParams2);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcNew.maxArrayAdd(key2, timestamp, 300, cpcUpdateParams2);
        assertEquals(300.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcNew.maxArrayGet(key2, timestamp);
        assertNull(getRet);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.size(7);
        cpcUpdateParams3.win(winsize);

        addRet = tairCpcNew.maxArrayAdd(key3, timestamp, 100, cpcUpdateParams3);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcNew.maxArrayAdd(key3, timestamp, 300, cpcUpdateParams3);
        assertEquals(300.00, addRet, 0.001);

        addRet = tairCpcNew.maxArrayAdd(key3, timestamp-winsize, 300, cpcUpdateParams3);
        assertEquals(300.00, addRet, 0.001);

        getRet = tairCpcNew.maxArrayGetRangeTimeMerge(key3, timestamp-winsize, timestamp);
        assertEquals(300.00, getRet, 0.001);

        getRet = tairCpcNew.maxArrayGetRangeMerge(key3, timestamp, 2);
        assertEquals(300.00, getRet, 0.001);
    }

    @Test
    public void minTest() throws Exception {

        Double addRet = tairCpcNew.minAdd(key, 150);
        assertEquals(150.00, addRet, 0.001);

        addRet = tairCpcNew.minAdd(key, 100);
        assertEquals(100.00, addRet, 0.001);

        Double getRet = tairCpcNew.minGet(key);
        assertEquals(100.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpcNew.minAdd(key2, 100, cpcUpdateParams2);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcNew.minAdd(key2, 150, cpcUpdateParams2);
        assertEquals(100.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcNew.minGet(key2);
        assertEquals(0.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.px(2000);

        addRet = tairCpcNew.minAdd(key3, 100, cpcUpdateParams3);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcNew.minAdd(key3, 150, cpcUpdateParams3);
        assertEquals(100.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcNew.minGet(key3);
        assertEquals(0.00, getRet, 0.001);

        Double setRet = tairCpcNew.minSet(key, 100);
        assertEquals(100.00, setRet, 0.001);
    }

    @Test
    public void minArrayTest() throws Exception {

        Double addRet = tairCpcNew.minArrayAdd(key, timestamp, 100);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcNew.minArrayAdd(key, timestamp, 300);
        assertEquals(100.00, addRet, 0.001);

        Double getRet = tairCpcNew.minArrayGet(key, timestamp);
        assertEquals(100.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);
        cpcUpdateParams2.size(7);
        cpcUpdateParams2.win(winsize);

        addRet = tairCpcNew.minArrayAdd(key2, timestamp, 100, cpcUpdateParams2);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcNew.minArrayAdd(key2, timestamp, 300, cpcUpdateParams2);
        assertEquals(100.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcNew.minArrayGet(key2, timestamp);
        assertNull(getRet);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.size(7);
        cpcUpdateParams3.win(winsize);

        addRet = tairCpcNew.minArrayAdd(key3, timestamp, 100, cpcUpdateParams3);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcNew.minArrayAdd(key3, timestamp, 300, cpcUpdateParams3);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcNew.minArrayAdd(key3, timestamp-winsize, 300, cpcUpdateParams3);
        assertEquals(300.00, addRet, 0.001);

        getRet = tairCpcNew.minArrayGetRangeTimeMerge(key3, timestamp-winsize, timestamp);
        assertEquals(100.00, getRet, 0.001);

        getRet = tairCpcNew.minArrayGetRangeMerge(key3, timestamp, 2);
        assertEquals(100.00, getRet, 0.001);
    }

    @Test
    public void firstTest() throws Exception {

        String addRet = tairCpcNew.firstAdd(key, content1, 150);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcNew.firstAdd(key, content2, 100);
        Assert.assertEquals(content2, addRet);

        String getRet = tairCpcNew.firstGet(key);
        Assert.assertEquals(content2, getRet);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpcNew.firstAdd(key2, content1, 100, cpcUpdateParams2);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcNew.firstAdd(key2, content2, 150, cpcUpdateParams2);
        Assert.assertEquals(content1, addRet);

        Thread.sleep(3000);

        getRet = tairCpcNew.firstGet(key2);
        Assert.assertNull(getRet);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.px(2000);

        addRet = tairCpcNew.firstAdd(key3, content1, 100, cpcUpdateParams3);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcNew.firstAdd(key3, content2, 150, cpcUpdateParams3);
        Assert.assertEquals(content1, addRet);

        Thread.sleep(3000);

        getRet = tairCpcNew.firstGet(key3);
        Assert.assertNull(getRet);

        String setRet = tairCpcNew.firstSet(key, content1, 100);
        Assert.assertEquals(content1, setRet);
    }

    @Test
    public void firstArrayTest() throws Exception {

        String addRet = tairCpcNew.firstArrayAdd(key, timestamp, content1, 100);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcNew.firstArrayAdd(key, timestamp, content2, 150);
        Assert.assertEquals(content1, addRet);

        String getRet = tairCpcNew.firstArrayGet(key, timestamp);
        Assert.assertEquals(content1, getRet);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpcNew.firstArrayAdd(key2, timestamp, content1, 100, cpcUpdateParams2);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcNew.firstArrayAdd(key2, timestamp, content2, 150, cpcUpdateParams2);
        Assert.assertEquals(content1, addRet);

        Thread.sleep(3000);

        getRet = tairCpcNew.firstArrayGet(key2, 1);
        Assert.assertNull(getRet);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.size(7);
        cpcUpdateParams3.win(winsize);

        addRet = tairCpcNew.firstArrayAdd(key3, timestamp, content1, 100, cpcUpdateParams3);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcNew.firstArrayAdd(key3, timestamp, content1, 300, cpcUpdateParams3);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcNew.firstArrayAdd(key3, timestamp-winsize, content1, 300, cpcUpdateParams3);
        Assert.assertEquals(content1, addRet);

        getRet = tairCpcNew.firstArrayGetRangeTimeMerge(key3, timestamp-winsize, timestamp);
        Assert.assertEquals(content1, addRet);

        getRet = tairCpcNew.firstArrayGetRangeMerge(key3, timestamp, 2);
        Assert.assertEquals(content1, addRet);
    }

    @Test
    public void lastTest() throws Exception {

        String addRet = tairCpcNew.lastAdd(key, content1, 150);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcNew.lastAdd(key, content2, 100);
        Assert.assertEquals(content1, addRet);

        String getRet = tairCpcNew.lastGet(key);
        Assert.assertEquals(content1, getRet);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpcNew.lastAdd(key2, content1, 100, cpcUpdateParams2);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcNew.lastAdd(key2, content2, 150, cpcUpdateParams2);
        Assert.assertEquals(content2, addRet);

        Thread.sleep(3000);

        getRet = tairCpcNew.lastGet(key2);
        Assert.assertNull(getRet);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.px(2000);

        addRet = tairCpcNew.lastAdd(key3, content1, 100, cpcUpdateParams3);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcNew.lastAdd(key3, content2, 150, cpcUpdateParams3);
        Assert.assertEquals(content2, addRet);

        Thread.sleep(3000);

        getRet = tairCpcNew.lastGet(key3);
        Assert.assertNull(getRet);

        String setRet = tairCpcNew.lastSet(key, content1, 120);
        Assert.assertEquals(content1, setRet);
    }

    @Test
    public void lastArrayTest() throws Exception {

        String addRet = tairCpcNew.lastArrayAdd(key, timestamp, content1, 100);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcNew.lastArrayAdd(key, timestamp, content2, 150);
        Assert.assertEquals(content2, addRet);

        String getRet = tairCpcNew.lastArrayGet(key, timestamp);
        Assert.assertEquals(content2, getRet);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpcNew.lastArrayAdd(key2, timestamp, content1, 100, cpcUpdateParams2);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcNew.lastArrayAdd(key2, timestamp, content2, 150, cpcUpdateParams2);
        Assert.assertEquals(content2, addRet);

        Thread.sleep(3000);

        getRet = tairCpcNew.lastArrayGet(key2, 1);
        Assert.assertNull(getRet);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.size(7);
        cpcUpdateParams3.win(winsize);

        addRet = tairCpcNew.lastArrayAdd(key3, timestamp, content1, 100, cpcUpdateParams3);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcNew.lastArrayAdd(key3, timestamp, content1, 300, cpcUpdateParams3);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcNew.lastArrayAdd(key3, timestamp-winsize, content1, 300, cpcUpdateParams3);
        Assert.assertEquals(content1, addRet);

        getRet = tairCpcNew.lastArrayGetRangeTimeMerge(key3, timestamp-winsize, timestamp);
        Assert.assertEquals(content1, addRet);

        getRet = tairCpcNew.lastArrayGetRangeMerge(key3, timestamp, 2);
        Assert.assertEquals(content1, addRet);
    }

    @Test
    public void avgTest() throws Exception {

        Double addRet = tairCpcNew.avgAdd(key, count1, 150);
        assertEquals(150, addRet, 0.001);

        addRet = tairCpcNew.avgAdd(key, count2, 100);
        assertEquals(116.666, addRet, 0.001);

        Double getRet = tairCpcNew.avgGet(key);
        assertEquals(116.666, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpcNew.avgAdd(key2, count1, 150, cpcUpdateParams2);
        assertEquals(150, addRet, 0.001);

        addRet = tairCpcNew.avgAdd(key2, count2, 100, cpcUpdateParams2);
        assertEquals(116.666, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcNew.avgGet(key2);
        assertEquals(0.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.px(2000);

        addRet = tairCpcNew.avgAdd(key3, count1, 150, cpcUpdateParams3);
        assertEquals(150, addRet, 0.001);

        addRet = tairCpcNew.avgAdd(key3, count2, 100, cpcUpdateParams3);
        assertEquals(116.666, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcNew.avgGet(key3);
        assertEquals(0.00, getRet, 0.001);

        Double setRet = tairCpcNew.avgSet(key, count1, 150);
        assertEquals(150, setRet, 0.001);


        addRet = tairCpcNew.avgAdd(bkey, count1, 150);
        assertEquals(150, addRet, 0.001);

        addRet = tairCpcNew.avgAdd(bkey, count2, 100);
        assertEquals(116.666, addRet, 0.001);

        getRet = tairCpcNew.avgGet(bkey);
        assertEquals(116.666, getRet, 0.001);

        addRet = tairCpcNew.avgAdd(bkey2, count1, 150, cpcUpdateParams2);
        assertEquals(150, addRet, 0.001);

        addRet = tairCpcNew.avgAdd(bkey2, count2, 100, cpcUpdateParams2);
        assertEquals(116.666, addRet, 0.001);
    }

    @Test
    public void avgArrayTest() throws Exception {

        Double addRet = tairCpcNew.avgArrayAdd(key, timestamp, count1, 100);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcNew.avgArrayAdd(key, timestamp, count1, 300);
        assertEquals(200.00, addRet, 0.001);

        Double getRet = tairCpcNew.avgArrayGet(key, timestamp);
        assertEquals(200.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);
        cpcUpdateParams2.size(7);
        cpcUpdateParams2.win(winsize);

        addRet = tairCpcNew.avgArrayAdd(key2, timestamp, count1, 100, cpcUpdateParams2);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcNew.avgArrayAdd(key2, timestamp, count1, 300, cpcUpdateParams2);
        assertEquals(200.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcNew.avgArrayGet(key2, timestamp);
        assertNull(getRet);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.size(7);
        cpcUpdateParams3.win(winsize);

        addRet = tairCpcNew.avgArrayAdd(key3, timestamp, count1, 100, cpcUpdateParams3);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcNew.avgArrayAdd(key3, timestamp, count1, 300, cpcUpdateParams3);
        assertEquals(200.00, addRet, 0.001);

        addRet = tairCpcNew.avgArrayAdd(key3, timestamp-winsize, count2, 300, cpcUpdateParams3);
        assertEquals(300.00, addRet, 0.001);

        getRet = tairCpcNew.avgArrayGetRangeTimeMerge(key3, timestamp-winsize, timestamp);
        assertEquals(250.00, getRet, 0.001);

        getRet = tairCpcNew.avgArrayGetRangeMerge(key3, timestamp, 2);
        assertEquals(250.00, getRet, 0.001);

    }

    @Test
    public void stddevTest() throws Exception {

        Double addRet = tairCpcNew.stddevAdd(key, 150);
        assertEquals(0.00, addRet, 0.001);

        addRet = tairCpcNew.stddevAdd(key, 100);
        assertEquals(25.00, addRet, 0.001);

        Double getRet = tairCpcNew.stddevGet(key);
        assertEquals(25.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpcNew.stddevAdd(key2, 100, cpcUpdateParams2);
        assertEquals(0.00, addRet, 0.001);

        addRet = tairCpcNew.stddevAdd(key2, 150, cpcUpdateParams2);
        assertEquals(25.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcNew.stddevGet(key2);
        assertEquals(0.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.px(2000);

        addRet = tairCpcNew.stddevAdd(key3, 100, cpcUpdateParams3);
        assertEquals(0.00, addRet, 0.001);

        addRet = tairCpcNew.stddevAdd(key3, 150, cpcUpdateParams3);
        assertEquals(25.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcNew.stddevGet(key3);
        assertEquals(0.00, getRet, 0.001);

//        Double setRet = tairCpcNew.stddevSet(key, 1, 100, 0);
//        assertEquals(0.00, setRet, 0.001);
    }

    @Test
    public void stddevArrayTest() throws Exception {

        Double addRet = tairCpcNew.stddevArrayAdd(key, timestamp, 100);
        assertEquals(0.00, addRet, 0.001);

        addRet = tairCpcNew.stddevArrayAdd(key, timestamp, 300);
        assertEquals(100.00, addRet, 0.001);

        Double getRet = tairCpcNew.stddevArrayGet(key, timestamp);
        assertEquals(100.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);
        cpcUpdateParams2.size(7);
        cpcUpdateParams2.win(winsize);

        addRet = tairCpcNew.stddevArrayAdd(key2, timestamp, 100, cpcUpdateParams2);
        assertEquals(0.00, addRet, 0.001);

        addRet = tairCpcNew.stddevArrayAdd(key2, timestamp, 300, cpcUpdateParams2);
        assertEquals(100.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcNew.stddevArrayGet(key2, timestamp);
        assertNull(getRet);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.size(7);
        cpcUpdateParams3.win(winsize);

        addRet = tairCpcNew.stddevArrayAdd(key3, timestamp, 100, cpcUpdateParams3);
        assertEquals(0.00, addRet, 0.001);

        addRet = tairCpcNew.stddevArrayAdd(key3, timestamp, 300, cpcUpdateParams3);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcNew.stddevArrayAdd(key3, timestamp-winsize, 300, cpcUpdateParams3);
        assertEquals(0.00, addRet, 0.001);

        getRet = tairCpcNew.stddevArrayGetRangeTimeMerge(key3, timestamp-winsize, timestamp);
        assertEquals(94.2809041, getRet, 0.001);

        getRet = tairCpcNew.stddevArrayGetRangeMerge(key3, timestamp, 2);
        assertEquals(94.2809041, getRet, 0.001);
    }

    @Test
    public void sketchesTest() throws Exception {

        Double addRet = tairCpcNew.avgArrayAdd(key, timestamp, count1, 100);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcNew.avgArrayAdd(key, timestamp, count1, 300);
        assertEquals(200.00, addRet, 0.001);

        Double getRet = tairCpcNew.avgArrayGet(key, timestamp);
        assertEquals(200.00, getRet, 0.001);


        addRet = tairCpcNew.stddevArrayAdd(key2, timestamp, 100);
        assertEquals(0.00, addRet, 0.001);

        addRet = tairCpcNew.stddevArrayAdd(key2, timestamp, 300);
        assertEquals(100.00, addRet, 0.001);

        getRet = tairCpcNew.stddevArrayGet(key2, timestamp);
        assertEquals(100.00, getRet, 0.001);

        Object sketGetRet = tairCpcNew.sketchesGet(key, timestamp);
        Double value= (Double) sketGetRet;
        assertEquals(200.00, value, 0.001);

        Object sketMergeRet = tairCpcNew.sketchesGetRangeMerge(key, timestamp-winsize, timestamp);
        value= (Double) sketMergeRet;
        assertEquals(200.00, value, 0.001);

//        List<Object> sketRangeRet = tairCpcNew.sketchesGetRange(key, timestamp-winsize, timestamp);
//        getValueStr = new String((byte[]) sketRangeRet.get(0));
//        value= Double.parseDouble(getValueStr);
//        assertEquals(200.00, value, 0.001);
    }

    @Test
    public void sketchesBatchWriteTest() throws Exception {
        CpcArrayMultiData multiData = CpcDataUtil.buildCpc(key, "sffjls", timestamp);
        multiData.setSize(10);
        multiData.setWinSize(10);
        CpcArrayMultiData multiData2 = CpcDataUtil.buildSum(key2, 2, timestamp);
        CpcArrayMultiData multiData3 = CpcDataUtil.buildFirst(key3, content1, 100, timestamp);
        ArrayList<CpcArrayMultiData> list = new ArrayList<>();
        list.add(multiData);
        list.add(multiData2);
        list.add(multiData3);
        String res = tairCpcNew.sketchesBatchWrite(list);
        assertEquals("OK", res);

        Object estimateRet = tairCpcNew.sketchesGet(key, timestamp);
        if (estimateRet instanceof Double) {
            assertEquals(1, Double.parseDouble(estimateRet.toString()), 0.001);
        }
        else if (estimateRet instanceof String) {
            Assert.assertEquals(content1, estimateRet);
        }

        estimateRet = tairCpcNew.sketchesGet(key3, timestamp);
        if (estimateRet instanceof Double) {
            assertEquals(1, Double.parseDouble(estimateRet.toString()), 0.001);
        }
        else if (estimateRet instanceof String) {
            Assert.assertEquals(content1, estimateRet);
        }

        List<Double> rangeRet = tairCpcNew.sketchesGetRange(key2, timestamp-winsize, timestamp);
        assertEquals(2, rangeRet.get(0), 0.001);

//        rangeRet = tairCpcNew.sketchesGetRange(key3, timestamp-winsize, timestamp);
//        assertEquals(2, rangeRet.get(0), 0.001);

//        List<Object> rangeRet = tairCpcNew.sketchesRange(key, timestamp-winsize, timestamp);
//        assertEquals(1, rangeRet.get(0));
    }

}
