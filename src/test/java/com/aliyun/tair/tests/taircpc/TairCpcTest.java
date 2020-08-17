package com.aliyun.tair.tests.taircpc;

import com.aliyun.tair.taircpc.params.CpcArrayData;
import com.aliyun.tair.taircpc.params.CpcData;
import com.aliyun.tair.taircpc.params.CpcUpdateParams;
import com.aliyun.tair.taircpc.results.Update2JudResult;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static redis.clients.jedis.Protocol.toByteArray;

public class TairCpcTest extends TairCpcTestBase {
    private String key;
    private String key2;
    private String key3;
    private String item;
    private String item2;
    private String item3;
    private String item4;
    private byte[] bkey;
    private byte[] bitem;
    private byte[] bitem2;

    public TairCpcTest() {
        key = "key" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        key2 = "key2" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        key3 = "key3" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        item = "item" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        item2 = "item2" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        item3 = "item3" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        item4 = "item4" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        bkey = ("bkey" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        bitem = ("bitem" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        bitem2 = ("bitem2" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
    }

    @Test
    public void cpcupdateTest() throws Exception {

        String addRet = tairCpc.cpcUpdate(key, item);
        Assert.assertEquals("OK", addRet);

        String addRetByte = tairCpc.cpcUpdate(bkey, bitem);
        Assert.assertEquals("OK", addRetByte);
    }

    @Test
    public void cpcupdateExpireTest() throws Exception {

        CpcUpdateParams cpcUpdateParams = new CpcUpdateParams();
        cpcUpdateParams.ex(2);
        String addRet = tairCpc.cpcUpdate(key, item, cpcUpdateParams);
        Assert.assertEquals("OK", addRet);
        addRet = tairCpc.cpcUpdate(key, item2);
        Assert.assertEquals("OK", addRet);

        Double estimateRet = tairCpc.cpcEstimate(key);
        assertEquals(2.00, estimateRet, 0.001);

        Thread.sleep(2000);

        estimateRet = tairCpc.cpcEstimate(key);
        assertNull(estimateRet);

        addRet = tairCpc.cpcUpdate(key, item);
        Assert.assertEquals("OK", addRet);
        cpcUpdateParams.ex(0);
        addRet = tairCpc.cpcUpdate(key, item2, cpcUpdateParams);
        Assert.assertEquals("OK", addRet);

        Thread.sleep(1000);
        estimateRet = tairCpc.cpcEstimate(key);
        assertEquals(2.00, estimateRet, 0.001);

        cpcUpdateParams.ex(2);
        addRet = tairCpc.cpcUpdate(key, item2, cpcUpdateParams);

        cpcUpdateParams.ex(-1);
        addRet = tairCpc.cpcUpdate(key, item2, cpcUpdateParams);

        Thread.sleep(2000);
        estimateRet = tairCpc.cpcEstimate(key);
        assertEquals(2.00, estimateRet, 0.001);

        cpcUpdateParams.ex(2);
        addRet = tairCpc.cpcUpdate(bkey, bitem, cpcUpdateParams);
        Assert.assertEquals("OK", addRet);
        addRet = tairCpc.cpcUpdate(bkey, bitem2);
        Assert.assertEquals("OK", addRet);

        estimateRet = tairCpc.cpcEstimate(bkey);
        assertEquals(2.00, estimateRet, 0.01);

        Thread.sleep(2000);

        estimateRet = tairCpc.cpcEstimate(bkey);
        assertNull(estimateRet);

        addRet = tairCpc.cpcUpdate(bkey, bitem);
        Assert.assertEquals("OK", addRet);
        cpcUpdateParams.ex(0);
        addRet = tairCpc.cpcUpdate(bkey, bitem2, cpcUpdateParams);
        Assert.assertEquals("OK", addRet);

        Thread.sleep(1000);
        estimateRet = tairCpc.cpcEstimate(bkey);
        assertEquals(2.00, estimateRet, 0.001);

        cpcUpdateParams.ex(2);
        addRet = tairCpc.cpcUpdate(bkey, bitem2, cpcUpdateParams);

        cpcUpdateParams.ex(-1);
        addRet = tairCpc.cpcUpdate(bkey, bitem2, cpcUpdateParams);

        Thread.sleep(2000);
        estimateRet = tairCpc.cpcEstimate(bkey);
        assertEquals(2.00, estimateRet, 0.001);
    }

    @Test
    public void cpcupdate2judTest() throws Exception {

        String addRet = tairCpc.cpcUpdate(key, item);
        Assert.assertEquals("OK", addRet);

        addRet = tairCpc.cpcUpdate(key, item2);
        Assert.assertEquals("OK", addRet);

        addRet = tairCpc.cpcUpdate(key, item3);
        Assert.assertEquals("OK", addRet);

        Double estimateRet = tairCpc.cpcEstimate(key);
        assertEquals(3.00, estimateRet, 0.001);

        Update2JudResult judRet = tairCpc.cpcUpdate2Jud(key, item);
        assertEquals(3.00, judRet.getValue(), 0.001);
        assertEquals(0.00, judRet.getDiffValue(), 0.001);

        judRet = tairCpc.cpcUpdate2Jud(key, item4);
        assertEquals(4.00, judRet.getValue(), 0.001);
        assertEquals(1.00, judRet.getDiffValue(), 0.001);
    }

    @Test
    public void cpcAccurateEstimationTest() throws Exception {

        int testnum = 120;
        String addRet = null;
        for (int i = 0; i < testnum; i++) {
            String itemtmp = item + String.valueOf(i);
            addRet = tairCpc.cpcUpdate(key, itemtmp);
            Assert.assertEquals("OK", addRet);
        }

        Double estimateRet = tairCpc.cpcEstimate(key);
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

        String addRet = tairCpc.cpcMUpdate(addList);
        Assert.assertEquals("OK", addRet);
        Double estimateRet = tairCpc.cpcEstimate(key);
        assertEquals(1.00, estimateRet, 0.1);
        Thread.sleep(5 * 1000);
        estimateRet = tairCpc.cpcEstimate(key);
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

        List<Double> addRet = tairCpc.cpcMUpdate2Est(addList);
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

        List<Update2JudResult> addRet = tairCpc.cpcMUpdate2Jud(addList);
        for (int j = 0; j < addRet.size() -1; j++) {
            Assert.assertEquals(1.0, addRet.get(j).getValue(), 0.1);
            Assert.assertEquals(1.0, addRet.get(j).getDiffValue(), 0.1);
        }
        Assert.assertEquals(2.0, addRet.get(2).getValue(), 0.1);
        Assert.assertEquals(1.0, addRet.get(2).getDiffValue(), 0.1);
    }

//    @Test
//    public void cpcMupdate2EstWithKeyTest() throws Exception {
//
//        ArrayList<CpcData> addList = new ArrayList<CpcData>();
//        CpcData add1 = new CpcData(key, item);
//        CpcData add2 = new CpcData(key2, item2);
//        CpcData add3 = new CpcData(key, item2);
//        addList.add(add1);
//        addList.add(add2);
//        addList.add(add3);
//
//        HashMap<String, Double> addRet = tairCpc.cpcMUpdate2EstWithKey(addList);
////        Assert.assertEquals(1.0, addRet.get(0).getValue(), 0.1);
//        Assert.assertEquals(1.0, addRet.get(key2), 0.1);
//        Assert.assertEquals(2.0, addRet.get(key), 0.1);
//    }
//
//    @Test
//    public void cpcMupdate2JudWithKeyTest() throws Exception {
//
//        ArrayList<CpcData> addList = new ArrayList<CpcData>();
//        CpcData add1 = new CpcData(key, item);
//        CpcData add2 = new CpcData(key2, item2);
//        CpcData add3 = new CpcData(key, item2);
//        addList.add(add1);
//        addList.add(add2);
//        addList.add(add3);
//
//        HashMap<String, Update2JudResult> addRet = tairCpc.cpcMUpdate2JudWithKey(addList);
//
//        Assert.assertEquals(1.0, addRet.get(key2).getValue(), 0.1);
//        Assert.assertEquals(1.0, addRet.get(key2).getDiffValue(), 0.1);
//        Assert.assertEquals(2.0, addRet.get(key).getValue(), 0.1);
//        Assert.assertEquals(1.0, addRet.get(key).getDiffValue(), 0.1);
//    }

    @Test
    public void cpcArrayUpdateTest() throws Exception {

        String addRet = tairCpc.cpcArrayUpdate(key, 1, item, 5);
        Assert.assertEquals("OK", addRet);
        addRet = tairCpc.cpcArrayUpdate(key, 1, item2, 5);
        Assert.assertEquals("OK", addRet);
        addRet = tairCpc.cpcArrayUpdate(key, 3, item, 5);
        Assert.assertEquals("OK", addRet);
        addRet = tairCpc.cpcArrayUpdate(key, 5, item, 5);
        Assert.assertEquals("OK", addRet);

        Double estimateRet = tairCpc.cpcArrayEstimate(key, 1);
        assertEquals(2.00, estimateRet, 0.001);
        estimateRet = tairCpc.cpcArrayEstimate(key, 3);
        assertEquals(1.00, estimateRet, 0.001);
        estimateRet = tairCpc.cpcArrayEstimate(key, 5);
        assertEquals(1.00, estimateRet, 0.001);
    }

    @Test
    public void cpcArrayUpdate2EstTest() throws Exception {

        Double addRet = tairCpc.cpcArrayUpdate2Est(key, 1, item, 5);
        assertEquals(1.00, addRet, 0.001);
        addRet = tairCpc.cpcArrayUpdate2Est(key, 1, item2, 5);
        assertEquals(2.00, addRet, 0.001);
        addRet = tairCpc.cpcArrayUpdate2Est(key, 3, item, 5);
        assertEquals(1.00, addRet, 0.001);
        addRet = tairCpc.cpcArrayUpdate2Est(key, 5, item, 5);
        assertEquals(1.00, addRet, 0.001);

        addRet = tairCpc.cpcArrayUpdate2Est(key, 7, item2, 5);
        assertEquals(1.00, addRet, 0.001);

        Double estimateRet = tairCpc.cpcArrayEstimate(key, 7);
        assertEquals(1.00, estimateRet, 0.001);
        estimateRet = tairCpc.cpcArrayEstimate(key, 3);
        assertEquals(1.00, estimateRet, 0.001);
        estimateRet = tairCpc.cpcArrayEstimate(key, 5);
        assertEquals(1.00, estimateRet, 0.001);
    }

    @Test
    public void cpcArrayUpdate2JudTest() throws Exception {

        Update2JudResult addRet = tairCpc.cpcArrayUpdate2Jud(key, 1, item, 5);
        assertEquals(1.00, addRet.getValue(), 0.001);
        assertEquals(1.00, addRet.getDiffValue(), 0.001);
        Update2JudResult addRet2 = tairCpc.cpcArrayUpdate2Jud(key, 1, item, 5);
        assertEquals(1.00, addRet2.getValue(), 0.001);
        assertEquals(0.00, addRet2.getDiffValue(), 0.001);
        addRet = tairCpc.cpcArrayUpdate2Jud(key, 1, item2, 5);
        assertEquals(2.00, addRet.getValue(), 0.001);
        assertEquals(1.00, addRet.getDiffValue(), 0.001);
        addRet = tairCpc.cpcArrayUpdate2Jud(key, 3, item, 5);
        assertEquals(1.00, addRet.getValue(), 0.001);
        assertEquals(1.00, addRet.getDiffValue(), 0.001);
        addRet = tairCpc.cpcArrayUpdate2Jud(key, 5, item, 5);
        assertEquals(1.00, addRet.getValue(), 0.001);
        assertEquals(1.00, addRet.getDiffValue(), 0.001);

        addRet = tairCpc.cpcArrayUpdate2Jud(key, 7, item2, 5);
        assertEquals(1.00, addRet.getValue(), 0.001);
        assertEquals(1.00, addRet.getDiffValue(), 0.001);

        Double estimateRet = tairCpc.cpcArrayEstimate(key, 7);
        assertEquals(1.00, estimateRet, 0.001);
        estimateRet = tairCpc.cpcArrayEstimate(key, 3);
        assertEquals(1.00, estimateRet, 0.001);
        estimateRet = tairCpc.cpcArrayEstimate(key, 5);
        assertEquals(1.00, estimateRet, 0.001);
    }

//    @Test
//    public void cpcArrayMUpdate2JudWithKeyTest() throws Exception {
//
//        ArrayList<CpcArrayData> addList = new ArrayList<CpcArrayData>();
//        CpcArrayData add1 = new CpcArrayData(key, 1, item, 5);
//        CpcArrayData add2 = new CpcArrayData(key, 1, item, 5);
//        CpcArrayData add3 = new CpcArrayData(key,1, item2, 5);
//        addList.add(add1);
//        addList.add(add2);
//        addList.add(add3);
//
//        HashMap<String, Update2JudResult> addRet = tairCpc.cpcArrayMUpdate2JudWithKey(addList);
//
//        Assert.assertEquals(2.0, addRet.get(key).getValue(), 0.1);
//        Assert.assertEquals(2.0, addRet.get(key).getDiffValue(), 0.1);
//    }

    @Test
    public void cpcArrayEstimateRangeTest() throws Exception {

        Update2JudResult addRet = tairCpc.cpcArrayUpdate2Jud(key, 1, item, 5);
        assertEquals(1.00, addRet.getValue(), 0.001);
        assertEquals(1.00, addRet.getDiffValue(), 0.001);
        Update2JudResult addRet2 = tairCpc.cpcArrayUpdate2Jud(key, 1, item, 5);
        assertEquals(1.00, addRet2.getValue(), 0.001);
        assertEquals(0.00, addRet2.getDiffValue(), 0.001);
        addRet = tairCpc.cpcArrayUpdate2Jud(key, 1, item2, 5);
        assertEquals(2.00, addRet.getValue(), 0.001);
        assertEquals(1.00, addRet.getDiffValue(), 0.001);
        addRet = tairCpc.cpcArrayUpdate2Jud(key, 3, item, 5);
        assertEquals(1.00, addRet.getValue(), 0.001);
        assertEquals(1.00, addRet.getDiffValue(), 0.001);
        addRet = tairCpc.cpcArrayUpdate2Jud(key, 5, item, 5);
        assertEquals(1.00, addRet.getValue(), 0.001);
        assertEquals(1.00, addRet.getDiffValue(), 0.001);

//        addRet = tairCpc.cpcArrayUpdate2Jud(key, 6, item2, 5);
//        assertEquals(1.00, addRet.getValue(), 0.001);
//        assertEquals(1.00, addRet.getDiffValue(), 0.001);

        List<Double> estimateRet = tairCpc.cpcArrayEstimateRange(key, 5, 5);
//        System.out.println(estimateRet.toString());
        assertEquals(2.00, estimateRet.get(0), 0.001);
        assertEquals(0.00, estimateRet.get(1), 0.001);
        assertEquals(1.00, estimateRet.get(2), 0.001);
        assertEquals(0.00, estimateRet.get(3), 0.001);
        assertEquals(1.00, estimateRet.get(4), 0.001);
    }

    @Test
    public void sumTest() throws Exception {

        Double addRet = tairCpc.sumAdd(key, 100);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpc.sumAdd(key, 150);
        assertEquals(250.00, addRet, 0.001);

        Double getRet = tairCpc.sumGet(key);
        assertEquals(250.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpc.sumAdd(key2, 100, cpcUpdateParams2);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpc.sumAdd(key2, 150, cpcUpdateParams2);
        assertEquals(250.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpc.sumGet(key2);
        assertEquals(0.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.px(2000);

        addRet = tairCpc.sumAdd(key3, 100, cpcUpdateParams3);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpc.sumAdd(key3, 150, cpcUpdateParams3);
        assertEquals(250.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpc.sumGet(key3);
        assertEquals(0.00, getRet, 0.001);

        Double setRet = tairCpc.sumSet(key, 100);
        assertEquals(100.00, setRet, 0.001);
    }

    @Test
    public void sumArrayTest() throws Exception {

        Double addRet = tairCpc.sumArrayAdd(key, 1, 100, 7);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpc.sumArrayAdd(key, 1, 150, 7);
        assertEquals(250.00, addRet, 0.001);

        Double getRet = tairCpc.sumArrayGet(key, 1);
        assertEquals(250.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpc.sumArrayAdd(key2, 1, 100, 7, cpcUpdateParams2);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpc.sumArrayAdd(key2, 1, 150, 7, cpcUpdateParams2);
        assertEquals(250.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpc.sumArrayGet(key2, 1);
        assertNull(getRet);
//        assertEquals(0.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.px(2000);

        addRet = tairCpc.sumArrayAdd(key3, 2, 100, 7, cpcUpdateParams3);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpc.sumArrayAdd(key3, 2, 150, 7, cpcUpdateParams3);
        assertEquals(250.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpc.sumArrayGet(key3, 2);
        assertNull(getRet);
//        assertEquals(0.00, getRet, 0.001);

        List<Double> rangeRet = tairCpc.sumArrayGetRange(key, 7, 7);
        assertEquals(7, rangeRet.size());
        assertEquals(250.00, rangeRet.get(0), 0.001);

        Double mergeRet = tairCpc.sumArrayGetRangeMerge(key, 7, 7);
        assertEquals(250.00, mergeRet, 0.001);
    }

    @Test
    public void maxTest() throws Exception {

        Double addRet = tairCpc.maxAdd(key, 150);
        assertEquals(150.00, addRet, 0.001);

        addRet = tairCpc.maxAdd(key, 100);
        assertEquals(150.00, addRet, 0.001);

        Double getRet = tairCpc.maxGet(key);
        assertEquals(150.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpc.maxAdd(key2, 100, cpcUpdateParams2);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpc.maxAdd(key2, 150, cpcUpdateParams2);
        assertEquals(150.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpc.maxGet(key2);
        assertEquals(0.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.px(2000);

        addRet = tairCpc.maxAdd(key3, 100, cpcUpdateParams3);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpc.maxAdd(key3, 150, cpcUpdateParams3);
        assertEquals(150.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpc.maxGet(key3);
        assertEquals(0.00, getRet, 0.001);

        Double setRet = tairCpc.maxSet(key, 100);
        assertEquals(100.00, setRet, 0.001);
    }

    @Test
    public void maxArrayTest() throws Exception {

        Double addRet = tairCpc.maxArrayAdd(key, 1, 100, 7);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpc.maxArrayAdd(key, 1, 150, 7);
        assertEquals(150.00, addRet, 0.001);

        Double getRet = tairCpc.maxArrayGet(key, 1);
        assertEquals(150.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpc.maxArrayAdd(key2, 1, 100, 7, cpcUpdateParams2);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpc.maxArrayAdd(key2, 1, 150, 7, cpcUpdateParams2);
        assertEquals(150.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpc.maxArrayGet(key2, 1);
        assertNull(getRet);
//        assertEquals(0.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.px(2000);

        addRet = tairCpc.maxArrayAdd(key3, 2, 100, 7, cpcUpdateParams3);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpc.maxArrayAdd(key3, 2, 150, 7, cpcUpdateParams3);
        assertEquals(150.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpc.maxArrayGet(key3, 2);
        assertNull(getRet);
//        assertEquals(0.00, getRet, 0.001);

        List<Double> rangeRet = tairCpc.maxArrayGetRange(key, 7, 7);
        assertEquals(7, rangeRet.size());
        assertEquals(150.00, rangeRet.get(0), 0.001);

        Double mergeRet = tairCpc.maxArrayGetRangeMerge(key, 7, 7);
        assertEquals(150.00, mergeRet, 0.001);
    }

    @Test
    public void minTest() throws Exception {

        Double addRet = tairCpc.minAdd(key, 150);
        assertEquals(150.00, addRet, 0.001);

        addRet = tairCpc.minAdd(key, 100);
        assertEquals(100.00, addRet, 0.001);

        Double getRet = tairCpc.minGet(key);
        assertEquals(100.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpc.minAdd(key2, 100, cpcUpdateParams2);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpc.minAdd(key2, 150, cpcUpdateParams2);
        assertEquals(100.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpc.minGet(key2);
        assertEquals(0.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.px(2000);

        addRet = tairCpc.minAdd(key3, 100, cpcUpdateParams3);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpc.minAdd(key3, 150, cpcUpdateParams3);
        assertEquals(100.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpc.minGet(key3);
        assertEquals(0.00, getRet, 0.001);

        Double setRet = tairCpc.minSet(key, 100);
        assertEquals(100.00, setRet, 0.001);
    }

    @Test
    public void minArrayTest() throws Exception {

        Double addRet = tairCpc.minArrayAdd(key, 1, 100, 7);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpc.minArrayAdd(key, 1, 150, 7);
        assertEquals(150.00, addRet, 0.001);

        Double getRet = tairCpc.minArrayGet(key, 1);
        assertEquals(150.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpc.minArrayAdd(key2, 1, 100, 7, cpcUpdateParams2);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpc.minArrayAdd(key2, 1, 150, 7, cpcUpdateParams2);
        assertEquals(150.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpc.minArrayGet(key2, 1);
        assertNull(getRet);
//        assertEquals(0.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.px(2000);

        addRet = tairCpc.minArrayAdd(key3, 2, 100, 7, cpcUpdateParams3);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpc.minArrayAdd(key3, 2, 150, 7, cpcUpdateParams3);
        assertEquals(150.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpc.minArrayGet(key3, 2);
        assertNull(getRet);
//        assertEquals(0.00, getRet, 0.001);

        List<Double> rangeRet = tairCpc.minArrayGetRange(key, 7, 7);
        assertEquals(7, rangeRet.size());
        assertEquals(150.00, rangeRet.get(0), 0.001);

        Double mergeRet = tairCpc.minArrayGetRangeMerge(key, 7, 7);
        assertEquals(150.00, mergeRet, 0.001);
    }
}
