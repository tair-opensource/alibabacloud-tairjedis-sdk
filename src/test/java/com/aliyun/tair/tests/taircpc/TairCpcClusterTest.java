package com.aliyun.tair.tests.taircpc;

import com.aliyun.tair.taircpc.params.CpcArrayData;
import com.aliyun.tair.taircpc.params.CpcData;
import com.aliyun.tair.taircpc.params.CpcUpdateParams;
import com.aliyun.tair.taircpc.results.Update2JudResult;
import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.*;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TairCpcClusterTest extends TairCpcTestBase {
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

    public TairCpcClusterTest() {
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
        count2 = 300;
    }

    @Test
    public void cpcupdateTest() throws Exception {

        String addRet = tairCpcCluster.cpcUpdate(key, item);
        Assert.assertEquals("OK", addRet);

        String addRetByte = tairCpcCluster.cpcUpdate(bkey, bitem);
        Assert.assertEquals("OK", addRetByte);
    }

    @Test
    public void cpcupdateExpireTest() throws Exception {

        CpcUpdateParams cpcUpdateParams = new CpcUpdateParams();
        cpcUpdateParams.ex(2);
        String addRet = tairCpcCluster.cpcUpdate(key, item, cpcUpdateParams);
        Assert.assertEquals("OK", addRet);
        addRet = tairCpcCluster.cpcUpdate(key, item2);
        Assert.assertEquals("OK", addRet);

        Double estimateRet = tairCpcCluster.cpcEstimate(key);
        assertEquals(2.00, estimateRet, 0.001);

        Thread.sleep(2000);

        estimateRet = tairCpcCluster.cpcEstimate(key);
        assertNull(estimateRet);

        addRet = tairCpcCluster.cpcUpdate(key, item);
        Assert.assertEquals("OK", addRet);
        cpcUpdateParams.ex(0);
        addRet = tairCpcCluster.cpcUpdate(key, item2, cpcUpdateParams);
        Assert.assertEquals("OK", addRet);

        Thread.sleep(1000);
        estimateRet = tairCpcCluster.cpcEstimate(key);
        assertEquals(2.00, estimateRet, 0.001);

        cpcUpdateParams.ex(2);
        addRet = tairCpcCluster.cpcUpdate(key, item2, cpcUpdateParams);

        cpcUpdateParams.ex(-1);
        addRet = tairCpcCluster.cpcUpdate(key, item2, cpcUpdateParams);

        Thread.sleep(2000);
        estimateRet = tairCpcCluster.cpcEstimate(key);
        assertEquals(2.00, estimateRet, 0.001);

        cpcUpdateParams.ex(2);
        addRet = tairCpcCluster.cpcUpdate(bkey, bitem, cpcUpdateParams);
        Assert.assertEquals("OK", addRet);
        addRet = tairCpcCluster.cpcUpdate(bkey, bitem2);
        Assert.assertEquals("OK", addRet);

        estimateRet = tairCpcCluster.cpcEstimate(bkey);
        assertEquals(2.00, estimateRet, 0.01);

        Thread.sleep(2000);

        estimateRet = tairCpcCluster.cpcEstimate(bkey);
        assertNull(estimateRet);

        addRet = tairCpcCluster.cpcUpdate(bkey, bitem);
        Assert.assertEquals("OK", addRet);
        cpcUpdateParams.ex(0);
        addRet = tairCpcCluster.cpcUpdate(bkey, bitem2, cpcUpdateParams);
        Assert.assertEquals("OK", addRet);

        Thread.sleep(1000);
        estimateRet = tairCpcCluster.cpcEstimate(bkey);
        assertEquals(2.00, estimateRet, 0.001);

        cpcUpdateParams.ex(2);
        addRet = tairCpcCluster.cpcUpdate(bkey, bitem2, cpcUpdateParams);

        cpcUpdateParams.ex(-1);
        addRet = tairCpcCluster.cpcUpdate(bkey, bitem2, cpcUpdateParams);

        Thread.sleep(2000);
        estimateRet = tairCpcCluster.cpcEstimate(bkey);
        assertEquals(2.00, estimateRet, 0.001);
    }

    @Test
    public void cpcupdate2judTest() throws Exception {

        String addRet = tairCpcCluster.cpcUpdate(key, item);
        Assert.assertEquals("OK", addRet);

        addRet = tairCpcCluster.cpcUpdate(key, item2);
        Assert.assertEquals("OK", addRet);

        addRet = tairCpcCluster.cpcUpdate(key, item3);
        Assert.assertEquals("OK", addRet);

        Double estimateRet = tairCpcCluster.cpcEstimate(key);
        assertEquals(3.00, estimateRet, 0.001);

        Update2JudResult judRet = tairCpcCluster.cpcUpdate2Jud(key, item);
        assertEquals(3.00, judRet.getValue(), 0.001);
        assertEquals(0.00, judRet.getDiffValue(), 0.001);

        judRet = tairCpcCluster.cpcUpdate2Jud(key, item4);
        assertEquals(4.00, judRet.getValue(), 0.001);
        assertEquals(1.00, judRet.getDiffValue(), 0.001);
    }

    @Test
    public void cpcAccurateEstimationTest() throws Exception {

        int testnum = 120;
        String addRet = null;
        for (int i = 0; i < testnum; i++) {
            String itemtmp = item + String.valueOf(i);
            addRet = tairCpcCluster.cpcUpdate(key, itemtmp);
            Assert.assertEquals("OK", addRet);
        }

        Double estimateRet = tairCpcCluster.cpcEstimate(key);
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

        String addRet = null;
        try {
            tairCpcCluster.cpcMUpdate(addList);
        } catch (JedisDataException e) {
            assertTrue(e.getMessage().contains("CROSSSLOT"));
            return;
        }

        Assert.assertEquals("OK", addRet);
        Double estimateRet = tairCpcCluster.cpcEstimate(key);
        assertEquals(1.00, estimateRet, 0.1);
        Thread.sleep(5 * 1000);
        estimateRet = tairCpcCluster.cpcEstimate(key);
        assertNull(estimateRet);
    }

//    @Test
//    public void cpcUpdateExTest(){
//        ArrayList<CpcData> datas = new ArrayList<>();
//        String unique = UUID.randomUUID().toString();
//        int repeat = 20;
//        HashMap<String, Set<String>> statistic = new HashMap<>();
//        IntStream.range(0, repeat).forEach(i -> {
//            String key = "key_" + unique +i%5;
//            String value = "value_" + unique +i%4;
//
//            CpcData cpcData = new CpcData(key, value);
//            cpcData.ex(4);
//            datas.add(cpcData);
//
//            //统计
//            statistic.putIfAbsent(key, new HashSet<>() );
//            Set<String> set = statistic.get(key);
//            set.add(value);
//            statistic.put(key, set);
//        });
//
//        String addRet = tairCpcCluster.cpcMUpdate(datas);
//        Assert.assertEquals("OK", addRet);
//
//        try {
//            Thread.sleep(1000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        statistic.forEach((key, valueSet) ->{
//            Double estimate = tairCpcCluster.cpcEstimate(key);
////            System.out.println("estimate: {}, distinct key: {}", estimate, valueSet.size());
//            Assert.assertEquals(Math.round(estimate), (long) valueSet.size());
//        });
//
//        try {
//            Thread.sleep(3000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        statistic.forEach((key, valueSet) ->{
//            Double estimate = tairCpcCluster.cpcEstimate(key);
//            assertNull(estimate);
//        });
//    }

    @Test
    public void cpcMupdate2EstTest() throws Exception {

        ArrayList<CpcData> addList = new ArrayList<CpcData>();
        CpcData add1 = new CpcData(key, item);
        CpcData add2 = new CpcData(key2, item2);
        CpcData add3 = new CpcData(key, item2);
        addList.add(add1);
        addList.add(add2);
        addList.add(add3);

        List<Double> addRet = null;
        try {
            addRet = tairCpcCluster.cpcMUpdate2Est(addList);
        } catch (JedisDataException e) {
            assertTrue(e.getMessage().contains("CROSSSLOT"));
            return;
        }

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

        List<Update2JudResult> addRet = null;
        try {
            tairCpcCluster.cpcMUpdate2Jud(addList);
        } catch (JedisDataException e) {
            assertTrue(e.getMessage().contains("CROSSSLOT"));
            return;
        }

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
//        HashMap<String, Double> addRet = tairCpcCluster.cpcMUpdate2EstWithKey(addList);
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
//        HashMap<String, Update2JudResult> addRet = tairCpcCluster.cpcMUpdate2JudWithKey(addList);
//
//        Assert.assertEquals(1.0, addRet.get(key2).getValue(), 0.1);
//        Assert.assertEquals(1.0, addRet.get(key2).getDiffValue(), 0.1);
//        Assert.assertEquals(2.0, addRet.get(key).getValue(), 0.1);
//        Assert.assertEquals(1.0, addRet.get(key).getDiffValue(), 0.1);
//    }

    @Test
    public void cpcArrayUpdateTest() throws Exception {

        String addRet = tairCpcCluster.cpcArrayUpdate(key, 1, item, 5);
        Assert.assertEquals("OK", addRet);
        addRet = tairCpcCluster.cpcArrayUpdate(key, 1, item2, 5);
        Assert.assertEquals("OK", addRet);
        addRet = tairCpcCluster.cpcArrayUpdate(key, 3, item, 5);
        Assert.assertEquals("OK", addRet);
        addRet = tairCpcCluster.cpcArrayUpdate(key, 5, item, 5);
        Assert.assertEquals("OK", addRet);

        Double estimateRet = tairCpcCluster.cpcArrayEstimate(key, 1);
        assertEquals(2.00, estimateRet, 0.001);
        estimateRet = tairCpcCluster.cpcArrayEstimate(key, 3);
        assertEquals(1.00, estimateRet, 0.001);
        estimateRet = tairCpcCluster.cpcArrayEstimate(key, 5);
        assertEquals(1.00, estimateRet, 0.001);
    }

    @Test
    public void cpcArrayUpdate2EstTest() throws Exception {

        Double addRet = tairCpcCluster.cpcArrayUpdate2Est(key, 1, item, 5);
        assertEquals(1.00, addRet, 0.001);
        addRet = tairCpcCluster.cpcArrayUpdate2Est(key, 1, item2, 5);
        assertEquals(2.00, addRet, 0.001);
        addRet = tairCpcCluster.cpcArrayUpdate2Est(key, 3, item, 5);
        assertEquals(1.00, addRet, 0.001);
        addRet = tairCpcCluster.cpcArrayUpdate2Est(key, 5, item, 5);
        assertEquals(1.00, addRet, 0.001);

        addRet = tairCpcCluster.cpcArrayUpdate2Est(key, 7, item2, 5);
        assertEquals(1.00, addRet, 0.001);

        Double estimateRet = tairCpcCluster.cpcArrayEstimate(key, 7);
        assertEquals(1.00, estimateRet, 0.001);
        estimateRet = tairCpcCluster.cpcArrayEstimate(key, 3);
        assertEquals(1.00, estimateRet, 0.001);
        estimateRet = tairCpcCluster.cpcArrayEstimate(key, 5);
        assertEquals(1.00, estimateRet, 0.001);
    }

    @Test
    public void cpcArrayUpdate2JudTest() throws Exception {

        Update2JudResult addRet = tairCpcCluster.cpcArrayUpdate2Jud(key, 1, item, 5);
        assertEquals(1.00, addRet.getValue(), 0.001);
        assertEquals(1.00, addRet.getDiffValue(), 0.001);
        Update2JudResult addRet2 = tairCpcCluster.cpcArrayUpdate2Jud(key, 1, item, 5);
        assertEquals(1.00, addRet2.getValue(), 0.001);
        assertEquals(0.00, addRet2.getDiffValue(), 0.001);
        addRet = tairCpcCluster.cpcArrayUpdate2Jud(key, 1, item2, 5);
        assertEquals(2.00, addRet.getValue(), 0.001);
        assertEquals(1.00, addRet.getDiffValue(), 0.001);
        addRet = tairCpcCluster.cpcArrayUpdate2Jud(key, 3, item, 5);
        assertEquals(1.00, addRet.getValue(), 0.001);
        assertEquals(1.00, addRet.getDiffValue(), 0.001);
        addRet = tairCpcCluster.cpcArrayUpdate2Jud(key, 5, item, 5);
        assertEquals(1.00, addRet.getValue(), 0.001);
        assertEquals(1.00, addRet.getDiffValue(), 0.001);

        addRet = tairCpcCluster.cpcArrayUpdate2Jud(key, 7, item2, 5);
        assertEquals(1.00, addRet.getValue(), 0.001);
        assertEquals(1.00, addRet.getDiffValue(), 0.001);

        Double estimateRet = tairCpcCluster.cpcArrayEstimate(key, 7);
        assertEquals(1.00, estimateRet, 0.001);
        estimateRet = tairCpcCluster.cpcArrayEstimate(key, 3);
        assertEquals(1.00, estimateRet, 0.001);
        estimateRet = tairCpcCluster.cpcArrayEstimate(key, 5);
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
//        HashMap<String, Update2JudResult> addRet = tairCpcCluster.cpcArrayMUpdate2JudWithKey(addList);
//
//        Assert.assertEquals(2.0, addRet.get(key).getValue(), 0.1);
//        Assert.assertEquals(2.0, addRet.get(key).getDiffValue(), 0.1);
//    }

    @Test
    public void cpcArrayEstimateRangeTest() throws Exception {

        Update2JudResult addRet = tairCpcCluster.cpcArrayUpdate2Jud(key, 1, item, 5);
        assertEquals(1.00, addRet.getValue(), 0.001);
        assertEquals(1.00, addRet.getDiffValue(), 0.001);
        Update2JudResult addRet2 = tairCpcCluster.cpcArrayUpdate2Jud(key, 1, item, 5);
        assertEquals(1.00, addRet2.getValue(), 0.001);
        assertEquals(0.00, addRet2.getDiffValue(), 0.001);
        addRet = tairCpcCluster.cpcArrayUpdate2Jud(key, 1, item2, 5);
        assertEquals(2.00, addRet.getValue(), 0.001);
        assertEquals(1.00, addRet.getDiffValue(), 0.001);
        addRet = tairCpcCluster.cpcArrayUpdate2Jud(key, 3, item, 5);
        assertEquals(1.00, addRet.getValue(), 0.001);
        assertEquals(1.00, addRet.getDiffValue(), 0.001);
        addRet = tairCpcCluster.cpcArrayUpdate2Jud(key, 5, item, 5);
        assertEquals(1.00, addRet.getValue(), 0.001);
        assertEquals(1.00, addRet.getDiffValue(), 0.001);

//        addRet = tairCpcCluster.cpcArrayUpdate2Jud(key, 6, item2, 5);
//        assertEquals(1.00, addRet.getValue(), 0.001);
//        assertEquals(1.00, addRet.getDiffValue(), 0.001);

        List<Double> estimateRet = tairCpcCluster.cpcArrayEstimateRange(key, 5, 5);
//        System.out.println(estimateRet.toString());
        assertEquals(2.00, estimateRet.get(0), 0.001);
        assertEquals(0.00, estimateRet.get(1), 0.001);
        assertEquals(1.00, estimateRet.get(2), 0.001);
        assertEquals(0.00, estimateRet.get(3), 0.001);
        assertEquals(1.00, estimateRet.get(4), 0.001);
    }

    @Test
    public void sumTest() throws Exception {

        Double addRet = tairCpcCluster.sumAdd(key, 100);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcCluster.sumAdd(key, 150);
        assertEquals(250.00, addRet, 0.001);

        Double getRet = tairCpcCluster.sumGet(key);
        assertEquals(250.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpcCluster.sumAdd(key2, 100, cpcUpdateParams2);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcCluster.sumAdd(key2, 150, cpcUpdateParams2);
        assertEquals(250.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcCluster.sumGet(key2);
        assertEquals(0.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.px(2000);

        addRet = tairCpcCluster.sumAdd(key3, 100, cpcUpdateParams3);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcCluster.sumAdd(key3, 150, cpcUpdateParams3);
        assertEquals(250.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcCluster.sumGet(key3);
        assertEquals(0.00, getRet, 0.001);

        Double setRet = tairCpcCluster.sumSet(key, 100);
        assertEquals(100.00, setRet, 0.001);
    }

    @Test
    public void sumArrayTest() throws Exception {

        Double addRet = tairCpcCluster.sumArrayAdd(key, 1, 100, 7);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcCluster.sumArrayAdd(key, 1, 150, 7);
        assertEquals(250.00, addRet, 0.001);

        Double getRet = tairCpcCluster.sumArrayGet(key, 1);
        assertEquals(250.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpcCluster.sumArrayAdd(key2, 1, 100, 7, cpcUpdateParams2);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcCluster.sumArrayAdd(key2, 1, 150, 7, cpcUpdateParams2);
        assertEquals(250.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcCluster.sumArrayGet(key2, 1);
        assertNull(getRet);
//        assertEquals(0.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.px(2000);

        addRet = tairCpcCluster.sumArrayAdd(key3, 2, 100, 7, cpcUpdateParams3);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcCluster.sumArrayAdd(key3, 2, 150, 7, cpcUpdateParams3);
        assertEquals(250.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcCluster.sumArrayGet(key3, 2);
        assertNull(getRet);
//        assertEquals(0.00, getRet, 0.001);

        List<Double> rangeRet = tairCpcCluster.sumArrayGetRange(key, 7, 7);
        assertEquals(7, rangeRet.size());
        assertEquals(250.00, rangeRet.get(0), 0.001);

        Double mergeRet = tairCpcCluster.sumArrayGetRangeMerge(key, 7, 7);
        assertEquals(250.00, mergeRet, 0.001);
    }

    @Test
    public void maxTest() throws Exception {

        Double addRet = tairCpcCluster.maxAdd(key, 150);
        assertEquals(150.00, addRet, 0.001);

        addRet = tairCpcCluster.maxAdd(key, 100);
        assertEquals(150.00, addRet, 0.001);

        Double getRet = tairCpcCluster.maxGet(key);
        assertEquals(150.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpcCluster.maxAdd(key2, 100, cpcUpdateParams2);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcCluster.maxAdd(key2, 150, cpcUpdateParams2);
        assertEquals(150.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcCluster.maxGet(key2);
        assertEquals(0.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.px(2000);

        addRet = tairCpcCluster.maxAdd(key3, 100, cpcUpdateParams3);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcCluster.maxAdd(key3, 150, cpcUpdateParams3);
        assertEquals(150.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcCluster.maxGet(key3);
        assertEquals(0.00, getRet, 0.001);

        Double setRet = tairCpcCluster.maxSet(key, 100);
        assertEquals(100.00, setRet, 0.001);
    }

    @Test
    public void maxArrayTest() throws Exception {

        Double addRet = tairCpcCluster.maxArrayAdd(key, 1, 100, 7);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcCluster.maxArrayAdd(key, 1, 150, 7);
        assertEquals(150.00, addRet, 0.001);

        Double getRet = tairCpcCluster.maxArrayGet(key, 1);
        assertEquals(150.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpcCluster.maxArrayAdd(key2, 1, 100, 7, cpcUpdateParams2);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcCluster.maxArrayAdd(key2, 1, 150, 7, cpcUpdateParams2);
        assertEquals(150.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcCluster.maxArrayGet(key2, 1);
        assertNull(getRet);
//        assertEquals(0.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.px(2000);

        addRet = tairCpcCluster.maxArrayAdd(key3, 2, 100, 7, cpcUpdateParams3);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcCluster.maxArrayAdd(key3, 2, 150, 7, cpcUpdateParams3);
        assertEquals(150.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcCluster.maxArrayGet(key3, 2);
        assertNull(getRet);
//        assertEquals(0.00, getRet, 0.001);

        List<Double> rangeRet = tairCpcCluster.maxArrayGetRange(key, 7, 7);
        assertEquals(7, rangeRet.size());
        assertEquals(150.00, rangeRet.get(0), 0.001);

        Double mergeRet = tairCpcCluster.maxArrayGetRangeMerge(key, 7, 7);
        assertEquals(150.00, mergeRet, 0.001);
    }

    @Test
    public void minTest() throws Exception {

        Double addRet = tairCpcCluster.minAdd(key, 150);
        assertEquals(150.00, addRet, 0.001);

        addRet = tairCpcCluster.minAdd(key, 100);
        assertEquals(100.00, addRet, 0.001);

        Double getRet = tairCpcCluster.minGet(key);
        assertEquals(100.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpcCluster.minAdd(key2, 100, cpcUpdateParams2);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcCluster.minAdd(key2, 150, cpcUpdateParams2);
        assertEquals(100.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcCluster.minGet(key2);
        assertEquals(0.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.px(2000);

        addRet = tairCpcCluster.minAdd(key3, 100, cpcUpdateParams3);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcCluster.minAdd(key3, 150, cpcUpdateParams3);
        assertEquals(100.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcCluster.minGet(key3);
        assertEquals(0.00, getRet, 0.001);

        Double setRet = tairCpcCluster.minSet(key, 100);
        assertEquals(100.00, setRet, 0.001);
    }

    @Test
    public void minArrayTest() throws Exception {

        Double addRet = tairCpcCluster.minArrayAdd(key, 1, 100, 7);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcCluster.minArrayAdd(key, 1, 150, 7);
        assertEquals(100.00, addRet, 0.001);

        Double getRet = tairCpcCluster.minArrayGet(key, 1);
        assertEquals(100.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpcCluster.minArrayAdd(key2, 1, 100, 7, cpcUpdateParams2);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcCluster.minArrayAdd(key2, 1, 150, 7, cpcUpdateParams2);
        assertEquals(100.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcCluster.minArrayGet(key2, 1);
        assertNull(getRet);
//        assertEquals(0.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.px(2000);

        addRet = tairCpcCluster.minArrayAdd(key3, 2, 100, 7, cpcUpdateParams3);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcCluster.minArrayAdd(key3, 2, 150, 7, cpcUpdateParams3);
        assertEquals(100.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcCluster.minArrayGet(key3, 2);
        assertNull(getRet);
//        assertEquals(0.00, getRet, 0.001);

        List<Double> rangeRet = tairCpcCluster.minArrayGetRange(key, 7, 7);
        assertEquals(7, rangeRet.size());
        assertEquals(100.00, rangeRet.get(0), 0.001);

        Double mergeRet = tairCpcCluster.minArrayGetRangeMerge(key, 7, 7);
        assertEquals(100.00, mergeRet, 0.001);
    }

    @Test
    public void firstTest() throws Exception {

        String addRet = tairCpcCluster.firstAdd(key, content1, 150);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcCluster.firstAdd(key, content2, 100);
        Assert.assertEquals(content2, addRet);

        String getRet = tairCpcCluster.firstGet(key);
        Assert.assertEquals(content2, getRet);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpcCluster.firstAdd(key2, content1, 100, cpcUpdateParams2);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcCluster.firstAdd(key2, content2, 150, cpcUpdateParams2);
        Assert.assertEquals(content1, addRet);

        Thread.sleep(3000);

        getRet = tairCpcCluster.firstGet(key2);
        Assert.assertNull(getRet);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.px(2000);

        addRet = tairCpcCluster.firstAdd(key3, content1, 100, cpcUpdateParams3);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcCluster.firstAdd(key3, content2, 150, cpcUpdateParams3);
        Assert.assertEquals(content1, addRet);

        Thread.sleep(3000);

        getRet = tairCpcCluster.firstGet(key3);
        Assert.assertNull(getRet);

        String setRet = tairCpcCluster.firstSet(key, content1, 100);
        Assert.assertEquals(content1, setRet);
    }

    @Test
    public void firstArrayTest() throws Exception {

        String addRet = tairCpcCluster.firstArrayAdd(key, 1, content1, 100, 7);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcCluster.firstArrayAdd(key, 1, content2, 150, 7);
        Assert.assertEquals(content1, addRet);

        String getRet = tairCpcCluster.firstArrayGet(key, 1);
        Assert.assertEquals(content1, getRet);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpcCluster.firstArrayAdd(key2, 1, content1, 100, 7, cpcUpdateParams2);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcCluster.firstArrayAdd(key2, 1, content2, 150, 7, cpcUpdateParams2);
        Assert.assertEquals(content1, addRet);

        Thread.sleep(3000);

        getRet = tairCpcCluster.firstArrayGet(key2, 1);
        Assert.assertNull(getRet);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.px(2000);

        addRet = tairCpcCluster.firstArrayAdd(key3, 2, content1, 100, 7, cpcUpdateParams3);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcCluster.firstArrayAdd(key3, 2, content2, 150, 7, cpcUpdateParams3);
        Assert.assertEquals(content1, addRet);

        Thread.sleep(3000);

        getRet = tairCpcCluster.firstArrayGet(key3, 2);
        Assert.assertNull(getRet);

        List<String> rangeRet = tairCpcCluster.firstArrayGetRange(key, 7, 7);
        assertEquals(7, rangeRet.size());
        Assert.assertEquals(content1, rangeRet.get(0));

//        String mergeRet = tairCpcCluster.firstArrayGetRangeMerge(key, 7, 7);
//        Assert.assertEquals(content1, mergeRet);
    }

    @Test
    public void lastTest() throws Exception {

        String addRet = tairCpcCluster.lastAdd(key, content1, 150);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcCluster.lastAdd(key, content2, 100);
        Assert.assertEquals(content1, addRet);

        String getRet = tairCpcCluster.lastGet(key);
        Assert.assertEquals(content1, getRet);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpcCluster.lastAdd(key2, content1, 100, cpcUpdateParams2);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcCluster.lastAdd(key2, content2, 150, cpcUpdateParams2);
        Assert.assertEquals(content2, addRet);

        Thread.sleep(3000);

        getRet = tairCpcCluster.lastGet(key2);
        Assert.assertNull(getRet);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.px(2000);

        addRet = tairCpcCluster.lastAdd(key3, content1, 100, cpcUpdateParams3);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcCluster.lastAdd(key3, content2, 150, cpcUpdateParams3);
        Assert.assertEquals(content2, addRet);

        Thread.sleep(3000);

        getRet = tairCpcCluster.lastGet(key3);
        Assert.assertNull(getRet);

        String setRet = tairCpcCluster.lastSet(key, content1, 120);
        Assert.assertEquals(content1, setRet);
    }

    @Test
    public void lastArrayTest() throws Exception {

        String addRet = tairCpcCluster.lastArrayAdd(key, 1, content1, 100, 7);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcCluster.lastArrayAdd(key, 1, content2, 150, 7);
        Assert.assertEquals(content2, addRet);

        String getRet = tairCpcCluster.lastArrayGet(key, 1);
        Assert.assertEquals(content2, getRet);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpcCluster.lastArrayAdd(key2, 1, content1, 100, 7, cpcUpdateParams2);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcCluster.lastArrayAdd(key2, 1, content2, 150, 7, cpcUpdateParams2);
        Assert.assertEquals(content2, addRet);

        Thread.sleep(3000);

        getRet = tairCpcCluster.lastArrayGet(key2, 1);
        Assert.assertNull(getRet);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.px(2000);

        addRet = tairCpcCluster.lastArrayAdd(key3, 2, content1, 100, 7, cpcUpdateParams3);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcCluster.lastArrayAdd(key3, 2, content2, 150, 7, cpcUpdateParams3);
        Assert.assertEquals(content2, addRet);

        Thread.sleep(3000);

        getRet = tairCpcCluster.lastArrayGet(key3, 2);
        Assert.assertNull(getRet);

        List<String> rangeRet = tairCpcCluster.lastArrayGetRange(key, 7, 7);
        assertEquals(7, rangeRet.size());
        Assert.assertEquals(content2, rangeRet.get(0));

        String mergeRet = tairCpcCluster.lastArrayGetRangeMerge(key, 7, 7);
        Assert.assertEquals(content2, mergeRet);
    }

    @Test
    public void avgTest() throws Exception {

        Double addRet = tairCpcCluster.avgAdd(key, count1, 150);
        assertEquals(150, addRet, 0.001);

        addRet = tairCpcCluster.avgAdd(key, count2, 100);
        assertEquals(112.5, addRet, 0.001);

        Double getRet = tairCpcCluster.avgGet(key);
        assertEquals(112.5, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpcCluster.avgAdd(key2, count1, 100, cpcUpdateParams2);
        assertEquals(100, addRet, 0.001);

        addRet = tairCpcCluster.avgAdd(key2, count2, 150, cpcUpdateParams2);
        assertEquals(137.5, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcCluster.avgGet(key2);
        assertEquals(0.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.px(2000);

        addRet = tairCpcCluster.avgAdd(key3, count1, 100, cpcUpdateParams3);
        assertEquals(100, addRet, 0.001);

        addRet = tairCpcCluster.avgAdd(key3, count2, 150, cpcUpdateParams3);
        assertEquals(137.5, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcCluster.avgGet(key3);
        assertEquals(0.00, getRet, 0.001);

        Double setRet = tairCpcCluster.avgSet(key, count1, 100);
        assertEquals(150, setRet, 0.001);


        addRet = tairCpcCluster.avgAdd(bkey, count1, 150);
        assertEquals(150, addRet, 0.001);

        addRet = tairCpcCluster.avgAdd(bkey, count2, 100);
        assertEquals(112.5, addRet, 0.001);

        getRet = tairCpcCluster.avgGet(bkey);
        assertEquals(112.5, getRet, 0.001);

        addRet = tairCpcCluster.avgAdd(bkey2, count1, 100, cpcUpdateParams2);
        assertEquals(100, addRet, 0.001);

        addRet = tairCpcCluster.avgAdd(bkey2, count2, 150, cpcUpdateParams2);
        assertEquals(137.5, addRet, 0.001);
    }

    @Test
    public void avgArrayTest() throws Exception {

        Double addRet = tairCpcCluster.avgArrayAdd(key, 1, count1, 100, 7);
        assertEquals(100, addRet, 0.001);

        addRet = tairCpcCluster.avgArrayAdd(key, 1, count2, 150, 7);
        assertEquals(137.5, addRet, 0.001);

        Double getRet = tairCpcCluster.avgArrayGet(key, 1);
        assertEquals(137.5, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpcCluster.avgArrayAdd(key2, 1, count1, 100, 7, cpcUpdateParams2);
        assertEquals(100, addRet, 0.001);

        addRet = tairCpcCluster.avgArrayAdd(key2, 1, count2, 150, 7, cpcUpdateParams2);
        assertEquals(137.5, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcCluster.avgArrayGet(key2, 1);
        assertNull(getRet);
//        assertEquals(0.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.px(2000);

        addRet = tairCpcCluster.avgArrayAdd(key3, 2, count1, 100, 7, cpcUpdateParams3);
        assertEquals(100, addRet, 0.001);

        addRet = tairCpcCluster.avgArrayAdd(key3, 2, count2, 150, 7, cpcUpdateParams3);
        assertEquals(137.5, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcCluster.avgArrayGet(key3, 2);
        assertNull(getRet);
//        assertEquals(0.00, getRet, 0.001);

        List<Double> rangeRet = tairCpcCluster.avgArrayGetRange(key, 7, 7);
        assertEquals(7, rangeRet.size());
        assertEquals(137.5, rangeRet.get(0), 0.001);

        Double mergeRet = tairCpcCluster.avgArrayGetRangeMerge(key, 7, 7);
        assertEquals(137.5, mergeRet, 0.001);
    }

    @Test
    public void stddevTest() throws Exception {

        Double addRet = tairCpcCluster.stddevAdd(key, 150);
        assertEquals(0.00, addRet, 0.001);

        addRet = tairCpcCluster.stddevAdd(key, 100);
        assertEquals(25.00, addRet, 0.001);

        Double getRet = tairCpcCluster.stddevGet(key);
        assertEquals(25.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpcCluster.stddevAdd(key2, 100, cpcUpdateParams2);
        assertEquals(0.00, addRet, 0.001);

        addRet = tairCpcCluster.stddevAdd(key2, 150, cpcUpdateParams2);
        assertEquals(25.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcCluster.stddevGet(key2);
        assertEquals(0.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.px(2000);

        addRet = tairCpcCluster.stddevAdd(key3, 100, cpcUpdateParams3);
        assertEquals(0.00, addRet, 0.001);

        addRet = tairCpcCluster.stddevAdd(key3, 150, cpcUpdateParams3);
        assertEquals(25.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcCluster.stddevGet(key3);
        assertEquals(0.00, getRet, 0.001);

//        Double setRet = tairCpcCluster.stddevSet(key, 1, 100, 0);
//        assertEquals(0.00, setRet, 0.001);
    }

    @Test
    public void stddevArrayTest() throws Exception {

        Double addRet = tairCpcCluster.stddevArrayAdd(key, 1, 1,100, 7);
        assertEquals(0.00, addRet, 0.001);

        addRet = tairCpcCluster.stddevArrayAdd(key, 1, 1,150, 7);
        assertEquals(25.00, addRet, 0.001);

        Double getRet = tairCpcCluster.stddevArrayGet(key, 1);
        assertEquals(25.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpcCluster.stddevArrayAdd(key2, 1, 1, 100, 7, cpcUpdateParams2);
        assertEquals(0.00, addRet, 0.001);

        addRet = tairCpcCluster.stddevArrayAdd(key2, 1, 1, 150, 7, cpcUpdateParams2);
        assertEquals(25.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcCluster.stddevArrayGet(key2, 1);
        assertNull(getRet);
//        assertEquals(0.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.px(2000);

        addRet = tairCpcCluster.stddevArrayAdd(key3, 2, 1, 100, 7, cpcUpdateParams3);
        assertEquals(0.00, addRet, 0.001);

        addRet = tairCpcCluster.stddevArrayAdd(key3, 2, 1, 150, 7, cpcUpdateParams3);
        assertEquals(25.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcCluster.stddevArrayGet(key3, 2);
        assertNull(getRet);
//        assertEquals(0.00, getRet, 0.001);

        List<Double> rangeRet = tairCpcCluster.stddevArrayGetRange(key, 7, 7);
        assertEquals(7, rangeRet.size());
        assertEquals(25.00, rangeRet.get(0), 0.001);

        Double mergeRet = tairCpcCluster.stddevArrayGetRangeMerge(key, 7, 7);
        assertEquals(25.00, mergeRet, 0.001);
    }
}
