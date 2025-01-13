package com.aliyun.tair.tests.taircpc;

import com.aliyun.tair.taircpc.params.CpcData;
import com.aliyun.tair.taircpc.params.CpcUpdateParams;
import com.aliyun.tair.taircpc.results.Update2JudResult;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TairCpcClusterTestNew extends TairCpcTestBase {
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

    public TairCpcClusterTestNew() {
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

        String addRet = tairCpcClusterNew.cpcUpdate(key, item);
        Assert.assertEquals("OK", addRet);

        String addRetByte = tairCpcClusterNew.cpcUpdate(bkey, bitem);
        Assert.assertEquals("OK", addRetByte);
    }

    @Test
    public void cpcupdateExpireTest() throws Exception {

        CpcUpdateParams cpcUpdateParams = new CpcUpdateParams();
        cpcUpdateParams.ex(2);
        String addRet = tairCpcClusterNew.cpcUpdate(key, item, cpcUpdateParams);
        Assert.assertEquals("OK", addRet);
        addRet = tairCpcClusterNew.cpcUpdate(key, item2);
        Assert.assertEquals("OK", addRet);

        Double estimateRet = tairCpcClusterNew.cpcEstimate(key);
        assertEquals(2.00, estimateRet, 0.001);

        Thread.sleep(2000);

        estimateRet = tairCpcClusterNew.cpcEstimate(key);
        assertNull(estimateRet);

        addRet = tairCpcClusterNew.cpcUpdate(key, item);
        Assert.assertEquals("OK", addRet);
        cpcUpdateParams.ex(0);
        addRet = tairCpcClusterNew.cpcUpdate(key, item2, cpcUpdateParams);
        Assert.assertEquals("OK", addRet);

        Thread.sleep(1000);
        estimateRet = tairCpcClusterNew.cpcEstimate(key);
        assertEquals(2.00, estimateRet, 0.001);

        cpcUpdateParams.ex(2);
        addRet = tairCpcClusterNew.cpcUpdate(key, item2, cpcUpdateParams);

        cpcUpdateParams.ex(-1);
        addRet = tairCpcClusterNew.cpcUpdate(key, item2, cpcUpdateParams);

        Thread.sleep(2000);
        estimateRet = tairCpcClusterNew.cpcEstimate(key);
        assertEquals(2.00, estimateRet, 0.001);

        cpcUpdateParams.ex(2);
        addRet = tairCpcClusterNew.cpcUpdate(bkey, bitem, cpcUpdateParams);
        Assert.assertEquals("OK", addRet);
        addRet = tairCpcClusterNew.cpcUpdate(bkey, bitem2);
        Assert.assertEquals("OK", addRet);

        estimateRet = tairCpcClusterNew.cpcEstimate(bkey);
        assertEquals(2.00, estimateRet, 0.01);

        Thread.sleep(2000);

        estimateRet = tairCpcClusterNew.cpcEstimate(bkey);
        assertNull(estimateRet);

        addRet = tairCpcClusterNew.cpcUpdate(bkey, bitem);
        Assert.assertEquals("OK", addRet);
        cpcUpdateParams.ex(0);
        addRet = tairCpcClusterNew.cpcUpdate(bkey, bitem2, cpcUpdateParams);
        Assert.assertEquals("OK", addRet);

        Thread.sleep(1000);
        estimateRet = tairCpcClusterNew.cpcEstimate(bkey);
        assertEquals(2.00, estimateRet, 0.001);

        cpcUpdateParams.ex(2);
        addRet = tairCpcClusterNew.cpcUpdate(bkey, bitem2, cpcUpdateParams);

        cpcUpdateParams.ex(-1);
        addRet = tairCpcClusterNew.cpcUpdate(bkey, bitem2, cpcUpdateParams);

        Thread.sleep(2000);
        estimateRet = tairCpcClusterNew.cpcEstimate(bkey);
        assertEquals(2.00, estimateRet, 0.001);
    }

    @Test
    public void cpcupdate2judTest() throws Exception {

        String addRet = tairCpcClusterNew.cpcUpdate(key, item);
        Assert.assertEquals("OK", addRet);

        addRet = tairCpcClusterNew.cpcUpdate(key, item2);
        Assert.assertEquals("OK", addRet);

        addRet = tairCpcClusterNew.cpcUpdate(key, item3);
        Assert.assertEquals("OK", addRet);

        Double estimateRet = tairCpcClusterNew.cpcEstimate(key);
        assertEquals(3.00, estimateRet, 0.001);

        Update2JudResult judRet = tairCpcClusterNew.cpcUpdate2Jud(key, item);
        assertEquals(3.00, judRet.getValue(), 0.001);
        assertEquals(0.00, judRet.getDiffValue(), 0.001);

        judRet = tairCpcClusterNew.cpcUpdate2Jud(key, item4);
        assertEquals(4.00, judRet.getValue(), 0.001);
        assertEquals(1.00, judRet.getDiffValue(), 0.001);
    }

    @Test
    public void cpcAccurateEstimationTest() throws Exception {

        int testnum = 120;
        String addRet = null;
        for (int i = 0; i < testnum; i++) {
            String itemtmp = item + String.valueOf(i);
            addRet = tairCpcClusterNew.cpcUpdate(key, itemtmp);
            Assert.assertEquals("OK", addRet);
        }

        Double estimateRet = tairCpcClusterNew.cpcEstimate(key);
        assertEquals(120.00, estimateRet, 0.1);
    }

    @Test
    @Ignore
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
            tairCpcClusterNew.cpcMUpdate(addList);
        } catch (JedisDataException e) {
            assertTrue(e.getMessage().contains("CROSSSLOT"));
            return;
        }

        Assert.assertEquals("OK", addRet);
        Double estimateRet = tairCpcClusterNew.cpcEstimate(key);
        assertEquals(1.00, estimateRet, 0.1);
        Thread.sleep(5 * 1000);
        estimateRet = tairCpcClusterNew.cpcEstimate(key);
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
//        String addRet = tairCpcClusterNew.cpcMUpdate(datas);
//        Assert.assertEquals("OK", addRet);
//
//        try {
//            Thread.sleep(1000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        statistic.forEach((key, valueSet) ->{
//            Double estimate = tairCpcClusterNew.cpcEstimate(key);
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
//            Double estimate = tairCpcClusterNew.cpcEstimate(key);
//            assertNull(estimate);
//        });
//    }

    @Test
    @Ignore
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
            addRet = tairCpcClusterNew.cpcMUpdate2Est(addList);
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
    @Ignore
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
            tairCpcClusterNew.cpcMUpdate2Jud(addList);
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
//        HashMap<String, Double> addRet = tairCpcClusterNew.cpcMUpdate2EstWithKey(addList);
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
//        HashMap<String, Update2JudResult> addRet = tairCpcClusterNew.cpcMUpdate2JudWithKey(addList);
//
//        Assert.assertEquals(1.0, addRet.get(key2).getValue(), 0.1);
//        Assert.assertEquals(1.0, addRet.get(key2).getDiffValue(), 0.1);
//        Assert.assertEquals(2.0, addRet.get(key).getValue(), 0.1);
//        Assert.assertEquals(1.0, addRet.get(key).getDiffValue(), 0.1);
//    }

//    @Test
//    public void cpcArrayUpdateTest() throws Exception {
//
//        String addRet = tairCpcClusterNew.cpcArrayUpdate(key, 1, item, 5);
//        Assert.assertEquals("OK", addRet);
//        addRet = tairCpcClusterNew.cpcArrayUpdate(key, 1, item2, 5);
//        Assert.assertEquals("OK", addRet);
//        addRet = tairCpcClusterNew.cpcArrayUpdate(key, 3, item, 5);
//        Assert.assertEquals("OK", addRet);
//        addRet = tairCpcClusterNew.cpcArrayUpdate(key, 5, item, 5);
//        Assert.assertEquals("OK", addRet);
//
//        Double estimateRet = tairCpcClusterNew.cpcArrayEstimate(key, 1);
//        assertEquals(2.00, estimateRet, 0.001);
//        estimateRet = tairCpcClusterNew.cpcArrayEstimate(key, 3);
//        assertEquals(1.00, estimateRet, 0.001);
//        estimateRet = tairCpcClusterNew.cpcArrayEstimate(key, 5);
//        assertEquals(1.00, estimateRet, 0.001);
//    }
//
//    @Test
//    public void cpcArrayUpdate2EstTest() throws Exception {
//
//        Double addRet = tairCpcClusterNew.cpcArrayUpdate2Est(key, 1, item, 5);
//        assertEquals(1.00, addRet, 0.001);
//        addRet = tairCpcClusterNew.cpcArrayUpdate2Est(key, 1, item2, 5);
//        assertEquals(2.00, addRet, 0.001);
//        addRet = tairCpcClusterNew.cpcArrayUpdate2Est(key, 3, item, 5);
//        assertEquals(1.00, addRet, 0.001);
//        addRet = tairCpcClusterNew.cpcArrayUpdate2Est(key, 5, item, 5);
//        assertEquals(1.00, addRet, 0.001);
//
//        addRet = tairCpcClusterNew.cpcArrayUpdate2Est(key, 7, item2, 5);
//        assertEquals(1.00, addRet, 0.001);
//
//        Double estimateRet = tairCpcClusterNew.cpcArrayEstimate(key, 7);
//        assertEquals(1.00, estimateRet, 0.001);
//        estimateRet = tairCpcClusterNew.cpcArrayEstimate(key, 3);
//        assertEquals(1.00, estimateRet, 0.001);
//        estimateRet = tairCpcClusterNew.cpcArrayEstimate(key, 5);
//        assertEquals(1.00, estimateRet, 0.001);
//    }
//
//    @Test
//    public void cpcArrayUpdate2JudTest() throws Exception {
//
//        Update2JudResult addRet = tairCpcClusterNew.cpcArrayUpdate2Jud(key, 1, item, 5);
//        assertEquals(1.00, addRet.getValue(), 0.001);
//        assertEquals(1.00, addRet.getDiffValue(), 0.001);
//        Update2JudResult addRet2 = tairCpcClusterNew.cpcArrayUpdate2Jud(key, 1, item, 5);
//        assertEquals(1.00, addRet2.getValue(), 0.001);
//        assertEquals(0.00, addRet2.getDiffValue(), 0.001);
//        addRet = tairCpcClusterNew.cpcArrayUpdate2Jud(key, 1, item2, 5);
//        assertEquals(2.00, addRet.getValue(), 0.001);
//        assertEquals(1.00, addRet.getDiffValue(), 0.001);
//        addRet = tairCpcClusterNew.cpcArrayUpdate2Jud(key, 3, item, 5);
//        assertEquals(1.00, addRet.getValue(), 0.001);
//        assertEquals(1.00, addRet.getDiffValue(), 0.001);
//        addRet = tairCpcClusterNew.cpcArrayUpdate2Jud(key, 5, item, 5);
//        assertEquals(1.00, addRet.getValue(), 0.001);
//        assertEquals(1.00, addRet.getDiffValue(), 0.001);
//
//        addRet = tairCpcClusterNew.cpcArrayUpdate2Jud(key, 7, item2, 5);
//        assertEquals(1.00, addRet.getValue(), 0.001);
//        assertEquals(1.00, addRet.getDiffValue(), 0.001);
//
//        Double estimateRet = tairCpcClusterNew.cpcArrayEstimate(key, 7);
//        assertEquals(1.00, estimateRet, 0.001);
//        estimateRet = tairCpcClusterNew.cpcArrayEstimate(key, 3);
//        assertEquals(1.00, estimateRet, 0.001);
//        estimateRet = tairCpcClusterNew.cpcArrayEstimate(key, 5);
//        assertEquals(1.00, estimateRet, 0.001);
//    }
//
////    @Test
////    public void cpcArrayMUpdate2JudWithKeyTest() throws Exception {
////
////        ArrayList<CpcArrayData> addList = new ArrayList<CpcArrayData>();
////        CpcArrayData add1 = new CpcArrayData(key, 1, item, 5);
////        CpcArrayData add2 = new CpcArrayData(key, 1, item, 5);
////        CpcArrayData add3 = new CpcArrayData(key,1, item2, 5);
////        addList.add(add1);
////        addList.add(add2);
////        addList.add(add3);
////
////        HashMap<String, Update2JudResult> addRet = tairCpcClusterNew.cpcArrayMUpdate2JudWithKey(addList);
////
////        Assert.assertEquals(2.0, addRet.get(key).getValue(), 0.1);
////        Assert.assertEquals(2.0, addRet.get(key).getDiffValue(), 0.1);
////    }
//
//    @Test
//    public void cpcArrayEstimateRangeTest() throws Exception {
//
//        Update2JudResult addRet = tairCpcClusterNew.cpcArrayUpdate2Jud(key, 1, item, 5);
//        assertEquals(1.00, addRet.getValue(), 0.001);
//        assertEquals(1.00, addRet.getDiffValue(), 0.001);
//        Update2JudResult addRet2 = tairCpcClusterNew.cpcArrayUpdate2Jud(key, 1, item, 5);
//        assertEquals(1.00, addRet2.getValue(), 0.001);
//        assertEquals(0.00, addRet2.getDiffValue(), 0.001);
//        addRet = tairCpcClusterNew.cpcArrayUpdate2Jud(key, 1, item2, 5);
//        assertEquals(2.00, addRet.getValue(), 0.001);
//        assertEquals(1.00, addRet.getDiffValue(), 0.001);
//        addRet = tairCpcClusterNew.cpcArrayUpdate2Jud(key, 3, item, 5);
//        assertEquals(1.00, addRet.getValue(), 0.001);
//        assertEquals(1.00, addRet.getDiffValue(), 0.001);
//        addRet = tairCpcClusterNew.cpcArrayUpdate2Jud(key, 5, item, 5);
//        assertEquals(1.00, addRet.getValue(), 0.001);
//        assertEquals(1.00, addRet.getDiffValue(), 0.001);
//
////        addRet = tairCpcClusterNew.cpcArrayUpdate2Jud(key, 6, item2, 5);
////        assertEquals(1.00, addRet.getValue(), 0.001);
////        assertEquals(1.00, addRet.getDiffValue(), 0.001);
//
//        List<Double> estimateRet = tairCpcClusterNew.cpcArrayEstimateRange(key, 5, 5);
////        System.out.println(estimateRet.toString());
//        assertEquals(2.00, estimateRet.get(0), 0.001);
//        assertEquals(0.00, estimateRet.get(1), 0.001);
//        assertEquals(1.00, estimateRet.get(2), 0.001);
//        assertEquals(0.00, estimateRet.get(3), 0.001);
//        assertEquals(1.00, estimateRet.get(4), 0.001);
//    }
//
@Test
@Ignore
public void sumTest() throws Exception {

    Double addRet = tairCpcClusterNew.sumAdd(key, 100);
    assertEquals(100.00, addRet, 0.001);

    addRet = tairCpcClusterNew.sumAdd(key, 150);
    assertEquals(250.00, addRet, 0.001);

    Double getRet = tairCpcClusterNew.sumGet(key);
    assertEquals(250.00, getRet, 0.001);

    CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
    cpcUpdateParams2.ex(2);

    addRet = tairCpcClusterNew.sumAdd(key2, 100, cpcUpdateParams2);
    assertEquals(100.00, addRet, 0.001);

    addRet = tairCpcClusterNew.sumAdd(key2, 150, cpcUpdateParams2);
    assertEquals(250.00, addRet, 0.001);

    Thread.sleep(3000);

    getRet = tairCpcClusterNew.sumGet(key2);
    assertEquals(0.00, getRet, 0.001);

    CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
    cpcUpdateParams3.px(2000);

    addRet = tairCpcClusterNew.sumAdd(key3, 100, cpcUpdateParams3);
    assertEquals(100.00, addRet, 0.001);

    addRet = tairCpcClusterNew.sumAdd(key3, 150, cpcUpdateParams3);
    assertEquals(250.00, addRet, 0.001);

    Thread.sleep(3000);

    getRet = tairCpcClusterNew.sumGet(key3);
    assertEquals(0.00, getRet, 0.001);

    Double setRet = tairCpcClusterNew.sumSet(key, 100);
    assertEquals(100.00, setRet, 0.001);
}

    @Test
    @Ignore
    public void sumArrayTest() throws Exception {

        Double addRet = tairCpcClusterNew.sumArrayAdd(key, timestamp, 100);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcClusterNew.sumArrayAdd(key, timestamp, 300);
        assertEquals(400.00, addRet, 0.001);

        Double getRet = tairCpcClusterNew.sumArrayGet(key, timestamp);
        assertEquals(400.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);
        cpcUpdateParams2.size(7);
        cpcUpdateParams2.win(winsize);

        addRet = tairCpcClusterNew.sumArrayAdd(key2, timestamp, 100, cpcUpdateParams2);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcClusterNew.sumArrayAdd(key2, timestamp, 300, cpcUpdateParams2);
        assertEquals(400.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcClusterNew.sumArrayGet(key2, timestamp);
        assertNull(getRet);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.size(7);
        cpcUpdateParams3.win(winsize);

        addRet = tairCpcClusterNew.sumArrayAdd(key3, timestamp, 100, cpcUpdateParams3);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcClusterNew.sumArrayAdd(key3, timestamp, 300, cpcUpdateParams3);
        assertEquals(400.00, addRet, 0.001);

        addRet = tairCpcClusterNew.sumArrayAdd(key3, timestamp-winsize, 300, cpcUpdateParams3);
        assertEquals(300.00, addRet, 0.001);

        getRet = tairCpcClusterNew.sumArrayGetRangeTimeMerge(key3, timestamp-winsize, timestamp);
        assertEquals(700.00, getRet, 0.001);

        getRet = tairCpcClusterNew.sumArrayGetRangeMerge(key3, timestamp, 2);
        assertEquals(700.00, getRet, 0.001);
    }

    @Test
    @Ignore
    public void maxTest() throws Exception {

        Double addRet = tairCpcClusterNew.maxAdd(key, 150);
        assertEquals(150.00, addRet, 0.001);

        addRet = tairCpcClusterNew.maxAdd(key, 100);
        assertEquals(150.00, addRet, 0.001);

        Double getRet = tairCpcClusterNew.maxGet(key);
        assertEquals(150.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpcClusterNew.maxAdd(key2, 100, cpcUpdateParams2);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcClusterNew.maxAdd(key2, 150, cpcUpdateParams2);
        assertEquals(150.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcClusterNew.maxGet(key2);
        assertEquals(0.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.px(2000);

        addRet = tairCpcClusterNew.maxAdd(key3, 100, cpcUpdateParams3);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcClusterNew.maxAdd(key3, 150, cpcUpdateParams3);
        assertEquals(150.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcClusterNew.maxGet(key3);
        assertEquals(0.00, getRet, 0.001);

        Double setRet = tairCpcClusterNew.maxSet(key, 100);
        assertEquals(100.00, setRet, 0.001);
    }

    @Test
    @Ignore
    public void maxArrayTest() throws Exception {

        Double addRet = tairCpcClusterNew.maxArrayAdd(key, timestamp, 100);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcClusterNew.maxArrayAdd(key, timestamp, 300);
        assertEquals(300.00, addRet, 0.001);

        Double getRet = tairCpcClusterNew.maxArrayGet(key, timestamp);
        assertEquals(300.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);
        cpcUpdateParams2.size(7);
        cpcUpdateParams2.win(winsize);

        addRet = tairCpcClusterNew.maxArrayAdd(key2, timestamp, 100, cpcUpdateParams2);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcClusterNew.maxArrayAdd(key2, timestamp, 300, cpcUpdateParams2);
        assertEquals(300.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcClusterNew.maxArrayGet(key2, timestamp);
        assertNull(getRet);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.size(7);
        cpcUpdateParams3.win(winsize);

        addRet = tairCpcClusterNew.maxArrayAdd(key3, timestamp, 100, cpcUpdateParams3);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcClusterNew.maxArrayAdd(key3, timestamp, 300, cpcUpdateParams3);
        assertEquals(300.00, addRet, 0.001);

        addRet = tairCpcClusterNew.maxArrayAdd(key3, timestamp-winsize, 300, cpcUpdateParams3);
        assertEquals(300.00, addRet, 0.001);

        getRet = tairCpcClusterNew.maxArrayGetRangeTimeMerge(key3, timestamp-winsize, timestamp);
        assertEquals(300.00, getRet, 0.001);

        getRet = tairCpcClusterNew.maxArrayGetRangeMerge(key3, timestamp, 2);
        assertEquals(300.00, getRet, 0.001);
    }

    @Test
    @Ignore
    public void minTest() throws Exception {

        Double addRet = tairCpcClusterNew.minAdd(key, 150);
        assertEquals(150.00, addRet, 0.001);

        addRet = tairCpcClusterNew.minAdd(key, 100);
        assertEquals(100.00, addRet, 0.001);

        Double getRet = tairCpcClusterNew.minGet(key);
        assertEquals(100.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpcClusterNew.minAdd(key2, 100, cpcUpdateParams2);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcClusterNew.minAdd(key2, 150, cpcUpdateParams2);
        assertEquals(100.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcClusterNew.minGet(key2);
        assertEquals(0.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.px(2000);

        addRet = tairCpcClusterNew.minAdd(key3, 100, cpcUpdateParams3);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcClusterNew.minAdd(key3, 150, cpcUpdateParams3);
        assertEquals(100.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcClusterNew.minGet(key3);
        assertEquals(0.00, getRet, 0.001);

        Double setRet = tairCpcClusterNew.minSet(key, 100);
        assertEquals(100.00, setRet, 0.001);
    }

    @Test
    @Ignore
    public void minArrayTest() throws Exception {

        Double addRet = tairCpcClusterNew.minArrayAdd(key, timestamp, 100);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcClusterNew.minArrayAdd(key, timestamp, 300);
        assertEquals(100.00, addRet, 0.001);

        Double getRet = tairCpcClusterNew.minArrayGet(key, timestamp);
        assertEquals(100.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);
        cpcUpdateParams2.size(7);
        cpcUpdateParams2.win(winsize);

        addRet = tairCpcClusterNew.minArrayAdd(key2, timestamp, 100, cpcUpdateParams2);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcClusterNew.minArrayAdd(key2, timestamp, 300, cpcUpdateParams2);
        assertEquals(100.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcClusterNew.minArrayGet(key2, timestamp);
        assertNull(getRet);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.size(7);
        cpcUpdateParams3.win(winsize);

        addRet = tairCpcClusterNew.minArrayAdd(key3, timestamp, 100, cpcUpdateParams3);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcClusterNew.minArrayAdd(key3, timestamp, 300, cpcUpdateParams3);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcClusterNew.minArrayAdd(key3, timestamp-winsize, 300, cpcUpdateParams3);
        assertEquals(300.00, addRet, 0.001);

        getRet = tairCpcClusterNew.minArrayGetRangeTimeMerge(key3, timestamp-winsize, timestamp);
        assertEquals(100.00, getRet, 0.001);

        getRet = tairCpcClusterNew.minArrayGetRangeMerge(key3, timestamp, 2);
        assertEquals(100.00, getRet, 0.001);
    }

    @Test
    @Ignore
    public void firstTest() throws Exception {

        String addRet = tairCpcClusterNew.firstAdd(key, content1, 150);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcClusterNew.firstAdd(key, content2, 100);
        Assert.assertEquals(content2, addRet);

        String getRet = tairCpcClusterNew.firstGet(key);
        Assert.assertEquals(content2, getRet);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpcClusterNew.firstAdd(key2, content1, 100, cpcUpdateParams2);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcClusterNew.firstAdd(key2, content2, 150, cpcUpdateParams2);
        Assert.assertEquals(content1, addRet);

        Thread.sleep(3000);

        getRet = tairCpcClusterNew.firstGet(key2);
        Assert.assertNull(getRet);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.px(2000);

        addRet = tairCpcClusterNew.firstAdd(key3, content1, 100, cpcUpdateParams3);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcClusterNew.firstAdd(key3, content2, 150, cpcUpdateParams3);
        Assert.assertEquals(content1, addRet);

        Thread.sleep(3000);

        getRet = tairCpcClusterNew.firstGet(key3);
        Assert.assertNull(getRet);

        String setRet = tairCpcClusterNew.firstSet(key, content1, 100);
        Assert.assertEquals(content1, setRet);
    }

    @Test
    @Ignore
    public void firstArrayTest() throws Exception {

        String addRet = tairCpcClusterNew.firstArrayAdd(key, timestamp, content1, 100);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcClusterNew.firstArrayAdd(key, timestamp, content2, 150);
        Assert.assertEquals(content1, addRet);

        String getRet = tairCpcClusterNew.firstArrayGet(key, timestamp);
        Assert.assertEquals(content1, getRet);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpcClusterNew.firstArrayAdd(key2, timestamp, content1, 100, cpcUpdateParams2);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcClusterNew.firstArrayAdd(key2, timestamp, content2, 150, cpcUpdateParams2);
        Assert.assertEquals(content1, addRet);

        Thread.sleep(3000);

        getRet = tairCpcClusterNew.firstArrayGet(key2, 1);
        Assert.assertNull(getRet);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.size(7);
        cpcUpdateParams3.win(winsize);

        addRet = tairCpcClusterNew.firstArrayAdd(key3, timestamp, content1, 100, cpcUpdateParams3);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcClusterNew.firstArrayAdd(key3, timestamp, content1, 300, cpcUpdateParams3);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcClusterNew.firstArrayAdd(key3, timestamp-winsize, content1, 300, cpcUpdateParams3);
        Assert.assertEquals(content1, addRet);

        getRet = tairCpcClusterNew.firstArrayGetRangeTimeMerge(key3, timestamp-winsize, timestamp);
        Assert.assertEquals(content1, addRet);

        getRet = tairCpcClusterNew.firstArrayGetRangeMerge(key3, timestamp, 2);
        Assert.assertEquals(content1, addRet);
    }

    @Test
    @Ignore
    public void lastTest() throws Exception {

        String addRet = tairCpcClusterNew.lastAdd(key, content1, 150);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcClusterNew.lastAdd(key, content2, 100);
        Assert.assertEquals(content1, addRet);

        String getRet = tairCpcClusterNew.lastGet(key);
        Assert.assertEquals(content1, getRet);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpcClusterNew.lastAdd(key2, content1, 100, cpcUpdateParams2);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcClusterNew.lastAdd(key2, content2, 150, cpcUpdateParams2);
        Assert.assertEquals(content2, addRet);

        Thread.sleep(3000);

        getRet = tairCpcClusterNew.lastGet(key2);
        Assert.assertNull(getRet);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.px(2000);

        addRet = tairCpcClusterNew.lastAdd(key3, content1, 100, cpcUpdateParams3);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcClusterNew.lastAdd(key3, content2, 150, cpcUpdateParams3);
        Assert.assertEquals(content2, addRet);

        Thread.sleep(3000);

        getRet = tairCpcClusterNew.lastGet(key3);
        Assert.assertNull(getRet);

        String setRet = tairCpcClusterNew.lastSet(key, content1, 120);
        Assert.assertEquals(content1, setRet);
    }

    @Test
    @Ignore
    public void lastArrayTest() throws Exception {

        String addRet = tairCpcClusterNew.lastArrayAdd(key, timestamp, content1, 100);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcClusterNew.lastArrayAdd(key, timestamp, content2, 150);
        Assert.assertEquals(content2, addRet);

        String getRet = tairCpcClusterNew.lastArrayGet(key, timestamp);
        Assert.assertEquals(content2, getRet);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpcClusterNew.lastArrayAdd(key2, timestamp, content1, 100, cpcUpdateParams2);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcClusterNew.lastArrayAdd(key2, timestamp, content2, 150, cpcUpdateParams2);
        Assert.assertEquals(content2, addRet);

        Thread.sleep(3000);

        getRet = tairCpcClusterNew.lastArrayGet(key2, 1);
        Assert.assertNull(getRet);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.size(7);
        cpcUpdateParams3.win(winsize);

        addRet = tairCpcClusterNew.lastArrayAdd(key3, timestamp, content1, 100, cpcUpdateParams3);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcClusterNew.lastArrayAdd(key3, timestamp, content1, 300, cpcUpdateParams3);
        Assert.assertEquals(content1, addRet);

        addRet = tairCpcClusterNew.lastArrayAdd(key3, timestamp-winsize, content1, 300, cpcUpdateParams3);
        Assert.assertEquals(content1, addRet);

        getRet = tairCpcClusterNew.lastArrayGetRangeTimeMerge(key3, timestamp-winsize, timestamp);
        Assert.assertEquals(content1, addRet);

        getRet = tairCpcClusterNew.lastArrayGetRangeMerge(key3, timestamp, 2);
        Assert.assertEquals(content1, addRet);
    }

    @Test
    @Ignore
    public void avgArrayTest() throws Exception {

        Double addRet = tairCpcClusterNew.avgArrayAdd(key, timestamp, count1, 100);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcClusterNew.avgArrayAdd(key, timestamp, count1, 300);
        assertEquals(200.00, addRet, 0.001);

        Double getRet = tairCpcClusterNew.avgArrayGet(key, timestamp);
        assertEquals(200.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);
        cpcUpdateParams2.size(7);
        cpcUpdateParams2.win(winsize);

        addRet = tairCpcClusterNew.avgArrayAdd(key2, timestamp, count1, 100, cpcUpdateParams2);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcClusterNew.avgArrayAdd(key2, timestamp, count1, 300, cpcUpdateParams2);
        assertEquals(200.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcClusterNew.avgArrayGet(key2, timestamp);
        assertNull(getRet);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.size(7);
        cpcUpdateParams3.win(winsize);

        addRet = tairCpcClusterNew.avgArrayAdd(key3, timestamp, count1, 100, cpcUpdateParams3);
        assertEquals(100.00, addRet, 0.001);

        addRet = tairCpcClusterNew.avgArrayAdd(key3, timestamp, count1, 300, cpcUpdateParams3);
        assertEquals(200.00, addRet, 0.001);

        addRet = tairCpcClusterNew.avgArrayAdd(key3, timestamp-winsize, count2, 300, cpcUpdateParams3);
        assertEquals(300.00, addRet, 0.001);

        getRet = tairCpcClusterNew.avgArrayGetRangeTimeMerge(key3, timestamp-winsize, timestamp);
        assertEquals(250.00, getRet, 0.001);

        getRet = tairCpcClusterNew.avgArrayGetRangeMerge(key3, timestamp, 2);
        assertEquals(250.00, getRet, 0.001);

    }

    @Test
    @Ignore
    public void stddevTest() throws Exception {

        Double addRet = tairCpcClusterNew.stddevAdd(key, 150);
        assertEquals(0.00, addRet, 0.001);

        addRet = tairCpcClusterNew.stddevAdd(key, 100);
        assertEquals(25.00, addRet, 0.001);

        Double getRet = tairCpcClusterNew.stddevGet(key);
        assertEquals(25.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
        cpcUpdateParams2.ex(2);

        addRet = tairCpcClusterNew.stddevAdd(key2, 100, cpcUpdateParams2);
        assertEquals(0.00, addRet, 0.001);

        addRet = tairCpcClusterNew.stddevAdd(key2, 150, cpcUpdateParams2);
        assertEquals(25.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcClusterNew.stddevGet(key2);
        assertEquals(0.00, getRet, 0.001);

        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
        cpcUpdateParams3.px(2000);

        addRet = tairCpcClusterNew.stddevAdd(key3, 100, cpcUpdateParams3);
        assertEquals(0.00, addRet, 0.001);

        addRet = tairCpcClusterNew.stddevAdd(key3, 150, cpcUpdateParams3);
        assertEquals(25.00, addRet, 0.001);

        Thread.sleep(3000);

        getRet = tairCpcClusterNew.stddevGet(key3);
        assertEquals(0.00, getRet, 0.001);

//        Double setRet = tairCpcClusterNew.stddevSet(key, 1, 100, 0);
//        assertEquals(0.00, setRet, 0.001);
    }

//    @Test
//    public void stddevArrayTest() throws Exception {
//
//        Double addRet = tairCpcClusterNew.stddevArrayAdd(key, 1,100, 7);
//        assertEquals(0.00, addRet, 0.001);
//
//        addRet = tairCpcClusterNew.stddevArrayAdd(key, 1,150, 7);
//        assertEquals(25.00, addRet, 0.001);
//
//        Double getRet = tairCpcClusterNew.stddevArrayGet(key, 1);
//        assertEquals(25.00, getRet, 0.001);
//
//        CpcUpdateParams cpcUpdateParams2 = new CpcUpdateParams();
//        cpcUpdateParams2.ex(2);
//
//        addRet = tairCpcClusterNew.stddevArrayAdd(key2, 1, 100, 7, cpcUpdateParams2);
//        assertEquals(0.00, addRet, 0.001);
//
//        addRet = tairCpcClusterNew.stddevArrayAdd(key2, 1, 150, 7, cpcUpdateParams2);
//        assertEquals(25.00, addRet, 0.001);
//
//        Thread.sleep(3000);
//
//        getRet = tairCpcClusterNew.stddevArrayGet(key2, 1);
//        assertNull(getRet);
////        assertEquals(0.00, getRet, 0.001);
//
//        CpcUpdateParams cpcUpdateParams3 = new CpcUpdateParams();
//        cpcUpdateParams3.px(2000);
//
//        addRet = tairCpcClusterNew.stddevArrayAdd(key3, 2, 100, 7, cpcUpdateParams3);
//        assertEquals(0.00, addRet, 0.001);
//
//        addRet = tairCpcClusterNew.stddevArrayAdd(key3, 2, 150, 7, cpcUpdateParams3);
//        assertEquals(25.00, addRet, 0.001);
//
//        Thread.sleep(3000);
//
//        getRet = tairCpcClusterNew.stddevArrayGet(key3, 2);
//        assertNull(getRet);
////        assertEquals(0.00, getRet, 0.001);
//
//        List<Double> rangeRet = tairCpcClusterNew.stddevArrayGetRange(key, 7, 7);
//        assertEquals(7, rangeRet.size());
//        assertEquals(25.00, rangeRet.get(0), 0.001);
//
//        Double mergeRet = tairCpcClusterNew.stddevArrayGetRangeMerge(key, 7, 7);
//        assertEquals(25.00, mergeRet, 0.001);
//    }
}