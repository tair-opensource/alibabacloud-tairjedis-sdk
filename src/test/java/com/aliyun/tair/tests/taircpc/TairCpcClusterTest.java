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

public class TairCpcClusterTest extends TairCpcTestBase {
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

    public TairCpcClusterTest() {
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

        String addRet = tairCpcCluster.cpcMUpdate(addList);
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

        List<Double> addRet = tairCpcCluster.cpcMUpdate2Est(addList);
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

        List<Update2JudResult> addRet = tairCpcCluster.cpcMUpdate2Jud(addList);
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
}
