package com.aliyun.tair.tests.tairai;

import com.aliyun.tair.tairai.*;
import com.aliyun.tair.tairai.results.TairAIKnngetResult;
import com.aliyun.tair.tairai.results.TairAIStatResult;
import org.junit.Test;
import redis.clients.jedis.util.SafeEncoder;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TairAIClusterTest extends TairAITestBase {
    private String randomkey_;
    private int dim;
    private List<Float> vector1;
    private List<Float> vector2;
    private List<Float> vector3;
    private List<Float> vector4;
    private String[] tobepush;
    private List<Float> tobeverify;


    private int id1;
    private int id2;
    private int id3;
    private int id4;

    private Map<Integer, List<Float>> vectors = new HashMap<Integer, List<Float>>();

    public TairAIClusterTest() {
        randomkey_ = "randomkey_" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        dim = 3;
        id1 = 1001;
        vector1 = Arrays.asList(new Float(1), new Float(2) ,new Float(3));

        id2 = 1002;
        id3 = 1003;
        id4 = 1004;
        tobepush = new String[]{"1","2","3"};
        tobeverify = Arrays.asList(new Float(1), new Float(2) ,new Float(3));

        vector2 = Arrays.asList(new Float(1.19), new Float(2.2) ,new Float(3.2));
        vector3 = Arrays.asList(new Float(2.91), new Float(3.2) ,new Float(4.2));
        vector4 = Arrays.asList(new Float(4.2), new Float(5.2) ,new Float(6.2));
        vectors.put(id2, vector2);
        vectors.put(id3, vector3);
        vectors.put(id4, vector4);
    }

    @Test
    public void taihnswsetTest() {
        if (!isSupportTairAI()) {
            return;
        }
        String ret;
        jedis.del(randomkey_);
        System.out.println(randomkey_);
        ret = tairAICluster.set(randomkey_, dim, id1, vector1);
        assertEquals("OK", ret);
        ret = tairAICluster.set(randomkey_, dim, id2, vector2);
        assertEquals("OK", ret);
        List<Float>  vecret = tairAICluster.get(randomkey_, id1);
        assertEquals(vecret, vector1);
    }

    @Test
    public void taihnswmsetTest() throws Exception{
        if (!isSupportTairAI()) {
            return;
        }
        String ret;
        jedis.del(randomkey_);
        System.out.println(randomkey_);
        ret = tairAICluster.set(randomkey_, dim, id1, vector1);
        assertEquals("OK", ret);
        ret = tairAICluster.mset(randomkey_, dim, vectors);
        assertEquals("OK", ret);
        List<Float>  vecret = tairAICluster.get(randomkey_, id2);
        assertEquals(vecret, vector2);
    }

    @Test
    public void taihnswwatchTest() {
        if (!isSupportTairAI()) {
            return;
        }
        String ret;
        jedis.del(randomkey_);
        System.out.println(randomkey_);
        ret = tairAICluster.set(randomkey_, dim, id1, vector1);
        assertEquals("OK", ret);
        ret = tairAICluster.watch(randomkey_, dim, "200*");
        jedis.del("2001");
        long pushret = jedis.rpush("2001", tobepush);
        assertEquals(3, pushret);
        try {
            Thread.sleep(2000);
        } catch(InterruptedException e) {
            e.printStackTrace();
        }

        try {
            List<Float> vecret = tairAICluster.get(randomkey_, 2001);
            assertEquals(vecret, tobeverify);
        } finally {

        }

        TairAIStatResult statret = tairAICluster.stat(randomkey_);
        assertEquals(2, statret.getIndexSize());
        assertEquals("200*", statret.getPattern());
        assertEquals(0, statret.getPenddingSize());
        assertEquals(1, statret.getTriggerNum());
    }

    @Test
    public void taihnswknngetTest() {
        if (!isSupportTairAI()) {
            return;
        }
        String ret;
        jedis.del(randomkey_);
        System.out.println(randomkey_);
        ret = tairAICluster.set(randomkey_, dim, id1, vector1);
        assertEquals("OK", ret);
        ret = tairAICluster.mset(randomkey_, dim, vectors);
        assertEquals("OK", ret);
        TairAIKnngetResult knnret = tairAICluster.knnget(randomkey_, 3, vector1);
        assertEquals(3, knnret.getNum());
        assertEquals(id1, knnret.getVector(0).getId());
        assertEquals(id2, knnret.getVector(1).getId());
        assertEquals(id3, knnret.getVector(2).getId());
    }

}
