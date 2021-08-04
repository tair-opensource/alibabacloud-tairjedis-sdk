package com.aliyun.tair.tests.tairai;

import com.aliyun.tair.tairai.*;
import com.aliyun.tair.tairai.results.TairAIKnngetResult;
import com.aliyun.tair.tairai.results.TairAIStatResult;
import org.junit.Test;
import redis.clients.jedis.util.SafeEncoder;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class TairAIPipelineTest extends TairAITestBase {
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

    public TairAIPipelineTest() {
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
        jedis.del(randomkey_);
        System.out.println(randomkey_);
        tairAIPipeline.set(randomkey_, 3, id1, vector1);
        tairAIPipeline.set(randomkey_, 3, id2, vector2);
        //tairaipipline.watch("phkey", 3, "20000*");
        //tairaipipline.knnget("phkey", 3, vector2);

        List<Object> objs = tairAIPipeline.syncAndReturnAll();
        int i = 0;
        assertEquals("OK", objs.get(i++));
        assertEquals("OK", objs.get(i++));
        //assertEquals("OK", objs.get(i++));
        //System.out.println(TairAIKnngetResult.class.cast(objs.get(i++)));
    }

    @Test
    public void taihnswmsetTest() throws Exception{
        if (!isSupportTairAI()) {
            return;
        }
        jedis.del(randomkey_);
        System.out.println(randomkey_);
        tairAIPipeline.set(randomkey_, 3, id1, vector1);
        tairAIPipeline.mset(randomkey_, 3, vectors);
        List<Object> objs = tairAIPipeline.syncAndReturnAll();
        int i = 0;
        assertEquals("OK", objs.get(i++));
        assertEquals("OK", objs.get(i++));
    }

    @Test
    public void taihnswwatchTest() {
        if (!isSupportTairAI()) {
            return;
        }
        String ret;
        jedis.del(randomkey_);
        System.out.println(randomkey_);
        tairAIPipeline.set(randomkey_, dim, id1, vector1);
        tairAIPipeline.watch(randomkey_, dim, "200*");
        List<Object> objs = tairAIPipeline.syncAndReturnAll();
        int i = 0;

        jedis.del("2001");
        long pushret = jedis.rpush("2001", tobepush);
        assertEquals(3, pushret);
        assertEquals("OK", objs.get(i++));
        assertEquals("OK", objs.get(i++));

        // set/mset for db is async
        try {
            Thread.sleep(1000);
        } catch(InterruptedException e) {
            e.printStackTrace();
        }

        tairAIPipeline.get(randomkey_, 2001);
        tairAIPipeline.stat(randomkey_);
        objs = tairAIPipeline.syncAndReturnAll();
        i = 0;
        try {
            assertEquals(tobeverify, objs.get(i++));
        } finally {

        }
        assertEquals(1, TairAIStatResult.class.cast(objs.get(i)).getTriggerNum());
        assertEquals("200*", TairAIStatResult.class.cast(objs.get(i)).getPattern());
        assertEquals(2, TairAIStatResult.class.cast(objs.get(i)).getIndexSize());
        i++;
    }

    @Test
    public void taihnswknngetTest() {
        if (!isSupportTairAI()) {
            return;
        }
        jedis.del(randomkey_);
        System.out.println(randomkey_);
        tairAIPipeline.set(randomkey_, dim, id1, vector1);
        tairAIPipeline.mset(randomkey_, dim, vectors);
        List<Object> objs = tairAIPipeline.syncAndReturnAll();
        int i = 0;
        assertEquals("OK", objs.get(i++));
        assertEquals("OK", objs.get(i++));
        // set/mset for db is async
        try {
            Thread.sleep(2000);
        } catch(InterruptedException e) {
            e.printStackTrace();
        }

        tairAIPipeline.knnget(randomkey_, 3, vector1);
        objs = tairAIPipeline.syncAndReturnAll();
        i = 0;
        assertEquals(id1, TairAIKnngetResult.class.cast(objs.get(i)).getVector(0).getId());
        assertEquals(id2, TairAIKnngetResult.class.cast(objs.get(i)).getVector(1).getId());
        assertEquals(id3, TairAIKnngetResult.class.cast(objs.get(i)).getVector(2).getId());
        i++;
    }
}
