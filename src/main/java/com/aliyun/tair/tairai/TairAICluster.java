package com.aliyun.tair.tairai;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairai.results.*;

import com.aliyun.tair.tairai.factory.TairAIBuilderFactory;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static redis.clients.jedis.Protocol.toByteArray;

public class TairAICluster {
    private JedisCluster jc;

    public TairAICluster(JedisCluster jc) {
        if (jc == null) {
            throw new NullPointerException("TairAICluster fail");
        }
        this.jc = jc;
    }
    /**
     * set vector.
     *
     * @param key       the key
     * @param dim       the dimension
     * @param id        the id for vector
     * @param vector    the vector
     * @return Success: "OK"; Fail: reason.
     */
    public String set(String key, int dim,  int id, final List<Float> vector) {
        final List<byte[]> args = new ArrayList<byte[]>();
        if (dim != vector.size()) {
            throw new IllegalArgumentException(String.format("TairAI set: the vector size:%d is not equal to dimension:%d",vector.size(), dim));
        }
        args.add(SafeEncoder.encode(key));
        args.add(toByteArray(dim));
        args.add(toByteArray(id));
        Iterator<Float> it = vector.iterator();
        while (it.hasNext()) {
            String str = Float.toString(it.next());
            args.add(str.getBytes());
        }
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.TAIHNSWSET, args.toArray(new byte[args.size()][]));
        return BuilderFactory.STRING.build(obj);
    }
    /**
     * mset vectors.
     *
     * @param key       the key
     * @param dim       the dimension
     * @param id        the id for vector
     * @param vectors   the vectors
     * @return Success: "OK"; Fail: reason.
     */
    public String mset(String key, int dim,  final Map<Integer, List<Float>> vectors) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(SafeEncoder.encode(key));
        args.add(toByteArray(dim));

        for (final Map.Entry<Integer, List<Float>> entry : vectors.entrySet()) {
            args.add(toByteArray(entry.getKey()));
            if (dim != entry.getValue().size()) {
                throw new IllegalArgumentException(String.format("TairAI mset: the vector size:%d for id %d is not equal to dimension:%d",entry.getValue().size(), entry.getKey(), dim));
            }

            Iterator<Float> it = entry.getValue().iterator();
            while (it.hasNext()) {
                args.add(Float.toString(it.next()).getBytes());
            }
        }

        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.TAIHNSWMSET, args.toArray(new byte[args.size()][]));
        return BuilderFactory.STRING.build(obj);
    }
    /**
     * watch list.
     *
     * @param key       the key
     * @param dim       the dimension
     * @param pattern   the pattern
     * @return Success: "OK"; Fail: reason.
     */
    public String watch(String key, int dim,  String pattern) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(SafeEncoder.encode(key));
        args.add(toByteArray(dim));
        args.add(pattern.getBytes());

        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.TAIHNSWWATCH, args.toArray(new byte[args.size()][]));
        return BuilderFactory.STRING.build(obj);
    }


    /**
     * get the vector by id (should be set/watch before).
     *
     * @param key       the key
     * @param id        the id
     * @return Success: "OK"; Fail: reason.
     */
    public List<Float> get(String key, long id) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(SafeEncoder.encode(key));
        args.add(toByteArray(id));

        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.TAIHNSWGET, args.toArray(new byte[args.size()][]));
        return TairAIBuilderFactory.TairAIGET_RESULT_STRING.build(obj);
    }
    /**
     * knnget topN vectors.
     *
     * @param key       the key
     * @param topN      top N nearest vectors to be found if exist
     * @return TairAIKnngetResult
     */
    public TairAIKnngetResult knnget(String key, int topN, final List<Float> vector) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(SafeEncoder.encode(key));
        args.add(toByteArray(topN));

        Iterator<Float> it = vector.iterator();
        while (it.hasNext()) {
            String str = Float.toString(it.next());
            args.add(str.getBytes());
        }

        Object obj =jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.TAIHNSWKNNGET, args.toArray(new byte[args.size()][]));
        return TairAIBuilderFactory.TairAIKNNGET_RESULT_STRING.build(obj);
    }
    /**
     * stat key.
     * @param key       the key
     * @return TairAIStatResult
     */
    public TairAIStatResult stat(String key) {
        //final List<byte[]> args = new ArrayList<byte[]>();
        Object obj = jc.sendCommand(key, ModuleCommand.TAIHNSWSTAT, key);
        return TairAIBuilderFactory.TairAIStat_RESULT_STRING.build(obj);
    }
    /**
     * debug key.
     * @param key       the key
     * @return TairAIDebugResult
     */
    public TairAIDebugResult debug(String key) {
        //final List<byte[]> args = new ArrayList<byte[]>();
        Object obj = jc.sendCommand(key, ModuleCommand.TAIHNSWDEBUG, key);
        return TairAIBuilderFactory.TairAIDebug_RESULT_STRING.build(obj);
    }
}
