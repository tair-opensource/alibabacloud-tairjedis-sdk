package com.aliyun.tair.tairai;
import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairai.results.*;
import com.aliyun.tair.tairai.factory.TairAIBuilderFactory;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.util.SafeEncoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static redis.clients.jedis.Protocol.toByteArray;

public class TairAI {
    private Jedis jedis;

    public TairAI(Jedis jedis) {
        this.jedis = jedis;
    }

    private Jedis getJedis() {
        return jedis;
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
            throw new IllegalArgumentException(String.format("TairAI set: the vector size:%d is not equal to dimension:%d, key %s",vector.size(), dim, key));
        }
        args.add(SafeEncoder.encode(key));
        args.add(toByteArray(dim));
        args.add(toByteArray(id));
        Iterator<Float> it = vector.iterator();
        while (it.hasNext()) {
            String str = Float.toString(it.next());
            args.add(str.getBytes());
        }
        Object obj = getJedis().sendCommand(ModuleCommand.TAIHNSWSET, args.toArray(new byte[args.size()][]));
        return BuilderFactory.STRING.build(obj);
    }
    /**
     * mset vectors.
     *
     * @param key       the key
     * @param dim       the dimension
     * @param vectors   the vectors
     * @return Success: "OK"; Fail: reason.
     */
    public String mset(String key, int dim, final Map<Integer, List<Float>> vectors) {
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

        Object obj = getJedis().sendCommand(ModuleCommand.TAIHNSWMSET, args.toArray(new byte[args.size()][]));
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

        Object obj = getJedis().sendCommand(ModuleCommand.TAIHNSWWATCH, args.toArray(new byte[args.size()][]));
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * get id.
     *
     * @param key       the key
     * @param id        the id
     * @return Success: "OK"; Fail: reason.
     */
    public List<Float> get(String key, long id) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(SafeEncoder.encode(key));
        args.add(toByteArray(id));

        Object obj = getJedis().sendCommand(ModuleCommand.TAIHNSWGET, args.toArray(new byte[args.size()][]));
        return TairAIBuilderFactory.TairAIGET_RESULT_STRING.build(obj);
    }

    /**
     * knnget topN vectors.
     *
     * @param key       the key
     * @param topN      top N nearest vectors to be found if exist
     * @return Success: "OK"; Fail: reason.
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

        Object obj = getJedis().sendCommand(ModuleCommand.TAIHNSWKNNGET, args.toArray(new byte[args.size()][]));
        return TairAIBuilderFactory.TairAIKNNGET_RESULT_STRING.build(obj);
    }

    /**
     * stat key.
     * @param key       the key
     */
    public TairAIStatResult stat(String key) {
        final List<byte[]> args = new ArrayList<byte[]>();
        Object obj = getJedis().sendCommand(ModuleCommand.TAIHNSWSTAT, key);
        return TairAIBuilderFactory.TairAIStat_RESULT_STRING.build(obj);
    }
    /**
     * debug key.
     * @param key       the key
     */
    public TairAIDebugResult debug(String key) {
        final List<byte[]> args = new ArrayList<byte[]>();
        Object obj = getJedis().sendCommand(ModuleCommand.TAIHNSWDEBUG, key);
        return TairAIBuilderFactory.TairAIDebug_RESULT_STRING.build(obj);
    }
}
