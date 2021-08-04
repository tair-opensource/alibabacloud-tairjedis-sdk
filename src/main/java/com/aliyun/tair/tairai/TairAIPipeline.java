package com.aliyun.tair.tairai;
import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairai.factory.TairAIBuilderFactory;
import com.aliyun.tair.tairai.results.TairAIKnngetResult;
import com.aliyun.tair.tairai.results.TairAIStatResult;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.util.SafeEncoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static redis.clients.jedis.Protocol.toByteArray;

public class TairAIPipeline extends Pipeline {

    /**
     * set vector.
     *
     * @param key       the key
     * @param dim       the dimension
     * @param id        the id for vector
     * @param vector    the vector
     * @return Success: "OK"; Fail: reason.
     */
    public Response<String> set(String key, int dim,  int id, final List<Float> vector) {
        return set(SafeEncoder.encode(key), dim, id, vector);
    }

    public Response<String> set(byte[] key, int dim,  int id, final List<Float> vector) {
        final List<byte[]> args = new ArrayList<byte[]>();
        if (dim != vector.size()) {
            throw new IllegalArgumentException(String.format("TairAI set: the vector size:%d is not equal to dimension:%d",vector.size(), dim));
        }
        args.add(key);
        args.add(toByteArray(dim));
        args.add(toByteArray(id));
        Iterator<Float> it = vector.iterator();
        while (it.hasNext()) {
            String str = Float.toString(it.next());
            args.add(str.getBytes());
        }
        getClient("").sendCommand(ModuleCommand.TAIHNSWSET, args.toArray(new byte[args.size()][]));
        return getResponse(BuilderFactory.STRING);
    }

    /**
     * mset vectors.
     *
     * @param key       the key
     * @param dim       the dimension
     * @param vectors   the vectors
     * @return Success: 1; Not exist: -1; Fail: 0.
     */
    public Response<String> mset(String key, int dim,  final Map<Integer, List<Float>> vectors) {
        return mset(SafeEncoder.encode(key), dim, vectors);
    }

    public Response<String> mset(byte[] key, int dim, final Map<Integer, List<Float>> vectors) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(key);
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
        getClient("").sendCommand(ModuleCommand.TAIHNSWMSET, args.toArray(new byte[args.size()][]));
        return getResponse(BuilderFactory.STRING);
    }

    /**
     * watch list.
     *
     * @param key       the key
     * @param dim       the dimension
     * @param pattern   the pattern
     * @return Success: 1; Not exist: -1; Fail: 0.
     */
    public Response<String> watch(String key, int dim,  String pattern) {
        return watch(SafeEncoder.encode(key), dim, pattern);
    }

    public Response<String> watch(byte[] key, int dim,  String pattern) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(key);
        args.add(toByteArray(dim));
        args.add(pattern.getBytes());


        getClient("").sendCommand(ModuleCommand.TAIHNSWWATCH, args.toArray(new byte[args.size()][]));
        return getResponse(BuilderFactory.STRING);
    }

    /**
     * get the vector by id (should be set/watch before).
     *
     * @param key       the key
     * @param id        the id
     * @return Success: "OK"; Fail: reason.
     */
    public Response<List<Float>> get(String key, long id) {
        return get(SafeEncoder.encode(key), id);
    }

    public Response<List<Float>> get(byte[] key, long id) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(key);
        args.add(toByteArray(id));

        getClient("").sendCommand(ModuleCommand.TAIHNSWGET, args.toArray(new byte[args.size()][]));
        return getResponse(TairAIBuilderFactory.TairAIGET_RESULT_STRING);
    }
    /**
     * knnget topN vectors.
     *
     * @param key       the key
     * @param topN      top N nearest vectors to be found if exist
     * @return TairAIKnngetResult
     */
    public Response<TairAIKnngetResult> knnget(String key, int topN, final List<Float> vector) {
        return knnget(SafeEncoder.encode(key), topN, vector);
    }

    public  Response<TairAIKnngetResult> knnget(byte[] key, int topN, final List<Float> vector) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(key);
        args.add(toByteArray(topN));

        Iterator<Float> it = vector.iterator();
        while (it.hasNext()) {
            String str = Float.toString(it.next());
            args.add(str.getBytes());
        }

        getClient("").sendCommand(ModuleCommand.TAIHNSWKNNGET, args.toArray(new byte[args.size()][]));
        return getResponse(TairAIBuilderFactory.TairAIKNNGET_RESULT_STRING);
    }

    /**
     * stat key.
     *
     * @param key       the key
     * @return TairAIStatResult
     */
    public Response<TairAIStatResult> stat(String key) {
        return stat(SafeEncoder.encode(key));
    }

    public  Response<TairAIStatResult> stat(byte[] key) {
        final List<byte[]> args = new ArrayList<byte[]>();
        args.add(key);

        getClient("").sendCommand(ModuleCommand.TAIHNSWSTAT, key);
        return getResponse(TairAIBuilderFactory.TairAIStat_RESULT_STRING);
    }
}
