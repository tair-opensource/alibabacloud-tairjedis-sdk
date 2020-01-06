package com.kvstore.jedis.tairbloom;

import com.kvstore.jedis.ModuleCommand;
import com.kvstore.jedis.tairbloom.factory.BloomBuilderFactory;
import com.kvstore.jedis.tairbloom.params.BfinsertParams;
import com.kvstore.jedis.tairbloom.params.BfmexistParams;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.util.SafeEncoder;

import java.util.List;

import static redis.clients.jedis.Protocol.toByteArray;

/**
 * @author dwan
 * @date 2019/12/25
 */
public class TairBloom {
    private Jedis jedis;
    private JedisPool jedisPool;

    public TairBloom(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    public TairBloom(Jedis jedis) {
        this.jedis = jedis;
    }

    private Jedis getJedis() {
        if (jedisPool != null) {
            return jedisPool.getResource();
        }
        return jedis;
    }

    /**
     * Create a empty bloomfilter with the initCapacity & errorRate.
     *
     * @param key          the key
     * @param initCapacity the initCapacity
     * @param errorRate    the errorRate
     * @return String simple-string-reply
     * {@literal OK} if success
     * {@literal error} if failure
     */
    public String bfreserve(String key, long initCapacity, double errorRate) {
        return bfreserve(SafeEncoder.encode(key), initCapacity, errorRate);
    }

    public String bfreserve(byte[] key, long initCapacity, double errorRate) {
        Object obj = getJedis().sendCommand(ModuleCommand.BFRESERVE, key, toByteArray(errorRate), toByteArray(initCapacity));
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Add a item to bloomfilter.
     *
     * @param key  the key
     * @param item the item
     * @return Success: true, fail: false
     */
    public Boolean bfadd(String key, String item) {
        return bfadd(SafeEncoder.encode(key), SafeEncoder.encode(item));
    }

    public Boolean bfadd(byte[] key, byte[] item) {
        Object obj = getJedis().sendCommand(ModuleCommand.BFADD, key, item);
        return BuilderFactory.BOOLEAN.build(obj);
    }

    /**
     * Add items to bloomfilter.
     *
     * @param args the args: key item [item...]
     * @return Boolean array; Success: true, fail: false
     */
    public Boolean[] bfmadd(String... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.BFMADD, args);
        return BloomBuilderFactory.BFMADD_RESULT_BOOLEAN_LIST.build(obj);
    }

    public Boolean[] bfmadd(byte[]... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.BFMADD, args);
        return BloomBuilderFactory.BFMADD_RESULT_BOOLEAN_LIST.build(obj);
    }

    /**
     * find if the item in bloomfilter.
     *
     * @param key  the key
     * @param item the item
     * @return Boolean; Success: true, fail: false
     */
    public Boolean bfexists(String key, String item) {
        Object obj = getJedis().sendCommand(ModuleCommand.BFEXISTS, key, item);
        return BuilderFactory.BOOLEAN.build(obj);
    }

    public Boolean bfexists(byte[] key, byte[] item) {
        Object obj = getJedis().sendCommand(ModuleCommand.BFEXISTS, key, item);
        return BuilderFactory.BOOLEAN.build(obj);
    }

    /**
     * find if the items in bloomfilter.
     *
     * @param key  the key
     * @param item the item: item [item...]
     * @return Boolean array; Success: true, fail: false
     */
    public Boolean[] bfmexists(String key, String... item) {
        BfmexistParams params = new BfmexistParams();
        Object obj = getJedis().sendCommand(ModuleCommand.BFMEXISTS, params.getByteParams(key, item));
        return BloomBuilderFactory.BFMADD_RESULT_BOOLEAN_LIST.build(obj);
    }

    public Boolean[] bfmexists(byte[] key, byte[]... item) {
        BfmexistParams params = new BfmexistParams();
        Object obj = getJedis().sendCommand(ModuleCommand.BFMEXISTS, params.getByteParams(key, item));
        return BloomBuilderFactory.BFMADD_RESULT_BOOLEAN_LIST.build(obj);
    }

    /**
     * insert the multiple items in bloomfilter.
     * {key} [CAPACITY {cap}] [ERROR {error}] ITEMS {item...}.
     * @param key             the key
     * @param initCapacityTag the initCapacityTag: "CAPACITY"
     * @param initCapacity    the initCapacity
     * @param errorRateTag    the errorRateTag: "ERROR"
     * @param errorRate       the errorRate
     * @param itemTag         the itemTag: "ITEMS"
     * @param items           the items: item [item...]
     * @return Boolean array; Success: true, fail: false
     */
    public Boolean[] bfinsert(String key, String initCapacityTag, long initCapacity, String errorRateTag, Double errorRate, String itemTag, String... items) {
        BfinsertParams params = new BfinsertParams();
        byte[][] metadata = params.getByteParamsMeta(key, initCapacityTag, String.valueOf(initCapacity), errorRateTag, String.valueOf(errorRate), itemTag);
        Object obj = getJedis().sendCommand(ModuleCommand.BFINSERT, params.getByteParams(metadata, items));
        return BloomBuilderFactory.BFINSERT_RESULT_BOOLEAN_LIST.build(obj);
    }

    /**
     * insert the multiple items in bloomfilter. It doesn't create bloomfilter, if bloomfilter isn't exist.
     * {key} [NOCREATE] ITEMS {item...}.
     * @param key             the key
     * @param nocreateTag     the nocreateTag: "NOCREATE"
     * @param itemTag         the itemTag: "ITEMS"
     * @param items           the items: item [item...]
     * @return Boolean array; Success: true, fail: false
     */
    public Boolean[] bfinsert(String key, String nocreateTag, String itemTag, String... items) {
        BfinsertParams params = new BfinsertParams();
        byte[][] metadata = params.getByteParamsMeta(key, nocreateTag, itemTag);
        Object obj = getJedis().sendCommand(ModuleCommand.BFINSERT, params.getByteParams(metadata, items));
        return BloomBuilderFactory.BFINSERT_RESULT_BOOLEAN_LIST.build(obj);
    }

    public Boolean[] bfinsert(String key, String itemTag, String... items) {
        BfinsertParams params = new BfinsertParams();
        byte[][] metadata = params.getByteParamsMeta(key, itemTag);
        Object obj = getJedis().sendCommand(ModuleCommand.BFINSERT, params.getByteParams(metadata, items));
        return BloomBuilderFactory.BFINSERT_RESULT_BOOLEAN_LIST.build(obj);
    }

    public Boolean[] bfinsert(byte[] key, byte[] initCapacityTag, long initCapacity, byte[] errorRateTag, Double errorRate, byte[] itemTag, byte[]... items) {
        BfinsertParams params = new BfinsertParams();
        byte[][] metadata = params.getByteParamsMeta(key, initCapacityTag, toByteArray(initCapacity), errorRateTag, toByteArray(errorRate), itemTag);
        Object obj = getJedis().sendCommand(ModuleCommand.BFINSERT, params.getByteParams(metadata, items));
        return BloomBuilderFactory.BFINSERT_RESULT_BOOLEAN_LIST.build(obj);
    }

    public Boolean[] bfinsert(byte[] key, byte[] nocreateTag, byte[] itemTag, byte[]... items) {
        BfinsertParams params = new BfinsertParams();
        byte[][] metadata = params.getByteParamsMeta(key, nocreateTag, itemTag);
        Object obj = getJedis().sendCommand(ModuleCommand.BFINSERT, params.getByteParams(metadata, items));
        return BloomBuilderFactory.BFINSERT_RESULT_BOOLEAN_LIST.build(obj);
    }

    public Boolean[] bfinsert(byte[] key, byte[] itemTag, byte[]... items) {
        BfinsertParams params = new BfinsertParams();
        byte[][] metadata = params.getByteParamsMeta(key, itemTag);
        Object obj = getJedis().sendCommand(ModuleCommand.BFINSERT, params.getByteParams(metadata, items));
        return BloomBuilderFactory.BFINSERT_RESULT_BOOLEAN_LIST.build(obj);
    }

    /**
     * debug the bloomfilter.
     *
     * @param key  the key
     * @return List for debug messages
     */
    public List<String> bfdebug(String key) {
        Object obj = getJedis().sendCommand(ModuleCommand.BFDEBUG, key);
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<String> bfdebug(byte[] key) {
        Object obj = getJedis().sendCommand(ModuleCommand.BFDEBUG, key);
        return BuilderFactory.STRING_LIST.build(obj);
    }
}
