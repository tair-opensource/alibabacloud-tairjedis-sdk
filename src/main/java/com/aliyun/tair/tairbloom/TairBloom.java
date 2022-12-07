package com.aliyun.tair.tairbloom;

import com.aliyun.tair.tairbloom.params.BfmaddParams;
import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairbloom.factory.BloomBuilderFactory;
import com.aliyun.tair.tairbloom.params.BfinsertParams;
import com.aliyun.tair.tairbloom.params.BfmexistParams;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.util.SafeEncoder;

import java.util.List;

import static redis.clients.jedis.Protocol.toByteArray;

public class TairBloom {
    private Jedis jedis;
    private JedisPool jedisPool;

    public TairBloom(Jedis jedis) {
        this.jedis = jedis;
    }

    public TairBloom(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    private Jedis getJedis() {
        if (jedisPool != null) {
            return jedisPool.getResource();
        }
        return jedis;
    }

    private void releaseJedis(Jedis jedis) {
        if (jedisPool != null) {
            jedis.close();
        }
    }

    /**
     * Create an empty bloomfilter with the initCapacity & errorRate.
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.BFRESERVE, key, toByteArray(errorRate), toByteArray(initCapacity));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.BFADD, key, item);
            return BuilderFactory.BOOLEAN.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Add items to bloomfilter.
     * @param key  the key
     * @param items the items: item [item...]
     * @return Boolean array; Success: true, fail: false
     */
    public Boolean[] bfmadd(String key, String... items) {
        Jedis jedis = getJedis();
        try {
            BfmaddParams params = new BfmaddParams();
            Object obj = jedis.sendCommand(ModuleCommand.BFMADD, params.getByteParams(key, items));
            return BloomBuilderFactory.BFMADD_RESULT_BOOLEAN_LIST.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Boolean[] bfmadd(byte[] key, byte[]... items) {
        Jedis jedis = getJedis();
        try {
            BfmaddParams params = new BfmaddParams();
            Object obj = jedis.sendCommand(ModuleCommand.BFMADD, params.getByteParams(key, items));
            return BloomBuilderFactory.BFMADD_RESULT_BOOLEAN_LIST.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * find if the item in bloomfilter.
     *
     * @param key  the key
     * @param item the item
     * @return Boolean; Success: true, fail: false
     */
    public Boolean bfexists(String key, String item) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.BFEXISTS, key, item);
            return BuilderFactory.BOOLEAN.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Boolean bfexists(byte[] key, byte[] item) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.BFEXISTS, key, item);
            return BuilderFactory.BOOLEAN.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * find if the items in bloomfilter.
     *
     * @param key  the key
     * @param items the items: item [item...]
     * @return Boolean array; Success: true, fail: false
     */
    public Boolean[] bfmexists(String key, String... items) {
        Jedis jedis = getJedis();
        try {
            BfmexistParams params = new BfmexistParams();
            Object obj = jedis.sendCommand(ModuleCommand.BFMEXISTS, params.getByteParams(key, items));
            return BloomBuilderFactory.BFMADD_RESULT_BOOLEAN_LIST.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Boolean[] bfmexists(byte[] key, byte[]... items) {
        Jedis jedis = getJedis();
        try {
            BfmexistParams params = new BfmexistParams();
            Object obj = jedis.sendCommand(ModuleCommand.BFMEXISTS, params.getByteParams(key, items));
            return BloomBuilderFactory.BFMADD_RESULT_BOOLEAN_LIST.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * insert the multiple items in bloomfilter.
     * @param key the key
     * @param params can set "CAPACITY", ERROR", "NOCREATE"
     * @param items the items
     * @return Boolean array; Success: true, fail: false
     */
    public Boolean[] bfinsert(String key, BfinsertParams params, String... items) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.BFINSERT,
                params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encodeMany(items)));
            return BloomBuilderFactory.BFINSERT_RESULT_BOOLEAN_LIST.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Boolean[] bfinsert(byte[] key, BfinsertParams params, byte[]... items) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.BFINSERT,
                params.getByteParams(key, items));
            return BloomBuilderFactory.BFINSERT_RESULT_BOOLEAN_LIST.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
    @Deprecated
    public Boolean[] bfinsert(String key, String initCapacityTag, long initCapacity, String errorRateTag, Double errorRate, String itemTag, String... items) {
        Jedis jedis = getJedis();
        try {
            BfinsertParams params = new BfinsertParams();
            byte[][] metadata = params.getByteParamsMeta(key, initCapacityTag, String.valueOf(initCapacity), errorRateTag, String.valueOf(errorRate), itemTag);
            Object obj = jedis.sendCommand(ModuleCommand.BFINSERT, params.getByteParams(metadata, items));
            return BloomBuilderFactory.BFINSERT_RESULT_BOOLEAN_LIST.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
    @Deprecated
    public Boolean[] bfinsert(String key, String nocreateTag, String itemTag, String... items) {
        Jedis jedis = getJedis();
        try {
            BfinsertParams params = new BfinsertParams();
            byte[][] metadata = params.getByteParamsMeta(key, nocreateTag, itemTag);
            Object obj = jedis.sendCommand(ModuleCommand.BFINSERT, params.getByteParams(metadata, items));
            return BloomBuilderFactory.BFINSERT_RESULT_BOOLEAN_LIST.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    @Deprecated
    public Boolean[] bfinsert(String key, String itemTag, String... items) {
        Jedis jedis = getJedis();
        try {
            BfinsertParams params = new BfinsertParams();
            byte[][] metadata = params.getByteParamsMeta(key, itemTag);
            Object obj = jedis.sendCommand(ModuleCommand.BFINSERT, params.getByteParams(metadata, items));
            return BloomBuilderFactory.BFINSERT_RESULT_BOOLEAN_LIST.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    @Deprecated
    public Boolean[] bfinsert(byte[] key, byte[] initCapacityTag, long initCapacity, byte[] errorRateTag, Double errorRate, byte[] itemTag, byte[]... items) {
        Jedis jedis = getJedis();
        try {
            BfinsertParams params = new BfinsertParams();
            byte[][] metadata = params.getByteParamsMeta(key, initCapacityTag, toByteArray(initCapacity), errorRateTag, toByteArray(errorRate), itemTag);
            Object obj = jedis.sendCommand(ModuleCommand.BFINSERT, params.getByteParams(metadata, items));
            return BloomBuilderFactory.BFINSERT_RESULT_BOOLEAN_LIST.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    @Deprecated
    public Boolean[] bfinsert(byte[] key, byte[] nocreateTag, byte[] itemTag, byte[]... items) {
        Jedis jedis = getJedis();
        try {
            BfinsertParams params = new BfinsertParams();
            byte[][] metadata = params.getByteParamsMeta(key, nocreateTag, itemTag);
            Object obj = jedis.sendCommand(ModuleCommand.BFINSERT, params.getByteParams(metadata, items));
            return BloomBuilderFactory.BFINSERT_RESULT_BOOLEAN_LIST.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    @Deprecated
    public Boolean[] bfinsert(byte[] key, byte[] itemTag, byte[]... items) {
        Jedis jedis = getJedis();
        try {
            BfinsertParams params = new BfinsertParams();
            byte[][] metadata = params.getByteParamsMeta(key, itemTag);
            Object obj = jedis.sendCommand(ModuleCommand.BFINSERT, params.getByteParams(metadata, items));
            return BloomBuilderFactory.BFINSERT_RESULT_BOOLEAN_LIST.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * debug the bloomfilter.
     *
     * @param key  the key
     * @return List for debug messages
     */
    public List<String> bfdebug(String key) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.BFDEBUG, key);
            return BuilderFactory.STRING_LIST.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public List<String> bfdebug(byte[] key) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.BFDEBUG, key);
            return BuilderFactory.STRING_LIST.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }
}
