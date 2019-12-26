package com.kvstore.jedis.tairbloom;

import com.kvstore.jedis.ModuleCommand;
import com.kvstore.jedis.tairbloom.factory.BloomBuilderFactory;
import com.kvstore.jedis.tairbloom.params.BfinsertParams;
import com.kvstore.jedis.tairbloom.params.BfmexistParams;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.util.SafeEncoder;

import java.util.List;

import static redis.clients.jedis.Protocol.toByteArray;

/**
 * @author dwan
 * @date 2019/12/26
 */
public class TairBloomCluster {
    private JedisCluster jc;

    public TairBloomCluster(JedisCluster jc) {
        this.jc = jc;
    }

    public String bfreserve(String sampleKey, String key, long initCapacity, double errorRate) {
        return bfreserve(SafeEncoder.encode(sampleKey), SafeEncoder.encode(key), initCapacity, errorRate);
    }

    public String bfreserve(byte[] sampleKey, byte[] key, long initCapacity, double errorRate) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.BFRESERVE, key, toByteArray(errorRate), toByteArray(initCapacity));
        return BuilderFactory.STRING.build(obj);
    }

    public Boolean bfadd(String sampleKey, String key, String item) {
        return bfadd(SafeEncoder.encode(sampleKey), SafeEncoder.encode(key), SafeEncoder.encode(item));
    }

    public Boolean bfadd(byte[] sampleKey, byte[] key, byte[] item) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.BFADD, key, item);
        return BuilderFactory.BOOLEAN.build(obj);
    }

    public Boolean[] bfmadd(String sampleKey, String... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.BFMADD, args);
        return BloomBuilderFactory.BFMADD_RESULT_BOOLEAN_LIST.build(obj);
    }

    public Boolean[] bfmadd(byte[] sampleKey, byte[]... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.BFMADD, args);
        return BloomBuilderFactory.BFMADD_RESULT_BOOLEAN_LIST.build(obj);
    }

    public Boolean bfexists(String sampleKey, String key, String value) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.BFEXISTS, key, value);
        return BuilderFactory.BOOLEAN.build(obj);
    }

    public Boolean bfexists(byte[] sampleKey, byte[] key, byte[] value) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.BFEXISTS, key, value);
        return BuilderFactory.BOOLEAN.build(obj);
    }

    public Boolean[] bfmexists(String sampleKey, String key, String... value) {
        BfmexistParams params = new BfmexistParams();
        Object obj = jc.sendCommand(SafeEncoder.encode(sampleKey), ModuleCommand.BFMEXISTS, params.getByteParams(key, value));
        return BloomBuilderFactory.BFMADD_RESULT_BOOLEAN_LIST.build(obj);
    }

    public Boolean[] bfmexists(byte[] sampleKey, byte[] key, byte[]... value) {
        BfmexistParams params = new BfmexistParams();
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.BFMEXISTS, params.getByteParams(key, value));
        return BloomBuilderFactory.BFMADD_RESULT_BOOLEAN_LIST.build(obj);
    }

    public Boolean[] bfinsert(String sampleKey, String key, String initCapacityTag, long initCapacity, String errorRateTag, Double errorRate, String itemTag, String... items) {
        BfinsertParams params = new BfinsertParams();
        byte[][] metadata = params.getByteParamsMeta(key, initCapacityTag, String.valueOf(initCapacity), errorRateTag, String.valueOf(errorRate), itemTag);
        Object obj = jc.sendCommand(SafeEncoder.encode(sampleKey), ModuleCommand.BFINSERT, params.getByteParams(metadata, items));
        return BloomBuilderFactory.BFINSERT_RESULT_BOOLEAN_LIST.build(obj);
    }

    public Boolean[] bfinsert(String sampleKey, String key, String nocreateTag, String itemTag, String... items) {
        BfinsertParams params = new BfinsertParams();
        byte[][] metadata = params.getByteParamsMeta(key, nocreateTag, itemTag);
        Object obj = jc.sendCommand(SafeEncoder.encode(sampleKey), ModuleCommand.BFINSERT, params.getByteParams(metadata, items));
        return BloomBuilderFactory.BFINSERT_RESULT_BOOLEAN_LIST.build(obj);
    }

    public Boolean[] bfinsert(String sampleKey, String key, String itemTag, String... items) {
        BfinsertParams params = new BfinsertParams();
        byte[][] metadata = params.getByteParamsMeta(key, itemTag);
        Object obj = jc.sendCommand(SafeEncoder.encode(sampleKey), ModuleCommand.BFINSERT, params.getByteParams(metadata, items));
        return BloomBuilderFactory.BFINSERT_RESULT_BOOLEAN_LIST.build(obj);
    }

    public Boolean[] bfinsert(byte[] sampleKey, byte[] key, byte[] initCapacityTag, long initCapacity, byte[] errorRateTag, Double errorRate, byte[] itemTag, byte[]... items) {
        BfinsertParams params = new BfinsertParams();
        byte[][] metadata = params.getByteParamsMeta(key, initCapacityTag, toByteArray(initCapacity), errorRateTag, toByteArray(errorRate), itemTag);
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.BFINSERT, params.getByteParams(metadata, items));
        return BloomBuilderFactory.BFINSERT_RESULT_BOOLEAN_LIST.build(obj);
    }

    public Boolean[] bfinsert(byte[] sampleKey, byte[] key, byte[] nocreateTag, byte[] itemTag, byte[]... items) {
        BfinsertParams params = new BfinsertParams();
        byte[][] metadata = params.getByteParamsMeta(key, nocreateTag, itemTag);
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.BFINSERT, params.getByteParams(metadata, items));
        return BloomBuilderFactory.BFINSERT_RESULT_BOOLEAN_LIST.build(obj);
    }

    public Boolean[] bfinsert(byte[] sampleKey, byte[] key, byte[] itemTag, byte[]... items) {
        BfinsertParams params = new BfinsertParams();
        byte[][] metadata = params.getByteParamsMeta(key, itemTag);
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.BFINSERT, params.getByteParams(metadata, items));
        return BloomBuilderFactory.BFINSERT_RESULT_BOOLEAN_LIST.build(obj);
    }

    public List<String> bfdebug(String sampleKey, String key) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.BFDEBUG, key);
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<String> bfdebug(byte[] sampleKey, byte[] key) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.BFDEBUG, key);
        return BuilderFactory.STRING_LIST.build(obj);
    }
}
