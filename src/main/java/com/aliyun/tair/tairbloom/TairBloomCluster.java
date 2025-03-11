package com.aliyun.tair.tairbloom;

import com.aliyun.tair.tairbloom.params.BfmaddParams;
import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairbloom.factory.BloomBuilderFactory;
import com.aliyun.tair.tairbloom.params.BfinsertParams;
import com.aliyun.tair.tairbloom.params.BfmexistParams;
import io.valkey.BuilderFactory;
import io.valkey.JedisCluster;
import io.valkey.util.SafeEncoder;

import java.util.List;

import static io.valkey.Protocol.toByteArray;

public class TairBloomCluster {
    private JedisCluster jc;

    public TairBloomCluster(JedisCluster jc) {
        this.jc = jc;
    }

    public String bfreserve(String key, long initCapacity, double errorRate) {
        return bfreserve(SafeEncoder.encode(key), initCapacity, errorRate);
    }

    public String bfreserve(byte[] key, long initCapacity, double errorRate) {
        Object obj = jc.sendCommand(key, ModuleCommand.BFRESERVE, key, toByteArray(errorRate), toByteArray(initCapacity));
        return BuilderFactory.STRING.build(obj);
    }

    public Boolean bfadd(String key, String item) {
        return bfadd(SafeEncoder.encode(key), SafeEncoder.encode(item));
    }

    public Boolean bfadd(byte[] key, byte[] item) {
        Object obj = jc.sendCommand(key, ModuleCommand.BFADD, key, item);
        return BuilderFactory.BOOLEAN.build(obj);
    }

    public Boolean[] bfmadd(String key, String... items) {
        BfmaddParams params = new BfmaddParams();
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.BFMADD, params.getByteParams(key, items));
        return BloomBuilderFactory.BFMADD_RESULT_BOOLEAN_LIST.build(obj);
    }

    public Boolean[] bfmadd(byte[] key, byte[]... items) {
        BfmaddParams params = new BfmaddParams();
        Object obj = jc.sendCommand(key, ModuleCommand.BFMADD, params.getByteParams(key, items));
        return BloomBuilderFactory.BFMADD_RESULT_BOOLEAN_LIST.build(obj);
    }

    public Boolean bfexists(String key, String value) {
        Object obj = jc.sendCommand(key, ModuleCommand.BFEXISTS, key, value);
        return BuilderFactory.BOOLEAN.build(obj);
    }

    public Boolean bfexists(byte[] key, byte[] value) {
        Object obj = jc.sendCommand(key, ModuleCommand.BFEXISTS, key, value);
        return BuilderFactory.BOOLEAN.build(obj);
    }

    public Boolean[] bfmexists(String key, String... value) {
        BfmexistParams params = new BfmexistParams();
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.BFMEXISTS, params.getByteParams(key, value));
        return BloomBuilderFactory.BFMADD_RESULT_BOOLEAN_LIST.build(obj);
    }

    public Boolean[] bfmexists(byte[] key, byte[]... value) {
        BfmexistParams params = new BfmexistParams();
        Object obj = jc.sendCommand(key, ModuleCommand.BFMEXISTS, params.getByteParams(key, value));
        return BloomBuilderFactory.BFMADD_RESULT_BOOLEAN_LIST.build(obj);
    }

    public Boolean[] bfinsert(String key, BfinsertParams params, String... items) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.BFINSERT,
            params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encodeMany(items)));
        return BloomBuilderFactory.BFINSERT_RESULT_BOOLEAN_LIST.build(obj);
    }

    public Boolean[] bfinsert(byte[] key, BfinsertParams params, byte[]... items) {
        Object obj = jc.sendCommand(key, ModuleCommand.BFINSERT, params.getByteParams(key, items));
        return BloomBuilderFactory.BFINSERT_RESULT_BOOLEAN_LIST.build(obj);
    }

    @Deprecated
    public Boolean[] bfinsert(String key, String initCapacityTag, long initCapacity, String errorRateTag, Double errorRate, String itemTag, String... items) {
        BfinsertParams params = new BfinsertParams();
        byte[][] metadata = params.getByteParamsMeta(key, initCapacityTag, String.valueOf(initCapacity), errorRateTag, String.valueOf(errorRate), itemTag);
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.BFINSERT, params.getByteParams(metadata, items));
        return BloomBuilderFactory.BFINSERT_RESULT_BOOLEAN_LIST.build(obj);
    }

    @Deprecated
    public Boolean[] bfinsert(String key, String nocreateTag, String itemTag, String... items) {
        BfinsertParams params = new BfinsertParams();
        byte[][] metadata = params.getByteParamsMeta(key, nocreateTag, itemTag);
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.BFINSERT, params.getByteParams(metadata, items));
        return BloomBuilderFactory.BFINSERT_RESULT_BOOLEAN_LIST.build(obj);
    }

    @Deprecated
    public Boolean[] bfinsert(String key, String itemTag, String... items) {
        BfinsertParams params = new BfinsertParams();
        byte[][] metadata = params.getByteParamsMeta(key, itemTag);
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.BFINSERT, params.getByteParams(metadata, items));
        return BloomBuilderFactory.BFINSERT_RESULT_BOOLEAN_LIST.build(obj);
    }

    @Deprecated
    public Boolean[] bfinsert(byte[] key, byte[] initCapacityTag, long initCapacity, byte[] errorRateTag, Double errorRate, byte[] itemTag, byte[]... items) {
        BfinsertParams params = new BfinsertParams();
        byte[][] metadata = params.getByteParamsMeta(key, initCapacityTag, toByteArray(initCapacity), errorRateTag, toByteArray(errorRate), itemTag);
        Object obj = jc.sendCommand(key, ModuleCommand.BFINSERT, params.getByteParams(metadata, items));
        return BloomBuilderFactory.BFINSERT_RESULT_BOOLEAN_LIST.build(obj);
    }

    @Deprecated
    public Boolean[] bfinsert(byte[] key, byte[] nocreateTag, byte[] itemTag, byte[]... items) {
        BfinsertParams params = new BfinsertParams();
        byte[][] metadata = params.getByteParamsMeta(key, nocreateTag, itemTag);
        Object obj = jc.sendCommand(key, ModuleCommand.BFINSERT, params.getByteParams(metadata, items));
        return BloomBuilderFactory.BFINSERT_RESULT_BOOLEAN_LIST.build(obj);
    }

    @Deprecated
    public Boolean[] bfinsert(byte[] key, byte[] itemTag, byte[]... items) {
        BfinsertParams params = new BfinsertParams();
        byte[][] metadata = params.getByteParamsMeta(key, itemTag);
        Object obj = jc.sendCommand(key, ModuleCommand.BFINSERT, params.getByteParams(metadata, items));
        return BloomBuilderFactory.BFINSERT_RESULT_BOOLEAN_LIST.build(obj);
    }

    public List<String> bfdebug(String key) {
        Object obj = jc.sendCommand(key, ModuleCommand.BFDEBUG, key);
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<String> bfdebug(byte[] key) {
        Object obj = jc.sendCommand(key, ModuleCommand.BFDEBUG, key);
        return BuilderFactory.STRING_LIST.build(obj);
    }
}
