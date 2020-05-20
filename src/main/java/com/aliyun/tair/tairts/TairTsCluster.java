package com.aliyun.tair.tairts;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairts.factory.TsBuilderFactory;
import com.aliyun.tair.tairts.params.*;
import com.aliyun.tair.tairts.results.ExtsDataPointResult;
import com.aliyun.tair.tairts.results.ExtsSkeyResult;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;
import java.util.List;

import static redis.clients.jedis.Protocol.toByteArray;

public class TairTsCluster {
    private JedisCluster jc;

    public TairTsCluster(JedisCluster jc) {
        this.jc = jc;
    }

    public String extsadd(String pkey, String skey, String ts, double value) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSADD, pkey, skey, ts, String.valueOf(value));
        return BuilderFactory.STRING.build(obj);
    }

    public String extsadd(byte[] pkey, byte[] skey, byte[] ts, double value) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSADD, pkey, skey, ts, toByteArray(value));
        return BuilderFactory.STRING.build(obj);
    }

    public String extsadd(String pkey, String skey, String ts, double value, ExtsAttributesParams params) {
        return extsadd(SafeEncoder.encode(pkey), SafeEncoder.encode(skey), SafeEncoder.encode(ts), value, params);
    }

    public String extsadd(byte[] pkey, byte[] skey, byte[] ts, double value, ExtsAttributesParams params) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSADD, params.getByteParams(pkey, skey, ts, toByteArray(value)));
        return BuilderFactory.STRING.build(obj);
    }

    public List<String> extsmadd(String pkey, ArrayList<ExtsDataPoint<String>> skeys) {
        ExtsMaddParams addList = new ExtsMaddParams();
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSSMADD, addList.getByteParams(pkey, skeys));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<String> extsmadd(byte[] pkey, ArrayList<ExtsDataPoint<byte[]>> skeys) {
        ExtsMaddParams addList = new ExtsMaddParams();
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSMADD, addList.getByteParams(pkey, skeys));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<String> extsmadd(String pkey, ArrayList<ExtsDataPoint<String>> skeys, ExtsAttributesParams params) {
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSSMADD, params.getByteParams(pkey, skeys));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<String> extsmadd(byte[] pkey, ArrayList<ExtsDataPoint<byte[]>> skeys, ExtsAttributesParams params) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSMADD, params.getByteParams(pkey, skeys));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public String extsalter(String pkey, String skey, long expireTime) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSALTER, pkey, skey, "DATA_ET", String.valueOf(expireTime));
        return BuilderFactory.STRING.build(obj);
    }

    public String extsalter(byte[] pkey, byte[] skey, long expireTime) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSALTER, pkey, skey, SafeEncoder.encode("DATA_ET"), toByteArray(expireTime));
        return BuilderFactory.STRING.build(obj);
    }

    public String extsincr(String pkey, String skey, String ts, double value) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSINCRBY, pkey, skey, ts, String.valueOf(value));
        return BuilderFactory.STRING.build(obj);
    }

    public String extsincr(byte[] pkey, byte[] skey, byte[] ts, double value) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSINCRBY, pkey, skey, ts, toByteArray(value));
        return BuilderFactory.STRING.build(obj);
    }

    public String extsincr(String pkey, String skey, String ts, double value, ExtsAttributesParams params) {
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSSINCRBY, params.getByteParams(pkey, skey, ts, String.valueOf(value)));
        return BuilderFactory.STRING.build(obj);
    }

    public String extsincr(byte[] pkey, byte[] skey, byte[] ts, double value, ExtsAttributesParams params) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSINCRBY, params.getByteParams(pkey, skey, ts, toByteArray(value)));
        return BuilderFactory.STRING.build(obj);
    }

    public List<String> extsmincr(String pkey, ArrayList<ExtsDataPoint<String>> skeys) {
        ExtsMaddParams addList = new ExtsMaddParams();
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSSMINCRBY, addList.getByteParams(pkey, skeys));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<String> extsmincr(byte[] pkey, ArrayList<ExtsDataPoint<byte[]>> skeys) {
        ExtsMaddParams addList = new ExtsMaddParams();
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSMINCRBY, addList.getByteParams(pkey, skeys));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<String> extsmincr(String pkey, ArrayList<ExtsDataPoint<String>> skeys, ExtsAttributesParams params) {
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSSMINCRBY, params.getByteParams(pkey, skeys));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<String> extsmincr(byte[] pkey, ArrayList<ExtsDataPoint<byte[]>> skeys, ExtsAttributesParams params) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSMINCRBY, params.getByteParams(pkey, skeys));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public String extsdel(String pkey, String skey) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSDEL, pkey, skey);
        return BuilderFactory.STRING.build(obj);
    }

    public String extsdel(byte[] pkey, byte[] skey) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSDEL, pkey, skey);
        return BuilderFactory.STRING.build(obj);
    }

    public ExtsDataPointResult extsget(String pkey, String skey) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSGET, pkey, skey);
        return TsBuilderFactory.EXTSGET_RESULT_STRING.build(obj);
    }

    public ExtsDataPointResult extsget(byte[] pkey, byte[] skey) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSGET, pkey, skey);
        return TsBuilderFactory.EXTSGET_RESULT_STRING.build(obj);
    }

    public List<String> extsquery(String pkey, ArrayList<ExtsFilter<String>> filters) {
        ExtsQueryParams addList = new ExtsQueryParams();
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSSQUERYINDEX, addList.getByteParams(pkey, filters));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<byte[]> extsquery(byte[] pkey, ArrayList<ExtsFilter<byte[]>> filters) {
        ExtsQueryParams addList = new ExtsQueryParams();
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSQUERYINDEX, addList.getByteParams(pkey, filters));
        return BuilderFactory.BYTE_ARRAY_LIST.build(obj);
    }

    public List<ExtsDataPointResult> extsrange(String pkey, String skey, String startTs, String endTs) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSRANGE, pkey, skey, startTs, endTs);
        return TsBuilderFactory.EXTSRANGE_RESULT_STRING.build(obj);
    }

    public List<ExtsDataPointResult> extsrange(byte[] pkey, byte[] skey, byte[] startTs, byte[] endTs) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSRANGE, pkey, skey, startTs, endTs);
        return TsBuilderFactory.EXTSRANGE_RESULT_STRING.build(obj);
    }

    public List<ExtsDataPointResult> extsrange(String pkey, String skey, String startTs, String endTs, ExtsAggregationParams params) {
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSSRANGE, params.getByteRangeParams(pkey, skey, startTs, endTs));
        return TsBuilderFactory.EXTSRANGE_RESULT_STRING.build(obj);
    }

    public List<ExtsDataPointResult> extsrange(byte[] pkey, byte[] skey, byte[] startTs, byte[] endTs, ExtsAggregationParams params) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSRANGE, params.getByteRangeParams(pkey, skey, startTs, endTs));
        return TsBuilderFactory.EXTSRANGE_RESULT_STRING.build(obj);
    }

    public List<ExtsSkeyResult> extsmrange(String pkey, String startTs, String endTs, ArrayList<ExtsFilter<String>> filters) {
        ExtsAggregationParams params = new ExtsAggregationParams();
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSSMRANGE, params.getByteMrangeParams(pkey, startTs, endTs, filters));
        return TsBuilderFactory.EXTSMRANGE_RESULT_STRING.build(obj);
    }

    public List<ExtsSkeyResult> extsmrange(byte[] pkey, byte[] startTs, byte[] endTs, ArrayList<ExtsFilter<byte[]>> filters) {
        ExtsAggregationParams params = new ExtsAggregationParams();
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSMRANGE, params.getByteMrangeParams(pkey, startTs, endTs, filters));
        return TsBuilderFactory.EXTSMRANGE_RESULT_STRING.build(obj);
    }

    public List<ExtsSkeyResult> extsmrange(String pkey, String startTs, String endTs, ExtsAggregationParams params, ArrayList<ExtsFilter<String>> filters) {
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSSMRANGE, params.getByteMrangeParams(pkey, startTs, endTs, filters));
        return TsBuilderFactory.EXTSMRANGE_RESULT_STRING.build(obj);
    }

    public List<ExtsSkeyResult> extsmrange(byte[] pkey, byte[] startTs, byte[] endTs, ExtsAggregationParams params, ArrayList<ExtsFilter<byte[]>> filters) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSMRANGE, params.getByteMrangeParams(pkey, startTs, endTs, filters));
        return TsBuilderFactory.EXTSMRANGE_RESULT_STRING.build(obj);
    }

    public List<ExtsDataPointResult> extsprange(String pkey, String startTs, String endTs, String pkeyAggregationType, long pkeyTimeBucket, ArrayList<ExtsFilter<String>> filters) {
        ExtsAggregationParams params = new ExtsAggregationParams();
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSPRANGE, params.getBytePrangeParams(pkey, startTs, endTs, pkeyAggregationType, pkeyTimeBucket, filters));
        return TsBuilderFactory.EXTSRANGE_RESULT_STRING.build(obj);
    }

    public List<ExtsDataPointResult> extsprange(byte[] pkey, byte[] startTs, byte[] endTs, byte[] pkeyAggregationType, long pkeyTimeBucket, ArrayList<ExtsFilter<byte[]>> filters) {
        ExtsAggregationParams params = new ExtsAggregationParams();
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSPRANGE, params.getBytePrangeParams(pkey, startTs, endTs, pkeyAggregationType, pkeyTimeBucket, filters));
        return TsBuilderFactory.EXTSRANGE_RESULT_STRING.build(obj);
    }

    public List<ExtsDataPointResult> extsprange(String pkey, String startTs, String endTs, String pkeyAggregationType, long pkeyTimeBucket, ExtsAggregationParams params, ArrayList<ExtsFilter<String>> filters) {
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSPRANGE, params.getBytePrangeParams(pkey, startTs, endTs, pkeyAggregationType, pkeyTimeBucket, filters));
        return TsBuilderFactory.EXTSRANGE_RESULT_STRING.build(obj);
    }

    public List<ExtsDataPointResult> extsprange(byte[] pkey, byte[] startTs, byte[] endTs, byte[] pkeyAggregationType, long pkeyTimeBucket, ExtsAggregationParams params, ArrayList<ExtsFilter<byte[]>> filters) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSPRANGE, params.getBytePrangeParams(pkey, startTs, endTs, pkeyAggregationType, pkeyTimeBucket, filters));
        return TsBuilderFactory.EXTSRANGE_RESULT_STRING.build(obj);
    }
}
