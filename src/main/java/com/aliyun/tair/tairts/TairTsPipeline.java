package com.aliyun.tair.tairts;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairts.factory.TsBuilderFactory;
import com.aliyun.tair.tairts.params.*;
import com.aliyun.tair.tairts.results.ExtsDataPointResult;
import com.aliyun.tair.tairts.results.ExtsSkeyResult;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;
import java.util.List;

import static redis.clients.jedis.Protocol.toByteArray;

public class TairTsPipeline extends Pipeline {

    public Response<String> extsadd(String pkey, String skey, String ts, double value) {
        return extsadd(SafeEncoder.encode(pkey), SafeEncoder.encode(skey), SafeEncoder.encode(ts), value);
    }

    public Response<String> extsadd(byte[] pkey, byte[] skey, byte[] ts, double value) {
        getClient("").sendCommand(ModuleCommand.TSSADD, pkey, skey, ts, toByteArray(value));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> extsadd(String pkey, String skey, String ts, double value, ExtsAttributesParams params) {
        return extsadd(SafeEncoder.encode(pkey), SafeEncoder.encode(skey), SafeEncoder.encode(ts), value, params);
    }

    public Response<String> extsadd(byte[] pkey, byte[] skey, byte[] ts, double value, ExtsAttributesParams params) {
        getClient("").sendCommand(ModuleCommand.TSSADD, params.getByteParams(pkey, skey, ts, toByteArray(value)));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<List<String>> extsmadd(String pkey, ArrayList<ExtsDataPoint<String>> skeys) {
        ExtsMaddParams addList = new ExtsMaddParams();
        getClient("").sendCommand(ModuleCommand.TSSMADD, addList.getByteParams(pkey, skeys));
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<String>> extsmadd(byte[] pkey, ArrayList<ExtsDataPoint<byte[]>> skeys) {
        ExtsMaddParams addList = new ExtsMaddParams();
        getClient("").sendCommand(ModuleCommand.TSSMADD, addList.getByteParams(pkey, skeys));
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<String>> extsmadd(String pkey, ArrayList<ExtsDataPoint<String>> skeys, ExtsAttributesParams params) {
        ExtsMaddParams addList = new ExtsMaddParams();
        getClient("").sendCommand(ModuleCommand.TSSMADD, params.getByteParams(pkey, skeys));
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<String>> extsmadd(byte[] pkey, ArrayList<ExtsDataPoint<byte[]>> skeys, ExtsAttributesParams params) {
        ExtsMaddParams addList = new ExtsMaddParams();
        getClient("").sendCommand(ModuleCommand.TSSMADD, params.getByteParams(pkey, skeys));
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<String> extsalter(String pkey, String skey, long expireTime) {
        return extsalter(SafeEncoder.encode(pkey), SafeEncoder.encode(skey), expireTime);
    }

    public Response<String> extsalter(byte[] pkey, byte[] skey, long expireTime) {
        getClient("").sendCommand(ModuleCommand.TSSALTER, pkey, skey, SafeEncoder.encode("DATA_ET"), toByteArray(expireTime));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> extsincr(String pkey, String skey, String ts, double value) {
        return extsincr(SafeEncoder.encode(pkey), SafeEncoder.encode(skey), SafeEncoder.encode(ts), value);
    }

    public Response<String> extsincr(byte[] pkey, byte[] skey, byte[] ts, double value) {
        getClient("").sendCommand(ModuleCommand.TSSINCRBY, pkey, skey, ts, toByteArray(value));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> extsincr(String pkey, String skey, String ts, double value, ExtsAttributesParams params) {
        return extsincr(SafeEncoder.encode(pkey), SafeEncoder.encode(skey), SafeEncoder.encode(ts), value, params);
    }

    public Response<String> extsincr(byte[] pkey, byte[] skey, byte[] ts, double value, ExtsAttributesParams params) {
        getClient("").sendCommand(ModuleCommand.TSSINCRBY, params.getByteParams(pkey, skey, ts, toByteArray(value)));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<List<String>> extsmincr(String pkey, ArrayList<ExtsDataPoint<String>> skeys) {
        ExtsMaddParams addList = new ExtsMaddParams();
        getClient("").sendCommand(ModuleCommand.TSSMINCRBY, addList.getByteParams(pkey, skeys));
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<String>> extsmincr(byte[] pkey, ArrayList<ExtsDataPoint<byte[]>> skeys) {
        ExtsMaddParams addList = new ExtsMaddParams();
        getClient("").sendCommand(ModuleCommand.TSSMINCRBY, addList.getByteParams(pkey, skeys));
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<String>> extsmincr(String pkey, ArrayList<ExtsDataPoint<String>> skeys, ExtsAttributesParams params) {
        ExtsMaddParams addList = new ExtsMaddParams();
        getClient("").sendCommand(ModuleCommand.TSSMINCRBY, params.getByteParams(pkey, skeys));
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<String>> extsmincr(byte[] pkey, ArrayList<ExtsDataPoint<byte[]>> skeys, ExtsAttributesParams params) {
        ExtsMaddParams addList = new ExtsMaddParams();
        getClient("").sendCommand(ModuleCommand.TSSMINCRBY, params.getByteParams(pkey, skeys));
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<String> extsdel(String pkey, String skey) {
        return extsdel(SafeEncoder.encode(pkey), SafeEncoder.encode(skey));
    }

    public Response<String> extsdel(byte[] pkey, byte[] skey) {
        getClient("").sendCommand(ModuleCommand.TSSDEL, pkey, skey);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<ExtsDataPointResult> extsget(String pkey, String skey) {
        return extsget(SafeEncoder.encode(pkey), SafeEncoder.encode(skey));
    }

    public Response<ExtsDataPointResult> extsget(byte[] pkey, byte[] skey) {
        getClient("").sendCommand(ModuleCommand.TSSGET, pkey, skey);
        return getResponse(TsBuilderFactory.EXTSGET_RESULT_STRING);
    }

    public Response<List<String>> extsquery(String pkey, ArrayList<ExtsFilter<String>> filters) {
        ExtsQueryParams addList = new ExtsQueryParams();
        getClient("").sendCommand(ModuleCommand.TSSQUERYINDEX, addList.getByteParams(pkey, filters));
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<String>> extsquery(byte[] pkey, ArrayList<ExtsFilter<byte[]>> filters) {
        ExtsQueryParams addList = new ExtsQueryParams();
        getClient("").sendCommand(ModuleCommand.TSSQUERYINDEX, addList.getByteParams(pkey, filters));
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<ExtsDataPointResult>> extsrange(String pkey, String skey, String startTs, String endTs) {
        ExtsQueryParams addList = new ExtsQueryParams();
        getClient("").sendCommand(ModuleCommand.TSSRANGE, pkey, skey, startTs, endTs);
        return getResponse(TsBuilderFactory.EXTSRANGE_RESULT_STRING);
    }

    public Response<List<ExtsDataPointResult>> extsrange(byte[] pkey, byte[] skey, byte[] startTs, byte[] endTs) {
        ExtsQueryParams addList = new ExtsQueryParams();
        getClient("").sendCommand(ModuleCommand.TSSRANGE, pkey, skey, startTs, endTs);
        return getResponse(TsBuilderFactory.EXTSRANGE_RESULT_STRING);
    }

    public Response<List<ExtsDataPointResult>> extsrange(String pkey, String skey, String startTs, String endTs, ExtsAggregationParams params) {
        ExtsQueryParams addList = new ExtsQueryParams();
        getClient("").sendCommand(ModuleCommand.TSSRANGE, params.getByteRangeParams(pkey, skey, startTs, endTs));
        return getResponse(TsBuilderFactory.EXTSRANGE_RESULT_STRING);
    }

    public Response<List<ExtsDataPointResult>> extsrange(byte[] pkey, byte[] skey, byte[] startTs, byte[] endTs, ExtsAggregationParams params) {
        ExtsQueryParams addList = new ExtsQueryParams();
        getClient("").sendCommand(ModuleCommand.TSSRANGE, params.getByteRangeParams(pkey, skey, startTs, endTs));
        return getResponse(TsBuilderFactory.EXTSRANGE_RESULT_STRING);
    }

    public Response<List<ExtsSkeyResult>> extsmrange(String pkey, String startTs, String endTs, ArrayList<ExtsFilter<String>> filters) {
        ExtsAggregationParams params = new ExtsAggregationParams();
        getClient("").sendCommand(ModuleCommand.TSSMRANGE, params.getByteMrangeParams(pkey, startTs, endTs, filters));
        return getResponse(TsBuilderFactory.EXTSMRANGE_RESULT_STRING);
    }

    public Response<List<ExtsSkeyResult>> extsmrange(byte[] pkey, byte[] startTs, byte[] endTs, ArrayList<ExtsFilter<byte[]>> filters) {
        ExtsAggregationParams params = new ExtsAggregationParams();
        getClient("").sendCommand(ModuleCommand.TSSMRANGE, params.getByteMrangeParams(pkey, startTs, endTs, filters));
        return getResponse(TsBuilderFactory.EXTSMRANGE_RESULT_STRING);
    }

    public Response<List<ExtsSkeyResult>> extsmrange(String pkey, String startTs, String endTs, ExtsAggregationParams params, ArrayList<ExtsFilter<String>> filters) {
        getClient("").sendCommand(ModuleCommand.TSSMRANGE, params.getByteMrangeParams(pkey, startTs, endTs, filters));
        return getResponse(TsBuilderFactory.EXTSMRANGE_RESULT_STRING);
    }

    public Response<List<ExtsSkeyResult>> extsmrange(byte[] pkey, byte[] startTs, byte[] endTs, ExtsAggregationParams params, ArrayList<ExtsFilter<byte[]>> filters) {
        getClient("").sendCommand(ModuleCommand.TSSMRANGE, params.getByteMrangeParams(pkey, startTs, endTs, filters));
        return getResponse(TsBuilderFactory.EXTSMRANGE_RESULT_STRING);
    }

    public Response<List<ExtsDataPointResult>> extsprange(String pkey, String startTs, String endTs, String pkeyAggregationType, long pkeyTimeBucket, ArrayList<ExtsFilter<String>> filters) {
        ExtsAggregationParams params = new ExtsAggregationParams();
        getClient("").sendCommand(ModuleCommand.TSPRANGE, params.getBytePrangeParams(pkey, startTs, endTs, pkeyAggregationType, pkeyTimeBucket, filters));
        return getResponse(TsBuilderFactory.EXTSRANGE_RESULT_STRING);
    }

    public Response<List<ExtsDataPointResult>> extsprange(byte[] pkey, byte[] startTs, byte[] endTs, byte[] pkeyAggregationType, long pkeyTimeBucket, ArrayList<ExtsFilter<byte[]>> filters) {
        ExtsAggregationParams params = new ExtsAggregationParams();
        getClient("").sendCommand(ModuleCommand.TSPRANGE, params.getBytePrangeParams(pkey, startTs, endTs, pkeyAggregationType, pkeyTimeBucket, filters));
        return getResponse(TsBuilderFactory.EXTSRANGE_RESULT_STRING);
    }

    public Response<List<ExtsDataPointResult>> extsprange(String pkey, String startTs, String endTs, String pkeyAggregationType, long pkeyTimeBucket, ExtsAggregationParams params, ArrayList<ExtsFilter<String>> filters) {
        getClient("").sendCommand(ModuleCommand.TSPRANGE, params.getBytePrangeParams(pkey, startTs, endTs, pkeyAggregationType, pkeyTimeBucket, filters));
        return getResponse(TsBuilderFactory.EXTSRANGE_RESULT_STRING);
    }

    public Response<List<ExtsDataPointResult>> extsprange(byte[] pkey, byte[] startTs, byte[] endTs, byte[] pkeyAggregationType, long pkeyTimeBucket, ExtsAggregationParams params, ArrayList<ExtsFilter<byte[]>> filters) {
        getClient("").sendCommand(ModuleCommand.TSPRANGE, params.getBytePrangeParams(pkey, startTs, endTs, pkeyAggregationType, pkeyTimeBucket, filters));
        return getResponse(TsBuilderFactory.EXTSRANGE_RESULT_STRING);
    }
}
