package com.aliyun.tair.tairts;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.jedis3.Jedis3BuilderFactory;
import com.aliyun.tair.tairts.factory.TsBuilderFactory;
import com.aliyun.tair.tairts.params.*;
import com.aliyun.tair.tairts.results.ExtsDataPointResult;
import com.aliyun.tair.tairts.results.ExtsSkeyResult;
import com.aliyun.tair.tairts.results.ExtsStringDataPointResult;
import com.aliyun.tair.tairts.results.ExtsStringSkeyResult;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.CommandArguments;
import redis.clients.jedis.CommandObject;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;
import java.util.List;

import static redis.clients.jedis.Protocol.toByteArray;

public class TairTsPipeline extends Pipeline {
    public TairTsPipeline(Jedis jedis) {
        super(jedis);
    }

    public Response<String> extsadd(String pkey, String skey, String ts, double value) {
        return extsadd(SafeEncoder.encode(pkey), SafeEncoder.encode(skey), SafeEncoder.encode(ts), value);
    }

    public Response<String> extsadd(byte[] pkey, byte[] skey, byte[] ts, double value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSADD)
            .add(pkey)
            .add(skey)
            .add(ts)
            .add(value), BuilderFactory.STRING));
    }

    public Response<String> extsadd(String pkey, String skey, String ts, double value, ExtsAttributesParams params) {
        return extsadd(SafeEncoder.encode(pkey), SafeEncoder.encode(skey), SafeEncoder.encode(ts), value, params);
    }

    public Response<String> extsadd(byte[] pkey, byte[] skey, byte[] ts, double value, ExtsAttributesParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSADD)
            .addObjects(params.getByteParams(pkey, skey, ts, toByteArray(value))), BuilderFactory.STRING));
    }

    public Response<List<String>> extsmadd(String pkey, ArrayList<ExtsDataPoint<String>> skeys) {
        ExtsMaddParams addList = new ExtsMaddParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSMADD)
            .addObjects(addList.getByteParams(pkey, skeys)), BuilderFactory.STRING_LIST));
    }

    public Response<List<String>> extsmadd(byte[] pkey, ArrayList<ExtsDataPoint<byte[]>> skeys) {
        ExtsMaddParams addList = new ExtsMaddParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSMADD)
            .addObjects(addList.getByteParams(pkey, skeys)), BuilderFactory.STRING_LIST));
    }

    public Response<List<String>> extsmadd(String pkey, ArrayList<ExtsDataPoint<String>> skeys, ExtsAttributesParams params) {
        ExtsMaddParams addList = new ExtsMaddParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSMADD)
            .addObjects(params.getByteParams(pkey, skeys)), BuilderFactory.STRING_LIST));
    }

    public Response<List<String>> extsmadd(byte[] pkey, ArrayList<ExtsDataPoint<byte[]>> skeys, ExtsAttributesParams params) {
        ExtsMaddParams addList = new ExtsMaddParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSMADD)
            .addObjects(params.getByteParams(pkey, skeys)), BuilderFactory.STRING_LIST));
    }

    public Response<String> extsalter(String pkey, String skey, ExtsAttributesParams params) {
        return extsalter(SafeEncoder.encode(pkey), SafeEncoder.encode(skey), params);
    }

    public Response<String> extsalter(byte[] pkey, byte[] skey, ExtsAttributesParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSALTER)
            .addObjects(params.getByteParams(pkey, skey)), BuilderFactory.STRING));
    }

    public Response<String> extsincr(String pkey, String skey, String ts, double value) {
        return extsincr(SafeEncoder.encode(pkey), SafeEncoder.encode(skey), SafeEncoder.encode(ts), value);
    }

    public Response<String> extsincr(byte[] pkey, byte[] skey, byte[] ts, double value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSINCRBY)
            .add(pkey).add(skey).add(ts).add(value), BuilderFactory.STRING));
    }

    public Response<String> extsincr(String pkey, String skey, String ts, double value, ExtsAttributesParams params) {
        return extsincr(SafeEncoder.encode(pkey), SafeEncoder.encode(skey), SafeEncoder.encode(ts), value, params);
    }

    public Response<String> extsincr(byte[] pkey, byte[] skey, byte[] ts, double value, ExtsAttributesParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSINCRBY)
            .addObjects(params.getByteParams(pkey, skey, ts, toByteArray(value))), BuilderFactory.STRING));
    }

    public Response<List<String>> extsmincr(String pkey, ArrayList<ExtsDataPoint<String>> skeys) {
        ExtsMaddParams addList = new ExtsMaddParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSMINCRBY)
            .addObjects(addList.getByteParams(pkey, skeys)), BuilderFactory.STRING_LIST));
    }

    public Response<List<String>> extsmincr(byte[] pkey, ArrayList<ExtsDataPoint<byte[]>> skeys) {
        ExtsMaddParams addList = new ExtsMaddParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSMINCRBY)
            .addObjects(addList.getByteParams(pkey, skeys)), BuilderFactory.STRING_LIST));
    }

    public Response<List<String>> extsmincr(String pkey, ArrayList<ExtsDataPoint<String>> skeys, ExtsAttributesParams params) {
        ExtsMaddParams addList = new ExtsMaddParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSMINCRBY)
            .addObjects(params.getByteParams(pkey, skeys)), BuilderFactory.STRING_LIST));
    }

    public Response<List<String>> extsmincr(byte[] pkey, ArrayList<ExtsDataPoint<byte[]>> skeys, ExtsAttributesParams params) {
        ExtsMaddParams addList = new ExtsMaddParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSMINCRBY)
            .addObjects(params.getByteParams(pkey, skeys)), BuilderFactory.STRING_LIST));
    }

    public Response<String> extsdel(String pkey, String skey) {
        return extsdel(SafeEncoder.encode(pkey), SafeEncoder.encode(skey));
    }

    public Response<String> extsdel(byte[] pkey, byte[] skey) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSDEL)
            .add(pkey).add(skey), BuilderFactory.STRING));
    }

    public Response<ExtsDataPointResult> extsget(String pkey, String skey) {
        return extsget(SafeEncoder.encode(pkey), SafeEncoder.encode(skey));
    }

    public Response<ExtsDataPointResult> extsget(byte[] pkey, byte[] skey) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSGET)
            .add(pkey).add(skey), TsBuilderFactory.EXTSGET_RESULT_STRING));
    }

    public Response<List<String>> extsquery(String pkey, ArrayList<ExtsFilter<String>> filters) {
        ExtsQueryParams addList = new ExtsQueryParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSQUERYINDEX)
            .addObjects(addList.getByteParams(pkey, filters)), BuilderFactory.STRING_LIST));
    }

    public Response<List<String>> extsquery(byte[] pkey, ArrayList<ExtsFilter<byte[]>> filters) {
        ExtsQueryParams addList = new ExtsQueryParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSQUERYINDEX)
            .addObjects(addList.getByteParams(pkey, filters)), BuilderFactory.STRING_LIST));
    }

    public Response<ExtsSkeyResult> extsrange(String pkey, String skey, String startTs, String endTs) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSRANGE)
            .add(pkey).add(skey).add(startTs).add(endTs), TsBuilderFactory.EXTSRANGE_RESULT_STRING));
    }

    public Response<ExtsSkeyResult> extsrange(byte[] pkey, byte[] skey, byte[] startTs, byte[] endTs) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSRANGE)
            .add(pkey).add(skey).add(startTs).add(endTs), TsBuilderFactory.EXTSRANGE_RESULT_STRING));
    }

    public Response<ExtsSkeyResult> extsrange(String pkey, String skey, String startTs, String endTs, ExtsAggregationParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSRANGE)
            .addObjects(params.getByteRangeParams(pkey, skey, startTs, endTs)), TsBuilderFactory.EXTSRANGE_RESULT_STRING));
    }

    public Response<ExtsSkeyResult> extsrange(byte[] pkey, byte[] skey, byte[] startTs, byte[] endTs, ExtsAggregationParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSRANGE)
            .addObjects(params.getByteRangeParams(pkey, skey, startTs, endTs)), TsBuilderFactory.EXTSRANGE_RESULT_STRING));
    }

    public Response<List<ExtsSkeyResult>> extsmrange(String pkey, ArrayList<String> skeys, String startTs, String endTs) {
        ExtsSpecifiedKeysParams params = new ExtsSpecifiedKeysParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSRANGESPECIFIEDKEYS)
            .addObjects(params.getByteParams(pkey, skeys, startTs, endTs)), TsBuilderFactory.EXTSMRANGE_RESULT_STRING));
    }

    public Response<List<ExtsSkeyResult>> extsmrange(byte[] pkey, ArrayList<byte[]> skeys, byte[] startTs, byte[] endTs) {
        ExtsSpecifiedKeysParams params = new ExtsSpecifiedKeysParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSRANGESPECIFIEDKEYS)
            .addObjects(params.getByteParams(pkey, skeys, startTs, endTs)), TsBuilderFactory.EXTSMRANGE_RESULT_STRING));
    }

    public Response<List<ExtsSkeyResult>> extsmrange(String pkey, ArrayList<String> skeys, String startTs, String endTs, ExtsAggregationParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSRANGESPECIFIEDKEYS)
            .addObjects(params.getByteRangeParams(pkey, skeys, startTs, endTs)), TsBuilderFactory.EXTSMRANGE_RESULT_STRING));
    }

    public Response<List<ExtsSkeyResult>> extsmrange(byte[] pkey, ArrayList<byte[]> skeys, byte[] startTs, byte[] endTs, ExtsAggregationParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSRANGESPECIFIEDKEYS)
            .addObjects(params.getByteRangeParams(pkey, skeys, startTs, endTs)), TsBuilderFactory.EXTSMRANGE_RESULT_STRING));
    }

    public Response<List<ExtsSkeyResult>> extsmrange(String pkey, String startTs, String endTs, ArrayList<ExtsFilter<String>> filters) {
        ExtsAggregationParams params = new ExtsAggregationParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSMRANGE)
            .addObjects(params.getByteMrangeParams(pkey, startTs, endTs, filters)), TsBuilderFactory.EXTSMRANGE_RESULT_STRING));
    }

    public Response<List<ExtsSkeyResult>> extsmrange(byte[] pkey, byte[] startTs, byte[] endTs, ArrayList<ExtsFilter<byte[]>> filters) {
        ExtsAggregationParams params = new ExtsAggregationParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSMRANGE)
            .addObjects(params.getByteMrangeParams(pkey, startTs, endTs, filters)), TsBuilderFactory.EXTSMRANGE_RESULT_STRING));
    }

    public Response<List<ExtsSkeyResult>> extsmrange(String pkey, String startTs, String endTs, ExtsAggregationParams params, ArrayList<ExtsFilter<String>> filters) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSMRANGE)
            .addObjects(params.getByteMrangeParams(pkey, startTs, endTs, filters)), TsBuilderFactory.EXTSMRANGE_RESULT_STRING));
    }

    public Response<List<ExtsSkeyResult>> extsmrange(byte[] pkey, byte[] startTs, byte[] endTs, ExtsAggregationParams params, ArrayList<ExtsFilter<byte[]>> filters) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSMRANGE)
            .addObjects(params.getByteMrangeParams(pkey, startTs, endTs, filters)), TsBuilderFactory.EXTSMRANGE_RESULT_STRING));
    }

    public Response<ExtsSkeyResult> extsprange(String pkey, String startTs, String endTs, String pkeyAggregationType, long pkeyTimeBucket, ArrayList<ExtsFilter<String>> filters) {
        ExtsAggregationParams params = new ExtsAggregationParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSPRANGE)
            .addObjects(params.getBytePrangeParams(pkey, startTs, endTs, pkeyAggregationType, pkeyTimeBucket, filters)), TsBuilderFactory.EXTSRANGE_RESULT_STRING));
    }

    public Response<ExtsSkeyResult> extsprange(byte[] pkey, byte[] startTs, byte[] endTs, byte[] pkeyAggregationType, long pkeyTimeBucket, ArrayList<ExtsFilter<byte[]>> filters) {
        ExtsAggregationParams params = new ExtsAggregationParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSPRANGE)
            .addObjects(params.getBytePrangeParams(pkey, startTs, endTs, pkeyAggregationType, pkeyTimeBucket, filters)), TsBuilderFactory.EXTSRANGE_RESULT_STRING));
    }

    public Response<ExtsSkeyResult> extsprange(String pkey, String startTs, String endTs, String pkeyAggregationType, long pkeyTimeBucket, ExtsAggregationParams params, ArrayList<ExtsFilter<String>> filters) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSPRANGE)
            .addObjects(params.getBytePrangeParams(pkey, startTs, endTs, pkeyAggregationType, pkeyTimeBucket, filters)), TsBuilderFactory.EXTSRANGE_RESULT_STRING));
    }

    public Response<ExtsSkeyResult> extsprange(byte[] pkey, byte[] startTs, byte[] endTs, byte[] pkeyAggregationType, long pkeyTimeBucket, ExtsAggregationParams params, ArrayList<ExtsFilter<byte[]>> filters) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSPRANGE)
            .addObjects(params.getBytePrangeParams(pkey, startTs, endTs, pkeyAggregationType, pkeyTimeBucket, filters)), TsBuilderFactory.EXTSRANGE_RESULT_STRING));
    }

    /**
     * Read and modify the old ts value of the key.
     * If the old ts is not exist, the operation will try to add a new ts value.
     * raw operation suit for concurrent update.
     *
     * @param pkey   the pkey
     * @param skey   the skey
     * @param ts     the timestamp
     * @param value  the value
     * @return Success: OK; Fail: error.
     */
    public Response<String> extsrawmodify(String pkey, String skey, String ts, double value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSRAWMODIFY)
            .add(pkey).add(skey).add(ts).add(value), BuilderFactory.STRING));
    }

    public Response<String> extsrawmodify(byte[] pkey, byte[] skey, byte[] ts, double value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSRAWMODIFY)
            .add(pkey).add(skey).add(ts).add(value), BuilderFactory.STRING));
    }

    /**
     * Read and modify the old ts value of the key.
     * If the old ts is not exist, the operation will try to add a new ts value.
     * raw operation suit for concurrent update.
     *
     * @param pkey   the pkey
     * @param skey   the skey
     * @param ts     the timestamp
     * @param value  the value
     * @param params the params: [DATA_ET time] [CHUNK_SIZE size] [UNCOMPRESSED] [LABELS label1 val1 ...]
     * `DATA_ET` - Set expire time (milliseconds)
     * `CHUNK_SIZE` - Set datapoints num per chunk 256~1024 (size)
     * `UNCOMPRESSED` - set the skey if compressed
     * `LABELS` - Set the skey's labels (label1 val1 label2 val2...)
     * @return Success: OK; Fail: error.
     */
    public Response<String> extsrawmodify(String pkey, String skey, String ts, double value, ExtsAttributesParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSRAWMODIFY)
            .addObjects(params.getByteParams(pkey, skey, ts, String.valueOf(value))), BuilderFactory.STRING));
    }

    public Response<String> extsrawmodify(byte[] pkey, byte[] skey, byte[] ts, double value, ExtsAttributesParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSRAWMODIFY)
            .addObjects(params.getByteParams(pkey, skey, ts, toByteArray(value))), BuilderFactory.STRING));
    }

    /**
     * Read and modify the multi old ts value of multi keys.
     * If the old ts is not exist, the operation will try to add a new ts value.
     * raw operation suit for concurrent update.
     *
     * @param pkey   the pkey
     * @param skeys   the {skey ts value}
     * @return Success: OK; Fail: error.
     */
    public Response<List<String>> extsmrawmodify(String pkey, ArrayList<ExtsDataPoint<String>> skeys) {
        ExtsMaddParams addList = new ExtsMaddParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSRAWMULTIMODIFY)
            .addObjects(addList.getByteParams(pkey, skeys)), BuilderFactory.STRING_LIST));
    }

    public Response<List<String>> extsmrawmodify(byte[] pkey, ArrayList<ExtsDataPoint<byte[]>> skeys) {
        ExtsMaddParams addList = new ExtsMaddParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSRAWMULTIMODIFY)
            .addObjects(addList.getByteParams(pkey, skeys)), BuilderFactory.STRING_LIST));
    }

    /**
     * Read and modify the multi old ts value of multi keys.
     * If the old ts is not exist, the operation will try to add a new ts value.
     * raw operation suit for concurrent update.
     *
     * @param pkey   the pkey
     * @param skeys   the {skey ts value}
     * @param params the params: [DATA_ET time] [CHUNK_SIZE size] [UNCOMPRESSED] [LABELS label1 val1 ...]
     * `DATA_ET` - Set expire time (milliseconds)
     * `CHUNK_SIZE` - Set datapoints num per chunk 256~1024 (size)
     * `UNCOMPRESSED` - set the skey if compressed
     * `LABELS` - Set the skey's labels (label1 val1 label2 val2...)
     * @return Success: List of OK; Fail: error.
     */
    public Response<List<String>> extsmrawmodify(String pkey, ArrayList<ExtsDataPoint<String>> skeys, ExtsAttributesParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSRAWMULTIMODIFY)
            .addObjects(params.getByteParams(pkey, skeys)), BuilderFactory.STRING_LIST));
    }

    public Response<List<String>> extsmrawmodify(byte[] pkey, ArrayList<ExtsDataPoint<byte[]>> skeys, ExtsAttributesParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSRAWMULTIMODIFY)
            .addObjects(params.getByteParams(pkey, skeys)), BuilderFactory.STRING_LIST));
    }

    /**
     * Read and incr the old ts value of the key.
     * If the old ts is not exist, the operation will try to add a new ts value.
     * raw operation suit for concurrent update.
     *
     * @param pkey   the pkey
     * @param skey   the skey
     * @param ts     the timestamp
     * @param value  the value
     * @return Success: OK; Fail: error.
     */
    public Response<String> extsrawincr(String pkey, String skey, String ts, double value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSRAWINCRBY)
            .add(pkey).add(skey).add(ts).add(value), BuilderFactory.STRING));
    }

    public Response<String> extsrawincr(byte[] pkey, byte[] skey, byte[] ts, double value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSRAWINCRBY)
            .add(pkey).add(skey).add(ts).add(value), BuilderFactory.STRING));
    }

    /**
     * Read and incr the old ts value of the key.
     * If the old ts is not exist, the operation will try to add a new ts value.
     * raw operation suit for concurrent update.
     *
     * @param pkey   the pkey
     * @param skey   the skey
     * @param ts     the timestamp
     * @param value  the value
     * @param params the params: [DATA_ET time] [CHUNK_SIZE size] [UNCOMPRESSED] [LABELS label1 val1 ...]
     * `DATA_ET` - Set expire time (milliseconds)
     * `CHUNK_SIZE` - Set datapoints num per chunk 256~1024 (size)
     * `UNCOMPRESSED` - set the skey if compressed
     * `LABELS` - Set the skey's labels (label1 val1 label2 val2...)
     * @return Success: OK; Fail: error.
     */
    public Response<String> extsrawincr(String pkey, String skey, String ts, double value, ExtsAttributesParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSRAWINCRBY)
            .addObjects(params.getByteParams(pkey, skey, ts, String.valueOf(value))), BuilderFactory.STRING));
    }

    public Response<String> extsrawincr(byte[] pkey, byte[] skey, byte[] ts, double value, ExtsAttributesParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSRAWINCRBY)
            .addObjects(params.getByteParams(pkey, skey, ts, toByteArray(value))), BuilderFactory.STRING));
    }

    /**
     * Read and incr the multi old ts value of multi keys.
     * If the old ts is not exist, the operation will try to add a new ts value.
     * raw operation suit for concurrent update.
     *
     * @param pkey   the pkey
     * @param skeys   the {skey ts value}
     * @return Success: OK; Fail: error.
     */
    public Response<List<String>> extsmrawincr(String pkey, ArrayList<ExtsDataPoint<String>> skeys) {
        ExtsMaddParams addList = new ExtsMaddParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSRAWMULTIINCRBY)
            .addObjects(addList.getByteParams(pkey, skeys)), BuilderFactory.STRING_LIST));
    }

    public Response<List<String>> extsmrawincr(byte[] pkey, ArrayList<ExtsDataPoint<byte[]>> skeys) {
        ExtsMaddParams addList = new ExtsMaddParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSRAWMULTIINCRBY)
            .addObjects(addList.getByteParams(pkey, skeys)), BuilderFactory.STRING_LIST));
    }

    /**
     * Read and incr the multi old ts value of multi keys.
     * If the old ts is not exist, the operation will try to add a new ts value.
     * raw operation suit for concurrent update.
     *
     * @param pkey   the pkey
     * @param skeys   the {skey ts value}
     * @param params the params: [DATA_ET time] [CHUNK_SIZE size] [UNCOMPRESSED] [LABELS label1 val1 ...]
     * `DATA_ET` - Set expire time (milliseconds)
     * `CHUNK_SIZE` - Set datapoints num per chunk 256~1024 (size)
     * `UNCOMPRESSED` - set the skey if compressed
     * `LABELS` - Set the skey's labels (label1 val1 label2 val2...)
     * @return Success: List of OK; Fail: error.
     */
    public Response<List<String>> extsmrawincr(String pkey, ArrayList<ExtsDataPoint<String>> skeys, ExtsAttributesParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSRAWMULTIINCRBY)
            .addObjects(params.getByteParams(pkey, skeys)), BuilderFactory.STRING_LIST));
    }

    public Response<List<String>> extsmrawincr(byte[] pkey, ArrayList<ExtsDataPoint<byte[]>> skeys, ExtsAttributesParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSRAWMULTIINCRBY)
            .addObjects(params.getByteParams(pkey, skeys)), BuilderFactory.STRING_LIST));
    }


    /**
     * Set the ts value of the key.
     *
     * @param pkey   the pkey
     * @param skey   the skey
     * @param ts     the timestamp
     * @param value  the value
     * @return Success: OK; Fail: error.
     */
    public Response<String> extsaddstr(String pkey, String skey, String ts, String value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSADDSTR)
            .add(pkey).add(skey).add(ts).add(value), BuilderFactory.STRING));
    }

    public Response<String> extsaddstr(byte[] pkey, byte[] skey, byte[] ts, byte[] value) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSADDSTR)
            .add(pkey).add(skey).add(ts).add(value), BuilderFactory.STRING));
    }

    /**
     * Set the ts value of the key.
     *
     * @param pkey   the pkey
     * @param skey   the skey
     * @param ts     the timestamp
     * @param value  the value
     * @param params the params: [DATA_ET time] [CHUNK_SIZE size] [LABELS label1 val1 ...]
     * `DATA_ET` - Set expire time (milliseconds)
     * `CHUNK_SIZE` - Set datapoints num per chunk 256~1024 (size)
     * `UNCOMPRESSED` - set the skey if compressed
     * `LABELS` - Set the skey's labels (label1 val1 label2 val2...)
     * @return Success: OK; Fail: error.
     */
    public Response<String> extsaddstr(String pkey, String skey, String ts, String value, ExtsAttributesParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSADDSTR)
            .addObjects(params.getByteParams(pkey, skey, ts, value)), BuilderFactory.STRING));
    }

    public Response<String> extsaddstr(byte[] pkey, byte[] skey, byte[] ts, byte[] value, ExtsAttributesParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSADDSTR)
            .addObjects(params.getByteParams(pkey, skey, ts, value)), BuilderFactory.STRING));
    }

    /**
     * Set multi ts value of multi key.
     *
     * @param pkey   the pkey
     * @param skeys   the {skey ts value}
     * @return Success: OK; Fail: error.
     */
    public Response<List<String>> extsmaddstr(String pkey, ArrayList<ExtsStringDataPoint<String>> skeys) {
        ExtsStringMaddParams addList = new ExtsStringMaddParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSMADDSTR)
            .addObjects(addList.getByteParams(pkey, skeys)), BuilderFactory.STRING_LIST));
    }

    public Response<List<String>> extsmaddstr(byte[] pkey, ArrayList<ExtsStringDataPoint<byte[]>> skeys) {
        ExtsStringMaddParams addList = new ExtsStringMaddParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSMADDSTR)
            .addObjects(addList.getByteParams(pkey, skeys)), BuilderFactory.STRING_LIST));
    }

    /**
     * Set multi ts value of multi key.
     *
     * @param pkey   the pkey
     * @param skeys   the {skey ts value}
     * @param params the params: [DATA_ET time] [CHUNK_SIZE size] [UNCOMPRESSED] [LABELS label1 val1 ...]
     * `DATA_ET` - Set expire time (milliseconds)
     * `CHUNK_SIZE` - Set datapoints num per chunk 256~1024 (size)
     * `UNCOMPRESSED` - set the skey if compressed
     * `LABELS` - Set the skey's labels (label1 val1 label2 val2...)
     * @return Success: List of OK; Fail: error.
     */
    public Response<List<String>> extsmaddstr(String pkey, ArrayList<ExtsStringDataPoint<String>> skeys, ExtsAttributesParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSMADDSTR)
            .addObjects(params.getByteParamsStr(pkey, skeys)), BuilderFactory.STRING_LIST));
    }

    public Response<List<String>> extsmaddstr(byte[] pkey, ArrayList<ExtsStringDataPoint<byte[]>> skeys, ExtsAttributesParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSMADDSTR)
            .addObjects(params.getByteParamsStr(pkey, skeys)), BuilderFactory.STRING_LIST));
    }

    /**
     * Alter the Attributes of the skey.
     *
     * @param pkey   the pkey
     * @param skey   the skey
     * @param params the params: [DATA_ET time] [LABELS label1 val1 ...]
     * `DATA_ET` - Set expire time (milliseconds)
     * `LABELS` - Set the skey's labels (label1 val1 label2 val2...)
     * Note that: `CHUNK_SIZE` `UNCOMPRESSED` can be set only first add.
     * @return Success: OK; Fail: error.
     */
    public Response<String> extsalterstr(String pkey, String skey, ExtsAttributesParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSALTERSTR)
            .addObjects(params.getByteParams(pkey, skey)), BuilderFactory.STRING));
    }

    public Response<String> extsalterstr(byte[] pkey, byte[] skey, ExtsAttributesParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSALTERSTR)
            .addObjects(params.getByteParams(pkey, skey)), BuilderFactory.STRING));
    }

    /**
     * Del the skey.
     *
     * @param pkey   the pkey
     * @param skey   the skey
     * @return Success: OK; Fail: error.
     */
    public Response<String> extsdelstr(String pkey, String skey) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSDELSTR)
            .add(pkey).add(skey), BuilderFactory.STRING));
    }

    public Response<String> extsdelstr(byte[] pkey, byte[] skey) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSDELSTR)
            .add(pkey).add(skey), BuilderFactory.STRING));
    }

    /**
     * Get the skey.
     *
     * @param pkey   the pkey
     * @param skey   the skey
     * @return Success: ExtsDataPointResult; Fail: error.
     */
    public Response<ExtsStringDataPointResult> extsgetstr(String pkey, String skey) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSGETSTR)
            .add(pkey).add(skey), TsBuilderFactory.EXTSSTRING_GET_RESULT_STRING));
    }

    public Response<ExtsStringDataPointResult> extsgetstr(byte[] pkey, byte[] skey) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSGETSTR)
            .add(pkey).add(skey), TsBuilderFactory.EXTSSTRING_GET_RESULT_STRING));
    }

    /**
     * Query skeys for the pkey.
     *
     * @param pkey   the pkey
     * @param filters   the filters used to query skeys
     * @return Success: OK; Fail: error.
     */
    public Response<List<String>> extsquerystr(String pkey, ArrayList<ExtsFilter<String>> filters) {
        ExtsQueryParams addList = new ExtsQueryParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSQUERYINDEXSTR)
            .addObjects(addList.getByteParams(pkey, filters)), BuilderFactory.STRING_LIST));
    }

    public Response<List<byte[]>> extsquerystr(byte[] pkey, ArrayList<ExtsFilter<byte[]>> filters) {
        ExtsQueryParams addList = new ExtsQueryParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSQUERYINDEXSTR)
            .addObjects(addList.getByteParams(pkey, filters)), Jedis3BuilderFactory.BYTE_ARRAY_LIST));
    }

    /**
     * Range one skey for the pkey.
     *
     * @param pkey   the pkey
     * @param skey   the skey
     * @param startTs   the start ts
     * @param endTs   the end ts
     * @return Success: OK; Fail: error.
     */
    public Response<ExtsStringSkeyResult> extsrangestr(String pkey, String skey, String startTs, String endTs) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSRANGESTR)
            .add(pkey).add(skey).add(startTs).add(endTs), TsBuilderFactory.EXTSSTRING_RANGE_RESULT_STRING));
    }

    public Response<ExtsStringSkeyResult> extsrangestr(byte[] pkey, byte[] skey, byte[] startTs, byte[] endTs) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSRANGESTR)
            .add(pkey).add(skey).add(startTs).add(endTs), TsBuilderFactory.EXTSSTRING_RANGE_RESULT_STRING));
    }

    /**
     * Range one skey for the pkey.
     *
     * @param pkey   the pkey
     * @param skey   the skey
     * @param startTs   the start ts
     * @param endTs   the end ts
     * @param params the aggregation params: [MAXCOUNT count] [aggregationType timeBucket]
     * `MAXCOUNT` - Set the maxcount for output
     * `aggregationType` - aggregation type MIN, MAX, SUM, AVG, STDP, STDS, COUNT, FIRST, LAST, RANGE.
     * `timeBucket` - set the timeBucket of the aggregation.
     * @return Success: OK; Fail: error.
     */
    public Response<ExtsStringSkeyResult> extsrangestr(String pkey, String skey, String startTs, String endTs, ExtsStringAggregationParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSRANGESTR)
            .addObjects(params.getByteRangeParams(pkey, skey, startTs, endTs)), TsBuilderFactory.EXTSSTRING_RANGE_RESULT_STRING));
    }

    public Response<ExtsStringSkeyResult> extsrangestr(byte[] pkey, byte[] skey, byte[] startTs, byte[] endTs, ExtsStringAggregationParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSRANGESTR)
            .addObjects(params.getByteRangeParams(pkey, skey, startTs, endTs)), TsBuilderFactory.EXTSSTRING_RANGE_RESULT_STRING));
    }

    /**
     * Mrange skeys for the pkey.
     *
     * @param pkey   the pkey
     * @param startTs   the start ts
     * @param endTs   the end ts
     * @param filters   the filters used to query skeys
     * @return Success: OK; Fail: error.
     */
    public Response<List<ExtsStringSkeyResult>> extsmrangestr(String pkey, String startTs, String endTs, ArrayList<ExtsFilter<String>> filters) {
        ExtsStringAggregationParams params = new ExtsStringAggregationParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSMRANGESTR)
            .addObjects(params.getByteMrangeParams(pkey, startTs, endTs, filters)), TsBuilderFactory.EXTSSTRING_MRANGE_RESULT_STRING));
    }

    public Response<List<ExtsStringSkeyResult>> extsmrangestr(byte[] pkey, byte[] startTs, byte[] endTs, ArrayList<ExtsFilter<byte[]>> filters) {
        ExtsStringAggregationParams params = new ExtsStringAggregationParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSMRANGESTR)
            .addObjects(params.getByteMrangeParams(pkey, startTs, endTs, filters)), TsBuilderFactory.EXTSSTRING_MRANGE_RESULT_STRING));
    }


    /**
     * Mrange skeys for the pkey.
     *
     * @param pkey   the pkey
     * @param startTs   the start ts
     * @param endTs   the end ts
     * @param filters   the filters used to query skeys
     * @param params the aggregation params: [MAXCOUNT count] [aggregationType timeBucket]
     * `MAXCOUNT` - Set the maxcount for output
     * `aggregationType` - aggregation type MIN, MAX, SUM, AVG, STDP, STDS, COUNT, FIRST, LAST, RANGE.
     * `timeBucket` - set the timeBucket of the aggregation.
     * `WITHLABELS` - output the labels.
     * @return Success: OK; Fail: error.
     */
    public Response<List<ExtsStringSkeyResult>> extsmrangestr(String pkey, String startTs, String endTs, ExtsStringAggregationParams params, ArrayList<ExtsFilter<String>> filters) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSMRANGESTR)
            .addObjects(params.getByteMrangeParams(pkey, startTs, endTs, filters)), TsBuilderFactory.EXTSSTRING_MRANGE_RESULT_STRING));
    }

    public Response<List<ExtsStringSkeyResult>> extsmrangestr(byte[] pkey, byte[] startTs, byte[] endTs, ExtsStringAggregationParams params, ArrayList<ExtsFilter<byte[]>> filters) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.TSSMRANGESTR)
            .addObjects(params.getByteMrangeParams(pkey, startTs, endTs, filters)), TsBuilderFactory.EXTSSTRING_MRANGE_RESULT_STRING));
    }

}
