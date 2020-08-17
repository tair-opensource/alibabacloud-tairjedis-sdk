package com.aliyun.tair.tairts;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairts.factory.TsBuilderFactory;
import com.aliyun.tair.tairts.params.*;
import com.aliyun.tair.tairts.results.ExtsDataPointResult;
import com.aliyun.tair.tairts.results.ExtsSkeyResult;
import com.aliyun.tair.tairts.results.ExtsStringDataPointResult;
import com.aliyun.tair.tairts.results.ExtsStringSkeyResult;
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

    public Response<List<String>> extsmadd(String pkey, long skeyNum, ArrayList<ExtsDataPoint<String>> skeys) {
        ExtsMaddParams addList = new ExtsMaddParams();
        getClient("").sendCommand(ModuleCommand.TSSMADD, addList.getByteParams(pkey, skeyNum, skeys));
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<String>> extsmadd(byte[] pkey, long skeyNum, ArrayList<ExtsDataPoint<byte[]>> skeys) {
        ExtsMaddParams addList = new ExtsMaddParams();
        getClient("").sendCommand(ModuleCommand.TSSMADD, addList.getByteParams(pkey, skeyNum, skeys));
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<String>> extsmadd(String pkey, long skeyNum, ArrayList<ExtsDataPoint<String>> skeys, ExtsAttributesParams params) {
        ExtsMaddParams addList = new ExtsMaddParams();
        getClient("").sendCommand(ModuleCommand.TSSMADD, params.getByteParams(pkey, skeyNum, skeys));
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<String>> extsmadd(byte[] pkey, long skeyNum, ArrayList<ExtsDataPoint<byte[]>> skeys, ExtsAttributesParams params) {
        ExtsMaddParams addList = new ExtsMaddParams();
        getClient("").sendCommand(ModuleCommand.TSSMADD, params.getByteParams(pkey, skeyNum, skeys));
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

    public Response<List<String>> extsmincr(String pkey, long skeyNum, ArrayList<ExtsDataPoint<String>> skeys) {
        ExtsMaddParams addList = new ExtsMaddParams();
        getClient("").sendCommand(ModuleCommand.TSSMINCRBY, addList.getByteParams(pkey, skeyNum, skeys));
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<String>> extsmincr(byte[] pkey, long skeyNum, ArrayList<ExtsDataPoint<byte[]>> skeys) {
        ExtsMaddParams addList = new ExtsMaddParams();
        getClient("").sendCommand(ModuleCommand.TSSMINCRBY, addList.getByteParams(pkey, skeyNum, skeys));
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<String>> extsmincr(String pkey, long skeyNum, ArrayList<ExtsDataPoint<String>> skeys, ExtsAttributesParams params) {
        ExtsMaddParams addList = new ExtsMaddParams();
        getClient("").sendCommand(ModuleCommand.TSSMINCRBY, params.getByteParams(pkey, skeyNum, skeys));
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<String>> extsmincr(byte[] pkey, long skeyNum, ArrayList<ExtsDataPoint<byte[]>> skeys, ExtsAttributesParams params) {
        ExtsMaddParams addList = new ExtsMaddParams();
        getClient("").sendCommand(ModuleCommand.TSSMINCRBY, params.getByteParams(pkey, skeyNum, skeys));
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

    public Response<ExtsSkeyResult> extsrange(String pkey, String skey, String startTs, String endTs) {
        ExtsQueryParams addList = new ExtsQueryParams();
        getClient("").sendCommand(ModuleCommand.TSSRANGE, pkey, skey, startTs, endTs);
        return getResponse(TsBuilderFactory.EXTSRANGE_RESULT_STRING);
    }

    public Response<ExtsSkeyResult> extsrange(byte[] pkey, byte[] skey, byte[] startTs, byte[] endTs) {
        ExtsQueryParams addList = new ExtsQueryParams();
        getClient("").sendCommand(ModuleCommand.TSSRANGE, pkey, skey, startTs, endTs);
        return getResponse(TsBuilderFactory.EXTSRANGE_RESULT_STRING);
    }

    public Response<ExtsSkeyResult> extsrange(String pkey, String skey, String startTs, String endTs, ExtsAggregationParams params) {
        ExtsQueryParams addList = new ExtsQueryParams();
        getClient("").sendCommand(ModuleCommand.TSSRANGE, params.getByteRangeParams(pkey, skey, startTs, endTs));
        return getResponse(TsBuilderFactory.EXTSRANGE_RESULT_STRING);
    }

    public Response<ExtsSkeyResult> extsrange(byte[] pkey, byte[] skey, byte[] startTs, byte[] endTs, ExtsAggregationParams params) {
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

    public Response<ExtsSkeyResult> extsprange(String pkey, String startTs, String endTs, String pkeyAggregationType, long pkeyTimeBucket, ArrayList<ExtsFilter<String>> filters) {
        ExtsAggregationParams params = new ExtsAggregationParams();
        getClient("").sendCommand(ModuleCommand.TSPRANGE, params.getBytePrangeParams(pkey, startTs, endTs, pkeyAggregationType, pkeyTimeBucket, filters));
        return getResponse(TsBuilderFactory.EXTSRANGE_RESULT_STRING);
    }

    public Response<ExtsSkeyResult> extsprange(byte[] pkey, byte[] startTs, byte[] endTs, byte[] pkeyAggregationType, long pkeyTimeBucket, ArrayList<ExtsFilter<byte[]>> filters) {
        ExtsAggregationParams params = new ExtsAggregationParams();
        getClient("").sendCommand(ModuleCommand.TSPRANGE, params.getBytePrangeParams(pkey, startTs, endTs, pkeyAggregationType, pkeyTimeBucket, filters));
        return getResponse(TsBuilderFactory.EXTSRANGE_RESULT_STRING);
    }

    public Response<ExtsSkeyResult> extsprange(String pkey, String startTs, String endTs, String pkeyAggregationType, long pkeyTimeBucket, ExtsAggregationParams params, ArrayList<ExtsFilter<String>> filters) {
        getClient("").sendCommand(ModuleCommand.TSPRANGE, params.getBytePrangeParams(pkey, startTs, endTs, pkeyAggregationType, pkeyTimeBucket, filters));
        return getResponse(TsBuilderFactory.EXTSRANGE_RESULT_STRING);
    }

    public Response<ExtsSkeyResult> extsprange(byte[] pkey, byte[] startTs, byte[] endTs, byte[] pkeyAggregationType, long pkeyTimeBucket, ExtsAggregationParams params, ArrayList<ExtsFilter<byte[]>> filters) {
        getClient("").sendCommand(ModuleCommand.TSPRANGE, params.getBytePrangeParams(pkey, startTs, endTs, pkeyAggregationType, pkeyTimeBucket, filters));
        return getResponse(TsBuilderFactory.EXTSRANGE_RESULT_STRING);
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
        getClient("").sendCommand(ModuleCommand.TSSRAWMODIFY, pkey, skey, ts, String.valueOf(value));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> extsrawmodify(byte[] pkey, byte[] skey, byte[] ts, double value) {
        getClient("").sendCommand(ModuleCommand.TSSRAWMODIFY, pkey, skey, ts, toByteArray(value));
        return getResponse(BuilderFactory.STRING);
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
        getClient("").sendCommand(ModuleCommand.TSSRAWMODIFY, params.getByteParams(pkey, skey, ts, String.valueOf(value)));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> extsrawmodify(byte[] pkey, byte[] skey, byte[] ts, double value, ExtsAttributesParams params) {
        getClient("").sendCommand(ModuleCommand.TSSRAWMODIFY, params.getByteParams(pkey, skey, ts, toByteArray(value)));
        return getResponse(BuilderFactory.STRING);
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
    public Response<List<String>> extsmrawmodify(String pkey, long skeyNum, ArrayList<ExtsDataPoint<String>> skeys) {
        ExtsMaddParams addList = new ExtsMaddParams();
        getClient("").sendCommand(ModuleCommand.TSSRAWMULTIMODIFY, addList.getByteParams(pkey, skeyNum, skeys));
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<String>> extsmrawmodify(byte[] pkey, long skeyNum, ArrayList<ExtsDataPoint<byte[]>> skeys) {
        ExtsMaddParams addList = new ExtsMaddParams();
        getClient("").sendCommand(ModuleCommand.TSSRAWMULTIMODIFY, addList.getByteParams(pkey, skeyNum, skeys));
        return getResponse(BuilderFactory.STRING_LIST);
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
    public Response<List<String>> extsmrawmodify(String pkey, long skeyNum, ArrayList<ExtsDataPoint<String>> skeys, ExtsAttributesParams params) {
        getClient("").sendCommand(ModuleCommand.TSSRAWMULTIMODIFY, params.getByteParams(pkey, skeyNum, skeys));
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<String>> extsmrawmodify(byte[] pkey, long skeyNum, ArrayList<ExtsDataPoint<byte[]>> skeys, ExtsAttributesParams params) {
        getClient("").sendCommand(ModuleCommand.TSSRAWMULTIMODIFY, params.getByteParams(pkey, skeyNum, skeys));
        return getResponse(BuilderFactory.STRING_LIST);
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
        getClient("").sendCommand(ModuleCommand.TSSRAWINCRBY, pkey, skey, ts, String.valueOf(value));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> extsrawincr(byte[] pkey, byte[] skey, byte[] ts, double value) {
        getClient("").sendCommand(ModuleCommand.TSSRAWINCRBY, pkey, skey, ts, toByteArray(value));
        return getResponse(BuilderFactory.STRING);
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
        getClient("").sendCommand(ModuleCommand.TSSRAWINCRBY, params.getByteParams(pkey, skey, ts, String.valueOf(value)));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> extsrawincr(byte[] pkey, byte[] skey, byte[] ts, double value, ExtsAttributesParams params) {
        getClient("").sendCommand(ModuleCommand.TSSRAWINCRBY, params.getByteParams(pkey, skey, ts, toByteArray(value)));
        return getResponse(BuilderFactory.STRING);
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
    public Response<List<String>> extsmrawincr(String pkey, long skeyNum, ArrayList<ExtsDataPoint<String>> skeys) {
        ExtsMaddParams addList = new ExtsMaddParams();
        getClient("").sendCommand(ModuleCommand.TSSRAWMULTIINCRBY, addList.getByteParams(pkey, skeyNum, skeys));
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<String>> extsmrawincr(byte[] pkey, long skeyNum, ArrayList<ExtsDataPoint<byte[]>> skeys) {
        ExtsMaddParams addList = new ExtsMaddParams();
        getClient("").sendCommand(ModuleCommand.TSSRAWMULTIINCRBY, addList.getByteParams(pkey, skeyNum, skeys));
        return getResponse(BuilderFactory.STRING_LIST);
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
    public Response<List<String>> extsmrawincr(String pkey, long skeyNum, ArrayList<ExtsDataPoint<String>> skeys, ExtsAttributesParams params) {
        getClient("").sendCommand(ModuleCommand.TSSRAWMULTIINCRBY, params.getByteParams(pkey, skeyNum, skeys));
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<String>> extsmrawincr(byte[] pkey, long skeyNum, ArrayList<ExtsDataPoint<byte[]>> skeys, ExtsAttributesParams params) {
        getClient("").sendCommand(ModuleCommand.TSSRAWMULTIINCRBY, params.getByteParams(pkey, skeyNum, skeys));
        return getResponse(BuilderFactory.STRING_LIST);
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
        getClient("").sendCommand(ModuleCommand.TSSADDSTR, pkey, skey, ts, value);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> extsaddstr(byte[] pkey, byte[] skey, byte[] ts, byte[] value) {
        getClient("").sendCommand(ModuleCommand.TSSADDSTR, pkey, skey, ts, value);
        return getResponse(BuilderFactory.STRING);
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
        getClient("").sendCommand(ModuleCommand.TSSADDSTR, params.getByteParams(pkey, skey, ts, value));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> extsaddstr(byte[] pkey, byte[] skey, byte[] ts, byte[] value, ExtsAttributesParams params) {
        getClient("").sendCommand(ModuleCommand.TSSADDSTR, params.getByteParams(pkey, skey, ts, value));
        return getResponse(BuilderFactory.STRING);
    }

    /**
     * Set multi ts value of multi key.
     *
     * @param pkey   the pkey
     * @param skeys   the {skey ts value}
     * @return Success: OK; Fail: error.
     */
    public Response<List<String>> extsmaddstr(String pkey, long skeyNum, ArrayList<ExtsStringDataPoint<String>> skeys) {
        ExtsStringMaddParams addList = new ExtsStringMaddParams();
        getClient("").sendCommand(ModuleCommand.TSSMADDSTR, addList.getByteParams(pkey, skeyNum, skeys));
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<String>> extsmaddstr(byte[] pkey, long skeyNum, ArrayList<ExtsStringDataPoint<byte[]>> skeys) {
        ExtsStringMaddParams addList = new ExtsStringMaddParams();
        getClient("").sendCommand(ModuleCommand.TSSMADDSTR, addList.getByteParams(pkey, skeyNum, skeys));
        return getResponse(BuilderFactory.STRING_LIST);
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
    public Response<List<String>> extsmaddstr(String pkey, long skeyNum, ArrayList<ExtsStringDataPoint<String>> skeys, ExtsAttributesParams params) {
        getClient("").sendCommand(ModuleCommand.TSSMADDSTR, params.getByteParamsStr(pkey, skeyNum, skeys));
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<String>> extsmaddstr(byte[] pkey, long skeyNum, ArrayList<ExtsStringDataPoint<byte[]>> skeys, ExtsAttributesParams params) {
        getClient("").sendCommand(ModuleCommand.TSSMADDSTR, params.getByteParamsStr(pkey, skeyNum, skeys));
        return getResponse(BuilderFactory.STRING_LIST);
    }

    /**
     * Alter the Attributes of the skey.
     *
     * @param pkey   the pkey
     * @param skey   the skey
     * @param expireTime the expireTime: [DATA_ET time]
     * `DATA_ET` - Set expire time (milliseconds)
     * Note that: `LABELS` `CHUNK_SIZE` `UNCOMPRESSED` can be set only first add.
     * @return Success: OK; Fail: error.
     */
    public Response<String> extsalterstr(String pkey, String skey, long expireTime) {
        getClient("").sendCommand(ModuleCommand.TSSALTERSTR, pkey, skey, "DATA_ET", String.valueOf(expireTime));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> extsalterstr(byte[] pkey, byte[] skey, long expireTime) {
        getClient("").sendCommand(ModuleCommand.TSSALTERSTR, pkey, skey, SafeEncoder.encode("DATA_ET"), toByteArray(expireTime));
        return getResponse(BuilderFactory.STRING);
    }

    /**
     * Del the skey.
     *
     * @param pkey   the pkey
     * @param skey   the skey
     * @return Success: OK; Fail: error.
     */
    public Response<String> extsdelstr(String pkey, String skey) {
        getClient("").sendCommand(ModuleCommand.TSSDELSTR, pkey, skey);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> extsdelstr(byte[] pkey, byte[] skey) {
        getClient("").sendCommand(ModuleCommand.TSSDELSTR, pkey, skey);
        return getResponse(BuilderFactory.STRING);
    }

    /**
     * Get the skey.
     *
     * @param pkey   the pkey
     * @param skey   the skey
     * @return Success: ExtsDataPointResult; Fail: error.
     */
    public Response<ExtsStringDataPointResult> extsgetstr(String pkey, String skey) {
        getClient("").sendCommand(ModuleCommand.TSSGETSTR, pkey, skey);
        return getResponse(TsBuilderFactory.EXTSSTRING_GET_RESULT_STRING);
    }

    public Response<ExtsStringDataPointResult> extsgetstr(byte[] pkey, byte[] skey) {
        getClient("").sendCommand(ModuleCommand.TSSGETSTR, pkey, skey);
        return getResponse(TsBuilderFactory.EXTSSTRING_GET_RESULT_STRING);
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
        getClient("").sendCommand(ModuleCommand.TSSQUERYINDEXSTR, addList.getByteParams(pkey, filters));
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<byte[]>> extsquerystr(byte[] pkey, ArrayList<ExtsFilter<byte[]>> filters) {
        ExtsQueryParams addList = new ExtsQueryParams();
        getClient("").sendCommand(ModuleCommand.TSSQUERYINDEXSTR, addList.getByteParams(pkey, filters));
        return getResponse(BuilderFactory.BYTE_ARRAY_LIST);
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
        getClient("").sendCommand(ModuleCommand.TSSRANGESTR, pkey, skey, startTs, endTs);
        return getResponse(TsBuilderFactory.EXTSSTRING_RANGE_RESULT_STRING);
    }

    public Response<ExtsStringSkeyResult> extsrangestr(byte[] pkey, byte[] skey, byte[] startTs, byte[] endTs) {
        getClient("").sendCommand(ModuleCommand.TSSRANGESTR, pkey, skey, startTs, endTs);
        return getResponse(TsBuilderFactory.EXTSSTRING_RANGE_RESULT_STRING);
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
        getClient("").sendCommand(ModuleCommand.TSSRANGESTR, params.getByteRangeParams(pkey, skey, startTs, endTs));
        return getResponse(TsBuilderFactory.EXTSSTRING_RANGE_RESULT_STRING);
    }

    public Response<ExtsStringSkeyResult> extsrangestr(byte[] pkey, byte[] skey, byte[] startTs, byte[] endTs, ExtsStringAggregationParams params) {
        getClient("").sendCommand(ModuleCommand.TSSRANGESTR, params.getByteRangeParams(pkey, skey, startTs, endTs));
        return getResponse(TsBuilderFactory.EXTSSTRING_RANGE_RESULT_STRING);
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
        getClient("").sendCommand(ModuleCommand.TSSMRANGESTR, params.getByteMrangeParams(pkey, startTs, endTs, filters));
        return getResponse(TsBuilderFactory.EXTSSTRING_MRANGE_RESULT_STRING);
    }

    public Response<List<ExtsStringSkeyResult>> extsmrangestr(byte[] pkey, byte[] startTs, byte[] endTs, ArrayList<ExtsFilter<byte[]>> filters) {
        ExtsStringAggregationParams params = new ExtsStringAggregationParams();
        getClient("").sendCommand(ModuleCommand.TSSMRANGESTR, params.getByteMrangeParams(pkey, startTs, endTs, filters));
        return getResponse(TsBuilderFactory.EXTSSTRING_MRANGE_RESULT_STRING);
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
        getClient("").sendCommand(ModuleCommand.TSSMRANGESTR, params.getByteMrangeParams(pkey, startTs, endTs, filters));
        return getResponse(TsBuilderFactory.EXTSSTRING_MRANGE_RESULT_STRING);
    }

    public Response<List<ExtsStringSkeyResult>> extsmrangestr(byte[] pkey, byte[] startTs, byte[] endTs, ExtsStringAggregationParams params, ArrayList<ExtsFilter<byte[]>> filters) {
        getClient("").sendCommand(ModuleCommand.TSSMRANGESTR, params.getByteMrangeParams(pkey, startTs, endTs, filters));
        return getResponse(TsBuilderFactory.EXTSSTRING_MRANGE_RESULT_STRING);
    }

}
