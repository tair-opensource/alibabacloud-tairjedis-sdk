package com.aliyun.tair.tairts;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairts.factory.TsBuilderFactory;
import com.aliyun.tair.tairts.params.*;
import com.aliyun.tair.tairts.results.ExtsDataPointResult;
import com.aliyun.tair.tairts.results.ExtsSkeyResult;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;
import java.util.List;

import static redis.clients.jedis.Protocol.toByteArray;

public class TairTs {

    private Jedis jedis;

    public TairTs(Jedis jedis) {
        this.jedis = jedis;
    }

    private Jedis getJedis() {
        return jedis;
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
    public String extsadd(String pkey, String skey, String ts, double value) {
        Object obj = getJedis().sendCommand(ModuleCommand.TSSADD, pkey, skey, ts, String.valueOf(value));
        return BuilderFactory.STRING.build(obj);
    }

    public String extsadd(byte[] pkey, byte[] skey, byte[] ts, double value) {
        Object obj = getJedis().sendCommand(ModuleCommand.TSSADD, pkey, skey, ts, toByteArray(value));
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Set the ts value of the key.
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
    public String extsadd(String pkey, String skey, String ts, double value, ExtsAttributesParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.TSSADD, params.getByteParams(pkey, skey, ts, String.valueOf(value)));
        return BuilderFactory.STRING.build(obj);
    }

    public String extsadd(byte[] pkey, byte[] skey, byte[] ts, double value, ExtsAttributesParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.TSSADD, params.getByteParams(pkey, skey, ts, toByteArray(value)));
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Set multi ts value of multi key.
     *
     * @param pkey   the pkey
     * @param skeys   the {skey ts value}
     * @return Success: OK; Fail: error.
     */
    public List<String> extsmadd(String pkey, ArrayList<ExtsDataPoint<String>> skeys) {
        ExtsMaddParams addList = new ExtsMaddParams();
        Object obj = getJedis().sendCommand(ModuleCommand.TSSMADD, addList.getByteParams(pkey, skeys));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<String> extsmadd(byte[] pkey, ArrayList<ExtsDataPoint<byte[]>> skeys) {
        ExtsMaddParams addList = new ExtsMaddParams();
        Object obj = getJedis().sendCommand(ModuleCommand.TSSMADD, addList.getByteParams(pkey, skeys));
        return BuilderFactory.STRING_LIST.build(obj);
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
    public List<String> extsmadd(String pkey, ArrayList<ExtsDataPoint<String>> skeys, ExtsAttributesParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.TSSMADD, params.getByteParams(pkey, skeys));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<String> extsmadd(byte[] pkey, ArrayList<ExtsDataPoint<byte[]>> skeys, ExtsAttributesParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.TSSMADD, params.getByteParams(pkey, skeys));
        return BuilderFactory.STRING_LIST.build(obj);
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
    public String extsalter(String pkey, String skey, long expireTime) {
        Object obj = getJedis().sendCommand(ModuleCommand.TSSALTER, pkey, skey, "DATA_ET", String.valueOf(expireTime));
        return BuilderFactory.STRING.build(obj);
    }

    public String extsalter(byte[] pkey, byte[] skey, long expireTime) {
        Object obj = getJedis().sendCommand(ModuleCommand.TSSALTER, pkey, skey, SafeEncoder.encode("DATA_ET"), toByteArray(expireTime));
        return BuilderFactory.STRING.build(obj);
    }


    /**
     * Incr the ts value of the key.
     *
     * @param pkey   the pkey
     * @param skey   the skey
     * @param ts     the timestamp
     * @param value  the value
     * @return Success: OK; Fail: error.
     */
    public String extsincr(String pkey, String skey, String ts, double value) {
        Object obj = getJedis().sendCommand(ModuleCommand.TSSINCRBY, pkey, skey, ts, String.valueOf(value));
        return BuilderFactory.STRING.build(obj);
    }

    public String extsincr(byte[] pkey, byte[] skey, byte[] ts, double value) {
        Object obj = getJedis().sendCommand(ModuleCommand.TSSINCRBY, pkey, skey, ts, toByteArray(value));
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Incr the ts value of the key.
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
    public String extsincr(String pkey, String skey, String ts, double value, ExtsAttributesParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.TSSINCRBY, params.getByteParams(pkey, skey, ts, String.valueOf(value)));
        return BuilderFactory.STRING.build(obj);
    }

    public String extsincr(byte[] pkey, byte[] skey, byte[] ts, double value, ExtsAttributesParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.TSSINCRBY, params.getByteParams(pkey, skey, ts, toByteArray(value)));
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Incr multi ts value of multi key.
     *
     * @param pkey   the pkey
     * @param skeys   the {skey ts value}
     * @return Success: OK; Fail: error.
     */
    public List<String> extsmincr(String pkey, ArrayList<ExtsDataPoint<String>> skeys) {
        ExtsMaddParams addList = new ExtsMaddParams();
        Object obj = getJedis().sendCommand(ModuleCommand.TSSMINCRBY, addList.getByteParams(pkey, skeys));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<String> extsmincr(byte[] pkey, ArrayList<ExtsDataPoint<byte[]>> skeys) {
        ExtsMaddParams addList = new ExtsMaddParams();
        Object obj = getJedis().sendCommand(ModuleCommand.TSSMINCRBY, addList.getByteParams(pkey, skeys));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    /**
     * Incr multi ts value of multi key.
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
    public List<String> extsmincr(String pkey, ArrayList<ExtsDataPoint<String>> skeys, ExtsAttributesParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.TSSMINCRBY, params.getByteParams(pkey, skeys));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<String> extsmincr(byte[] pkey, ArrayList<ExtsDataPoint<byte[]>> skeys, ExtsAttributesParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.TSSMINCRBY, params.getByteParams(pkey, skeys));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    /**
     * Del the skey.
     *
     * @param pkey   the pkey
     * @param skey   the skey
     * @return Success: OK; Fail: error.
     */
    public String extsdel(String pkey, String skey) {
        Object obj = getJedis().sendCommand(ModuleCommand.TSSDEL, pkey, skey);
        return BuilderFactory.STRING.build(obj);
    }

    public String extsdel(byte[] pkey, byte[] skey) {
        Object obj = getJedis().sendCommand(ModuleCommand.TSSDEL, pkey, skey);
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Get the skey.
     *
     * @param pkey   the pkey
     * @param skey   the skey
     * @return Success: ExtsDataPointResult; Fail: error.
     */
    public ExtsDataPointResult extsget(String pkey, String skey) {
        Object obj = getJedis().sendCommand(ModuleCommand.TSSGET, pkey, skey);
        return TsBuilderFactory.EXTSGET_RESULT_STRING.build(obj);
    }

    public ExtsDataPointResult extsget(byte[] pkey, byte[] skey) {
        Object obj = getJedis().sendCommand(ModuleCommand.TSSGET, pkey, skey);
        return TsBuilderFactory.EXTSGET_RESULT_STRING.build(obj);
    }

    /**
     * Query skeys for the pkey.
     *
     * @param pkey   the pkey
     * @param filters   the filters used to query skeys
     * @return Success: OK; Fail: error.
     */
    public List<String> extsquery(String pkey, ArrayList<ExtsFilter<String>> filters) {
        ExtsQueryParams addList = new ExtsQueryParams();
        Object obj = getJedis().sendCommand(ModuleCommand.TSSQUERYINDEX, addList.getByteParams(pkey, filters));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<byte[]> extsquery(byte[] pkey, ArrayList<ExtsFilter<byte[]>> filters) {
        ExtsQueryParams addList = new ExtsQueryParams();
        Object obj = getJedis().sendCommand(ModuleCommand.TSSQUERYINDEX, addList.getByteParams(pkey, filters));
        return BuilderFactory.BYTE_ARRAY_LIST.build(obj);
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
    public List<ExtsDataPointResult> extsrange(String pkey, String skey, String startTs, String endTs) {
        Object obj = getJedis().sendCommand(ModuleCommand.TSSRANGE, pkey, skey, startTs, endTs);
        return TsBuilderFactory.EXTSRANGE_RESULT_STRING.build(obj);
    }

    public List<ExtsDataPointResult> extsrange(byte[] pkey, byte[] skey, byte[] startTs, byte[] endTs) {
        Object obj = getJedis().sendCommand(ModuleCommand.TSSRANGE, pkey, skey, startTs, endTs);
        return TsBuilderFactory.EXTSRANGE_RESULT_STRING.build(obj);
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
    public List<ExtsDataPointResult> extsrange(String pkey, String skey, String startTs, String endTs, ExtsAggregationParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.TSSRANGE, params.getByteRangeParams(pkey, skey, startTs, endTs));
        return TsBuilderFactory.EXTSRANGE_RESULT_STRING.build(obj);
    }

    public List<ExtsDataPointResult> extsrange(byte[] pkey, byte[] skey, byte[] startTs, byte[] endTs, ExtsAggregationParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.TSSRANGE, params.getByteRangeParams(pkey, skey, startTs, endTs));
        return TsBuilderFactory.EXTSRANGE_RESULT_STRING.build(obj);
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
    public List<ExtsSkeyResult> extsmrange(String pkey, String startTs, String endTs, ArrayList<ExtsFilter<String>> filters) {
        ExtsAggregationParams params = new ExtsAggregationParams();
        Object obj = getJedis().sendCommand(ModuleCommand.TSSMRANGE, params.getByteMrangeParams(pkey, startTs, endTs, filters));
        return TsBuilderFactory.EXTSMRANGE_RESULT_STRING.build(obj);
    }

    public List<ExtsSkeyResult> extsmrange(byte[] pkey, byte[] startTs, byte[] endTs, ArrayList<ExtsFilter<byte[]>> filters) {
        ExtsAggregationParams params = new ExtsAggregationParams();
        Object obj = getJedis().sendCommand(ModuleCommand.TSSMRANGE, params.getByteMrangeParams(pkey, startTs, endTs, filters));
        return TsBuilderFactory.EXTSMRANGE_RESULT_STRING.build(obj);
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
    public List<ExtsSkeyResult> extsmrange(String pkey, String startTs, String endTs, ExtsAggregationParams params, ArrayList<ExtsFilter<String>> filters) {
        Object obj = getJedis().sendCommand(ModuleCommand.TSSMRANGE, params.getByteMrangeParams(pkey, startTs, endTs, filters));
        return TsBuilderFactory.EXTSMRANGE_RESULT_STRING.build(obj);
    }

    public List<ExtsSkeyResult> extsmrange(byte[] pkey, byte[] startTs, byte[] endTs, ExtsAggregationParams params, ArrayList<ExtsFilter<byte[]>> filters) {
        Object obj = getJedis().sendCommand(ModuleCommand.TSSMRANGE, params.getByteMrangeParams(pkey, startTs, endTs, filters));
        return TsBuilderFactory.EXTSMRANGE_RESULT_STRING.build(obj);
    }

    /**
     * Prange the pkey.
     *
     * @param pkey   the pkey
     * @param startTs   the start ts
     * @param endTs   the end ts
     * @param pkeyAggregationType   the aggregation type for the pkey
     * @param pkeyTimeBucket   the timeBucket for the pkey
     * @param filters   the filters used to query skeys
     * @return Success: OK; Fail: error.
     */
    public List<ExtsDataPointResult> extsprange(String pkey, String startTs, String endTs, String pkeyAggregationType, long pkeyTimeBucket, ArrayList<ExtsFilter<String>> filters) {
        ExtsAggregationParams params = new ExtsAggregationParams();
        Object obj = getJedis().sendCommand(ModuleCommand.TSPRANGE, params.getBytePrangeParams(pkey, startTs, endTs, pkeyAggregationType, pkeyTimeBucket, filters));
        return TsBuilderFactory.EXTSRANGE_RESULT_STRING.build(obj);
    }

    public List<ExtsDataPointResult> extsprange(byte[] pkey, byte[] startTs, byte[] endTs, byte[] pkeyAggregationType, long pkeyTimeBucket, ArrayList<ExtsFilter<byte[]>> filters) {
        ExtsAggregationParams params = new ExtsAggregationParams();
        Object obj = getJedis().sendCommand(ModuleCommand.TSPRANGE, params.getBytePrangeParams(pkey, startTs, endTs, pkeyAggregationType, pkeyTimeBucket, filters));
        return TsBuilderFactory.EXTSRANGE_RESULT_STRING.build(obj);
    }

    /**
     * Prange the pkey.
     *
     * @param pkey   the pkey
     * @param startTs   the start ts
     * @param endTs   the end ts
     * @param pkeyAggregationType   the aggregation type for the pkey
     *                              - aggregation type MIN, MAX, SUM, AVG, STDP, STDS, COUNT, FIRST, LAST, RANGE.
     * @param pkeyTimeBucket   the timeBucket for the pkey
     * @param filters   the filters used to query skeys
     * @param params the aggregation params: [MAXCOUNT count] [aggregationType timeBucket]
     * `MAXCOUNT` - Set the maxcount for output
     * `aggregationType` - aggregation type MIN, MAX, SUM, AVG, STDP, STDS, COUNT, FIRST, LAST, RANGE.
     * `timeBucket` - set the timeBucket of the aggregation.
     * @return Success: OK; Fail: error.
     */

    public List<ExtsDataPointResult> extsprange(String pkey, String startTs, String endTs, String pkeyAggregationType, long pkeyTimeBucket, ExtsAggregationParams params, ArrayList<ExtsFilter<String>> filters) {
        Object obj = getJedis().sendCommand(ModuleCommand.TSPRANGE, params.getBytePrangeParams(pkey, startTs, endTs, pkeyAggregationType, pkeyTimeBucket, filters));
        return TsBuilderFactory.EXTSRANGE_RESULT_STRING.build(obj);
    }

    public List<ExtsDataPointResult> extsprange(byte[] pkey, byte[] startTs, byte[] endTs, byte[] pkeyAggregationType, long pkeyTimeBucket, ExtsAggregationParams params, ArrayList<ExtsFilter<byte[]>> filters) {
        Object obj = getJedis().sendCommand(ModuleCommand.TSPRANGE, params.getBytePrangeParams(pkey, startTs, endTs, pkeyAggregationType, pkeyTimeBucket, filters));
        return TsBuilderFactory.EXTSRANGE_RESULT_STRING.build(obj);
    }

}
