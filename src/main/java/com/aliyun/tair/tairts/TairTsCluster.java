package com.aliyun.tair.tairts;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.jedis3.Jedis3BuilderFactory;
import com.aliyun.tair.tairts.factory.TsBuilderFactory;
import com.aliyun.tair.tairts.params.*;
import com.aliyun.tair.tairts.results.ExtsDataPointResult;
import com.aliyun.tair.tairts.results.ExtsSkeyResult;
import com.aliyun.tair.tairts.results.ExtsStringDataPointResult;
import com.aliyun.tair.tairts.results.ExtsStringSkeyResult;
import io.valkey.BuilderFactory;
import io.valkey.JedisCluster;
import io.valkey.util.SafeEncoder;

import java.util.ArrayList;
import java.util.List;

import static io.valkey.Protocol.toByteArray;

public class TairTsCluster {
    private JedisCluster jc;

    public TairTsCluster(JedisCluster jc) {
        this.jc = jc;
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
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSADD, pkey, skey, ts, String.valueOf(value));
        return BuilderFactory.STRING.build(obj);
    }

    public String extsadd(byte[] pkey, byte[] skey, byte[] ts, double value) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSADD, pkey, skey, ts, toByteArray(value));
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
     * `CHUNK_SIZE` - Set datapoints num per chunk 2~1024 (size)
     * `UNCOMPRESSED` - set the skey if compressed
     * `LABELS` - Set the skey's labels (label1 val1 label2 val2...)
     * @return Success: OK; Fail: error.
     */

    public String extsadd(String pkey, String skey, String ts, double value, ExtsAttributesParams params) {
        return extsadd(SafeEncoder.encode(pkey), SafeEncoder.encode(skey), SafeEncoder.encode(ts), value, params);
    }

    public String extsadd(byte[] pkey, byte[] skey, byte[] ts, double value, ExtsAttributesParams params) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSADD, params.getByteParams(pkey, skey, ts, toByteArray(value)));
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
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSSMADD, addList.getByteParams(pkey, skeys));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<String> extsmadd(byte[] pkey, ArrayList<ExtsDataPoint<byte[]>> skeys) {
        ExtsMaddParams addList = new ExtsMaddParams();
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSMADD, addList.getByteParams(pkey, skeys));
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
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSSMADD, params.getByteParams(pkey, skeys));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<String> extsmadd(byte[] pkey, ArrayList<ExtsDataPoint<byte[]>> skeys, ExtsAttributesParams params) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSMADD, params.getByteParams(pkey, skeys));
        return BuilderFactory.STRING_LIST.build(obj);
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
    public String extsalter(String pkey, String skey, ExtsAttributesParams params) {
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSSALTER, params.getByteParams(pkey, skey));
        return BuilderFactory.STRING.build(obj);
    }

    public String extsalter(byte[] pkey, byte[] skey, ExtsAttributesParams params) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSALTER, params.getByteParams(pkey, skey));
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
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSINCRBY, pkey, skey, ts, String.valueOf(value));
        return BuilderFactory.STRING.build(obj);
    }

    public String extsincr(byte[] pkey, byte[] skey, byte[] ts, double value) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSINCRBY, pkey, skey, ts, toByteArray(value));
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
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSSINCRBY, params.getByteParams(pkey, skey, ts, String.valueOf(value)));
        return BuilderFactory.STRING.build(obj);
    }

    public String extsincr(byte[] pkey, byte[] skey, byte[] ts, double value, ExtsAttributesParams params) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSINCRBY, params.getByteParams(pkey, skey, ts, toByteArray(value)));
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
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSSMINCRBY, addList.getByteParams(pkey, skeys));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<String> extsmincr(byte[] pkey, ArrayList<ExtsDataPoint<byte[]>> skeys) {
        ExtsMaddParams addList = new ExtsMaddParams();
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSMINCRBY, addList.getByteParams(pkey, skeys));
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
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSSMINCRBY, params.getByteParams(pkey, skeys));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<String> extsmincr(byte[] pkey, ArrayList<ExtsDataPoint<byte[]>> skeys, ExtsAttributesParams params) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSMINCRBY, params.getByteParams(pkey, skeys));
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
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSDEL, pkey, skey);
        return BuilderFactory.STRING.build(obj);
    }

    public String extsdel(byte[] pkey, byte[] skey) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSDEL, pkey, skey);
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
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSGET, pkey, skey);
        return TsBuilderFactory.EXTSGET_RESULT_STRING.build(obj);
    }

    public ExtsDataPointResult extsget(byte[] pkey, byte[] skey) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSGET, pkey, skey);
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
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSSQUERYINDEX, addList.getByteParams(pkey, filters));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<byte[]> extsquery(byte[] pkey, ArrayList<ExtsFilter<byte[]>> filters) {
        ExtsQueryParams addList = new ExtsQueryParams();
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSQUERYINDEX, addList.getByteParams(pkey, filters));
        return Jedis3BuilderFactory.BYTE_ARRAY_LIST.build(obj);
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
    public ExtsSkeyResult extsrange(String pkey, String skey, String startTs, String endTs) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSRANGE, pkey, skey, startTs, endTs);
        return TsBuilderFactory.EXTSRANGE_RESULT_STRING.build(obj);
    }

    public ExtsSkeyResult extsrange(byte[] pkey, byte[] skey, byte[] startTs, byte[] endTs) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSRANGE, pkey, skey, startTs, endTs);
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
     * `REVERSE` - reverse output.
     * `aggregationType` - aggregation type MIN, MAX, SUM, AVG, STDP, STDS, COUNT, FIRST, LAST, RANGE.
     * `timeBucket` - set the timeBucket of the aggregation.
     * @return Success: OK; Fail: error.
     */
    public ExtsSkeyResult extsrange(String pkey, String skey, String startTs, String endTs, ExtsAggregationParams params) {
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSSRANGE, params.getByteRangeParams(pkey, skey, startTs, endTs));
        return TsBuilderFactory.EXTSRANGE_RESULT_STRING.build(obj);
    }

    public ExtsSkeyResult extsrange(byte[] pkey, byte[] skey, byte[] startTs, byte[] endTs, ExtsAggregationParams params) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSRANGE, params.getByteRangeParams(pkey, skey, startTs, endTs));
        return TsBuilderFactory.EXTSRANGE_RESULT_STRING.build(obj);
    }

    /**
     * Range one skey for the pkey.
     *
     * @param pkey   the pkey
     * @param skeys   the skeys
     * @param startTs   the start ts
     * @param endTs   the end ts
     * @return Success: OK; Fail: error.
     */
    public List<ExtsSkeyResult> extsmrange(String pkey, ArrayList<String> skeys, String startTs, String endTs) {
        ExtsSpecifiedKeysParams params = new ExtsSpecifiedKeysParams();
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSSRANGESPECIFIEDKEYS, params.getByteParams(pkey, skeys, startTs, endTs));
        return TsBuilderFactory.EXTSMRANGE_RESULT_STRING.build(obj);
    }

    public List<ExtsSkeyResult> extsmrange(byte[] pkey, ArrayList<byte[]> skeys, byte[] startTs, byte[] endTs) {
        ExtsSpecifiedKeysParams params = new ExtsSpecifiedKeysParams();
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSRANGESPECIFIEDKEYS, params.getByteParams(pkey, skeys, startTs, endTs));
        return TsBuilderFactory.EXTSMRANGE_RESULT_STRING.build(obj);
    }

    /**
     * Range one skey for the pkey.
     *
     * @param pkey   the pkey
     * @param skeys   the skeys
     * @param startTs   the start ts
     * @param endTs   the end ts
     * @param params the aggregation params: [MAXCOUNT count] [aggregationType timeBucket]
     * `MAXCOUNT` - Set the maxcount for output
     * `REVERSE` - reverse output.
     * `aggregationType` - aggregation type MIN, MAX, SUM, AVG, STDP, STDS, COUNT, FIRST, LAST, RANGE.
     * `timeBucket` - set the timeBucket of the aggregation.
     * @return Success: OK; Fail: error.
     */
    public List<ExtsSkeyResult> extsmrange(String pkey, ArrayList<String> skeys, String startTs, String endTs, ExtsAggregationParams params) {
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSSRANGESPECIFIEDKEYS, params.getByteRangeParams(pkey, skeys, startTs, endTs));
        return TsBuilderFactory.EXTSMRANGE_RESULT_STRING.build(obj);
    }

    public List<ExtsSkeyResult> extsmrange(byte[] pkey, ArrayList<byte[]> skeys, byte[] startTs, byte[] endTs, ExtsAggregationParams params) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSRANGESPECIFIEDKEYS, params.getByteRangeParams(pkey, skeys, startTs, endTs));
        return TsBuilderFactory.EXTSMRANGE_RESULT_STRING.build(obj);
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
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSSMRANGE, params.getByteMrangeParams(pkey, startTs, endTs, filters));
        return TsBuilderFactory.EXTSMRANGE_RESULT_STRING.build(obj);
    }

    public List<ExtsSkeyResult> extsmrange(byte[] pkey, byte[] startTs, byte[] endTs, ArrayList<ExtsFilter<byte[]>> filters) {
        ExtsAggregationParams params = new ExtsAggregationParams();
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSMRANGE, params.getByteMrangeParams(pkey, startTs, endTs, filters));
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
     * `REVERSE` - reverse output.
     * @return Success: OK; Fail: error.
     */
    public List<ExtsSkeyResult> extsmrange(String pkey, String startTs, String endTs, ExtsAggregationParams params, ArrayList<ExtsFilter<String>> filters) {
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSSMRANGE, params.getByteMrangeParams(pkey, startTs, endTs, filters));
        return TsBuilderFactory.EXTSMRANGE_RESULT_STRING.build(obj);
    }

    public List<ExtsSkeyResult> extsmrange(byte[] pkey, byte[] startTs, byte[] endTs, ExtsAggregationParams params, ArrayList<ExtsFilter<byte[]>> filters) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSMRANGE, params.getByteMrangeParams(pkey, startTs, endTs, filters));
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
    public ExtsSkeyResult extsprange(String pkey, String startTs, String endTs, String pkeyAggregationType, long pkeyTimeBucket, ArrayList<ExtsFilter<String>> filters) {
        ExtsAggregationParams params = new ExtsAggregationParams();
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSPRANGE, params.getBytePrangeParams(pkey, startTs, endTs, pkeyAggregationType, pkeyTimeBucket, filters));
        return TsBuilderFactory.EXTSRANGE_RESULT_STRING.build(obj);
    }

    public ExtsSkeyResult extsprange(byte[] pkey, byte[] startTs, byte[] endTs, byte[] pkeyAggregationType, long pkeyTimeBucket, ArrayList<ExtsFilter<byte[]>> filters) {
        ExtsAggregationParams params = new ExtsAggregationParams();
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSPRANGE, params.getBytePrangeParams(pkey, startTs, endTs, pkeyAggregationType, pkeyTimeBucket, filters));
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
     * `REVERSE` - reverse output.
     * @return Success: OK; Fail: error.
     */
    public ExtsSkeyResult extsprange(String pkey, String startTs, String endTs, String pkeyAggregationType, long pkeyTimeBucket, ExtsAggregationParams params, ArrayList<ExtsFilter<String>> filters) {
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSPRANGE, params.getBytePrangeParams(pkey, startTs, endTs, pkeyAggregationType, pkeyTimeBucket, filters));
        return TsBuilderFactory.EXTSRANGE_RESULT_STRING.build(obj);
    }

    public ExtsSkeyResult extsprange(byte[] pkey, byte[] startTs, byte[] endTs, byte[] pkeyAggregationType, long pkeyTimeBucket, ExtsAggregationParams params, ArrayList<ExtsFilter<byte[]>> filters) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSPRANGE, params.getBytePrangeParams(pkey, startTs, endTs, pkeyAggregationType, pkeyTimeBucket, filters));
        return TsBuilderFactory.EXTSRANGE_RESULT_STRING.build(obj);
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
    public String extsrawmodify(String pkey, String skey, String ts, double value) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSRAWMODIFY, pkey, skey, ts, String.valueOf(value));
        return BuilderFactory.STRING.build(obj);
    }

    public String extsrawmodify(byte[] pkey, byte[] skey, byte[] ts, double value) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSRAWMODIFY, pkey, skey, ts, toByteArray(value));
        return BuilderFactory.STRING.build(obj);
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
    public String extsrawmodify(String pkey, String skey, String ts, double value, ExtsAttributesParams params) {
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSSRAWMODIFY, params.getByteParams(pkey, skey, ts, String.valueOf(value)));
        return BuilderFactory.STRING.build(obj);
    }

    public String extsrawmodify(byte[] pkey, byte[] skey, byte[] ts, double value, ExtsAttributesParams params) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSRAWMODIFY, params.getByteParams(pkey, skey, ts, toByteArray(value)));
        return BuilderFactory.STRING.build(obj);
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
    public List<String> extsmrawmodify(String pkey, ArrayList<ExtsDataPoint<String>> skeys) {
        ExtsMaddParams addList = new ExtsMaddParams();
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSSRAWMULTIMODIFY, addList.getByteParams(pkey, skeys));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<String> extsmrawmodify(byte[] pkey, ArrayList<ExtsDataPoint<byte[]>> skeys) {
        ExtsMaddParams addList = new ExtsMaddParams();
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSRAWMULTIMODIFY, addList.getByteParams(pkey, skeys));
        return BuilderFactory.STRING_LIST.build(obj);
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
    public List<String> extsmrawmodify(String pkey, ArrayList<ExtsDataPoint<String>> skeys, ExtsAttributesParams params) {
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSSRAWMULTIMODIFY, params.getByteParams(pkey, skeys));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<String> extsmrawmodify(byte[] pkey, ArrayList<ExtsDataPoint<byte[]>> skeys, ExtsAttributesParams params) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSRAWMULTIMODIFY, params.getByteParams(pkey, skeys));
        return BuilderFactory.STRING_LIST.build(obj);
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
    public String extsrawincr(String pkey, String skey, String ts, double value) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSRAWINCRBY, pkey, skey, ts, String.valueOf(value));
        return BuilderFactory.STRING.build(obj);
    }

    public String extsrawincr(byte[] pkey, byte[] skey, byte[] ts, double value) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSRAWINCRBY, pkey, skey, ts, toByteArray(value));
        return BuilderFactory.STRING.build(obj);
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
    public String extsrawincr(String pkey, String skey, String ts, double value, ExtsAttributesParams params) {
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSSRAWINCRBY, params.getByteParams(pkey, skey, ts, String.valueOf(value)));
        return BuilderFactory.STRING.build(obj);
    }

    public String extsrawincr(byte[] pkey, byte[] skey, byte[] ts, double value, ExtsAttributesParams params) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSRAWINCRBY, params.getByteParams(pkey, skey, ts, toByteArray(value)));
        return BuilderFactory.STRING.build(obj);
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
    public List<String> extsmrawincr(String pkey, ArrayList<ExtsDataPoint<String>> skeys) {
        ExtsMaddParams addList = new ExtsMaddParams();
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSSRAWMULTIINCRBY, addList.getByteParams(pkey, skeys));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<String> extsmrawincr(byte[] pkey, ArrayList<ExtsDataPoint<byte[]>> skeys) {
        ExtsMaddParams addList = new ExtsMaddParams();
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSRAWMULTIINCRBY, addList.getByteParams(pkey, skeys));
        return BuilderFactory.STRING_LIST.build(obj);
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
    public List<String> extsmrawincr(String pkey, ArrayList<ExtsDataPoint<String>> skeys, ExtsAttributesParams params) {
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSSRAWMULTIINCRBY, params.getByteParams(pkey, skeys));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<String> extsmrawincr(byte[] pkey, ArrayList<ExtsDataPoint<byte[]>> skeys, ExtsAttributesParams params) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSRAWMULTIINCRBY, params.getByteParams(pkey, skeys));
        return BuilderFactory.STRING_LIST.build(obj);
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
    public String extsaddstr(String pkey, String skey, String ts, String value) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSADDSTR, pkey, skey, ts, value);
        return BuilderFactory.STRING.build(obj);
    }

    public String extsaddstr(byte[] pkey, byte[] skey, byte[] ts, byte[] value) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSADDSTR, pkey, skey, ts, value);
        return BuilderFactory.STRING.build(obj);
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
    public String extsaddstr(String pkey, String skey, String ts, String value, ExtsAttributesParams params) {
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSSADDSTR, params.getByteParams(pkey, skey, ts, value));
        return BuilderFactory.STRING.build(obj);
    }

    public String extsaddstr(byte[] pkey, byte[] skey, byte[] ts, byte[] value, ExtsAttributesParams params) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSADDSTR, params.getByteParams(pkey, skey, ts, value));
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Set multi ts value of multi key.
     *
     * @param pkey   the pkey
     * @param skeys   the {skey ts value}
     * @return Success: OK; Fail: error.
     */
    public List<String> extsmaddstr(String pkey, ArrayList<ExtsStringDataPoint<String>> skeys) {
        ExtsStringMaddParams addList = new ExtsStringMaddParams();
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSSMADDSTR, addList.getByteParams(pkey, skeys));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<String> extsmaddstr(byte[] pkey, ArrayList<ExtsStringDataPoint<byte[]>> skeys) {
        ExtsStringMaddParams addList = new ExtsStringMaddParams();
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSMADDSTR, addList.getByteParams(pkey, skeys));
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
    public List<String> extsmaddstr(String pkey, ArrayList<ExtsStringDataPoint<String>> skeys, ExtsAttributesParams params) {
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSSMADDSTR, params.getByteParamsStr(pkey, skeys));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<String> extsmaddstr(byte[] pkey, ArrayList<ExtsStringDataPoint<byte[]>> skeys, ExtsAttributesParams params) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSMADDSTR, params.getByteParamsStr(pkey, skeys));
        return BuilderFactory.STRING_LIST.build(obj);
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
    public String extsalterstr(String pkey, String skey, ExtsAttributesParams params) {
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSSALTERSTR, params.getByteParams(pkey, skey));
        return BuilderFactory.STRING.build(obj);
    }

    public String extsalterstr(byte[] pkey, byte[] skey, ExtsAttributesParams params) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSALTERSTR, params.getByteParams(pkey, skey));
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Del the skey.
     *
     * @param pkey   the pkey
     * @param skey   the skey
     * @return Success: OK; Fail: error.
     */
    public String extsdelstr(String pkey, String skey) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSDELSTR, pkey, skey);
        return BuilderFactory.STRING.build(obj);
    }

    public String extsdelstr(byte[] pkey, byte[] skey) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSDELSTR, pkey, skey);
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Get the skey.
     *
     * @param pkey   the pkey
     * @param skey   the skey
     * @return Success: ExtsDataPointResult; Fail: error.
     */
    public ExtsStringDataPointResult extsgetstr(String pkey, String skey) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSGETSTR, pkey, skey);
        return TsBuilderFactory.EXTSSTRING_GET_RESULT_STRING.build(obj);
    }

    public ExtsStringDataPointResult extsgetstr(byte[] pkey, byte[] skey) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSGETSTR, pkey, skey);
        return TsBuilderFactory.EXTSSTRING_GET_RESULT_STRING.build(obj);
    }

    /**
     * Query skeys for the pkey.
     *
     * @param pkey   the pkey
     * @param filters   the filters used to query skeys
     * @return Success: OK; Fail: error.
     */
    public List<String> extsquerystr(String pkey, ArrayList<ExtsFilter<String>> filters) {
        ExtsQueryParams addList = new ExtsQueryParams();
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSSQUERYINDEXSTR, addList.getByteParams(pkey, filters));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<byte[]> extsquerystr(byte[] pkey, ArrayList<ExtsFilter<byte[]>> filters) {
        ExtsQueryParams addList = new ExtsQueryParams();
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSQUERYINDEXSTR, addList.getByteParams(pkey, filters));
        return Jedis3BuilderFactory.BYTE_ARRAY_LIST.build(obj);
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
    public ExtsStringSkeyResult extsrangestr(String pkey, String skey, String startTs, String endTs) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSRANGESTR, pkey, skey, startTs, endTs);
        return TsBuilderFactory.EXTSSTRING_RANGE_RESULT_STRING.build(obj);
    }

    public ExtsStringSkeyResult extsrangestr(byte[] pkey, byte[] skey, byte[] startTs, byte[] endTs) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSRANGESTR, pkey, skey, startTs, endTs);
        return TsBuilderFactory.EXTSSTRING_RANGE_RESULT_STRING.build(obj);
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
    public ExtsStringSkeyResult extsrangestr(String pkey, String skey, String startTs, String endTs, ExtsStringAggregationParams params) {
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSSRANGESTR, params.getByteRangeParams(pkey, skey, startTs, endTs));
        return TsBuilderFactory.EXTSSTRING_RANGE_RESULT_STRING.build(obj);
    }

    public ExtsStringSkeyResult extsrangestr(byte[] pkey, byte[] skey, byte[] startTs, byte[] endTs, ExtsStringAggregationParams params) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSRANGESTR, params.getByteRangeParams(pkey, skey, startTs, endTs));
        return TsBuilderFactory.EXTSSTRING_RANGE_RESULT_STRING.build(obj);
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
    public List<ExtsStringSkeyResult> extsmrangestr(String pkey, String startTs, String endTs, ArrayList<ExtsFilter<String>> filters) {
        ExtsStringAggregationParams params = new ExtsStringAggregationParams();
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSSMRANGESTR, params.getByteMrangeParams(pkey, startTs, endTs, filters));
        return TsBuilderFactory.EXTSSTRING_MRANGE_RESULT_STRING.build(obj);
    }

    public List<ExtsStringSkeyResult> extsmrangestr(byte[] pkey, byte[] startTs, byte[] endTs, ArrayList<ExtsFilter<byte[]>> filters) {
        ExtsStringAggregationParams params = new ExtsStringAggregationParams();
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSMRANGESTR, params.getByteMrangeParams(pkey, startTs, endTs, filters));
        return TsBuilderFactory.EXTSSTRING_MRANGE_RESULT_STRING.build(obj);
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
    public List<ExtsStringSkeyResult> extsmrangestr(String pkey, String startTs, String endTs, ExtsStringAggregationParams params, ArrayList<ExtsFilter<String>> filters) {
        Object obj = jc.sendCommand(SafeEncoder.encode(pkey), ModuleCommand.TSSMRANGESTR, params.getByteMrangeParams(pkey, startTs, endTs, filters));
        return TsBuilderFactory.EXTSSTRING_MRANGE_RESULT_STRING.build(obj);
    }

    public List<ExtsStringSkeyResult> extsmrangestr(byte[] pkey, byte[] startTs, byte[] endTs, ExtsStringAggregationParams params, ArrayList<ExtsFilter<byte[]>> filters) {
        Object obj = jc.sendCommand(pkey, ModuleCommand.TSSMRANGESTR, params.getByteMrangeParams(pkey, startTs, endTs, filters));
        return TsBuilderFactory.EXTSSTRING_MRANGE_RESULT_STRING.build(obj);
    }

}
