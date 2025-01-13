package com.aliyun.tair.tairts.params;

import com.aliyun.tair.jedis3.Params;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;
import java.util.Arrays;

import static redis.clients.jedis.Protocol.toByteArray;

public class ExtsAggregationParams extends Params {

    private static final String MAXCOUNT = "MAXCOUNT";
    private static final String WITHLABELS = "WITHLABELS";
    private static final String REVERSE = "REVERSE";
    private static final String FILTER = "FILTER";
    private static final String AGGREGATION = "AGGREGATION";
    private static final String MIN = "MIN";
    private static final String MAX = "MAX";
    private static final String SUM = "SUM";
    private static final String AVG = "AVG";
    private static final String STDP = "STD.P";
    private static final String STDS = "STD.S";
    private static final String COUNT = "COUNT";
    private static final String FIRST = "FIRST";
    private static final String LAST = "LAST";
    private static final String RANGE = "RANGE";

    private static final ArrayList<String> MENUS = new ArrayList<String>(Arrays.asList(
            MIN, MAX, SUM, AVG, STDP, STDS, COUNT, FIRST, LAST, RANGE));

    /**
     * Output withlabels.
     * @return SetParams
     */
    public ExtsAggregationParams withLabels() {
        addParam(WITHLABELS);
        return this;
    }

    /**
     * Output reverse.
     * @return SetParams
     */
    public ExtsAggregationParams reverse() {
        addParam(REVERSE);
        return this;
    }

    /**
     * Set the maxcount for output.
     * @param count maxcount.
     * @return SetParams
     */
    public ExtsAggregationParams maxCountSize(long count) {
        addParam(MAXCOUNT, count);
        return this;
    }

    /**
     * MIN Aggregation.
     * @param timeBucket Set the timeBucket for aggregation
     * @return SetParams
     */
    public ExtsAggregationParams aggMin(long timeBucket) {
        addParam(MIN, timeBucket);
        return this;
    }

    /**
     * MAX Aggregation.
     * @param timeBucket Set the timeBucket for aggregation
     * @return SetParams
     */
    public ExtsAggregationParams aggMax(long timeBucket) {
        addParam(MAX, timeBucket);
        return this;
    }

    /**
     * SUM Aggregation.
     * @param timeBucket Set the timeBucket for aggregation
     * @return SetParams
     */
    public ExtsAggregationParams aggSum(long timeBucket) {
        addParam(SUM, timeBucket);
        return this;
    }

    /**
     * AVG Aggregation.
     * @param timeBucket Set the timeBucket for aggregation
     * @return SetParams
     */
    public ExtsAggregationParams aggAvg(long timeBucket) {
        addParam(AVG, timeBucket);
        return this;
    }

    /**
     * STD.P Aggregation.
     * @param timeBucket Set the timeBucket for aggregation
     * @return SetParams
     */
    public ExtsAggregationParams aggStdP(long timeBucket) {
        addParam(STDP, timeBucket);
        return this;
    }

    /**
     * STD.S Aggregation.
     * @param timeBucket Set the timeBucket for aggregation
     * @return SetParams
     */
    public ExtsAggregationParams aggStdS(long timeBucket) {
        addParam(STDS, timeBucket);
        return this;
    }

    /**
     * COUNT Aggregation.
     * @param timeBucket Set the timeBucket for aggregation
     * @return SetParams
     */
    public ExtsAggregationParams aggCount(long timeBucket) {
        addParam(COUNT, timeBucket);
        return this;
    }

    /**
     * FIRST Aggregation.
     * @param timeBucket Set the timeBucket for aggregation
     * @return SetParams
     */
    public ExtsAggregationParams aggFirst(long timeBucket) {
        addParam(FIRST, timeBucket);
        return this;
    }

    /**
     * LAST Aggregation.
     * @param timeBucket Set the timeBucket for aggregation
     * @return SetParams
     */
    public ExtsAggregationParams aggLast(long timeBucket) {
        addParam(LAST, timeBucket);
        return this;
    }

    /**
     * RANGE Aggregation.
     * @param timeBucket Set the timeBucket for aggregation
     * @return SetParams
     */
    public ExtsAggregationParams aggRange(long timeBucket) {
        addParam(RANGE, timeBucket);
        return this;
    }

    private void addParamWithValue(ArrayList<byte[]> byteParams, String option) {
        if (contains(option)) {
            byteParams.add(SafeEncoder.encode(option));
            byteParams.add(SafeEncoder.encode(String.valueOf((Object)getParam(option))));
        }
    }

    public byte[][] getByteRangeParams(String... args) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();
        for (String arg : args) {
            byteParams.add(SafeEncoder.encode(arg));
        }

        if (contains(MAXCOUNT)) {
            addParamWithValue(byteParams, MAXCOUNT);
        }

        if (contains(REVERSE)) {
            byteParams.add(SafeEncoder.encode(REVERSE));
        }


        for (String menu: MENUS) {
            if (contains(menu)) {
                byteParams.add(SafeEncoder.encode(AGGREGATION));
                addParamWithValue(byteParams, menu);
                break;
            }
        }

        return byteParams.toArray(new byte[byteParams.size()][]);
    }

    public byte[][] getByteRangeParams(byte[]... args) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();
        for (byte[] arg : args) {
            byteParams.add(arg);
        }

        if (contains(MAXCOUNT)) {
            addParamWithValue(byteParams, MAXCOUNT);
        }

        if (contains(REVERSE)) {
            byteParams.add(SafeEncoder.encode(REVERSE));
        }

        for (String menu: MENUS) {
            if (contains(menu)) {
                byteParams.add(SafeEncoder.encode(AGGREGATION));
                addParamWithValue(byteParams, menu);
                break;
            }
        }
        return byteParams.toArray(new byte[byteParams.size()][]);
    }

    public byte[][] getByteRangeParams(String pkey, ArrayList<String> skeys, String startTs, String endTs) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();
        byteParams.add(SafeEncoder.encode(pkey));
        byteParams.add(toByteArray(skeys.size()));
        for (String arg : skeys) {
            byteParams.add(SafeEncoder.encode(arg));
        }
        byteParams.add(SafeEncoder.encode(startTs));
        byteParams.add(SafeEncoder.encode(endTs));

        if (contains(MAXCOUNT)) {
            addParamWithValue(byteParams, MAXCOUNT);
        }

        if (contains(WITHLABELS)) {
            byteParams.add(SafeEncoder.encode(WITHLABELS));
        }

        if (contains(REVERSE)) {
            byteParams.add(SafeEncoder.encode(REVERSE));
        }

        for (String menu: MENUS) {
            if (contains(menu)) {
                byteParams.add(SafeEncoder.encode(AGGREGATION));
                addParamWithValue(byteParams, menu);
                break;
            }
        }

        return byteParams.toArray(new byte[byteParams.size()][]);
    }

    public byte[][] getByteRangeParams(byte[] pkey, ArrayList<byte[]> skeys, byte[] startTs, byte[] endTs) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();
        byteParams.add(pkey);
        byteParams.add(toByteArray(skeys.size()));
        for (byte[] arg : skeys) {
            byteParams.add(arg);
        }
        byteParams.add(startTs);
        byteParams.add(endTs);

        if (contains(MAXCOUNT)) {
            addParamWithValue(byteParams, MAXCOUNT);
        }

        if (contains(WITHLABELS)) {
            byteParams.add(SafeEncoder.encode(WITHLABELS));
        }

        if (contains(REVERSE)) {
            byteParams.add(SafeEncoder.encode(REVERSE));
        }

        for (String menu: MENUS) {
            if (contains(menu)) {
                byteParams.add(SafeEncoder.encode(AGGREGATION));
                addParamWithValue(byteParams, menu);
                break;
            }
        }
        return byteParams.toArray(new byte[byteParams.size()][]);
    }

    public byte[][] getByteMrangeParams(String pkey, String startTs, String endTs, ArrayList<ExtsFilter<String>> args) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();
        byteParams.add(SafeEncoder.encode(pkey));
        byteParams.add(SafeEncoder.encode(startTs));
        byteParams.add(SafeEncoder.encode(endTs));

        if (contains(MAXCOUNT)) {
            addParamWithValue(byteParams, MAXCOUNT);
        }

        for (String menu: MENUS) {
            if (contains(menu)) {
                byteParams.add(SafeEncoder.encode(AGGREGATION));
                addParamWithValue(byteParams, menu);
                break;
            }
        }

        if (contains(WITHLABELS)) {
            byteParams.add(SafeEncoder.encode(WITHLABELS));
        }

        if (contains(REVERSE)) {
            byteParams.add(SafeEncoder.encode(REVERSE));
        }

        byteParams.add(SafeEncoder.encode(FILTER));
        for (ExtsFilter<String> arg : args) {
            byteParams.add(SafeEncoder.encode(arg.getFilter()));
        }
        return byteParams.toArray(new byte[byteParams.size()][]);
    }

    public byte[][] getByteMrangeParams(byte[] pkey, byte[] startTs, byte[] endTs, ArrayList<ExtsFilter<byte[]>> args) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();
        byteParams.add(pkey);
        byteParams.add(startTs);
        byteParams.add(endTs);

        if (contains(MAXCOUNT)) {
            addParamWithValue(byteParams, MAXCOUNT);
        }

        for (String menu: MENUS) {
            if (contains(menu)) {
                byteParams.add(SafeEncoder.encode(AGGREGATION));
                addParamWithValue(byteParams, menu);
                break;
            }
        }

        if (contains(WITHLABELS)) {
            byteParams.add(SafeEncoder.encode(WITHLABELS));
        }

        if (contains(REVERSE)) {
            byteParams.add(SafeEncoder.encode(REVERSE));
        }

        byteParams.add(SafeEncoder.encode(FILTER));
        for (ExtsFilter<byte[]> arg : args) {
            byteParams.add(arg.getFilter());
        }
        return byteParams.toArray(new byte[byteParams.size()][]);
    }


    public byte[][] getBytePrangeParams(String pkey, String startTs, String endTs, String pkeyAggregationType, long pkeyTimeBucket, ArrayList<ExtsFilter<String>> args) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();
        byteParams.add(SafeEncoder.encode(pkey));
        byteParams.add(SafeEncoder.encode(startTs));
        byteParams.add(SafeEncoder.encode(endTs));
        byteParams.add(SafeEncoder.encode(pkeyAggregationType));
        byteParams.add(SafeEncoder.encode(String.valueOf(pkeyTimeBucket)));

        if (contains(MAXCOUNT)) {
            addParamWithValue(byteParams, MAXCOUNT);
        }

        for (String menu: MENUS) {
            if (contains(menu)) {
                byteParams.add(SafeEncoder.encode(AGGREGATION));
                addParamWithValue(byteParams, menu);
                break;
            }
        }

        if (contains(WITHLABELS)) {
            byteParams.add(SafeEncoder.encode(WITHLABELS));
        }

        if (contains(REVERSE)) {
            byteParams.add(SafeEncoder.encode(REVERSE));
        }

        byteParams.add(SafeEncoder.encode(FILTER));
        for (ExtsFilter<String> arg : args) {
            byteParams.add(SafeEncoder.encode(arg.getFilter()));
        }
        return byteParams.toArray(new byte[byteParams.size()][]);
    }

    public byte[][] getBytePrangeParams(byte[] pkey, byte[] startTs, byte[] endTs, byte[] pkeyAggregationType, long pkeyTimeBucket, ArrayList<ExtsFilter<byte[]>> args) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();
        byteParams.add(pkey);
        byteParams.add(startTs);
        byteParams.add(endTs);
        byteParams.add(pkeyAggregationType);
        byteParams.add(SafeEncoder.encode(String.valueOf(pkeyTimeBucket)));

        if (contains(MAXCOUNT)) {
            addParamWithValue(byteParams, MAXCOUNT);
        }

        for (String menu: MENUS) {
            if (contains(menu)) {
                byteParams.add(SafeEncoder.encode(AGGREGATION));
                addParamWithValue(byteParams, menu);
                break;
            }
        }

        if (contains(WITHLABELS)) {
            byteParams.add(SafeEncoder.encode(WITHLABELS));
        }

        if (contains(REVERSE)) {
            byteParams.add(SafeEncoder.encode(REVERSE));
        }

        byteParams.add(SafeEncoder.encode(FILTER));
        for (ExtsFilter<byte[]> arg : args) {
            byteParams.add(arg.getFilter());
        }
        return byteParams.toArray(new byte[byteParams.size()][]);
    }

}
