package com.aliyun.tair.tairts.params;

import com.aliyun.tair.jedis3.Params;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;
import java.util.Arrays;

public class ExtsStringAggregationParams extends Params {

    private static final String MAXCOUNT = "MAXCOUNT";
    private static final String WITHLABELS = "WITHLABELS";
    private static final String REVERSE = "REVERSE";
    private static final String FILTER = "FILTER";

    /**
     * Output withlabels.
     * @return SetParams
     */
    public ExtsStringAggregationParams withLabels() {
        addParam(WITHLABELS);
        return this;
    }

    /**
     * Output reverse.
     * @return SetParams
     */
    public ExtsStringAggregationParams reverse() {
        addParam(REVERSE);
        return this;
    }

    /**
     * Set the maxcount for output.
     * @param count maxcount.
     * @return SetParams
     */
    public ExtsStringAggregationParams maxCountSize(long count) {
        addParam(MAXCOUNT, count);
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

        if (contains(REVERSE)) {
            byteParams.add(SafeEncoder.encode(REVERSE));
        }

        if (contains(MAXCOUNT)) {
            addParamWithValue(byteParams, MAXCOUNT);
        }

        return byteParams.toArray(new byte[byteParams.size()][]);
    }

    public byte[][] getByteRangeParams(byte[]... args) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();
        for (byte[] arg : args) {
            byteParams.add(arg);
        }

        if (contains(REVERSE)) {
            byteParams.add(SafeEncoder.encode(REVERSE));
        }

        if (contains(MAXCOUNT)) {
            addParamWithValue(byteParams, MAXCOUNT);
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

