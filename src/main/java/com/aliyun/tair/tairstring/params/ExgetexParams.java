package com.aliyun.tair.tairstring.params;

import redis.clients.jedis.params.Params;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;

public class ExgetexParams extends Params {

    private static final String PX = "px";
    private static final String EX = "ex";
    private static final String EXAT = "exat";
    private static final String PXAT = "pxat";

    private static final String KEEPTTL = "keepttl";

    /**
     * Set the specified expire time, in seconds.
     * @param secondsToExpire
     * @return SetParams
     */
    public ExgetexParams ex(int secondsToExpire) {
        addParam(EX, secondsToExpire);
        return this;
    }

    /**
     * Set the specified expire time, in milliseconds.
     * @param millisecondsToExpire
     * @return SetParams
     */
    public ExgetexParams px(long millisecondsToExpire) {
        addParam(PX, millisecondsToExpire);
        return this;
    }

    /**
     * Set the specified absolute expire time, in seconds.
     * @param secondsToExpire
     * @return SetParams
     */
    public ExgetexParams exat(int secondsToExpire) {
        addParam(EXAT, secondsToExpire);
        return this;
    }

    /**
     * Set the specified absolute expire time, in milliseconds.
     * @param millisecondsToExpire
     * @return SetParams
     */
    public ExgetexParams pxat(long millisecondsToExpire) {
        addParam(PXAT, millisecondsToExpire);
        return this;
    }

    public ExgetexParams keepttl() {
        addParam(KEEPTTL);
        return this;
    }

    private void addParamWithValue(ArrayList<byte[]> byteParams, String option) {
        if (contains(option)) {
            byteParams.add(SafeEncoder.encode(option));
            byteParams.add(SafeEncoder.encode(String.valueOf((Object)getParam(option))));
        }
    }

    public byte[][] getByteParams(String... args) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();
        for (String arg : args) {
            byteParams.add(SafeEncoder.encode(arg));
        }

        addParamWithValue(byteParams, EX);
        addParamWithValue(byteParams, PX);
        addParamWithValue(byteParams, EXAT);
        addParamWithValue(byteParams, PXAT);

        if (contains(KEEPTTL)) {
            byteParams.add(SafeEncoder.encode(KEEPTTL));
        }

        return byteParams.toArray(new byte[byteParams.size()][]);
    }

    public byte[][] getByteParams(byte[]... args) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();
        for (byte[] arg : args) {
            byteParams.add(arg);
        }

        addParamWithValue(byteParams, EX);
        addParamWithValue(byteParams, PX);
        addParamWithValue(byteParams, EXAT);
        addParamWithValue(byteParams, PXAT);

        if (contains(KEEPTTL)) {
            byteParams.add(SafeEncoder.encode(KEEPTTL));
        }

        return byteParams.toArray(new byte[byteParams.size()][]);
    }
}

