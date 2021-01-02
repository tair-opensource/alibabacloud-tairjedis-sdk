package com.aliyun.tair.taircpc.params;

import com.aliyun.tair.taircpc.CommonResult;
import redis.clients.jedis.params.Params;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;

public class OpAddParams extends Params {

    private static final String PX = "px";
    private static final String EX = "ex";
    private static final String EXAT = "exat";
    private static final String PXAT = "pxat";

    public OpAddParams() {}

    public static OpAddParams OpAddParams() {
        return new OpAddParams();
    }

    /**
     * Set the specified expire time, in seconds.
     *
     * @param secondsToExpire
     * @return SetParams
     */
    public OpAddParams ex(long secondsToExpire) {
        addParam(EX, secondsToExpire);
        return this;
    }

    /**
     * Set the specified expire time, in milliseconds.
     *
     * @param millisecondsToExpire
     * @return SetParams
     */
    public OpAddParams px(long millisecondsToExpire) {
        addParam(PX, millisecondsToExpire);
        return this;
    }

    /**
     * Set the specified absolute expire time, in seconds.
     *
     * @param secondsToExpire
     * @return SetParams
     */
    public OpAddParams exat(long secondsToExpire) {
        addParam(EXAT, secondsToExpire);
        return this;
    }

    /**
     * Set the specified absolute expire time, in milliseconds.
     *
     * @param millisecondsToExpire
     * @return SetParams
     */
    public OpAddParams pxat(long millisecondsToExpire) {
        addParam(PXAT, millisecondsToExpire);
        return this;
    }

    private int addParamWithValue(ArrayList<byte[]> byteParams, String option) {
        if (contains(option)) {
            byteParams.add(SafeEncoder.encode(option));
            byteParams.add(SafeEncoder.encode(String.valueOf((Object)getParam(option))));
            return 1;
        }
        return 0;
    }

    public byte[][] getByteParams(byte[]... args) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();
        for (byte[] arg : args) {
            byteParams.add(arg);
        }

        int ex = addParamWithValue(byteParams, EX);
        int px = addParamWithValue(byteParams, PX);
        int exat = addParamWithValue(byteParams, EXAT);
        int pxat = addParamWithValue(byteParams, PXAT);
        if (ex + px + exat + pxat > 1) {
            throw new IllegalArgumentException(CommonResult.multiExpireParam);
        }

        return byteParams.toArray(new byte[byteParams.size()][]);
    }
}
