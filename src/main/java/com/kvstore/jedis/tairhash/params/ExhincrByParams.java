package com.kvstore.jedis.tairhash.params;

import java.util.ArrayList;

import com.kvstore.jedis.tairhash.TairHash;
import redis.clients.jedis.params.Params;
import redis.clients.jedis.util.SafeEncoder;

/**
 * @author bodong.ybd
 * @date 2019/12/30
 */
public class ExhincrByParams extends Params {
    private static final String EX = "ex";
    private static final String EXAT = "exat";
    private static final String PX = "px";
    private static final String PXAT = "pxat";

    public ExhincrByParams() {
    }

    public static ExhincrByParams ExhincrByParams() {
        return new ExhincrByParams();
    }

    public ExhincrByParams ex(int secondsToExpire) {
        if (!contains(EXAT)) {
            addParam(EX, secondsToExpire);
        }
        return this;
    }

    public ExhincrByParams exat(long unixTime) {
        if (!contains(EX)) {
            addParam(EXAT, unixTime);
        }
        return this;
    }

    public ExhincrByParams px(long millisecondsToExpire) {
        if (!contains(PXAT)) {
            addParam(PX, millisecondsToExpire);
        }
        return this;
    }

    public ExhincrByParams pxat(long millisecondsToExpire) {
        if (!contains(PX)) {
            addParam(PXAT, millisecondsToExpire);
        }
        return this;
    }

    private void addParamWithValue(ArrayList<byte[]> byteParams, String option) {
        if (contains(option)) {
            byteParams.add(SafeEncoder.encode(option));
            byteParams.add(SafeEncoder.encode(String.valueOf(getParam(option))));
        }
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
        return byteParams.toArray(new byte[byteParams.size()][]);
    }
}
