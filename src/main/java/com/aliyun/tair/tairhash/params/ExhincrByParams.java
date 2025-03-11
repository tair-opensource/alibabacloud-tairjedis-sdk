package com.aliyun.tair.tairhash.params;

import com.aliyun.tair.jedis3.Params;
import io.valkey.util.SafeEncoder;

import java.util.ArrayList;


public class ExhincrByParams extends Params {
    private static final String EX = "ex";
    private static final String EXAT = "exat";
    private static final String PX = "px";
    private static final String PXAT = "pxat";
    private static final String VER = "ver";
    private static final String ABS = "abs";
    private static final String MIN = "min";
    private static final String MAX = "max";
    private static final String NOACTIVE = "noactive";
    private static final String DEF = "def";
    private static final String KEEPTTL = "keepttl";

    public ExhincrByParams() {
    }

    public static ExhincrByParams ExhincrByParams() {
        return new ExhincrByParams();
    }

    public ExhincrByParams def(long defValue) {
        addParam(DEF, defValue);
        return this;
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

    public ExhincrByParams ver(long version) {
        addParam(VER, version);
        return this;
    }

    public ExhincrByParams abs(long absoluteVersion) {
        addParam(ABS, absoluteVersion);
        return this;
    }

    public ExhincrByParams max(long max) {
        addParam(MAX, max);
        return this;
    }

    public ExhincrByParams min(long min) {
        addParam(MIN, min);
        return this;
    }

    public ExhincrByParams noactive( ) {
        addParam(NOACTIVE, "");
        return this;
    }

    public ExhincrByParams keepttl() {
        addParam(KEEPTTL);
        return this;
    }

    private void addParamWithValue(ArrayList<byte[]> byteParams, String option) {
        if (contains(option)) {
            byteParams.add(SafeEncoder.encode(option));
            byteParams.add(SafeEncoder.encode(String.valueOf((Object)getParam(option))));
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
        addParamWithValue(byteParams, MAX);
        addParamWithValue(byteParams, MIN);
        addParamWithValue(byteParams, VER);
        addParamWithValue(byteParams, ABS);
        addParamWithValue(byteParams, DEF);
        if(contains(NOACTIVE)){
            byteParams.add(SafeEncoder.encode(NOACTIVE));
        }
        if (contains(KEEPTTL)) {
            byteParams.add(SafeEncoder.encode(KEEPTTL));
        }
        return byteParams.toArray(new byte[byteParams.size()][]);
    }
}
