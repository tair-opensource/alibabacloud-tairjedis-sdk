package com.aliyun.tair.tairsearch.params;

import redis.clients.jedis.params.Params;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;


public class TFTAnalyzerParams extends Params {
    private static final String INDEX = "index";
    private static final String SHOW_TIME = "show_time";

    public TFTAnalyzerParams index(final byte[] index) {
        addParam(INDEX, index);
        return this;
    }

    public TFTAnalyzerParams index(final String index) {
        addParam(INDEX, index);
        return this;
    }

    public TFTAnalyzerParams showTime() {
        addParam(SHOW_TIME, "");
        return this;
    }

    public byte[] getByteParam(String name) {
        Object value = getParam(name);
        if (value != null) {
            if (value instanceof byte[]) {
                return (byte[])((byte[])value);
            } else {
                return SafeEncoder.encode(String.valueOf(value));
            }
        } else {
            return null;
        }
    }

    private void addParamWithValue(ArrayList<byte[]> byteParams, String option) {
        if (contains(option)) {
            byteParams.add(SafeEncoder.encode(option));
            byte[] value = getByteParam(option);
            byteParams.add(value);
        }
    }

    public byte[][] getByteParams(byte[]... args) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();
        for (byte[] arg : args) {
            byteParams.add(arg);
        }
        addParamWithValue(byteParams, INDEX);
        if (contains(SHOW_TIME)) {
            byteParams.add(SafeEncoder.encode(SHOW_TIME));
        }
        return byteParams.toArray(new byte[byteParams.size()][]);
    }
}
