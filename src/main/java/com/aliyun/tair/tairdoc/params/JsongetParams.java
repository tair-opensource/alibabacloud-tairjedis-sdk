package com.aliyun.tair.tairdoc.params;

import java.util.ArrayList;

import redis.clients.jedis.params.Params;
import redis.clients.jedis.util.SafeEncoder;

public class JsongetParams extends Params {
    private static final String FORMAT = "format";
    private static final String ROOTNAME = "rootname";
    private static final String ARRNAME = "arrname";

    public static JsongetParams JsongetParams() {
        return new JsongetParams();
    }

    public JsongetParams format(String format) {
        addParam(FORMAT, format);
        return this;
    }

    public JsongetParams rootname(String rootname) {
        addParam(ROOTNAME, rootname);
        return this;
    }

    public JsongetParams arrname(String arrname) {
        addParam(ARRNAME, arrname);
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

        addParamWithValue(byteParams, FORMAT);
        addParamWithValue(byteParams, ROOTNAME);
        addParamWithValue(byteParams, ARRNAME);

        return byteParams.toArray(new byte[byteParams.size()][]);
    }
}
