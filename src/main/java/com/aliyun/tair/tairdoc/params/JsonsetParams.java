package com.aliyun.tair.tairdoc.params;

import java.util.ArrayList;

import redis.clients.jedis.params.Params;
import redis.clients.jedis.util.SafeEncoder;

public class JsonsetParams extends Params {
    private static final String XX = "xx";
    private static final String NX = "nx";

    public static JsonsetParams JsonsetParams() {
        return new JsonsetParams();
    }

    /**
     * Only set the key if it already exist.
     *
     * @return SetParams
     */
    public JsonsetParams xx() {
        addParam(XX);
        return this;
    }

    /**
     * Only set the key if it does not already exist.
     *
     * @return SetParams
     */
    public JsonsetParams nx() {
        addParam(NX);
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

        if (contains(XX)) {
            byteParams.add(SafeEncoder.encode(XX));
        }
        if (contains(NX)) {
            byteParams.add(SafeEncoder.encode(NX));
        }

        return byteParams.toArray(new byte[byteParams.size()][]);
    }
}
