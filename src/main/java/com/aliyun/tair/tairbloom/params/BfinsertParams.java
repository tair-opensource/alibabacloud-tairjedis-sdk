package com.aliyun.tair.tairbloom.params;

import redis.clients.jedis.params.Params;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;

public class BfinsertParams extends Params {
    private static final String CAPACITY = "CAPACITY";
    private static final String ERROR = "ERROR";
    private static final String NOCREATE = "NOCREATE";

    public BfinsertParams() {
    }

    public static BfinsertParams BfinsertParams() {
        return new BfinsertParams();
    }

    public BfinsertParams capacity(long initCapacity) {
        addParam(CAPACITY, initCapacity);
        return this;
    }

    public BfinsertParams error(long errorRate) {
        addParam(ERROR, errorRate);
        return this;
    }

    public BfinsertParams nocreate() {
        addParam(NOCREATE);
        return this;
    }

    private void addParamWithValue(ArrayList<byte[]> byteParams, String option) {
        if (contains(option)) {
            byteParams.add(SafeEncoder.encode(option));
            byteParams.add(SafeEncoder.encode(String.valueOf((Object)getParam(option))));
        }
    }

    public byte[][] getByteParamsMeta(String key, String... meta) {
        ArrayList<byte[]> byteParamsMeta = new ArrayList<byte[]>();
        byteParamsMeta.add(SafeEncoder.encode(key));
        for (String s : meta) {
            byteParamsMeta.add(SafeEncoder.encode(s));
        }

        return byteParamsMeta.toArray(new byte[byteParamsMeta.size()][]);
    }

    public byte[][] getByteParamsMeta(byte[] key, byte[]... meta) {
        ArrayList<byte[]> byteParamsMeta = new ArrayList<byte[]>();
        byteParamsMeta.add(key);
        for (byte[] bytes : meta) {
            byteParamsMeta.add(bytes);
        }

        return byteParamsMeta.toArray(new byte[byteParamsMeta.size()][]);
    }

    public byte[][] getByteParams(byte[][] meta, String... args) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();

        for (byte[] bytes : meta) {
            byteParams.add(bytes);
        }

        for (String s : args) {
            byteParams.add(SafeEncoder.encode(s));
        }

        return byteParams.toArray(new byte[byteParams.size()][]);
    }

    public byte[][] getByteParams(byte[][] meta, byte[]... args) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();

        for (byte[] bytes : meta) {
            byteParams.add(bytes);
        }

        for (byte[] arg : args) {
            byteParams.add(arg);
        }
        return byteParams.toArray(new byte[byteParams.size()][]);
    }
}
