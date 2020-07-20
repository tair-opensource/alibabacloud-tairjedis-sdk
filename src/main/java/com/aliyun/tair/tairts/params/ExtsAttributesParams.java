package com.aliyun.tair.tairts.params;

import redis.clients.jedis.params.Params;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;

import static redis.clients.jedis.Protocol.toByteArray;

public class ExtsAttributesParams extends Params {

    private static final String UNCOMPRESSED = "UNCOMPRESSED";
    private static final String DATA_ET = "DATA_ET";
    private static final String CHUNK_SIZE = "CHUNK_SIZE";
    private static final String LABELS = "LABELS";

    /**
     * set the skey if compressed.
     * @return SetParams
     */
    public ExtsAttributesParams uncompressed() {
        addParam(UNCOMPRESSED);
        return this;
    }

    /**
     * Set the specified expire time, in milliseconds.
     * @param millisecondsToExpire
     * @return SetParams
     */
    public ExtsAttributesParams dataEt(long millisecondsToExpire) {
        addParam(DATA_ET, millisecondsToExpire);
        return this;
    }

    /**
     * Set the skey's chunk size: 256~1024.
     * @param maxDataPointsPerChunk
     * @return SetParams
     */
    public ExtsAttributesParams chunkSize(long maxDataPointsPerChunk) {
        addParam(CHUNK_SIZE, maxDataPointsPerChunk);
        return this;
    }

    /**
     * Set the skey's labels.
     * @param labels
     * @return SetParams
     */
    public ExtsAttributesParams labels(ArrayList<String> labels) {
        addParam(LABELS, labels);
        return this;
    }

    private void addParamWithValue(ArrayList<byte[]> byteParams, String option) {
        if (contains(option)) {
            byteParams.add(SafeEncoder.encode(option));
            byteParams.add(SafeEncoder.encode(String.valueOf((Object)getParam(option))));
        }
    }

    private void addParamWithLabel(ArrayList<byte[]> byteParams, String option) {
        if (contains(option)) {
            byteParams.add(SafeEncoder.encode(option));
            ArrayList<String> labels = (ArrayList<String>)getParam(option);
            for (String label : labels) {
                byteParams.add(SafeEncoder.encode(label));
            }
        }
    }

    public byte[][] getByteParams(String... args) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();
        for (String arg : args) {
            byteParams.add(SafeEncoder.encode(arg));
        }

        if (contains(UNCOMPRESSED)) {
            byteParams.add(SafeEncoder.encode(UNCOMPRESSED));
        }

        addParamWithValue(byteParams, DATA_ET);
        addParamWithValue(byteParams, CHUNK_SIZE);
        addParamWithLabel(byteParams, LABELS);

        return byteParams.toArray(new byte[byteParams.size()][]);
    }

    public byte[][] getByteParams(byte[]... args) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();
        for (byte[] arg : args) {
            byteParams.add(arg);
        }

        if (contains(UNCOMPRESSED)) {
            byteParams.add(SafeEncoder.encode(UNCOMPRESSED));
        }

        addParamWithValue(byteParams, DATA_ET);
        addParamWithValue(byteParams, CHUNK_SIZE);
        addParamWithLabel(byteParams, LABELS);

        return byteParams.toArray(new byte[byteParams.size()][]);
    }

    public byte[][] getByteParams(String pkey, long skeyNum, ArrayList<ExtsDataPoint<String>> args) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();
        byteParams.add(SafeEncoder.encode(pkey));
        byteParams.add(SafeEncoder.encode(String.valueOf(skeyNum)));
        for (ExtsDataPoint<String> arg : args) {
            byteParams.add(SafeEncoder.encode(arg.getSkey()));
            byteParams.add(SafeEncoder.encode(arg.getTs()));
            byteParams.add(SafeEncoder.encode(String.valueOf(arg.getValue())));
        }

        if (contains(UNCOMPRESSED)) {
            byteParams.add(SafeEncoder.encode(UNCOMPRESSED));
        }

        addParamWithValue(byteParams, DATA_ET);
        addParamWithValue(byteParams, CHUNK_SIZE);
        addParamWithLabel(byteParams, LABELS);

        return byteParams.toArray(new byte[byteParams.size()][]);
    }

    public byte[][] getByteParams(byte[] pkey, long skeyNum, ArrayList<ExtsDataPoint<byte[]>> args) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();
        byteParams.add(pkey);
        byteParams.add(toByteArray(skeyNum));
        for (ExtsDataPoint<byte[]> arg : args) {
            byteParams.add(arg.getSkey());
            byteParams.add(arg.getTs());
            byteParams.add(toByteArray(arg.getValue()));
        }

        if (contains(UNCOMPRESSED)) {
            byteParams.add(SafeEncoder.encode(UNCOMPRESSED));
        }

        addParamWithValue(byteParams, DATA_ET);
        addParamWithValue(byteParams, CHUNK_SIZE);
        addParamWithLabel(byteParams, LABELS);

        return byteParams.toArray(new byte[byteParams.size()][]);
    }
}
