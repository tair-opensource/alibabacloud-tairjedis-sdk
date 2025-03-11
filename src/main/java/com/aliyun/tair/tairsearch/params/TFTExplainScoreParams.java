package com.aliyun.tair.tairsearch.params;

import io.valkey.util.SafeEncoder;

import java.util.ArrayList;

public class TFTExplainScoreParams {
    public byte[][] getByteParams(String key, String request, String... docIds) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();

        byteParams.add(SafeEncoder.encode(key));
        byteParams.add(SafeEncoder.encode(request));

        for (String s : docIds) {
            byteParams.add(SafeEncoder.encode(s));
        }

        return byteParams.toArray(new byte[byteParams.size()][]);
    }

    public byte[][] getByteParams(byte[] key, byte[] request, byte[]... docIds) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();

        byteParams.add(key);
        byteParams.add(request);

        for (byte[] arg : docIds) {
            byteParams.add(arg);
        }
        return byteParams.toArray(new byte[byteParams.size()][]);
    }
}
