package com.aliyun.tair.tairsearch.params;

import io.valkey.util.SafeEncoder;

import java.util.ArrayList;
import java.util.Map;

import static io.valkey.Protocol.toByteArray;

public class TFTAddSugParams {
    public byte[][] getByteParams(String key, Map<String, Integer> docs) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();

        byteParams.add(SafeEncoder.encode(key));

        for (Map.Entry<String, Integer> entry : docs.entrySet()) {
            byteParams.add(SafeEncoder.encode(entry.getKey()));
            byteParams.add(toByteArray(entry.getValue()));
        }

        return byteParams.toArray(new byte[byteParams.size()][]);
    }

    public byte[][] getByteParams(byte[] key, Map<byte[], Integer> docs) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();

        byteParams.add(key);

        for (Map.Entry<byte[],Integer> entry : docs.entrySet()) {
            byteParams.add(entry.getKey());
            byteParams.add(toByteArray(entry.getValue()));
        }
        return byteParams.toArray(new byte[byteParams.size()][]);
    }
}
