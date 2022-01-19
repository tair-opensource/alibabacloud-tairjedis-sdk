package com.aliyun.tair.tairsearch.params;

import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;
import java.util.Map;

public class TFTAddDocParams {
    public byte[][] getByteParams(String key, Map<String, String> docs) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();

        byteParams .add(SafeEncoder.encode(key));

        for (Map.Entry<String, String> entry : docs.entrySet()) {
            byteParams.add(SafeEncoder.encode(entry.getKey()));
            byteParams.add(SafeEncoder.encode(entry.getValue()));
        }

        return byteParams.toArray(new byte[byteParams.size()][]);
    }

    public byte[][] getByteParams(byte[] key, Map<byte[], byte[]> docs) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();

        byteParams .add(key);

        for (Map.Entry<byte[], byte[]> entry : docs.entrySet()) {
            byteParams.add(entry.getKey());
            byteParams.add(entry.getValue());
        }
        return byteParams.toArray(new byte[byteParams.size()][]);
    }
}
