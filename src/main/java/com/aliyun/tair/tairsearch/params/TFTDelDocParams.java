package com.aliyun.tair.tairsearch.params;

import java.util.ArrayList;

import redis.clients.jedis.util.SafeEncoder;

public class TFTDelDocParams {
    public byte[][] getByteParams(String key, String... args) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();

        byteParams.add(SafeEncoder.encode(key));

        for (String s : args) {
            byteParams.add(SafeEncoder.encode(s));
        }

        return byteParams.toArray(new byte[byteParams.size()][]);
    }

    public byte[][] getByteParams(byte[] key, byte[]... args) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();

        byteParams.add(key);

        for (byte[] arg : args) {
            byteParams.add(arg);
        }
        return byteParams.toArray(new byte[byteParams.size()][]);
    }
}
