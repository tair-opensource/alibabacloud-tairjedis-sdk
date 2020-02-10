package com.aliyun.tair.tairbloom.params;

import redis.clients.jedis.params.Params;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;

public class BfmexistParams extends Params {

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
