package com.aliyun.tair.tairsearch.params;

import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;
import java.util.Map;

import static redis.clients.jedis.Protocol.toByteArray;

public class TFTMSearchParams {
    public byte[][] getByteParams(String request, String... indexes) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();

        byteParams.add(toByteArray(indexes.length));

        for (String index : indexes) {
            byteParams.add(SafeEncoder.encode(index));
        }
        byteParams.add(SafeEncoder.encode(request));

        return byteParams.toArray(new byte[byteParams.size()][]);
    }

    public byte[][] getByteParams(byte[] request, byte[]... indexes) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();

        byteParams.add(toByteArray(indexes.length));

        for (byte[] index : indexes) {
            byteParams.add(index);
        }
        byteParams.add(request);

        return byteParams.toArray(new byte[byteParams.size()][]);
    }
}
