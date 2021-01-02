package com.aliyun.tair.tairts.params;

import redis.clients.jedis.params.Params;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;

import static redis.clients.jedis.Protocol.toByteArray;

public class ExtsMaddParams extends Params {

    public byte[][] getByteParams(String pkey, ArrayList<ExtsDataPoint<String>> args) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();
        byteParams.add(SafeEncoder.encode(pkey));
        byteParams.add(SafeEncoder.encode(String.valueOf(args.size())));
        for (ExtsDataPoint<String> arg : args) {
            byteParams.add(SafeEncoder.encode(arg.getSkey()));
            byteParams.add(SafeEncoder.encode(arg.getTs()));
            byteParams.add(SafeEncoder.encode(String.valueOf(arg.getValue())));
        }
        return byteParams.toArray(new byte[byteParams.size()][]);
    }

    public byte[][] getByteParams(byte[] pkey, ArrayList<ExtsDataPoint<byte[]>> args) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();
        byteParams.add(pkey);
        byteParams.add(toByteArray(args.size()));
        for (ExtsDataPoint<byte[]> arg : args) {
            byteParams.add(arg.getSkey());
            byteParams.add(arg.getTs());
            byteParams.add(toByteArray(arg.getValue()));
        }
        return byteParams.toArray(new byte[byteParams.size()][]);
    }
}
