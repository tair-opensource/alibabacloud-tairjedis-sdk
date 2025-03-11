package com.aliyun.tair.tairts.params;

import com.aliyun.tair.jedis3.Params;
import io.valkey.util.SafeEncoder;

import java.util.ArrayList;

import static io.valkey.Protocol.toByteArray;

public class ExtsSpecifiedKeysParams extends Params {

    public byte[][] getByteParams(String pkey, ArrayList<String> args, String startTs, String endTs) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();
        byteParams.add(SafeEncoder.encode(pkey));
        byteParams.add(toByteArray(args.size()));
        for (String arg : args) {
            byteParams.add(SafeEncoder.encode(arg));
        }
        byteParams.add(SafeEncoder.encode(startTs));
        byteParams.add(SafeEncoder.encode(endTs));
        return byteParams.toArray(new byte[byteParams.size()][]);
    }

    public byte[][] getByteParams(byte[] pkey, ArrayList<byte[]> args, byte[] startTs, byte[] endTs) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();
        byteParams.add(pkey);
        byteParams.add(toByteArray(args.size()));
        for (byte[] arg : args) {
            byteParams.add(arg);
        }
        byteParams.add(startTs);
        byteParams.add(endTs);
        return byteParams.toArray(new byte[byteParams.size()][]);
    }
}