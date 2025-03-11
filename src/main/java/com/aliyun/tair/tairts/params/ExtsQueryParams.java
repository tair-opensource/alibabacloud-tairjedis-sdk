package com.aliyun.tair.tairts.params;

import com.aliyun.tair.jedis3.Params;
import io.valkey.util.SafeEncoder;

import java.util.ArrayList;

public class ExtsQueryParams extends Params {

    public byte[][] getByteParams(String pkey, ArrayList<ExtsFilter<String>> args) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();
        byteParams.add(SafeEncoder.encode(pkey));
        for (ExtsFilter<String> arg : args) {
            byteParams.add(SafeEncoder.encode(arg.getFilter()));
        }
        return byteParams.toArray(new byte[byteParams.size()][]);
    }

    public byte[][] getByteParams(byte[] pkey, ArrayList<ExtsFilter<byte[]>> args) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();
        byteParams.add(pkey);
        for (ExtsFilter<byte[]> arg : args) {
            byteParams.add(arg.getFilter());
        }
        return byteParams.toArray(new byte[byteParams.size()][]);
    }
}
