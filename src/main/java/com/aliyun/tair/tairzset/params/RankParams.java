package com.aliyun.tair.tairzset.params;

import java.util.ArrayList;

import com.aliyun.tair.jedis3.Params;
import io.valkey.util.SafeEncoder;

public class RankParams extends Params {
    private static final String APPROXIMATE = "APPROXIMATE";

    public RankParams() {}

    public static RankParams RankParams() {
        return new RankParams();
    }

    public RankParams approximate() {
        addParam(APPROXIMATE);
        return this;
    }

    private void addParamWithValue(ArrayList<byte[]> byteParams, String option) {
        if (contains(option)) {
            byteParams.add(SafeEncoder.encode(option));
            byteParams.add(SafeEncoder.encode(String.valueOf((Object)getParam(option))));
        }
    }

    public byte[][] getByteParams(byte[]... args) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();
        for (byte[] arg : args) {
            byteParams.add(arg);
        }

        if (contains(APPROXIMATE)) {
            byteParams.add(SafeEncoder.encode(APPROXIMATE));
        }

        return byteParams.toArray(new byte[byteParams.size()][]);
    }
}
