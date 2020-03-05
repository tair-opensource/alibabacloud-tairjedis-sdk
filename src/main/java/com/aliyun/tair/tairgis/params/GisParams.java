package com.aliyun.tair.tairgis.params;

import java.util.ArrayList;

import redis.clients.jedis.params.Params;
import redis.clients.jedis.util.SafeEncoder;

public class GisParams extends Params {
    private static final String WITHOUTWKT = "withoutwkt";

    public GisParams() {}

    public static GisParams gisParams() {
        return new GisParams();
    }

    public GisParams withoutWkt() {
        addParam(WITHOUTWKT);
        return this;
    }

    public byte[][] getByteParams(byte[]... args) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();
        for (byte[] arg : args) {
            byteParams.add(arg);
        }

        if (contains(WITHOUTWKT)) {
            byteParams.add(SafeEncoder.encode(WITHOUTWKT));
        }

        return byteParams.toArray(new byte[byteParams.size()][]);
    }

}
