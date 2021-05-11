package com.aliyun.tair.tairhash.params;

import redis.clients.jedis.params.Params;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;
import java.util.List;

import static redis.clients.jedis.Protocol.toByteArray;

public class ExhmincrbywithoptsParams extends Params {
    private static final String MIN = "min";
    private static final String MAX = "max";

    public ExhmincrbywithoptsParams() {
    }

    public static ExhmincrbywithoptsParams ExhmincrbywithoptsParams() {
        return new ExhmincrbywithoptsParams();
    }

    public ExhmincrbywithoptsParams max(long max) {
        addParam(MAX, max);
        return this;
    }

    public ExhmincrbywithoptsParams min(long min) {
        addParam(MIN, min);
        return this;
    }

    private void addParamWithValue(ArrayList<byte[]> byteParams, String option) {
        if (contains(option)) {
            byteParams.add(SafeEncoder.encode(option));
            byteParams.add(SafeEncoder.encode(String.valueOf((Object)getParam(option))));
        }
    }

    public byte[][] getByteParams(String key, List<ExhmincrbyFields<String>> fields) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();
        byteParams.add(SafeEncoder.encode(key));
        byteParams.add(toByteArray(fields.size()));
        for (ExhmincrbyFields<String> field : fields) {
            byteParams.add(SafeEncoder.encode(field.getField()));
            byteParams.add(toByteArray(field.getValue()));
            byteParams.add(toByteArray(field.getDefaultValue()));
            byteParams.add(toByteArray(field.getExp()));
        }

        addParamWithValue(byteParams, MAX);
        addParamWithValue(byteParams, MIN);

        return byteParams.toArray(new byte[byteParams.size()][]);
    }

    public byte[][] getByteParams(byte[] key, List<ExhmincrbyFields<byte[]>> fields) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();
        byteParams.add(key);
        byteParams.add(toByteArray(fields.size()));
        for (ExhmincrbyFields<byte[]> field : fields) {
            byteParams.add(field.getField());
            byteParams.add(toByteArray(field.getValue()));
            byteParams.add(toByteArray(field.getDefaultValue()));
            byteParams.add(toByteArray(field.getExp()));
        }

        addParamWithValue(byteParams, MAX);
        addParamWithValue(byteParams, MIN);

        return byteParams.toArray(new byte[byteParams.size()][]);
    }
}
