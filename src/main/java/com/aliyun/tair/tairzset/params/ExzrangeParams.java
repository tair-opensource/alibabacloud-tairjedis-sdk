package com.aliyun.tair.tairzset.params;

import java.util.ArrayList;

import com.aliyun.tair.jedis3.Params;
import redis.clients.jedis.util.SafeEncoder;

import static redis.clients.jedis.Protocol.toByteArray;

public class ExzrangeParams extends Params {
    private static final String WITHSCORES = "withscores";
    private static final String LIMIT = "limit";

    private long offset;
    private long count;

    public ExzrangeParams() {}

    public static ExzrangeParams ZRangeParams() {
        return new ExzrangeParams();
    }

    public ExzrangeParams limit(final long offset, final long count) {
        addParam(LIMIT);
        this.offset = offset;
        this.count = count;
        return this;
    }

    public ExzrangeParams withscores() {
        addParam(WITHSCORES);
        return this;
    }

    public byte[][] getByteParams(byte[] key, byte[]... args) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();
        byteParams.add(key);

        for (byte[] arg : args) {
            byteParams.add(arg);
        }

        if (contains(WITHSCORES)) {
            byteParams.add(SafeEncoder.encode(WITHSCORES));
        }
        if (contains(LIMIT)) {
            byteParams.add(SafeEncoder.encode(LIMIT));
            byteParams.add(toByteArray(offset));
            byteParams.add(toByteArray(count));
        }

        return byteParams.toArray(new byte[byteParams.size()][]);
    }
}
