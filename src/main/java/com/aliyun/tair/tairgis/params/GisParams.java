package com.aliyun.tair.tairgis.params;

import java.util.ArrayList;

import redis.clients.jedis.Protocol;
import redis.clients.jedis.params.Params;
import redis.clients.jedis.util.SafeEncoder;

public class GisParams extends Params {
    public static final String RADIUS = "radius";
    public static final String MEMBER = "member";

    private static final String WITHOUTWKT = "withoutwkt";
    private static final String WITHVALUE = "withvalue";
    private static final String WITHOUTVALUE = "withoutvalue";
    private static final String WITHDIST = "withdist";


    private static final String ASC = "asc";
    private static final String DESC = "desc";
    private static final String COUNT = "count";
    private static final String LIMIT = "limit";

    public GisParams() {}

    public static GisParams gisParams() {
        return new GisParams();
    }

    public GisParams withoutWkt() {
        addParam(WITHOUTWKT);
        return this;
    }

    public GisParams withValue() {
        addParam(WITHVALUE);
        return this;
    }

    public GisParams withoutValue() {
        addParam(WITHOUTVALUE);
        return this;
    }

    public GisParams withDist() {
        addParam(WITHDIST);
        return this;
    }

    public GisParams sortAscending() {
        addParam(ASC);
        return this;
    }

    public GisParams sortDescending() {
        addParam(DESC);
        return this;
    }

    public GisParams count(int count) {
        if (count > 0) {
            addParam(COUNT, count);
        }
        return this;
    }

    public GisParams limit(int limit) {
        if (limit > 0) {
            addParam(LIMIT, limit);
        }
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
        if (contains(WITHVALUE)) {
            byteParams.add(SafeEncoder.encode(WITHVALUE));
        }
        if (contains(WITHOUTVALUE)) {
            byteParams.add(SafeEncoder.encode(WITHOUTVALUE));
        }
        if (contains(WITHDIST)) {
            byteParams.add(SafeEncoder.encode(WITHDIST));
        }

        if (contains(COUNT)) {
            byteParams.add(SafeEncoder.encode(COUNT));
            byteParams.add(Protocol.toByteArray((int) getParam(COUNT)));
        }

        if (contains(LIMIT)) {
            byteParams.add(SafeEncoder.encode(LIMIT));
            byteParams.add(Protocol.toByteArray((int) getParam(LIMIT)));
        }

        if (contains(ASC)) {
            byteParams.add(SafeEncoder.encode(ASC));
        } else if (contains(DESC)) {
            byteParams.add(SafeEncoder.encode(DESC));
        }

        return byteParams.toArray(new byte[byteParams.size()][]);
    }

}
