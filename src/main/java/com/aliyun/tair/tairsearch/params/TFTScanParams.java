package com.aliyun.tair.tairsearch.params;

import redis.clients.jedis.Protocol;
import redis.clients.jedis.util.SafeEncoder;

import java.nio.ByteBuffer;
import java.util.*;

import static redis.clients.jedis.Protocol.Keyword.COUNT;
import static redis.clients.jedis.Protocol.Keyword.MATCH;

public class TFTScanParams {
    public static final String SCAN_POINTER_START = String.valueOf(0);
    public static final byte[] SCAN_POINTER_START_BINARY = SafeEncoder.encode(SCAN_POINTER_START);
    private final Map<Protocol.Keyword, ByteBuffer> params = new EnumMap<>(Protocol.Keyword.class);

    public TFTScanParams match(final byte[] pattern) {
        params.put(MATCH, ByteBuffer.wrap(pattern));
        return this;
    }

    public TFTScanParams match(final String pattern) {
        params.put(MATCH, ByteBuffer.wrap(SafeEncoder.encode(pattern)));
        return this;
    }

    public TFTScanParams count(final Integer count) {
        params.put(COUNT, ByteBuffer.wrap(Protocol.toByteArray(count)));
        return this;
    }

    public Collection<byte[]> getParams() {
        List<byte[]> paramsList = new ArrayList<>(params.size());
        for (Map.Entry<Protocol.Keyword, ByteBuffer> param : params.entrySet()) {
            paramsList.add(param.getKey().raw);
            paramsList.add(param.getValue().array());
        }
        return Collections.unmodifiableCollection(paramsList);
    }

    byte[] binaryMatch() {
        if (params.containsKey(MATCH)) {
            return params.get(MATCH).array();
        } else {
            return null;
        }
    }

    String match() {
        if (params.containsKey(MATCH)) {
            return new String(params.get(MATCH).array());
        } else {
            return null;
        }
    }

    Integer count() {
        if (params.containsKey(COUNT)) {
            return params.get(COUNT).getInt();
        } else {
            return null;
        }
    }
}
