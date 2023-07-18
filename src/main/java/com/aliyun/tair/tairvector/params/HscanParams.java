package com.aliyun.tair.tairvector.params;

import redis.clients.jedis.Protocol;
import redis.clients.jedis.util.SafeEncoder;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class HscanParams {
    private final static String MATCH = "MATCH";
    private final static String COUNT = "COUNT";
    private final static String VECTOR = "VECTOR";
    private final static String MAX_DIST = "MAX_DIST";
    private final static String FILTER = "FILTER";

    private final Map<String, ByteBuffer> params = new HashMap<>();
    public static final String SCAN_POINTER_START = String.valueOf(0);
    public static final byte[] SCAN_POINTER_START_BINARY;

    public HscanParams() {
    }

    public HscanParams match(byte[] pattern) {
        this.params.put(MATCH, ByteBuffer.wrap(pattern));
        return this;
    }

    public HscanParams match(String pattern) {
        this.params.put(MATCH, ByteBuffer.wrap(SafeEncoder.encode(pattern)));
        return this;
    }

    public HscanParams count(Integer count) {
        this.params.put(COUNT, ByteBuffer.wrap(Protocol.toByteArray(count)));
        return this;
    }

    public HscanParams vector(byte[] vector) {
        this.params.put(VECTOR, ByteBuffer.wrap(vector));
        return this;
    }

    public HscanParams vector(String vector) {
        this.params.put(VECTOR, ByteBuffer.wrap(SafeEncoder.encode(vector)));
        return this;
    }

    public HscanParams max_dist(float max_dist) {
        this.params.put(MAX_DIST, ByteBuffer.wrap(Protocol.toByteArray(max_dist)));
        return this;
    }

    public HscanParams filter(String filter) {
        this.params.put(FILTER, ByteBuffer.wrap(SafeEncoder.encode(filter)));
        return this;
    }

    public Collection<byte[]> getParams() {
        List<byte[]> paramsList = new ArrayList(this.params.size());
        Iterator var2 = this.params.entrySet().iterator();

        while (var2.hasNext()) {
            Map.Entry<String, ByteBuffer> param = (Map.Entry) var2.next();
            paramsList.add(SafeEncoder.encode(param.getKey()));
            paramsList.add((param.getValue()).array());
        }

        return Collections.unmodifiableCollection(paramsList);
    }

    byte[] binaryMatch() {
        return this.params.containsKey(MATCH) ? ((ByteBuffer) this.params.get(MATCH)).array() : null;
    }

    String match() {
        return this.params.containsKey(MATCH) ? new String(((ByteBuffer) this.params.get(MATCH)).array()) : null;
    }

    Integer count() {
        return this.params.containsKey(COUNT) ? ((ByteBuffer) this.params.get(COUNT)).getInt() : null;
    }

    static {
        SCAN_POINTER_START_BINARY = SafeEncoder.encode(SCAN_POINTER_START);
    }

}
