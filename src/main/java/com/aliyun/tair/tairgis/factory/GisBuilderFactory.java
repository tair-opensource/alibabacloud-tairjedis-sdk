package com.aliyun.tair.tairgis.factory;

import redis.clients.jedis.Builder;
import redis.clients.jedis.util.SafeEncoder;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class GisBuilderFactory {

    public static final Builder<Map<String, String>> GISSEARCH_RESULT_MAP_STRING = new Builder<Map<String, String>>() {
        @Override
        public Map<String, String> build(Object data) {
            List<byte[]> rawResults;
            List<Object> result = (List<Object>) data;
            if (null == result || 0 == result.size()) {
                rawResults = null;
            } else {
                rawResults = (List) result.get(1);
            }

            final List<byte[]> flatHash = (List<byte[]>) rawResults;
            final Map<String, String> hash = new HashMap<String, String>(flatHash.size()/2, 1);
            final Iterator<byte[]> iterator = flatHash.iterator();
            while (iterator.hasNext()) {
                hash.put(SafeEncoder.encode(iterator.next()), SafeEncoder.encode(iterator.next()));
            }
            return hash;
        }

        @Override
        public String toString() {
            return "gisResult";
        }
    };

    public static final Builder<Map<byte[], byte[]>> GISSEARCH_RESULT_MAP_BYTE = new Builder<Map<byte[], byte[]>>() {
        @Override
        public Map<byte[], byte[]> build(Object data) {
            List<byte[]> rawResults;
            List<Object> result = (List<Object>) data;
            if (null == result || 0 == result.size()) {
                rawResults = null;
            } else {
                rawResults = (List) result.get(1);
            }

            final List<byte[]> flatHash = (List<byte[]>) rawResults;
            final Map<byte[], byte[]> hash = new HashMap<byte[], byte[]>(flatHash.size()/2, 1);
            final Iterator<byte[]> iterator = flatHash.iterator();
            while (iterator.hasNext()) {
                hash.put(iterator.next(), iterator.next());
            }
            return hash;
        }

        @Override
        public String toString() {
            return "gisResult";
        }
    };
}
