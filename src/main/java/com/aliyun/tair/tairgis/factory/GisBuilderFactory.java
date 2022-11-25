package com.aliyun.tair.tairgis.factory;

import com.aliyun.tair.tairgis.params.GisSearchResponse;
import redis.clients.jedis.Builder;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;
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
                return new HashMap<>();
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
                return new HashMap<>();
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

    public static final Builder<List<String>> GISSEARCH_RESULT_LIST_STRING = new Builder<List<String>>() {
        @Override
        @SuppressWarnings("unchecked")
        public List<String> build(Object data) {
            if (null == data) {
                return null;
            }

            List<byte[]> l = (List)((List<Object>)data).get(1);
            final ArrayList<String> result = new ArrayList<String>(l.size());
            for (final byte[] barray : l) {
                if (barray == null) {
                    result.add(null);
                } else {
                    result.add(SafeEncoder.encode(barray));
                }
            }
            return result;
        }

        @Override
        public String toString() {
            return "gisResult<List<String>>";
        }
    };

    public static final Builder<List<byte[]>> GISSEARCH_RESULT_BYTE_ARRAY_LIST = new Builder<List<byte[]>>() {
        @Override
        @SuppressWarnings("unchecked")
        public List<byte[]> build(Object data) {
            if (null == data) {
                return null;
            }
            return (List<byte[]>)data;
        }

        @Override
        public String toString() {
            return "gisResult<List<byte[]>>";
        }
    };

    public static final Builder<List<GisSearchResponse>> GISSEARCH_WITH_PARAMS_RESULT = new Builder<List<GisSearchResponse>>() {
        @Override
        public List<GisSearchResponse> build(Object data) {
            List<Object> objectList = (List<Object>)data;
            if (objectList == null || objectList.isEmpty()) {
                return  null;
            }

            long number = (Long) objectList.get(0);
            List<Object> rawResults = (List) objectList.get(1);
            List<GisSearchResponse> responses = new ArrayList<>();
            if ((number == 0) || (rawResults.isEmpty())) {
                return responses;
            }

            int size = rawResults.size() / (int)number;

            for (int i = 0; i < number; i++) {
                GisSearchResponse resp = new GisSearchResponse();
                resp.setField((byte[])rawResults.get(i * size));
                for (int j = i * size + 1; j < (i + 1) * size; j++) {
                    Object obj = rawResults.get(j);
                    if (canConvertToDouble(obj)) {
                        resp.setDistance(BuilderFactory.DOUBLE.build(obj));
                    } else {
                        resp.setValue((byte[])obj);
                    }
                }
                responses.add(resp);
            }
            return responses;
        }
    };

    private static boolean canConvertToDouble(Object data) {
        String string = BuilderFactory.STRING.build(data);
        if (string == null) return false;

        try {
             Double.valueOf(string);
             return true;
        } catch (NumberFormatException e) {
            if (string.equals("inf") || string.equals("+inf") || string.equals("-inf")) return true;
            return false;
        }
    }
}
