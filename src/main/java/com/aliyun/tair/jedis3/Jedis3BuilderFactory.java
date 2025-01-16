package com.aliyun.tair.jedis3;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Builder;
import redis.clients.jedis.util.JedisByteHashMap;
import redis.clients.jedis.util.SafeEncoder;

/**
 * paste from jedis3
 */
public final class Jedis3BuilderFactory {
    public static final Builder<byte[]> BYTE_ARRAY = new Builder<byte[]>() {
        @Override
        public byte[] build(Object data) {
            return ((byte[]) data);
        }

        @Override
        public String toString() {
            return "byte[]";
        }
    };

    public static final Builder<List<byte[]>> BYTE_ARRAY_LIST = new Builder<List<byte[]>>() {
        @Override
        @SuppressWarnings("unchecked")
        public List<byte[]> build(Object data) {
            if (null == data) {
                return null;
            }
            return (List<byte[]>) data;
        }

        @Override
        public String toString() {
            return "List<byte[]>";
        }
    };

    public static final Builder<Map<byte[], byte[]>> BYTE_ARRAY_MAP = new Builder<Map<byte[], byte[]>>() {
        @Override
        @SuppressWarnings("unchecked")
        public Map<byte[], byte[]> build(Object data) {
            final List<byte[]> flatHash = (List<byte[]>) data;
            final Map<byte[], byte[]> hash = new JedisByteHashMap();
            final Iterator<byte[]> iterator = flatHash.iterator();
            while (iterator.hasNext()) {
                hash.put(iterator.next(), iterator.next());
            }

            return hash;
        }

        @Override
        public String toString() {
            return "Map<byte[], byte[]>";
        }

    };

    public static final Builder<Set<String>> STRING_ZSET = new Builder<Set<String>>() {
        @Override
        @SuppressWarnings("unchecked")
        public Set<String> build(Object data) {
            if (null == data) {
                return null;
            }
            List<byte[]> l = (List<byte[]>) data;
            final Set<String> result = new LinkedHashSet<>(l.size(), 1);
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
            return "ZSet<String>";
        }

    };

    public static final Builder<Set<byte[]>> BYTE_ARRAY_ZSET = new Builder<Set<byte[]>>() {
        @Override
        @SuppressWarnings("unchecked")
        public Set<byte[]> build(Object data) {
            if (null == data) {
                return null;
            }
            List<byte[]> l = (List<byte[]>) data;
            final Set<byte[]> result = new LinkedHashSet<>(l);
            for (final byte[] barray : l) {
                if (barray == null) {
                    result.add(null);
                } else {
                    result.add(barray);
                }
            }
            return result;
        }

        @Override
        public String toString() {
            return "ZSet<byte[]>";
        }
    };
}
