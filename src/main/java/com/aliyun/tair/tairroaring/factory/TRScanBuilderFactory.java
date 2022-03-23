package com.aliyun.tair.tairhash.factory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import redis.clients.jedis.Builder;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.util.SafeEncoder;
import static redis.clients.jedis.Protocol.toByteArray;

public class RoaringBuilderFactory {

    public static final Builder<ScanResult<Entry<Long, Long>>> TRSCAN_RESULT_LONG
        = new Builder<ScanResult<Entry<Long, Long>>>() {
        @Override
        @SuppressWarnings("unchecked")
        public ScanResult<Entry<String, String>> build(Object data) {
            if (data == null) {
                return null;
            }
            List<Object> result = (List<Object>)data;
            String newcursor = new String((byte[])result.get(0));
            List<Map.Entry<Long, Long>> results = new ArrayList<Map.Entry<Long, Long>>();
            List<byte[]> rawResults = (List<byte[]>)result.get(1);
            Iterator<byte[]> iterator = rawResults.iterator();
            while (iterator.hasNext()) {
                results.add(new AbstractMap.SimpleEntry<Long, Long>(toByteArray(iterator.next()),
                    toByteArray(iterator.next())));
            }
            return new ScanResult<Map.Entry<Long, Long>>(newcursor, results);
        }

        @Override
        public String toString() {
            return "tr.scan";
        }
    };

    public static final Builder<ScanResult<Entry<byte[], byte[]>>> TRSCAN_RESULT_BYTE
        = new Builder<ScanResult<Entry<byte[], byte[]>>>() {
        @Override
        @SuppressWarnings("unchecked")
        public ScanResult<Entry<byte[], byte[]>> build(Object data) {
            if (data == null) {
                return null;
            }
            List<Object> result = (List<Object>)data;
            byte[] newcursor = (byte[])result.get(0);
            List<Map.Entry<byte[], byte[]>> results = new ArrayList<Map.Entry<byte[], byte[]>>();
            List<byte[]> rawResults = (List<byte[]>)result.get(1);
            Iterator<byte[]> iterator = rawResults.iterator();
            while (iterator.hasNext()) {
                results.add(new AbstractMap.SimpleEntry<byte[], byte[]>(iterator.next(), iterator.next()));
            }
            return new ScanResult<Map.Entry<byte[], byte[]>>(newcursor, results);
        }

        @Override
        public String toString() {
            return "tr.scan";
        }
    };
}
