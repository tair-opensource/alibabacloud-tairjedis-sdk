package com.aliyun.tair.tairhash.factory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.aliyun.tair.jedis3.ScanResult;
import com.aliyun.tair.tairhash.params.ExhgetwithverResult;
import redis.clients.jedis.Builder;
import redis.clients.jedis.util.SafeEncoder;

public class HashBuilderFactory {

    public static final Builder<ExhgetwithverResult<String>> EXHGETWITHVER_RESULT_STRING
        = new Builder<ExhgetwithverResult<String>>() {
        @Override
        @SuppressWarnings("unchecked")
        public ExhgetwithverResult<String> build(Object data) {
            if (data == null) {
                return null;
            }
            List<Object> result = (List<Object>)data;
            if (result.size() == 0) {
                return null;
            } else {
                String value = SafeEncoder.encode((byte[])result.get(0));
                long version = (Long)result.get(1);
                return new ExhgetwithverResult<String>(value, version);
            }
        }

        @Override
        public String toString() {
            return "exhgetwithver";
        }
    };

    public static final Builder<ExhgetwithverResult<byte[]>> EXHGETWITHVER_RESULT_BYTE
        = new Builder<ExhgetwithverResult<byte[]>>() {
        @Override
        @SuppressWarnings("unchecked")
        public ExhgetwithverResult<byte[]> build(Object data) {
            if (data == null) {
                return null;
            }
            List<Object> result = (List<Object>)data;
            if (result.size() == 0) {
                return null;
            } else {
                byte[] value = (byte[])result.get(0);
                long version = (Long)result.get(1);
                return new ExhgetwithverResult<byte[]>(value, version);
            }
        }

        @Override
        public String toString() {
            return "exhgetwithver";
        }
    };

    public static final Builder<List<ExhgetwithverResult<String>>> EXHMGETWITHVER_RESULT_STRING_LIST
        = new Builder<List<ExhgetwithverResult<String>>>() {
        @Override
        @SuppressWarnings("unchecked")
        public List<ExhgetwithverResult<String>> build(Object data) {
            if (data == null) {
                return null;
            }
            List<Object> result = (List<Object>)data;
            if (result.size() == 0) {
                return null;
            } else {
                List<ExhgetwithverResult<String>> results = new ArrayList<ExhgetwithverResult<String>>();
                for (Object o : result) {
                    if (o == null) {
                        results.add(null);
                    } else {
                        List<Object> lo = (List<Object>)o;
                        String value = SafeEncoder.encode((byte[])lo.get(0));
                        long version = (Long)lo.get(1);
                        results.add(new ExhgetwithverResult<String>(value, version));
                    }
                }
                return results;
            }
        }

        @Override
        public String toString() {
            return "exhmgetwithver";
        }
    };

    public static final Builder<List<ExhgetwithverResult<byte[]>>> EXHMGETWITHVER_RESULT_BYTE_LIST
        = new Builder<List<ExhgetwithverResult<byte[]>>>() {
        @Override
        @SuppressWarnings("unchecked")
        public List<ExhgetwithverResult<byte[]>> build(Object data) {
            if (data == null) {
                return null;
            }
            List<Object> result = (List<Object>)data;
            if (result.size() == 0) {
                return null;
            } else {
                List<ExhgetwithverResult<byte[]>> results = new ArrayList<ExhgetwithverResult<byte[]>>();
                for (Object o : result) {
                    if (o == null) {
                        results.add(null);
                    } else {
                        List<Object> lo = (List<Object>)o;
                        byte[] value = (byte[])lo.get(0);
                        long version = (Long)lo.get(1);
                        results.add(new ExhgetwithverResult<byte[]>(value, version));
                    }
                }
                return results;
            }
        }

        @Override
        public String toString() {
            return "exhmgetwithver";
        }
    };

    public static final Builder<ScanResult<Entry<String, String>>> EXHSCAN_RESULT_STRING
        = new Builder<ScanResult<Entry<String, String>>>() {
        @Override
        @SuppressWarnings("unchecked")
        public ScanResult<Entry<String, String>> build(Object data) {
            if (data == null) {
                return null;
            }
            List<Object> result = (List<Object>)data;
            String newcursor = new String((byte[])result.get(0));
            List<Map.Entry<String, String>> results = new ArrayList<Map.Entry<String, String>>();
            List<byte[]> rawResults = (List<byte[]>)result.get(1);
            Iterator<byte[]> iterator = rawResults.iterator();
            while (iterator.hasNext()) {
                results.add(new AbstractMap.SimpleEntry<String, String>(SafeEncoder.encode(iterator.next()),
                    SafeEncoder.encode(iterator.next())));
            }
            return new ScanResult<Map.Entry<String, String>>(newcursor, results);
        }

        @Override
        public String toString() {
            return "exhscan";
        }
    };

    public static final Builder<ScanResult<Entry<byte[], byte[]>>> EXHSCAN_RESULT_BYTE
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
            return "exhscan";
        }
    };
}
