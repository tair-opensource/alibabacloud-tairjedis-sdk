package com.aliyun.tair.tairvector.factory;

import redis.clients.jedis.Builder;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class VectorBuilderFactory {
    public static final String VECTOR_TAG = "VECTOR";

    public static class KnnItem<T> {
        private T id;
        private double score;

        public KnnItem(T id, double score) {
            this.id = id;
            this.score = score;
        }

        public T getId() {
            return id;
        }

        public double getScore() {
            return score;
        }

        @Override
        public String toString() {
            return "id =" + id + ", score =" + score + ";";
        }
    }

    public static class Knn<T> {
        private Collection<KnnItem<T>> knnItems = new ArrayList<>();

        public void add(KnnItem item) {
            knnItems.add(item);
        }

        public Collection<KnnItem<T>> getKnnResults() {
            return knnItems;
        }

        @Override
        public String toString() {
            return knnItems.toString();
        }
    }

    public static final Builder<Knn<String>> STRING_KNN_RESULT = new Builder<Knn<String>>() {
        @Override
        @SuppressWarnings("unchecked")
        public Knn<String> build(Object data) {
            final Iterator<byte[]> iterator = ((List<byte[]>) data).iterator();
            final Knn<String> results = new Knn<String>();
            while (iterator.hasNext()) {
                results.add(new KnnItem<>(SafeEncoder.encode(iterator.next()), Double.valueOf(SafeEncoder.encode(iterator.next()))));
            }
            return results;
        }

        @Override
        public String toString() {
            return "Knn<String>";
        }
    };

    public static final Builder<Knn<byte[]>> BYTE_KNN_RESULT = new Builder<Knn<byte[]>>() {
        @Override
        @SuppressWarnings("unchecked")
        public Knn<byte[]> build(Object data) {
            final Iterator<byte[]> iterator = ((List<byte[]>) data).iterator();
            final Knn<byte[]> results = new Knn<byte[]>();
            while (iterator.hasNext()) {
                results.add(new KnnItem<>(iterator.next(), Double.valueOf(SafeEncoder.encode(iterator.next()))));
            }
            return results;
        }

        @Override
        public String toString() {
            return "Knn<Byte[]>";
        }
    };

    public static final Builder<Collection<Knn<String>>> STRING_KNN_BATCH_RESULT = new Builder<Collection<Knn<String>>>() {
        @Override
        @SuppressWarnings("unchecked")
        public Collection<Knn<String>> build(Object data) {
            final Collection<Knn<String>> results = new ArrayList<>();
            List<Object> resp = (List<Object>) data;
            resp.forEach(knn -> {
                results.add(STRING_KNN_RESULT.build(knn));
            });
            return results;
        }

        @Override
        public String toString() {
            return "Collection<Knn<String>>";
        }
    };

    public static final Builder<Collection<Knn<byte[]>>> BYTE_KNN_BATCH_RESULT = new Builder<Collection<Knn<byte[]>>>() {
        @Override
        @SuppressWarnings("unchecked")
        public Collection<Knn<byte[]>> build(Object data) {
            final Collection<Knn<byte[]>> results = new ArrayList<>();
            List<Object> resp = (List<Object>) data;
            resp.forEach(knn -> {
                results.add(BYTE_KNN_RESULT.build(knn));
            });
            return results;
        }

        @Override
        public String toString() {
            return "Collection<Knn<String>>";
        }
    };

    public static class KnnFieldItem<T> {
        private T id;
        private double score;
        private T index;

        private Map<T, T> fields;
        public KnnFieldItem(T id, double score, T index, Map<T, T> fields) {
            this.id = id;
            this.score = score;
            this.index = index;
            this.fields = fields;
        }
        public T getId() {
            return id;
        }
        public double getScore() {
            return score;
        }

        public T getIndex() {
            return index;
        }

        public Map<T, T> getFields() {
            return fields;
        }

        @Override
        public String toString() {
            return "id =" + id + ", score =" + score + ", index=" + index + ", fields=" + fields + ";";
        }
    }

    public static class KnnField<T> {
        private Collection<KnnFieldItem<T>> knnFieldItems = new ArrayList<>();

        public void add(KnnFieldItem item) {
            knnFieldItems.add(item);
        }

        public Collection<KnnFieldItem<T>> getKnnResults() {
            return knnFieldItems;
        }

        @Override
        public String toString() {
            return knnFieldItems.toString();
        }
    }

    public static final Builder<KnnField<String>> STRING_KNNFIELD_RESULT = new Builder<KnnField<String>>() {
        @Override
        @SuppressWarnings("unchecked")
        public KnnField<String> build(Object data) {
            final KnnField<String> results = new KnnField<>();
            List<Object> resp = (List<Object>) data;
            resp.forEach(knnField -> {
                final List<byte[]> knnFieldByte = (List<byte[]>) knnField;
                final Iterator<byte[]> iterator = knnFieldByte.iterator();
                String id = SafeEncoder.encode(iterator.next());
                double score = Double.parseDouble(SafeEncoder.encode(iterator.next()));
                String index = knnFieldByte.size() % 2 == 0 ? "" : SafeEncoder.encode(iterator.next());
                Map<String, String> fields = new TreeMap<>();
                while (iterator.hasNext()) {
                    fields.put(SafeEncoder.encode(iterator.next()), SafeEncoder.encode(iterator.next()));
                }
                results.add(new KnnFieldItem<>(id, score, index, fields));
            });
            return results;
        }

        @Override
        public String toString() {
            return "KnnField<String>";
        }
    };

    static final Comparator<byte[]> byteArrComparator = new Comparator<byte[]>() {
        @Override
        public int compare(byte[] a, byte[] b) {
            int minLength = Math.min(a.length, b.length);
            for (int i = 0; i < minLength; i++) {
                int aByte = a[i] & 0xFF; // 将byte转换成无符号的int进行比较
                int bByte = b[i] & 0xFF;
                if (aByte != bByte) {
                    return aByte - bByte;
                }
            }
            // 如果前面都相同，那就比较数组长度
            return a.length - b.length;
        }
    };

    public static final Builder<KnnField<byte[]>> BYTE_KNNFIELD_RESULT = new Builder<KnnField<byte[]>>() {
        @Override
        @SuppressWarnings("unchecked")
        public KnnField<byte[]> build(Object data) {
            final KnnField<byte[]> results = new KnnField<>();
            List<Object> resp = (List<Object>) data;
            resp.forEach(knnField -> {
                final List<byte[]> knnFieldByte = (List<byte[]>) knnField;
                final Iterator<byte[]> iterator = knnFieldByte.iterator();
                byte[] id = iterator.next();
                double score = Double.parseDouble(SafeEncoder.encode(iterator.next()));
                byte[] index = knnFieldByte.size() % 2 == 0 ?  SafeEncoder.encode("") : iterator.next();
                Map<byte[], byte[]> fields = new TreeMap<>(byteArrComparator);
                while (iterator.hasNext()) {
                    fields.put(iterator.next(), iterator.next());
                }
                results.add(new KnnFieldItem<>(id, score, index, fields));
            });
            return results;
        }

        @Override
        public String toString() {
            return "KnnField<Byte[]>";
        }
    };


    public static final Builder<ScanResult<String>> SCAN_CURSOR_STRING = new Builder<ScanResult<String>>() {
        @Override
        @SuppressWarnings("unchecked")
        public ScanResult<String> build(Object data) {
            if (data == null) {
                return null;
            }
            List<Object> result = (List<Object>) data;
            String newcursor = new String((byte[]) result.get(0));
            List<String> results = new ArrayList<>();
            List<byte[]> rawResults = (List<byte[]>) result.get(1);
            Iterator<byte[]> iterator = rawResults.iterator();
            while (iterator.hasNext()) {
                results.add(SafeEncoder.encode(iterator.next()));
            }
            return new ScanResult<>(newcursor, results);
        }

        @Override
        public String toString() {
            return "scan_index_string";
        }
    };

    public static final Builder<ScanResult<byte[]>> SCAN_CURSOR_BYTE = new Builder<ScanResult<byte[]>>() {
        @Override
        @SuppressWarnings("unchecked")
        public ScanResult<byte[]> build(Object data) {
            if (data == null) {
                return null;
            }
            List<Object> result = (List<Object>) data;
            byte[] newcursor = (byte[]) result.get(0);
            return new ScanResult<>(newcursor, (List<byte[]>) result.get(1));
        }

        @Override
        public String toString() {
            return "scan_index_byte";
        }
    };
}
