package com.aliyun.tair.tairvector.factory;

import redis.clients.jedis.Builder;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.util.SafeEncoder;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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

    public static final Builder<ScanResult<String>> SCAN_CURSOR_STRING
            = new Builder<ScanResult<String>>() {
        @Override
        @SuppressWarnings("unchecked")
        public ScanResult<String> build(Object data) {
            if (data == null) {
                return null;
            }
            List<Object> result = (List<Object>)data;
            String newcursor = new String((byte[])result.get(0));
            List<String> results = new ArrayList<>();
            List<byte[]> rawResults = (List<byte[]>)result.get(1);
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

    public static final Builder<ScanResult<byte[]>> SCAN_CURSOR_BYTE
            = new Builder<ScanResult<byte[]>>() {
        @Override
        @SuppressWarnings("unchecked")
        public ScanResult<byte[]> build(Object data) {
            if (data == null) {
                return null;
            }
            List<Object> result = (List<Object>)data;
            byte[] newcursor = (byte[])result.get(0);
            return new ScanResult<>(newcursor, (List<byte[]>)result.get(1));
        }

        @Override
        public String toString() {
            return "scan_index_byte";
        }
    };
}
