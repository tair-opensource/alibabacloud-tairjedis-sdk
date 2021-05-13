package com.aliyun.tair.tairstring.factory;

import com.aliyun.tair.tairstring.results.ExcasResult;
import com.aliyun.tair.tairstring.results.ExgetResult;
import redis.clients.jedis.Builder;

import java.util.ArrayList;
import java.util.List;

public class StringBuilderFactory {

    public static final Builder<ExgetResult<String>> EXGET_RESULT_STRING = new Builder<ExgetResult<String>>() {
        @Override
        public ExgetResult<String> build(Object data) {
            if (data == null) {
                return null;
            }
            List l = (List) data;
            return new ExgetResult<String>(new String((byte[]) l.get(0)),((Number) l.get(1)).longValue());

        }

        @Override
        public String toString() {
            return "ExgetResult";
        }
    };

    public static final Builder<ExgetResult<byte[]>> EXGET_RESULT_BYTE = new Builder<ExgetResult<byte[]>>() {
        @Override
        @SuppressWarnings("unchecked")
        public ExgetResult<byte[]> build(Object data) {
            if (data == null) {
                return null;
            }
            List l = (List) data;
            return new ExgetResult<byte[]>((byte[])l.get(0),((Number) l.get(1)).longValue());
        }

        @Override
        public String toString() {
            return "ExgetResult";
        }
    };

    public static final Builder<List<ExgetResult<String>>> EXGET_MULTI_RESULT_STRING = new Builder<List<ExgetResult<String>>>() {
        @Override
        public List<ExgetResult<String>> build(Object data) {
            if (data == null) {
                return null;
            }
            List l = (List) data;
            final ArrayList<ExgetResult<String>> results = new ArrayList<ExgetResult<String>>();
            int num = l.size();
            for (int i = 0; i < num; i++) {
                List subl = (List) l.get(i);
                if (subl == null || subl.get(0) == null) {
                    results.add(null);
                } else {
                    results.add(new ExgetResult<String>(new String((byte[]) subl.get(0)),((Number) subl.get(1)).longValue()));
                }
            }
            return results;
        }

        @Override
        public String toString() {
            return "ExmgetResult";
        }
    };

    public static final Builder<List<ExgetResult<byte[]>>> EXGET_MULTI_RESULT_BYTE = new Builder<List<ExgetResult<byte[]>>>() {
        @Override
        public List<ExgetResult<byte[]>> build(Object data) {
            if (data == null) {
                return null;
            }
            List l = (List) data;
            final ArrayList<ExgetResult<byte[]>> results = new ArrayList<ExgetResult<byte[]>>();
            int num = l.size();
            for (int i = 0; i < num; i++) {
                List subl = (List) l.get(i);
                results.add(new ExgetResult<byte[]>((byte[])l.get(0),((Number) l.get(1)).longValue()));
            }
            return results;
        }

        @Override
        public String toString() {
            return "ExmgetResult";
        }
    };

    public static final Builder<ExcasResult<String>> EXCAS_RESULT_STRING = new Builder<ExcasResult<String>>() {
        @Override
        public ExcasResult<String> build(Object data) {
            if (data == null) {
                return null;
            }
            List l = (List) data;
            return new ExcasResult<String>(new String((byte[])l.get(0)), new String((byte[]) l.get(1)),((Number) l.get(2)).longValue());

        }

        @Override
        public String toString() {
            return "ExcasResult";
        }
    };

    public static final Builder<ExcasResult<byte[]>> EXCAS_RESULT_BYTE = new Builder<ExcasResult<byte[]>>() {
        @Override
        @SuppressWarnings("unchecked")
        public ExcasResult<byte[]> build(Object data) {
            if (data == null) {
                return null;
            }
            List l = (List) data;
            return new ExcasResult<byte[]>((byte[])l.get(0), (byte[])l.get(1), ((Number) l.get(2)).longValue());
        }

        @Override
        public String toString() {
            return "ExcasResult";
        }
    };
}
