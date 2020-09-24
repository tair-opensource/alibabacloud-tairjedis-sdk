package com.aliyun.tair.taircpc.factory;

import com.aliyun.tair.taircpc.results.Update2EstWithKeyResult;
import com.aliyun.tair.taircpc.results.Update2JudResult;
import com.aliyun.tair.taircpc.results.Update2JudWithKeyResult;
import redis.clients.jedis.Builder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class CpcBuilderFactory {

    public static final Builder<Update2JudResult> CPCUPDATE2JUD_RESULT = new Builder<Update2JudResult>() {
        @Override
        public Update2JudResult build(Object data) {
            if (data == null) {
                return null;
            }
            List l = (List) data;
            String valueStr = new String((byte[]) l.get(0));
            String diffValueStr = new String((byte[]) l.get(1));
            Double value= Double.parseDouble(valueStr);
            Double diffValue= Double.parseDouble(diffValueStr);
            return new Update2JudResult(value, diffValue);
        }

        @Override
        public String toString() {
            return "CpcUpdate2JudResult";
        }
    };

    public static final Builder<List<Update2JudResult>> CPCUPDATE2JUD_MULTI_RESULT = new Builder<List<Update2JudResult>>() {
        @Override
        public List<Update2JudResult> build(Object data) {
            if (data == null) {
                return null;
            }
            List l = (List) data;
            final ArrayList<Update2JudResult> results = new ArrayList<Update2JudResult>();
            int num = l.size();
            for (int i = 0; i < num; i++) {
                List subl = (List) l.get(i);
                String valueStr = new String((byte[]) subl.get(0));
                String diffValueStr = new String((byte[]) subl.get(1));
                Double value= Double.parseDouble(valueStr);
                Double diffValue= Double.parseDouble(diffValueStr);
                results.add(new Update2JudResult(value, diffValue));
            }
            return results;
        }

        @Override
        public String toString() {
            return "CpcMUpdate2JudResult";
        }
    };

    public static final Builder<HashMap<String, Update2JudResult>> CPCUPDATE2JUDWITHKEY_MULTI_RESULT = new Builder<HashMap<String, Update2JudResult>>() {
        @Override
        public HashMap<String, Update2JudResult> build(Object data) {
            if (data == null) {
                return null;
            }
            List l = (List) data;
            final HashMap<String, Update2JudResult> results = new HashMap<String, Update2JudResult>();
            int num = l.size();
            for (int i = 0; i < num; i++) {
                List subl = (List) l.get(i);
                String keyStr = new String((byte[]) subl.get(0));
                String valueStr = new String((byte[]) subl.get(1));
                String diffValueStr = new String((byte[]) subl.get(2));
                Double value= Double.parseDouble(valueStr);
                Double diffValue= Double.parseDouble(diffValueStr);
                Update2JudResult update2JudResult = new Update2JudResult(value, diffValue);
                results.put(keyStr, update2JudResult);
            }
            return results;
        }

        @Override
        public String toString() {
            return "CpcMUpdate2JudWithKeyMapResult";
        }
    };

    public static final Builder<List<Double>> CPCUPDATE2EST_MULTI_RESULT = new Builder<List<Double>>() {
        @Override
        public List<Double> build(Object data) {
            if (data == null) {
                return null;
            }
            List l = (List) data;
            final ArrayList<Double> results = new ArrayList<Double>();
            int num = l.size();
            for (int i = 0; i < num; i++) {
                String valueStr = new String((byte[]) l.get(i));
                Double value= Double.parseDouble(valueStr);
                results.add(value);
            }
            return results;
        }

        @Override
        public String toString() {
            return "CpcMUpdate2estResult";
        }
    };

    public static final Builder<HashMap<String, Double>> CPCUPDATE2ESTWITHKEY_MULTI_RESULT = new Builder<HashMap<String, Double>>() {
        @Override
        public HashMap<String, Double> build(Object data) {
            if (data == null) {
                return null;
            }
            List l = (List) data;
            final HashMap<String, Double> results = new HashMap<String, Double>();
            int num = l.size();
            for (int i = 0; i < num; i++) {
                List subl = (List) l.get(i);
                String keyStr = new String((byte[]) subl.get(0));
                String valueStr = new String((byte[]) subl.get(1));
                Double value = Double.parseDouble(valueStr);
                results.put(keyStr, value);
            }
            return results;
        }

        @Override
        public String toString() {
            return "CpcMUpdate2estWithKeyMapResult";
        }
    };

    public static final Builder<List<Update2JudResult>> CPCARRAYUPDATE2JUD_MULTI_RESULT = new Builder<List<Update2JudResult>>() {
        @Override
        public List<Update2JudResult> build(Object data) {
            if (data == null) {
                return null;
            }
            List l = (List) data;
            final ArrayList<Update2JudResult> results = new ArrayList<Update2JudResult>();
            int num = l.size();
            for (int i = 0; i < num; i++) {
                List subl = (List) l.get(i);
                String valueStr = new String((byte[]) subl.get(0));
                String diffValueStr = new String((byte[]) subl.get(1));
                Double value= Double.parseDouble(valueStr);
                Double diffValue= Double.parseDouble(diffValueStr);
                results.add(new Update2JudResult(value, diffValue));
            }
            return results;
        }

        @Override
        public String toString() {
            return "CpcMArrayUpdate2judResult";
        }
    };

    public static final Builder<List<Double>> CPCARRAYUPDATE2EST_MULTI_RESULT = new Builder<List<Double>>() {
        @Override
        public List<Double> build(Object data) {
            if (data == null) {
                return null;
            }
            List l = (List) data;
            final ArrayList<Double> results = new ArrayList<Double>();
            int num = l.size();
            for (int i = 0; i < num; i++) {
                String valueStr = new String((byte[]) l.get(i));
                Double value= Double.parseDouble(valueStr);
                results.add(value);
            }
            return results;
        }

        @Override
        public String toString() {
            return "CpcMArrayUpdate2estResult";
        }
    };

    public static final Builder<List<Double>> CPCARRAY_ESTIMATE_RANGE_RESULT = new Builder<List<Double>>() {
        @Override
        public List<Double> build(Object data) {
            if (data == null) {
                return null;
            }
            List l = (List) data;
            final ArrayList<Double> results = new ArrayList<Double>();
            int num = l.size();
            for (int i = 0; i < num; i++) {
                String valueStr = new String((byte[]) l.get(i));
                Double value= Double.parseDouble(valueStr);
                results.add(value);
            }
            return results;
        }

        @Override
        public String toString() {
            return "CpcArrayEstimateRangeResult";
        }
    };

    public static final Builder<List<Double>> CPCARRAY_RANGE_RESULT = new Builder<List<Double>>() {
        @Override
        public List<Double> build(Object data) {
            if (data == null) {
                return null;
            }
            List l = (List) data;
            final ArrayList<Double> results = new ArrayList<Double>();
            int num = l.size();
            for (int i = 0; i < num; i++) {
                String valueStr = new String((byte[]) l.get(i));
                Double value= Double.parseDouble(valueStr);
                results.add(value);
            }
            return results;
        }

        @Override
        public String toString() {
            return "CpcArrayRangeResult";
        }
    };

    public static final Builder<List<Object>> SKETCHES_RANGE_RESULT = new Builder<List<Object>>() {
        @Override
        public List<Object> build(Object data) {
            if (data == null) {
                return null;
            }
            List l = (List) data;
            final ArrayList<Object> results = new ArrayList<Object>();
            int num = l.size();
            for (int i = 0; i < num; i++) {
                results.add(l.get(i));
            }
            return results;
        }

        @Override
        public String toString() {
            return "sketchesRangeResult";
        }
    };

}
