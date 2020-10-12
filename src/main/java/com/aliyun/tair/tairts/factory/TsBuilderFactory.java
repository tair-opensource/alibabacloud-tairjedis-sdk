package com.aliyun.tair.tairts.factory;

import com.aliyun.tair.tairts.results.*;
import redis.clients.jedis.Builder;

import java.util.ArrayList;
import java.util.List;

public class TsBuilderFactory {

    public static final Builder<ExtsDataPointResult> EXTSGET_RESULT_STRING = new Builder<ExtsDataPointResult>() {
        @Override
        public ExtsDataPointResult build(Object data) {
            if (data == null) {
                return null;
            }
            List l = (List) data;
            return new ExtsDataPointResult(((Number) l.get(0)).longValue(),new String((byte[]) l.get(1)));
        }

        @Override
        public String toString() {
            return "ExTsGetResult";
        }
    };

    public static final Builder<ExtsSkeyResult> EXTSRANGE_RESULT_STRING = new Builder<ExtsSkeyResult>() {
        @Override
        public ExtsSkeyResult build(Object data) {
            if (data == null) {
                return null;
            }
            List l = (List) data;
            List dataPointsList = (List) l.get(0);
            return new ExtsSkeyResult(null, new ArrayList<ExtsLabelResult>(), dataPointsList, ((long) l.get(1)));
        }

        @Override
        public String toString() {
            return "ExTsRangeResult";
        }
    };

    public static final Builder<List<ExtsSkeyResult>> EXTSMRANGE_RESULT_STRING = new Builder<List<ExtsSkeyResult>>() {
        @Override
        public List<ExtsSkeyResult> build(Object data) {
            if (data == null) {
                return null;
            }
            List l = (List) data;
            final ArrayList<ExtsSkeyResult> results = new ArrayList<ExtsSkeyResult>();
            int num = l.size();
            for (int i = 0; i < num; i++) {
                if (l.get(i) == null) {
                    return results;
                }
                List subl = (List) l.get(i);
                List labelsList = (List) subl.get(1);
                List dataPointsList = (List) subl.get(2);
                results.add(new ExtsSkeyResult(new String((byte[]) subl.get(0)), labelsList, dataPointsList, ((long) subl.get(3))));
            }
            return results;
        }

        @Override
        public String toString() {
            return "ExTsMrangeResult";
        }
    };

    public static final Builder<ExtsStringDataPointResult> EXTSSTRING_GET_RESULT_STRING = new Builder<ExtsStringDataPointResult>() {
        @Override
        public ExtsStringDataPointResult build(Object data) {
            if (data == null) {
                return null;
            }
            List l = (List) data;
            return new ExtsStringDataPointResult(((Number) l.get(0)).longValue(),new String((byte[]) l.get(1)));
        }

        @Override
        public String toString() {
            return "ExTsStringGetResult";
        }
    };

    public static final Builder<ExtsStringSkeyResult> EXTSSTRING_RANGE_RESULT_STRING = new Builder<ExtsStringSkeyResult>() {
        @Override
        public ExtsStringSkeyResult build(Object data) {
            if (data == null) {
                return null;
            }
            List l = (List) data;

            List dataPointsList = (List) l.get(0);
            return new ExtsStringSkeyResult(null, new ArrayList<ExtsLabelResult>(), dataPointsList, ((long) l.get(1)));
        }

        @Override
        public String toString() {
            return "ExTsStringRangeResult";
        }
    };

    public static final Builder<List<ExtsStringSkeyResult>> EXTSSTRING_MRANGE_RESULT_STRING = new Builder<List<ExtsStringSkeyResult>>() {
        @Override
        public List<ExtsStringSkeyResult> build(Object data) {
            if (data == null) {
                return null;
            }
            List l = (List) data;
            final ArrayList<ExtsStringSkeyResult> results = new ArrayList<ExtsStringSkeyResult>();
            int num = l.size();
            for (int i = 0; i < num; i++) {
                List subl = (List) l.get(i);
                List labelsList = (List) subl.get(1);
                List dataPointsList = (List) subl.get(2);
                results.add(new ExtsStringSkeyResult(new String((byte[]) subl.get(0)), labelsList, dataPointsList, ((long) subl.get(3))));
            }
            return results;
        }

        @Override
        public String toString() {
            return "ExTsStringMrangeResult";
        }
    };
}
