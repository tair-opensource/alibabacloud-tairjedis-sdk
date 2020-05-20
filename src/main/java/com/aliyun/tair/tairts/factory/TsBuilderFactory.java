package com.aliyun.tair.tairts.factory;

import com.aliyun.tair.tairts.results.ExtsDataPointResult;
import com.aliyun.tair.tairts.results.ExtsSkeyResult;
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

    public static final Builder<List<ExtsDataPointResult>> EXTSRANGE_RESULT_STRING = new Builder<List<ExtsDataPointResult>>() {
        @Override
        public List<ExtsDataPointResult> build(Object data) {
            if (data == null) {
                return null;
            }
            List l = (List) data;
            final ArrayList<ExtsDataPointResult> results = new ArrayList<ExtsDataPointResult>();
            int num = l.size();
            for (int i = 0; i < num; i++) {
                List subl = (List) l.get(i);
                results.add(new ExtsDataPointResult(((Number) subl.get(0)).longValue(), new String((byte[]) subl.get(1))));
            }
            return results;
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
                List subl = (List) l.get(i);
                List labelsList = (List) subl.get(1);
                List dataPointsList = (List) subl.get(2);
                results.add(new ExtsSkeyResult(new String((byte[]) subl.get(0)), labelsList, dataPointsList));
            }
            return results;
        }

        @Override
        public String toString() {
            return "ExTsMrangeResult";
        }
    };
}
