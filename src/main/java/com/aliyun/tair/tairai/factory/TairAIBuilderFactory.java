package com.aliyun.tair.tairai.factory;
import com.aliyun.tair.tairai.results.*;
import redis.clients.jedis.Builder;
import java.util.ArrayList;
import java.util.List;

public class TairAIBuilderFactory {

    public static final Builder<TairAIStatResult> TairAIStat_RESULT_STRING = new Builder<TairAIStatResult>() {
        @Override
        public TairAIStatResult build(Object data) {
            if (data == null) {
                return null;
            }
            List l = (List) data;

            return new TairAIStatResult(((Number) l.get(0)).longValue(),
                new String((byte[]) l.get(1)),
                    ((Number) l.get(2)).longValue(),
                    ((Number) l.get(3)).longValue(),
                    ((Number) l.get(4)).longValue(),
                    ((Number) l.get(5)).longValue(),
                    ((Number) l.get(6)).longValue()
                );
        }

        @Override
        public String toString() {
            return "TairAIStat";
        }
    };

    public static final Builder<TairAIDebugResult> TairAIDebug_RESULT_STRING = new Builder<TairAIDebugResult>() {
        @Override
        public TairAIDebugResult build(Object data) {
            if (data == null) {
                return null;
            }
            List l = (List) data;
            return new TairAIDebugResult(((Number) l.get(0)).longValue(),
                    ((Number) l.get(1)).longValue(),
                    ((Number) l.get(2)).longValue());
        }

        @Override
        public String toString() {
            return "TairAIDebug";
        }
    };

    public static final Builder<TairAIKnngetResult> TairAIKNNGET_RESULT_STRING = new Builder<TairAIKnngetResult>() {
        @Override
        public TairAIKnngetResult build(Object data) {
            if (data == null) {
                return null;
            }
            List l = (List) data;
            return new TairAIKnngetResult(l);
        }

        @Override
        public String toString() {
            return "TairAIKnngetResult";
        }
    };


    public static final Builder<List<Float>> TairAIGET_RESULT_STRING = new Builder<List<Float>>() {
        @Override
        public List<Float> build(Object data) {
            List<Float> ret = new ArrayList<>();
            if (data == null) {
                return null;
            }
            List l = (List)data;
            for (int i = 0; i < l.size(); i++) {
                ret.add(Float.parseFloat(new String((byte[]) l.get(i))));
            }

            return ret;
        }

        @Override
        public String toString() {
            return "TairAIgetResult";
        }
    };


}
