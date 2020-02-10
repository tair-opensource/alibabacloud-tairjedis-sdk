package com.aliyun.tair.tairbloom.factory;

import redis.clients.jedis.Builder;

import java.util.List;

public class BloomBuilderFactory {

    public static final Builder<Boolean[]> BFMADD_RESULT_BOOLEAN_LIST = new Builder<Boolean[]>() {
        @Override
        public Boolean[] build(Object data) {
            if (data == null) {
                return null;
            }
            List l = (List) data;
            Boolean ret[] = new Boolean[l.size()];
            for (int i = 0; i < l.size(); i++) {
                ret[i] = ((Number) l.get(i)).longValue() != 0;
            }
            return ret;
        }

        @Override
        public String toString() {
            return "BfmaddResult";
        }
    };

    public static final Builder<Boolean[]> BFINSERT_RESULT_BOOLEAN_LIST = new Builder<Boolean[]>() {
        @Override
        public Boolean[] build(Object data) {
            if (data == null) {
                return null;
            }
            List l = (List) data;
            Boolean ret[] = new Boolean[l.size()];
            for (int i = 0; i < l.size(); i++) {
                ret[i] = ((Number) l.get(i)).longValue() != 0;
            }
            return ret;
        }

        @Override
        public String toString() {
            return "BfinsertResult";
        }
    };
}
