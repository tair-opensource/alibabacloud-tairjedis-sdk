package com.aliyun.tair.tairstring.factory;

import com.aliyun.tair.tairstring.results.ExcasResult;
import com.aliyun.tair.tairstring.results.ExgetResult;
import com.aliyun.tair.tairstring.results.ExincrbyVersionResult;
import io.valkey.Builder;

import java.util.List;

public class StringBuilderFactory {

    public static final Builder<ExgetResult<String>> EXGET_RESULT_STRING = new Builder<ExgetResult<String>>() {
        @Override
        public ExgetResult<String> build(Object data) {
            if (data == null) {
                return null;
            }
            List l = (List) data;
            if (l.size() == 3) {
                // WITHFLAGS, Success: [value, version, flags]
                return new ExgetResult<String>((String)l.get(0), ((Number)l.get(1)).longValue(),
                    ((Number)l.get(2)).longValue());
            }
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
            if (l.size() == 3) {
                // WITHFLAGS, Success: [value, version, flags]
                return new ExgetResult<byte[]>((byte[])l.get(0), ((Number)l.get(1)).longValue(),
                    ((Number)l.get(2)).longValue());
            }
            return new ExgetResult<byte[]>((byte[])l.get(0),((Number) l.get(1)).longValue());
        }

        @Override
        public String toString() {
            return "ExgetResult";
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

    public static final Builder<ExincrbyVersionResult> EXINCRBY_VERSION_RESULT_STRING = new Builder<ExincrbyVersionResult>() {
        @Override
        public ExincrbyVersionResult build(Object data) {
            if (data == null) {
                return null;
            }
            List l = (List) data;
            return new ExincrbyVersionResult(((Number) l.get(0)).longValue(),((Number) l.get(1)).longValue());
        }

        @Override
        public String toString() {
            return "ExincrbyVersionResult";
        }
    };
}
