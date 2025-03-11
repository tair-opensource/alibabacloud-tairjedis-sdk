package com.aliyun.tair.tairroaring.factory;

import java.util.*;
import io.valkey.Builder;
import com.aliyun.tair.jedis3.ScanResult;

public class RoaringBuilderFactory {

    public static final Builder<ScanResult<Long>> TRSCAN_RESULT_LONG
        = new Builder<ScanResult<Long>>() {
        @Override
        @SuppressWarnings("unchecked")
        public ScanResult<Long> build(Object data) {
            if (data == null) {
                return null;
            }
            List<Object> result = (List<Object>)data;
            String newcursor = String.valueOf(result.get(0));
            List<Long> results = (List<Long>)result.get(1);
            return new ScanResult<Long>(newcursor, results);
        }

        @Override
        public String toString() {
            return "tr.scan";
        }
    };

    public static final Builder<ScanResult<byte[]>> TRSCAN_RESULT_BYTE
        = new Builder<ScanResult<byte[]>>() {
        @Override
        @SuppressWarnings("unchecked")
        public ScanResult<byte[]> build(Object data) {
            if (data == null) {
                return null;
            }
            List<Object> result = (List<Object>)data;
            byte[] newcursor = (byte[])result.get(0);
            List<byte[]> rawResults = (List<byte[]>)result.get(1);
            return new ScanResult<byte[]>(newcursor, rawResults);
        }

        @Override
        public String toString() {
            return "tr.scan";
        }
    };
}
