package com.aliyun.tair.tairts.results;

public class ExtsDataPointResult {
    private long ts;
    private String value;

    public ExtsDataPointResult(long ts, String value) {
        this.ts = ts;
        this.value = value;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public double getDoubleValue() {
        return Double.parseDouble(value);
    }

    public void setValue(String value) {
        this.value = value;
    }
}
