package com.aliyun.tair.tairts.results;

public class ExtsStringDataPointResult {
    private long ts;
    private String value;

    public ExtsStringDataPointResult(long ts, String value) {
        this.ts = ts;
        this.value = value;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
