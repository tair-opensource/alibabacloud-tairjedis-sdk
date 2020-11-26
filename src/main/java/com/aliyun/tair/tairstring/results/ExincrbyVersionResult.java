package com.aliyun.tair.tairstring.results;

public class ExincrbyVersionResult {
    private long value;
    private long version;

    public ExincrbyVersionResult(long value, long version) {
        this.value = value;
        this.version = version;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }
}
