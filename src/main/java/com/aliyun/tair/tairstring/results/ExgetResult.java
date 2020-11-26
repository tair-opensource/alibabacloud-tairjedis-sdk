package com.aliyun.tair.tairstring.results;

public class ExgetResult<T> {
    private long version;
    private T value;
    private long flags;

    public ExgetResult(T value, long version) {
        this.value = value;
        this.version = version;
    }

    public ExgetResult(T value, long version, long flags) {
        this.value = value;
        this.version = version;
        this.flags = flags;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public long getFlags() {
        return flags;
    }

    public void setFlags(long flags) {
        this.flags = flags;
    }
}
