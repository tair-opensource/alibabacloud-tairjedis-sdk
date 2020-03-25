package com.aliyun.tair.tairstring.results;

public class ExgetResult<T> {
    private long version;
    private T value;

    public ExgetResult(T value, long version) {
        this.value = value;
        this.version = version;
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
}
