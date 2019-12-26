package com.kvstore.jedis.tairstring.results;

public class ExcasResult<T> {
    private long version;
    private T value;
    private T msg;

    public ExcasResult(T msg, T value, long version) {
        this.msg = msg;
        this.value = value;
        this.version = version;
    }

    public T getMsg() {
        return msg;
    }

    public void setMsg(T msg) {
        this.msg = msg;
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
