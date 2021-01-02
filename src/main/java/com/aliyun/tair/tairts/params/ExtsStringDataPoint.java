package com.aliyun.tair.tairts.params;

public class ExtsStringDataPoint<T> {
    private T skey;
    private T ts;
    private T value;

    public ExtsStringDataPoint(T skey, T ts, T value) {
        this.skey = skey;
        this.ts = ts;
        this.value = value;
    }

    public T getSkey() {
        return skey;
    }

    public void setSkey(T skey) {
        this.skey = skey;
    }

    public T getTs() {
        return ts;
    }

    public void setTs(T ts) {
        this.ts = ts;
    }


    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }
}
