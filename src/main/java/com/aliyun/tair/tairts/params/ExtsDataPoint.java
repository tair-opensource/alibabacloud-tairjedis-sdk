package com.aliyun.tair.tairts.params;

public class ExtsDataPoint<T> {
    private T skey;
    private T ts;
    private double value;

    public ExtsDataPoint(T skey, T ts, double value) {
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


    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }
}
