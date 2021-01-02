package com.aliyun.tair.tairts.params;

public class ExtsFilter <T> {
    private T filter;

    public ExtsFilter(T filter) {
        this.filter = filter;
    }

    public T getFilter() {
        return filter;
    }

    public void setFilter(T filter) {
        this.filter = filter;
    }
}
