package com.aliyun.tairjedis.tairhash.params;

public class ExhmsetwithoptsParams<T> {
    private T field;
    private T value;
    private long ver;
    private long exp;

    public ExhmsetwithoptsParams(T field, T value, long ver, long exp) {
        this.field = field;
        this.value = value;
        this.ver = ver;
        this.exp = exp;
    }

    public T getField() {
        return field;
    }

    public void setField(T field) {
        this.field = field;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public long getVer() {
        return ver;
    }

    public void setVer(long ver) {
        this.ver = ver;
    }

    public long getExp() {
        return exp;
    }

    public void setExp(long exp) {
        this.exp = exp;
    }
}
