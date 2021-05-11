package com.aliyun.tair.tairhash.params;

public class ExhmincrbyFields<T> {
    private T field;
    private long value;
    private long defaultValue;
    private long exp;

    public ExhmincrbyFields(T field, long value, long defaultValue, long exp) {
        this.field = field;
        this.value = value;
        this.defaultValue = defaultValue;
        this.exp = exp;
    }

    public T getField() {
        return field;
    }

    public void setField(T field) {
        this.field = field;
    }


    public long getExp() {
        return exp;
    }

    public void setExp(long exp) {
        this.exp = exp;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public long getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(long defaultValue) {
        this.defaultValue = defaultValue;
    }
}
