package com.aliyun.tair.taircpc.results;

public class Update2EstWithKeyResult {
    private String key;
    private double value;

    public Update2EstWithKeyResult(String key, double value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }
}
