package com.aliyun.tair.taircpc.results;

public class Update2JudWithKeyResult {
    private String key;
    private double value;
    private double diffValue;

    public Update2JudWithKeyResult(String key, double value, double diffValue) {
        this.key = key;
        this.value = value;
        this.diffValue = diffValue;
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

    public double getDiffValue() {
        return diffValue;
    }

    public void setDiffValue(double diffValue) {
        this.diffValue = diffValue;
    }
}
