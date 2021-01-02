package com.aliyun.tair.taircpc.results;

public class Update2JudResult {
    private double value;
    private double diffValue;

    public Update2JudResult(double value, double diffValue) {
        this.value = value;
        this.diffValue = diffValue;
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
