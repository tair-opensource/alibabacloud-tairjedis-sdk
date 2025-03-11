package com.aliyun.tair.tairgis.params;

import io.valkey.util.SafeEncoder;

public class GisSearchResponse {
    private byte[] field;
    private byte[] value;
    private double distance;

    public GisSearchResponse() {
    }

    public byte[] getField() {
        return field;
    }

    public String getFieldByString() {
        if (field == null) {
            return null;
        }
        return SafeEncoder.encode(field);
    }

    public void setField(byte[] field) {
        this.field = field;
    }

    public byte[] getValue() {
        return value;
    }

    public String getValueByString() {
        if (value == null) {
            return null;
        }
        return SafeEncoder.encode(value);
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public double getDistance() {
        return distance;
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }

    @Override
    public String toString() {
        return "GisSearchResponse{" +
            "field=" + getFieldByString() +
            ", value=" + getValueByString() +
            ", distance=" + distance +
            '}';
    }
}
