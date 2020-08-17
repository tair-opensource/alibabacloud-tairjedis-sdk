package com.aliyun.tair.tairts.results;

import java.util.ArrayList;
import java.util.List;

public class ExtsStringSkeyResult {
    private String skey;
    private ArrayList<ExtsLabelResult> labels = new ArrayList<ExtsLabelResult>();
    private ArrayList<ExtsStringDataPointResult> dataPoints = new ArrayList<ExtsStringDataPointResult>();
    private long token;

    public ExtsStringSkeyResult(String skey, List labels, List dataPoints, long token) {
        this.skey = skey;
        this.token = token;

        int labelsNum = labels.size();
        for (int i = 0; i < labelsNum; i++) {
            List subl = (List) labels.get(i);
            this.labels.add(new ExtsLabelResult(new String((byte[]) subl.get(0)), new String((byte[]) subl.get(1))));
        }

        int dataPointsNum = dataPoints.size();
        for (int i = 0; i < dataPointsNum; i++) {
            List subl = (List) dataPoints.get(i);
            this.dataPoints.add(new ExtsStringDataPointResult(((Number) subl.get(0)).longValue(), new String((byte[]) subl.get(1))));
        }
    }

    public String getSkey() {
        return skey;
    }

    public void setSkey(String skey) {
        this.skey = skey;
    }

    public ArrayList<ExtsLabelResult> getLabels() {
        return labels;
    }

    public ArrayList<ExtsStringDataPointResult> getDataPoints() {
        return dataPoints;
    }

    public long getToken() {
        return token;
    }

    public void setToken(long token) {
        this.token = token;
    }
}
