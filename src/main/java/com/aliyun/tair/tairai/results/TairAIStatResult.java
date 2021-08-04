package com.aliyun.tair.tairai.results;

public class TairAIStatResult {
    private long penddingSize;
    private String pattern;
    private long indexSize;
    private long trigger;
    private long timeCost;
    private long errCnt;
    private long innerSize;

    public TairAIStatResult(long penddingSize, String pattern, long indexSize, long trigger, long timeCost, long errCnt, long innerSize) {
        this.penddingSize = penddingSize;
        this.pattern = pattern;
        this.indexSize = indexSize;
        this.trigger = trigger;
        this.timeCost = timeCost;
        this.errCnt = errCnt;
        this.innerSize = innerSize;
    }


    public long getIndexSize() {
        return indexSize;
    }
    public String getPattern() {
        return pattern;
    }
    public long getPenddingSize() {
        return penddingSize;
    }
    public long getTriggerNum() {
        return trigger;
    }
    public long getTimeCost() { return timeCost; }
    public long getErrCnt() {
        return errCnt;
    }
    public long getInnerSize() {
        return innerSize;
    }

    @Override
    public String toString() {
        return "pendding =" + penddingSize +
                ", pattern =" + pattern +
                ", size =" + indexSize +
                ", trigger by list =" + trigger +
                ", time cost =" + timeCost +
                ", error cnt =" + errCnt +
                ", inner size =" + innerSize
                ;
    }
}
