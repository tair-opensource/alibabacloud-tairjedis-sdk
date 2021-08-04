package com.aliyun.tair.tairai.results;

public class TairAIDebugResult {
    private long aiKeyNum;
    private long cycle;
    private long pendding;

    public TairAIDebugResult(long aiKeyNum, long cycle, long pendding) {
        this.aiKeyNum = aiKeyNum;
        this.cycle = cycle;
        this.pendding = pendding;
    }

    public long getAiKeyNum() {return aiKeyNum; }
    public long getCycle() {
        return cycle;
    }
    public long getPendding() {
        return pendding;
    }

    public String toString() {
        return "total ai key num =" + aiKeyNum +
                ", async cycle =" + cycle +
                ", total pendding =" + pendding +
                ";"
                ;
    }
}
