package com.aliyun.tair.taircpc.params;

import com.aliyun.tair.taircpc.CommonResult;

public class CpcData {
    private String key;
    private String item;
    private String expStr;
    private long exp;
    private boolean hasSetExp;

    private static final String PX = "px";
    private static final String EX = "ex";
    private static final String EXAT = "exat";
    private static final String PXAT = "pxat";

    public CpcData(String key, String item) {
        this.key = key;
        this.item = item;
        this.expStr = EX;
        this.exp = 0;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getItem() {
        return item;
    }

    public void setItem(String item) {
        this.item = item;
    }

    public String getExpStr() {
        return expStr;
    }

    public long getExp() {
        return exp;
    }

    /**
     * Set the specified expire time, in seconds.
     *
     * @param secondsToExpire
     * @return SetParams
     */
    public CpcData ex(long secondsToExpire) {
        if (hasSetExp)
            throw new IllegalArgumentException(CommonResult.ExpIsSet);
        this.hasSetExp = true;
        this.expStr = EX;
        this.exp = secondsToExpire;
        return this;
    }

    public CpcData px(long millisecondsToExpire) {
        if (hasSetExp)
            throw new IllegalArgumentException(CommonResult.ExpIsSet);
        this.hasSetExp = true;
        this.expStr = PX;
        this.exp = millisecondsToExpire;
        return this;
    }

    public CpcData exat(long secondsToExpire) {
        if (hasSetExp)
            throw new IllegalArgumentException(CommonResult.ExpIsSet);
        this.hasSetExp = true;
        this.expStr = EXAT;
        this.exp = secondsToExpire;
        return this;
    }

    public CpcData pxat(long millisecondsToExpire) {
        if (hasSetExp)
            throw new IllegalArgumentException(CommonResult.ExpIsSet);
        this.hasSetExp = true;
        this.expStr = PXAT;
        this.exp = millisecondsToExpire;
        return this;
    }
}
