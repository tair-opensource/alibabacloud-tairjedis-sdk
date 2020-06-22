package com.aliyun.tair.taircpc.params;

import com.aliyun.tair.taircpc.CommonResult;

public class CpcArrayData {
    private String key;
    private long offset;
    private String item;
    private long size;
    private String expStr;
    private long exp;
    private boolean hasSetExp;

    private static final String PX = "px";
    private static final String EX = "ex";
    private static final String EXAT = "exat";
    private static final String PXAT = "pxat";

    public CpcArrayData(String key, long offset, String item, long size) {
        this.key = key;
        this.offset = offset;
        this.item = item;
        this.size = size;
        this.expStr = EX;
        this.exp = 0;
        this.hasSetExp = false;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String getItem() {
        return item;
    }

    public void setItem(String item) {
        this.item = item;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
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
    public CpcArrayData ex(long secondsToExpire) {
        if (hasSetExp)
            throw new IllegalArgumentException(CommonResult.ExpIsSet);
        this.hasSetExp = true;
        this.expStr = EX;
        this.exp = secondsToExpire;
        return this;
    }

    public CpcArrayData px(long millisecondsToExpire) {
        if (hasSetExp)
            throw new IllegalArgumentException(CommonResult.ExpIsSet);
        this.hasSetExp = true;
        this.expStr = PX;
        this.exp = millisecondsToExpire;
        return this;
    }

    public CpcArrayData exat(long secondsToExpire) {
        if (hasSetExp)
            throw new IllegalArgumentException(CommonResult.ExpIsSet);
        this.hasSetExp = true;
        this.expStr = EXAT;
        this.exp = secondsToExpire;
        return this;
    }

    public CpcArrayData pxat(long millisecondsToExpire) {
        if (hasSetExp)
            throw new IllegalArgumentException(CommonResult.ExpIsSet);
        this.hasSetExp = true;
        this.expStr = PXAT;
        this.exp = millisecondsToExpire;
        return this;
    }
}
