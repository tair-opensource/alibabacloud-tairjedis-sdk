package com.aliyun.tair.taircpc.params;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.taircpc.CommonResult;

import java.util.HashMap;
import java.util.Map;

public class CpcArrayMultiData  {

    private String key;
    private long timestamp;
    private Map<String, Object> values = new HashMap<>();
    private String expStr;
    private long exp;
    private boolean hasSetExp;
    private String sizeStr;
    private long size;
    private String winSizeStr;
    private long winSize;
    private ModuleCommand type;

    private static final String PX = "px";
    private static final String EX = "ex";
    private static final String EXAT = "exat";
    private static final String PXAT = "pxat";

    public CpcArrayMultiData(){
        this.expStr = EX;
        this.exp = 0;
        this.hasSetExp = false;
        this.sizeStr = "size";
        this.size = 10;
        this.winSizeStr = "win";
        this.winSize = 1*60*1000;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public long getWinSize() {
        return winSize;
    }

    public void setWinSize(long winSize) {
        this.winSize = winSize;
    }

    public String getExpStr() {
        return expStr;
    }

    public long getExp() {
        return exp;
    }

    public String getSizeStr() {
        return sizeStr;
    }

    public void setSizeStr(String sizeStr) {
        this.sizeStr = sizeStr;
    }

    public String getWinSizeStr() {
        return winSizeStr;
    }

    public void setWinSizeStr(String winSizeStr) {
        this.winSizeStr = winSizeStr;
    }

    public ModuleCommand getType() {
        return type;
    }

    public void setType(ModuleCommand type) {
        this.type = type;
    }

    /**
     * Set the specified expire time, in seconds.
     *
     * @param secondsToExpire
     * @return SetParams
     */
    public CpcArrayMultiData ex(long secondsToExpire) {
        if (hasSetExp) {
            throw new IllegalArgumentException(CommonResult.ExpIsSet);
        }
        this.hasSetExp = true;
        this.expStr = EX;
        this.exp = secondsToExpire;
        return this;
    }

    public CpcArrayMultiData px(long millisecondsToExpire) {
        if (hasSetExp) {
            throw new IllegalArgumentException(CommonResult.ExpIsSet);
        }
        this.hasSetExp = true;
        this.expStr = PX;
        this.exp = millisecondsToExpire;
        return this;
    }

    public CpcArrayMultiData exat(long secondsToExpire) {
        if (hasSetExp) {
            throw new IllegalArgumentException(CommonResult.ExpIsSet);
        }
        this.hasSetExp = true;
        this.expStr = EXAT;
        this.exp = secondsToExpire;
        return this;
    }

    public CpcArrayMultiData pxat(long millisecondsToExpire) {
        if (hasSetExp) {
            throw new IllegalArgumentException(CommonResult.ExpIsSet);
        }
        this.hasSetExp = true;
        this.expStr = PXAT;
        this.exp = millisecondsToExpire;
        return this;
    }

    public Map<String, Object> getValues() {
        return values;
    }

    public void setValues(Map<String, Object> values) {
        this.values = values;
    }


}