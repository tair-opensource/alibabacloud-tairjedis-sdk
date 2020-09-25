package com.aliyun.tair.taircpc.params;

import com.aliyun.tair.ModuleCommand;

import java.util.HashMap;
import java.util.Map;

public class CpcDataUtil {

    public final static String VALUE = "value";
    public final static String CONTENT = "content";
    public final static String WEIGHT = "weight";

    public final static String CPC_TYPE = "CPC.ARRAY.UPDATE";
    public final static String SUM_TYPE = "SUM.ARRAY.ADD";
    public final static String MAX_TYPE = "MAX.ARRAY.ADD";
    public final static String MIN_TYPE = "MIN.ARRAY.ADD";
    public final static String FIRST_TYPE = "FIRST.ARRAY.ADD";
    public final static String LAST_TYPE = "LAST.ARRAY.ADD";
    public final static String AVG_TYPE = "AVG.ARRAY.ADD";
    public final static String STDDEV_TYPE = "STDDEV.ARRAY.ADD";

    public static CpcArrayMultiData buildCpc(String key, String value, long timeStamp) {
        CpcArrayMultiData data = new CpcArrayMultiData();
        data.setKey(key);
        data.setTimestamp(timeStamp);
        data.setType(ModuleCommand.CPCARRAYUPDATE);
        Map<String, Object> map = new HashMap<>();
        map.put(VALUE, value);
        data.setValues(map);
        return data;
    }

    public static CpcArrayMultiData buildSum(String key, double value, long timeStamp) {
        CpcArrayMultiData data = new CpcArrayMultiData();
        data.setKey(key);
        data.setTimestamp(timeStamp);
        data.setType(ModuleCommand.SUMARRAYADD);
        Map<String, Object> map = new HashMap<>();
        map.put(VALUE, value);
        data.setValues(map);
        return data;
    }

    public static CpcArrayMultiData buildMax(String key, double value, long timeStamp) {
        CpcArrayMultiData data = new CpcArrayMultiData();
        data.setKey(key);
        data.setTimestamp(timeStamp);
        data.setType(ModuleCommand.MAXARRAYADD);
        Map<String, Object> map = new HashMap<>();
        map.put(VALUE, value);
        data.setValues(map);
        return data;
    }

    public static CpcArrayMultiData buildMin(String key, double value, long timeStamp) {
        CpcArrayMultiData data = new CpcArrayMultiData();
        data.setKey(key);
        data.setTimestamp(timeStamp);
        data.setType(ModuleCommand.MINARRAYADD);
        Map<String, Object> map = new HashMap<>();
        map.put(VALUE, value);
        data.setValues(map);
        return data;
    }

    public static CpcArrayMultiData buildFirst(String key, String content, double value, long timeStamp) {
        CpcArrayMultiData data = new CpcArrayMultiData();
        data.setKey(key);
        data.setTimestamp(timeStamp);
        data.setType(ModuleCommand.FIRSTARRAYADD);
        Map<String, Object> map = new HashMap<>();
        map.put(CONTENT, content);
        map.put(VALUE, value);
        data.setValues(map);
        return data;
    }

    public static CpcArrayMultiData buildLast(String key, String content, double value, long timeStamp) {
        CpcArrayMultiData data = new CpcArrayMultiData();
        data.setKey(key);
        data.setTimestamp(timeStamp);
        data.setType(ModuleCommand.LASTARRAYADD);
        Map<String, Object> map = new HashMap<>();
        map.put(CONTENT, content);
        map.put(VALUE, value);
        data.setValues(map);
        return data;
    }

    public static CpcArrayMultiData buildAvg(String key, double value, long weight, long timeStamp) {
        CpcArrayMultiData data = new CpcArrayMultiData();
        data.setKey(key);
        data.setTimestamp(timeStamp);
        data.setType(ModuleCommand.AVGARRAYADD);
        Map<String, Object> map = new HashMap<>();
        map.put(WEIGHT, weight);
        map.put(VALUE, value);
        data.setValues(map);
        return data;
    }

    public static CpcArrayMultiData buildStddev(String key, double value, long timeStamp) {
        CpcArrayMultiData data = new CpcArrayMultiData();
        data.setKey(key);
        data.setTimestamp(timeStamp);
        data.setType(ModuleCommand.STDDEVARRAYADD);
        Map<String, Object> map = new HashMap<>();
        map.put(VALUE, value);
        data.setValues(map);
        return data;
    }
}