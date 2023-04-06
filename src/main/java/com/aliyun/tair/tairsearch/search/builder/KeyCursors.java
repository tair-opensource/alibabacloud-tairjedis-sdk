package com.aliyun.tair.tairsearch.search.builder;

import com.google.gson.JsonObject;

import java.util.HashMap;
import java.util.Map;

public class KeyCursors {
    private Map<String, Integer> cursors;

    public KeyCursors() {
        cursors = new HashMap<>();
    }

    public KeyCursors add(String index, int cursor) {
        if(cursor < 0) {
            throw new IllegalArgumentException("[cursor] parameter in" + index + "cannot be negative");
        }
        cursors.put(index, cursor);
        return this;
    }

    public Map<String, Integer> get() {
        return cursors;
    }

    public Integer get(String index) {
        return cursors.get(index);
    }

    public JsonObject constructJSON() {
        JsonObject cursorsJson = new JsonObject();
        for(Map.Entry<String, Integer> entry : cursors.entrySet()){
            cursorsJson.addProperty(entry.getKey(), entry.getValue());
        }
        return cursorsJson;
    }

    @Override
    public String toString(){
        return constructJSON().toString();
    }
}
