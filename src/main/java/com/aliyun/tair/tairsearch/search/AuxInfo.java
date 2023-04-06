package com.aliyun.tair.tairsearch.search;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.HashMap;
import java.util.Map;

public class AuxInfo {
    public static final String INDEX_CRC64 = "index_crc64";
    public static final String KEYS_CURSOR = "keys_cursor";

    private final JsonObject auxInfo;
    private Map<String, Integer> keysCursors;
    private long crc64;

    public AuxInfo(JsonObject in) {
        auxInfo = in;
        crc64 = Long.parseUnsignedLong(in.get(INDEX_CRC64).getAsString());
        keysCursors = new HashMap<>();
        JsonObject cursorsJson = in.getAsJsonObject(KEYS_CURSOR);
        if(cursorsJson != null) {
            for (Map.Entry<String, JsonElement> entry : cursorsJson.entrySet())
            {
                JsonElement value = entry.getValue();
                if (value.isJsonPrimitive()) {
                    keysCursors.put(entry.getKey(), value.getAsInt());
                } else {
                    throw new IllegalArgumentException("Error aux_info format");
                }
            }
        }
    }

    public long getCrc64() {
        return crc64;
    }

    public Map<String, Integer> getKeysCursorsAsMap() {
        return keysCursors;
    }

}
