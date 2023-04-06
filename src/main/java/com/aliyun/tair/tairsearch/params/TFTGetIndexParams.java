package com.aliyun.tair.tairsearch.params;

public class TFTGetIndexParams {
    private static byte[] MAPPINGS = "mappings".getBytes();
    private static byte[] SETTINGS = "settings".getBytes();

    private boolean mappings = false;
    private boolean settings = false;

    public TFTGetIndexParams mappings() {
        mappings = true;
        settings = false;
        return this;
    }

    public TFTGetIndexParams settings() {
        mappings = false;
        settings = true;
        return this;
    }

    public boolean isMappings() {
        return mappings;
    }

    public boolean isSettings() {
        return settings;
    }

    public byte[] getParams() {
        if(mappings) {
            return MAPPINGS;
        } else if(settings) {
            return SETTINGS;
        }
        return null;
    }

}
