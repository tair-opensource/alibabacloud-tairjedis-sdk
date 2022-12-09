package com.aliyun.tair.tairvector.params;

import redis.clients.jedis.Protocol;
import redis.clients.jedis.util.SafeEncoder;

import java.nio.ByteBuffer;
import java.util.*;

public class MIndexKnnsearchParams {
    private final static String EF_SEARCH = "ef_search";
    private final static String FILTER = "filter";

    private final Map<String, String> params = new HashMap<>();


    public MIndexKnnsearchParams() {
    }

    public MIndexKnnsearchParams efSearch(byte[] efsearch) {
        this.params.put(EF_SEARCH, SafeEncoder.encode(efsearch));
        return this;
    }

    public MIndexKnnsearchParams efSearch(String efsearch) {
        this.params.put(EF_SEARCH, efsearch);
        return this;
    }

    public MIndexKnnsearchParams filter(byte[] filter) {
        this.params.put(FILTER, SafeEncoder.encode(filter));
        return this;
    }

    public MIndexKnnsearchParams filter(String filter) {
        this.params.put(FILTER, filter);
        return this;
    }

    public byte[] getParams(){
        String result="{";
        if(params.isEmpty()){
            result+="}";
            return SafeEncoder.encode(result);
        }
        if(params.containsKey(FILTER)){
            result+=FILTER;
            result+=":";
            result+=params.get(FILTER);
            result+=",";
        }
        if(params.containsKey(EF_SEARCH)){
            result+=EF_SEARCH;
            result+=":";
            result+=params.get(EF_SEARCH);
            result+=",";
        }
        result = result.substring(0,result.length()-1)+"}";
        return SafeEncoder.encode(result);
    }

}
