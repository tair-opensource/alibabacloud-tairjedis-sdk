package com.aliyun.tair.tairsearch.params;

import io.valkey.util.SafeEncoder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


public class TFTGetSugParams {
    private final List<byte[]> paramsList = new ArrayList<>();
    private final boolean hasMaxCount = false;
    private boolean fuzzy = false;
    private Integer maxCount;

    public TFTGetSugParams fuzzy() {
        paramsList.add(SafeEncoder.encode("FUZZY"));
        fuzzy = true;
        return this;
    }

    public TFTGetSugParams maxCount(final Integer count) {
        paramsList.add(SafeEncoder.encode("MAX_COUNT"));
        paramsList.add(SafeEncoder.encode(count.toString()));
        maxCount = count;
        return this;
    }

    public Collection<byte[]> getParams() {
        return Collections.unmodifiableCollection(paramsList);
    }

    Integer maxCount() {
        if (hasMaxCount) {
            return maxCount;
        } else {
            return null;
        }
    }

    Boolean isFuzzy() {
        return fuzzy;
    }
}
