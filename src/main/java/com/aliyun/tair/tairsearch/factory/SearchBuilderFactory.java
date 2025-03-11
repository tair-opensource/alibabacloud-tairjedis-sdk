package com.aliyun.tair.tairsearch.factory;

import com.aliyun.tair.tairsearch.action.search.SearchResponse;
import io.valkey.Builder;
import io.valkey.util.SafeEncoder;

public class SearchBuilderFactory {
    public static final Builder<SearchResponse> SEARCH_RESPONSE = new Builder<SearchResponse>() {
        @Override
        public SearchResponse build(Object data) {
            if (data == null) {
                return null;
            }
            String in = SafeEncoder.encode((byte[])data);
            return new SearchResponse(in);
        }

        @Override
        public String toString() {
            return "SearchResponse";
        }
    };

}
