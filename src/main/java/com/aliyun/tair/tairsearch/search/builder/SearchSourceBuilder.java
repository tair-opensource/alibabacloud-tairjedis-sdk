package com.aliyun.tair.tairsearch.search.builder;

import com.aliyun.tair.tairsearch.search.sort.SortBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class SearchSourceBuilder extends BaseSearchSourceBuilder<SearchSourceBuilder>{
    public static final String FROM_FIELD = "from";

    private int from = -1;

    /**
     * A static factory method to construct a new search source.
     */
    public static SearchSourceBuilder searchSource() {
        return new SearchSourceBuilder();
    }

    /**
     * Constructs a new search source builder.
     */
    public SearchSourceBuilder() {}

    /**
     * From index to start the search from. Defaults to {@code 0}.
     */
    public SearchSourceBuilder from(int from) {
        if (from < 0) {
            throw new IllegalArgumentException("[from] parameter cannot be negative");
        }
        this.from = from;
        return this;
    }

    /**
     * Gets the from index to start the search from.
     **/
    public int from() {
        return from;
    }

    @Override
    public JsonObject constructJSON(){
        JsonObject queryObject = new JsonObject();
        if (size != -1) {
            queryObject.addProperty(SIZE_FIELD, size);
        }
        if (from != -1) {
            queryObject.addProperty(FROM_FIELD, from);
        }
        if(trackTotalHits){
            queryObject.addProperty(TRACK_TOTAL_HITS_FIELD, trackTotalHits);
        }
        if (queryBuilder != null) {
            queryObject.add(QUERY_FIELD, queryBuilder.constructJSON());
        }
        if (fetchSourceContext != null) {
            queryObject.add(SOURCE_FIELD, fetchSourceContext.constructJSON());
        }
        if(sorts != null) {
            JsonArray sortArray = new JsonArray();
            for (SortBuilder<?> sort : sorts) {
                sortArray.add(sort.constructJSON());
            }
            queryObject.add(SORT_FIELD, sortArray);
        }
        if(aggregations != null) {
            queryObject.add(AGGS_FIELD, aggregations.constructJSON());
        }
        return queryObject;
    }
}
