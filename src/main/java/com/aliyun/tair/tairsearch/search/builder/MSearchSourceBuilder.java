package com.aliyun.tair.tairsearch.search.builder;

import com.aliyun.tair.tairsearch.search.sort.SortBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class MSearchSourceBuilder extends BaseSearchSourceBuilder<MSearchSourceBuilder>{
    public static final String REPLY_FIELD = "reply_with_keys_cursor";
    public static final String CURSORS_FIELD = "keys_cursor";

    private Boolean replyWithKeysCursor = false;

    private KeyCursors keysCursors;

    /**
     * A static factory method to construct a new msearch source.
     */
    public static MSearchSourceBuilder searchSource() {
        return new MSearchSourceBuilder();
    }

    /**
     * Constructs a new msearch source builder.
     */
    public MSearchSourceBuilder() {}

    /**
     * sets the reply_with_keys_cursor for this request. Default to {@code false}.
     */
    public MSearchSourceBuilder replyWithKeysCursor(boolean replyWithKeysCursor) {
        this.replyWithKeysCursor = replyWithKeysCursor;
        return this;
    }

    /**
     * Gets the reply_with_keys_cursor for this request
     **/
    public boolean replyWithKeysCursor() {
        return replyWithKeysCursor;
    }

    /**
     * sets the reply_with_keys_cursor for this request. Default to {@code false}.
     */
    public MSearchSourceBuilder keysCursors(KeyCursors keysCursors) {
        this.keysCursors = keysCursors;
        return this;
    }

    /**
     * Gets the reply_with_keys_cursor for this request
     **/
    public KeyCursors keysCursors() {
        return keysCursors;
    }

    @Override
    public JsonObject constructJSON(){
        JsonObject queryObject = new JsonObject();
        if(size != -1) {
            queryObject.addProperty(SIZE_FIELD, size);
        }
        if(trackTotalHits){
            queryObject.addProperty(TRACK_TOTAL_HITS_FIELD, trackTotalHits);
        }
        if(queryBuilder != null) {
            queryObject.add(QUERY_FIELD, queryBuilder.constructJSON());
        }
        if(fetchSourceContext != null) {
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

        if(replyWithKeysCursor != false) {
            queryObject.addProperty(REPLY_FIELD, replyWithKeysCursor);
        }

        if(keysCursors != null) {
            queryObject.add(CURSORS_FIELD, keysCursors.constructJSON());
        }

        return queryObject;
    }
}
