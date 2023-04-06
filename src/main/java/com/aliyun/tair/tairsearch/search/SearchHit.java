/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package com.aliyun.tair.tairsearch.search;

import com.google.gson.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A single search hit.
 *
 * @see SearchHits
 *
 */
public class SearchHit {
    public static final String ID = "_id";
    public static final String INDEX = "_index";
    public static final String SCORE = "_score";
    public static final String SOURCE = "_source";

    private final JsonObject searchHit;
    private final String id;
    private final float score;
    private final String index;
    private final JsonObject source;
    private Map<String, Object> sourceAsMap;

    public SearchHit(JsonObject in){
        searchHit = in;
        id = in.get(ID).getAsString();
        index = in.get(INDEX).getAsString();
        score = in.get(SCORE).getAsFloat();
        source = in.get(SOURCE).getAsJsonObject();
    }

    /**
     * The index of the hit.
     */
    public String getIndex() {
        return this.index;
    }

    /**
     * The id of the document.
     */
    public String getId() {
        return id;
    }

    /**
     * The score.
     */
    public float getScore() { return score; }

    /**
     * Is the source available or not. A source with no fields will return true. This will return false if {@code fields} doesn't contain
     * {@code _source} or if source is disabled in the mapping.
     */
    public boolean hasSource() { return source!=null; }

    /**
     * The source of the document as string (can be {@code null}).
     */
    public String getSourceAsString(){
        if(source.size() == 0){
            return null;
        }
        return source.toString();
    }

    /**
     * The source of the document as a map (can be {@code null}).
     */
    public Map<String, Object> getSourceAsMap() {
        if(source.size() == 0){
            return null;
        }
        if(sourceAsMap != null) {
            return sourceAsMap;
        }

        sourceAsMap = getJsonObjectAsMap(source);

        return sourceAsMap;
    }

    private Map<String, Object> getJsonObjectAsMap(JsonObject json)
    {
        Map<String, Object> result = new HashMap<>();
        for(Map.Entry<String, JsonElement> entry : json.entrySet())
        {
            JsonElement value = entry.getValue();
            if(value.isJsonPrimitive()) {
                JsonPrimitive v = (JsonPrimitive) value;
                if (v.isString()) {
                    result.put(entry.getKey(), value.getAsString());
                } else if (v.isNumber()) {
                    result.put(entry.getKey(), value.getAsNumber());
                } else if (v.isBoolean()) {
                    result.put(entry.getKey(), value.getAsBoolean());
                }
                else {
                    result.put(entry.getKey(), null);
                }
            }
            else if(value.isJsonObject()){
                result.put(entry.getKey(), getJsonObjectAsMap(value.getAsJsonObject()));
            }
        }
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SearchHit other = (SearchHit) obj;
        return Objects.equals(id, other.id)
                && Objects.equals(source, other.source)
                && Objects.equals(index, other.index);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                id,
                source,
                index
        );
    }

    @Override
    public String toString(){return searchHit.toString();}
}
