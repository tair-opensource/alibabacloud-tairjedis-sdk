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

import com.aliyun.tair.tairsearch.common.Nullable;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.util.Arrays;
import java.util.Objects;


/**
 * Encapsulates the results of a search operation
 *
 *
 */
public class SearchHits {
    public static final String HITS = "hits";
    public static final String TOTAL = "total";
    public static final String MAX_SCORE = "max_score";

    private final JsonObject searchHits;
    private final SearchHit[] hits;
    private final TotalHits totalHits;
    private float maxScore = 0;

    public SearchHits(JsonObject in){
        searchHits = in;
        JsonArray hitsJSON = in.getAsJsonArray(HITS);
        hits = new SearchHit[hitsJSON.size()];
        for(int i = 0; i < hitsJSON.size(); i++){
            hits[i]=new SearchHit(hitsJSON.get(i).getAsJsonObject());
        }
        totalHits = new TotalHits(in.getAsJsonObject(TOTAL));
        if(!in.get(MAX_SCORE).isJsonNull()) {
            maxScore = in.get(MAX_SCORE).getAsFloat();
        }
    }

    /**
     * The total number of hits for the query or null if the tracking of total hits
     * is disabled in the request.
     */
    @Nullable
    public TotalHits getTotalHits() {
        return totalHits;
    }

    /**
     * The maximum score of this query.
     */
    public float getMaxScore() {
        return maxScore;
    }

    /**
     * The hits of the search request (based on the search type, and from / size provided).
     */
    public SearchHit[] getHits() {
        return this.hits;
    }

    /**
     * Return the hit as the provided position.
     */
    public SearchHit getAt(int position) {
        return hits[position];
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SearchHits other = (SearchHits) obj;
        return Objects.equals(totalHits, other.totalHits)
                && Objects.equals(maxScore, other.maxScore)
                && Arrays.equals(hits, other.hits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                totalHits,
                maxScore,
                Arrays.hashCode(hits)
        );
    }

    @Override
    public String toString()
    {
        return searchHits.toString();
    }
}
