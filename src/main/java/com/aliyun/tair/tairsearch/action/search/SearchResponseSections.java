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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package com.aliyun.tair.tairsearch.action.search;

import com.aliyun.tair.tairsearch.search.SearchHits;
import com.aliyun.tair.tairsearch.search.aggregations.Aggregations;
import com.google.gson.JsonObject;

/**
 * Base class that holds the various sections which a search response is
 * composed of (hits, aggs, suggestions etc.) and allows to retrieve them.
 *
 * The reason why this class exists is that the high level REST client uses its own classes
 * to parse aggregations into, which are not serializable. This is the common part that can be
 * shared between core and client.
 *
 */
public class SearchResponseSections {
    public static final String HITS="hits";
    public static final String AGGS="aggregations";
    private final JsonObject searchResponseSections;
    protected SearchHits hits;
    protected Aggregations aggregations;
    public SearchResponseSections(JsonObject in){
        searchResponseSections = in;
        if(in.get(HITS) != null) {
            this.hits = new SearchHits(in.getAsJsonObject(HITS));
        }
        if(in.get(AGGS) != null) {
            this.aggregations = new Aggregations(in.getAsJsonObject(AGGS));
        }
    }
    public final SearchHits hits() {
        return hits;
    }
    public final Aggregations aggregations() {
        return aggregations;
    }
    @Override
    public String toString(){return searchResponseSections.toString();}


}
