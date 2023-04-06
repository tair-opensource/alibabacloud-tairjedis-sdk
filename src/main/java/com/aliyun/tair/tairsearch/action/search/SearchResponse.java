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
import com.google.gson.JsonParser;

/**
 * A response of a search request.
 *
 *
 */
public class SearchResponse {
    private final String searchResponse;
    private final SearchResponseSections internalResponse;

    public SearchResponse(String in) {
        searchResponse = in;
        JsonObject result = JsonParser.parseString(in).getAsJsonObject();
        internalResponse = new SearchResponseSections(result);
    }

    public SearchResponseSections getInternalResponse() {
        return internalResponse;
    }

    /**
     * The search hits.
     */
    public SearchHits getHits() {
        return internalResponse.hits();
    }

    public Aggregations getAggregations() {
        return internalResponse.aggregations();
    }

    @Override
    public String toString(){
        return searchResponse;
    }

}
