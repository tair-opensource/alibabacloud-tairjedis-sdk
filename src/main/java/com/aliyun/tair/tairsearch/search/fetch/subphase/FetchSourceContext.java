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

package com.aliyun.tair.tairsearch.search.fetch.subphase;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.util.Arrays;

/**
 * Context used to fetch the {@code _source}.
 *
 *
 */
public class FetchSourceContext {
    public static final String INCLUDES_FIELD = "includes";
    public static final String EXCLUDES_FIELD = "excludes";
    public static final String[] EMPTY_ARRAY = new String[0];

    public static final FetchSourceContext FETCH_SOURCE = new FetchSourceContext(true);
    public static final FetchSourceContext DO_NOT_FETCH_SOURCE = new FetchSourceContext(false);
    private final boolean fetchSource;
    private final String[] includes;
    private final String[] excludes;

    public FetchSourceContext(boolean fetchSource, String[] includes, String[] excludes) {
        this.fetchSource = fetchSource;
        this.includes = includes == null ? EMPTY_ARRAY : includes;
        this.excludes = excludes == null ? EMPTY_ARRAY : excludes;
    }

    public FetchSourceContext(boolean fetchSource) {
        this(fetchSource, EMPTY_ARRAY, EMPTY_ARRAY);
    }

    public boolean fetchSource() {
        return this.fetchSource;
    }

    public String[] includes() {
        return this.includes;
    }

    public String[] excludes() {
        return this.excludes;
    }

    public JsonObject constructJSON(){
        JsonObject valueObject = new JsonObject();
        JsonArray includeJSON = new JsonArray();
        for (String include : includes) {
            includeJSON.add(include);
        }
        JsonArray excludeJSON = new JsonArray();
        for (String exclude : excludes) {
            excludeJSON.add(exclude);
        }
        valueObject.add(INCLUDES_FIELD, includeJSON);
        valueObject.add(EXCLUDES_FIELD, excludeJSON);
        return valueObject;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FetchSourceContext that = (FetchSourceContext) o;

        if (fetchSource != that.fetchSource) {
            return false;
        }
        if (!Arrays.equals(excludes, that.excludes)) {
            return false;
        }
        if (!Arrays.equals(includes, that.includes)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = (fetchSource ? 1 : 0);
        result = 31 * result + (includes != null ? Arrays.hashCode(includes) : 0);
        result = 31 * result + (excludes != null ? Arrays.hashCode(excludes) : 0);
        return result;
    }

    @Override
    public String toString(){
        return constructJSON().toString();
    }

}
