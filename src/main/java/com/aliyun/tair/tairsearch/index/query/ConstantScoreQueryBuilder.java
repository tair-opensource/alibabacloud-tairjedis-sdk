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

package com.aliyun.tair.tairsearch.index.query;

import com.google.gson.JsonObject;

import java.util.Objects;

/**
 * A query that wraps a filter and simply returns a constant score equal to the
 * query boost for every document in the filter.
 *
 *
 */
public class ConstantScoreQueryBuilder extends AbstractQueryBuilder<ConstantScoreQueryBuilder> {
    public static final String NAME = "constant_score";

    private static final String FILTER_FIELD = "filter";
    private final QueryBuilder filterBuilder;

    /**
     * A query that wraps another query and simply returns a constant score equal to the
     * query boost for every document in the query.
     *
     * @param filterBuilder The query to wrap in a constant score query
     */
    public ConstantScoreQueryBuilder(QueryBuilder filterBuilder) {
        if (filterBuilder == null) {
            throw new IllegalArgumentException("inner clause [filter] cannot be null.");
        }
        this.filterBuilder = filterBuilder;
    }

    /**
     * @return the query that was wrapped in this constant score query
     */
    public QueryBuilder innerQuery() {
        return this.filterBuilder;
    }

    @Override
    public JsonObject constructJSON() {
        JsonObject newQueryObject = new JsonObject();
        newQueryObject.addProperty(AbstractQueryBuilder.BOOST,boost);
        newQueryObject.add(FILTER_FIELD, filterBuilder.constructJSON());
        JsonObject queryObject = new JsonObject();
        queryObject.add(NAME, newQueryObject);
        return queryObject;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(filterBuilder);
    }

    @Override
    protected boolean doEquals(ConstantScoreQueryBuilder other) {
        return Objects.equals(filterBuilder, other.filterBuilder);
    }
}
