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

package com.aliyun.tair.tairsearch.search.aggregations.bucket.filter;

import com.aliyun.tair.tairsearch.index.query.QueryBuilder;
import com.aliyun.tair.tairsearch.search.aggregations.AbstractAggregationBuilder;
import com.google.gson.JsonObject;

import java.util.Objects;

/**
 * Aggregation Builder for filter agg
 *
 *
 */
public class FilterAggregationBuilder extends AbstractAggregationBuilder<FilterAggregationBuilder> {
    public static final String NAME = "filter";

    private final QueryBuilder filter;

    /**
     * @param name
     *            the name of this aggregation
     * @param filter
     *            Set the filter to use, only documents that match this
     *            filter will fall into the bucket defined by this
     *            Filter aggregation.
     */
    public FilterAggregationBuilder(String name, QueryBuilder filter) {
        super(name);
        if (filter == null) {
            throw new IllegalArgumentException("[filter] must not be null: [" + name + "]");
        }
        this.filter = filter;
    }

    @Override
    public String getType() {
        return NAME;
    }

    public QueryBuilder getFilter() {
        return filter;
    }

    @Override
    protected JsonObject constructInternalJSON() {
        return filter.constructJSON();
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), filter);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        if (super.equals(obj) == false) {
            return false;
        }
        FilterAggregationBuilder other = (FilterAggregationBuilder) obj;
        return Objects.equals(filter, other.filter);
    }
}
