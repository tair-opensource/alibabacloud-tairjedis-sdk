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

package com.aliyun.tair.tairsearch.search.builder;

import com.aliyun.tair.tairsearch.common.Nullable;
import com.aliyun.tair.tairsearch.index.query.QueryBuilder;
import com.aliyun.tair.tairsearch.index.query.QueryBuilders;
import com.aliyun.tair.tairsearch.search.aggregations.AggregationBuilder;
import com.aliyun.tair.tairsearch.search.aggregations.AggregatorFactories;
import com.aliyun.tair.tairsearch.search.fetch.subphase.FetchSourceContext;
import com.aliyun.tair.tairsearch.search.sort.ScoreSortBuilder;
import com.aliyun.tair.tairsearch.search.sort.SortBuilder;
import com.aliyun.tair.tairsearch.search.sort.SortBuilders;
import com.aliyun.tair.tairsearch.search.sort.SortOrder;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.List;

public abstract class BaseSearchSourceBuilder<SSB extends BaseSearchSourceBuilder<SSB>> {
    public static final String SIZE_FIELD = "size";
    public static final String QUERY_FIELD = "query";
    public static final String SOURCE_FIELD = "_source";
    public static final String TRACK_TOTAL_HITS_FIELD = "track_total_hits";
    public static final String AGGREGATIONS_FIELD = "aggregations";
    public static final String AGGS_FIELD = "aggs";
    public static final String SORT_FIELD = "sort";

    protected QueryBuilder queryBuilder;

    protected int size = -1;

    protected boolean trackTotalHits = false;

    protected FetchSourceContext fetchSourceContext;

    protected List<SortBuilder<?>> sorts;

    protected AggregatorFactories.Builder aggregations;

    /**
     * Sets the search query for this request.
     *
     * @see QueryBuilders
     */
    public SSB query(QueryBuilder query) {
        this.queryBuilder = query;
        return (SSB)this;
    }

    /**
     * Gets the query for this request
     */
    public QueryBuilder query() {
        return queryBuilder;
    }

    /**
     * Indicates if the total hit count for the query should be tracked.
     */
    public SSB trackTotalHits(boolean trackTotalHits) {
        this.trackTotalHits = trackTotalHits;
        return (SSB)this;
    }

    public boolean trackTotalHits() {
        return trackTotalHits;
    }


    /**
     * The number of search hits to return. Defaults to {@code 10}.
     */
    public SSB size(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("[size] parameter cannot be negative, found [" + size + "]");
        }
        this.size = size;
        return (SSB)this;
    }

    /**
     * Gets the number of search hits to return.
     */
    public int size() {
        return size;
    }

    /**
     * Indicates whether the response should contain the stored _source for
     * every hit
     */
    public SSB fetchSource(boolean fetch) {
        FetchSourceContext fetchSourceContext = this.fetchSourceContext != null ? this.fetchSourceContext : FetchSourceContext.FETCH_SOURCE;
        this.fetchSourceContext = new FetchSourceContext(fetch, fetchSourceContext.includes(), fetchSourceContext.excludes());
        return (SSB) this;
    }

    /**
     * Indicate that _source should be returned with every hit, with an
     * "include" and/or "exclude" set which can include simple wildcard
     * elements.
     *
     * @param include
     *            An optional include (optionally wildcarded) pattern to filter
     *            the returned _source
     * @param exclude
     *            An optional exclude (optionally wildcarded) pattern to filter
     *            the returned _source
     */
    public SSB fetchSource(@Nullable String include, @Nullable String exclude) {
        return fetchSource(
            include == null ? FetchSourceContext.EMPTY_ARRAY : new String[] { include },
            exclude == null ? FetchSourceContext.EMPTY_ARRAY : new String[] { exclude }
        );
    }

    /**
     * Indicate that _source should be returned with every hit, with an
     * "include" and/or "exclude" set which can include simple wildcard
     * elements.
     *
     * @param includes
     *            An optional list of include (optionally wildcarded) pattern to
     *            filter the returned _source
     * @param excludes
     *            An optional list of exclude (optionally wildcarded) pattern to
     *            filter the returned _source
     */
    public SSB fetchSource(@Nullable String[] includes, @Nullable String[] excludes) {
        FetchSourceContext fetchSourceContext = this.fetchSourceContext != null ? this.fetchSourceContext : FetchSourceContext.FETCH_SOURCE;
        this.fetchSourceContext = new FetchSourceContext(fetchSourceContext.fetchSource(), includes, excludes);
        return (SSB)this;
    }

    /**
     * Indicate how the _source should be fetched.
     */
    public SSB fetchSource(@Nullable FetchSourceContext fetchSourceContext) {
        this.fetchSourceContext = fetchSourceContext;
        return (SSB)this;
    }

    /**
     * Gets the {@link FetchSourceContext} which defines how the _source should
     * be fetched.
     */
    public FetchSourceContext fetchSource() {
        return fetchSourceContext;
    }

    /**
     * Adds a sort against the given field name and the sort ordering.
     *
     * @param name
     *            The name of the field
     * @param order
     *            The sort ordering
     */
    public SSB sort(String name, SortOrder order) {
        if (name.equals(ScoreSortBuilder.NAME)) {
            return sort(SortBuilders.scoreSort().order(order));
        }
        return sort(SortBuilders.fieldSort(name).order(order));
    }

    /**
     * Add a sort against the given field name.
     *
     * @param name
     *            The name of the field to sort by
     */
    public SSB sort(String name) {
        if (name.equals(ScoreSortBuilder.NAME)) {
            return sort(SortBuilders.scoreSort());
        }
        return sort(SortBuilders.fieldSort(name));
    }

    /**
     * Adds a sort builder.
     */
    public SSB sort(SortBuilder<?> sort) {
        if (sorts == null) {
            sorts = new ArrayList<>();
        }
        sorts.add(sort);
        return (SSB)this;
    }

    /**
     * Gets the sort builders for this request.
     */
    public List<SortBuilder<?>> sorts() {
        return sorts;
    }

    /**
     * Add an aggregation to perform as part of the search.
     */
    public SSB aggregation(AggregationBuilder aggregation) {
        if (aggregations == null) {
            aggregations = AggregatorFactories.builder();
        }
        aggregations.addAggregator(aggregation);
        return (SSB)this;
    }

    /**
     * Gets the bytes representing the aggregation builders for this request.
     */
    public AggregatorFactories.Builder aggregations() {
        return aggregations;
    }

    public abstract JsonObject constructJSON();

    @Override
    public String toString(){
        return constructJSON().toString();
    }
}
