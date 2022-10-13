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

package com.aliyun.tair.tairsearch.search.aggregations.bucket.terms;

import com.aliyun.tair.tairsearch.search.aggregations.BucketOrder;
import com.aliyun.tair.tairsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.util.Objects;

/**
 * Aggregation Builder for terms agg
 *
 *
 */
public class TermsAggregationBuilder extends ValuesSourceAggregationBuilder<TermsAggregationBuilder> {
    public static final String NAME = "terms";

    public static final String FIELD_FIELD = "field";
    public static final String ORDER_FIELD = "order";
    public static final String MIN_DOC_COUNT_FIELD = "min_doc_count";
    public static final String INCLUDE_FIELD = "include";
    public static final String EXCLUDE_FIELD = "exclude";
    public static final String SIZE_FIELD = "size";

    static final TermsAggregator.BucketCountThresholds DEFAULT_BUCKET_COUNT_THRESHOLDS = new TermsAggregator.BucketCountThresholds(
            1,10
    );

    private BucketOrder order = BucketOrder.count(false); // automatically adds tie-breaker key asc order
    private IncludeExclude includeExclude = null;
    private TermsAggregator.BucketCountThresholds bucketCountThresholds = new TermsAggregator.BucketCountThresholds(
            DEFAULT_BUCKET_COUNT_THRESHOLDS
    );

    public TermsAggregationBuilder(String name) {
        super(name);
    }

    /**
     * Sets the size - indicating how many term buckets should be returned
     * (defaults to 10)
     */
    public TermsAggregationBuilder size(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("[size] must be greater than 0. Found [" + size + "] in [" + name + "]");
        }
        bucketCountThresholds.setRequiredSize(size);
        return this;
    }

    /**
     * Returns the number of term buckets currently configured
     */
    public int size() {
        return bucketCountThresholds.getRequiredSize();
    }

    /**
     * Set the minimum document count terms should have in order to appear in
     * the response.
     */
    public TermsAggregationBuilder minDocCount(long minDocCount) {
        if (minDocCount < 0) {
            throw new IllegalArgumentException(
                    "[minDocCount] must be greater than or equal to 0. Found [" + minDocCount + "] in [" + name + "]"
            );
        }
        bucketCountThresholds.setMinDocCount(minDocCount);
        return this;
    }

    /**
     * Returns the minimum document count required per term
     */
    public long minDocCount() {
        return bucketCountThresholds.getMinDocCount();
    }

    /** Set a new order on this builder and return the builder so that calls
     *  can be chained. A tie-breaker may be added to avoid non-deterministic ordering. */
    public TermsAggregationBuilder order(BucketOrder order) {
        if (order == null) {
            throw new IllegalArgumentException("[order] must not be null: [" + name + "]");
        }
        this.order = order;
        return this;
    }

    /**
     * Gets the order in which the buckets will be returned.
     */
    public BucketOrder order() {
        return order;
    }

    /**
     * Set terms to include and exclude from the aggregation results
     */
    public TermsAggregationBuilder includeExclude(IncludeExclude includeExclude) {
        this.includeExclude = includeExclude;
        return this;
    }

    /**
     * Get terms to include and exclude from the aggregation results
     */
    public IncludeExclude includeExclude() {
        return includeExclude;
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    protected JsonObject constructInternalJSON(){
        JsonObject termsObject = new JsonObject();
        if(field != null)
        {
            termsObject.addProperty(FIELD_FIELD, field);
        }
        termsObject.addProperty(SIZE_FIELD,size());
        termsObject.addProperty(MIN_DOC_COUNT_FIELD, minDocCount());
        if(includeExclude != null){
            if(includeExclude.include() != null) {
                termsObject.addProperty(INCLUDE_FIELD, includeExclude.include());
            }
            else if(includeExclude.includeValues().size()>0){
                JsonArray include = new JsonArray();
                for(String str : includeExclude.includeValues()){
                    include.add(str);
                }
                termsObject.add(INCLUDE_FIELD, include);
            }
            if(includeExclude.exclude() != null) {
                termsObject.addProperty(EXCLUDE_FIELD, includeExclude.exclude());
            }
            else if(includeExclude.excludeValues().size()>0){
                JsonArray exclude = new JsonArray();
                for(String str : includeExclude.excludeValues()){
                    exclude.add(str);
                }
                termsObject.add(EXCLUDE_FIELD, exclude);
            }
        }
        termsObject.add(ORDER_FIELD,order.constructJSON());
        return termsObject;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(),
                bucketCountThresholds,
                includeExclude,
                order
        );
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
        TermsAggregationBuilder other = (TermsAggregationBuilder) obj;
        return Objects.equals(bucketCountThresholds, other.bucketCountThresholds)
                && Objects.equals(includeExclude, other.includeExclude)
                && Objects.equals(order, other.order);
    }

}
