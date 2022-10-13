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

package com.aliyun.tair.tairsearch.search.aggregations;

import com.aliyun.tair.tairsearch.index.query.QueryBuilder;
import com.aliyun.tair.tairsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import com.aliyun.tair.tairsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import com.aliyun.tair.tairsearch.search.aggregations.metrics.*;

public class AggregationBuilders {

    private AggregationBuilders() {}

    /**
     * Create a new ValueCount aggregation with the given name.
     */
    public static ValueCountAggregationBuilder count(String name) {
        return new ValueCountAggregationBuilder(name);
    }

    /**
     * Create a new Avg aggregation with the given name.
     */
    public static AvgAggregationBuilder avg(String name) {
        return new AvgAggregationBuilder(name);
    }

    /**
     * Create a new Max aggregation with the given name.
     */
    public static MaxAggregationBuilder max(String name) {
        return new MaxAggregationBuilder(name);
    }

    /**
     * Create a new Min aggregation with the given name.
     */
    public static MinAggregationBuilder min(String name) {
        return new MinAggregationBuilder(name);
    }

    /**
     * Create a new Sum aggregation with the given name.
     */
    public static SumAggregationBuilder sum(String name) {
        return new SumAggregationBuilder(name);
    }

    /**
     * Create a new ExtendedStats aggregation with the given name.
     */
    public static ExtendedStatsAggregationBuilder extendedStats(String name) {
        return new ExtendedStatsAggregationBuilder(name);
    }

    /**
     * Create a new StdDeviation aggregation with the given name.
     */
    public static StdDeviationAggregationBuilder stdDeviation(String name) {
        return new StdDeviationAggregationBuilder(name);
    }

    /**
     * Create a new Variance aggregation with the given name.
     */
    public static VarianceAggregationBuilder variance(String name) {
        return new VarianceAggregationBuilder(name);
    }

    /**
     * Create a new SumOfSquares aggregation with the given name.
     */
    public static SumOfSquaresAggregationBuilder sumOfSquares(String name) {
        return new SumOfSquaresAggregationBuilder(name);
    }

    /**
     * Create a new Filter aggregation with the given name.
     */
    public static FilterAggregationBuilder filter(String name, QueryBuilder filter) {
        return new FilterAggregationBuilder(name, filter);
    }

    /**
     * Create a new Terms aggregation with the given name.
     */
    public static TermsAggregationBuilder terms(String name) {
        return new TermsAggregationBuilder(name);
    }


}
