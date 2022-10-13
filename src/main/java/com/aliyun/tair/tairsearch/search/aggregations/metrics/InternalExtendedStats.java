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

package com.aliyun.tair.tairsearch.search.aggregations.metrics;

import com.google.gson.JsonObject;

import java.util.Objects;

/**
 * Implementation of extended stats agg
 *
 */
public class InternalExtendedStats extends InternalStats implements ExtendedStats {

    enum Metrics {
        count,
        sum,
        min,
        max,
        avg,
        sum_of_squares,
        variance,
        std_deviation;

        public static Metrics resolve(String name) {
            return Metrics.valueOf(name);
        }
    }

    private final double sumOfSqrs;
    protected final double variance;
    protected final double stdDeviation;

    public InternalExtendedStats(
            String name,
            JsonObject in
    ) {
        super(name, in);
        variance = in.get(Metrics.variance.name()).getAsDouble();
        stdDeviation = in.get(Metrics.std_deviation.name()).getAsDouble();
        sumOfSqrs = in.get(Metrics.sum_of_squares.name()).getAsDouble();
    }

    @Override
    public String getWriteableName() {
        return ExtendedStatsAggregationBuilder.NAME;
    }

    @Override
    public double value(String name) {
        if ("sum_of_squares".equals(name)) {
            return sumOfSqrs;
        }
        if ("variance".equals(name)) {
            return variance;
        }
        if ("std_deviation".equals(name)) {
            return stdDeviation;
        }
        return super.value(name);
    }


    @Override
    public double getSumOfSquares() {
        return sumOfSqrs;
    }

    @Override
    public double getVariance() {
        return variance;
    }

    @Override
    public double getStdDeviation() {
        return stdDeviation;
    }

    @Override
    public String getSumOfSquaresAsString() {
        return valueAsString(Metrics.sum_of_squares.name());
    }

    @Override
    public String getVarianceAsString() {
        return valueAsString(Metrics.variance.name());
    }

    @Override
    public String getStdDeviationAsString() {
        return valueAsString(Metrics.std_deviation.name());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sumOfSqrs, variance, stdDeviation);
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

        InternalExtendedStats other = (InternalExtendedStats) obj;
        return Double.compare(sumOfSqrs, other.sumOfSqrs) == 0
                && Double.compare(variance, other.variance) == 0
                && Double.compare(stdDeviation, other.stdDeviation) == 0;
    }
}
