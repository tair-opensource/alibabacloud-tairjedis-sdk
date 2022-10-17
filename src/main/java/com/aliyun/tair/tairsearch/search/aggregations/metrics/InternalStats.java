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

package com.aliyun.tair.tairsearch.search.aggregations.metrics;

import com.google.gson.JsonObject;

import java.util.Objects;

/**
 * Implementation of stats agg
 *
 */
public class InternalStats extends InternalNumericMetricsAggregation.MultiValue implements Stats {

    /**
     * The metrics for the internal stats
     *
     */
    enum Metrics {

        count,
        sum,
        min,
        max,
        avg;

        public static Metrics resolve(String name) {
            return Metrics.valueOf(name);
        }
    }

    protected final long count;
    protected final double min;
    protected final double max;
    protected final double sum;
    protected final double avg;

    public InternalStats(
            String name,
            JsonObject in
    ) {
        super(name, in);
        count = in.get(Metrics.count.name()).getAsLong();
        sum = in.get(Metrics.sum.name()).getAsDouble();
        min = in.get(Metrics.min.name()).getAsDouble();
        max = in.get(Metrics.max.name()).getAsDouble();
        avg = in.get(Metrics.avg.name()).getAsDouble();
    }

    @Override
    public String getWriteableName() {
        throw new IllegalArgumentException("not support stats");
    }

    @Override
    public long getCount() {
        return count;
    }

    @Override
    public double getMin() {
        return min;
    }

    @Override
    public double getMax() {
        return max;
    }

    @Override
    public double getAvg() {
        return avg;
    }

    @Override
    public double getSum() {
        return sum;
    }

    @Override
    public String getMinAsString() {
        return valueAsString(Metrics.min.name());
    }

    @Override
    public String getMaxAsString() {
        return valueAsString(Metrics.max.name());
    }

    @Override
    public String getAvgAsString() {
        return valueAsString(Metrics.avg.name());
    }

    @Override
    public String getSumAsString() {
        return valueAsString(Metrics.sum.name());
    }

    @Override
    public double value(String name) {
        Metrics metrics = Metrics.valueOf(name);
        switch (metrics) {
            case min:
                return this.min;
            case max:
                return this.max;
            case avg:
                return this.getAvg();
            case count:
                return this.count;
            case sum:
                return this.sum;
            default:
                throw new IllegalArgumentException("Unknown value [" + name + "] in common stats aggregation");
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), count, min, max, sum, avg);
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

        InternalStats other = (InternalStats) obj;
        return count == other.count
                && Double.compare(min, other.min) == 0
                && Double.compare(max, other.max) == 0
                && Double.compare(sum, other.sum) == 0
                && Double.compare(avg, other.avg) == 0;
    }
}
