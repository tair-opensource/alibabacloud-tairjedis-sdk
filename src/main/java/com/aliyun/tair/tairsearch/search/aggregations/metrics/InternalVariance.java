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

public class InternalVariance extends InternalNumericMetricsAggregation.SingleValue implements com.aliyun.tair.tairsearch.search.aggregations.metrics.Variance {
    private final double Variance;

    public InternalVariance(String name, JsonObject in) {
        super(name);
        this.Variance = in.get("value").getAsDouble();
    }

    @Override
    public String getWriteableName() {
        return VarianceAggregationBuilder.NAME;
    }

    @Override
    public double value() {
        return Variance;
    }

    @Override
    public double getValue() {
        return Variance;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), Variance);
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

        InternalVariance that = (InternalVariance) obj;
        return Objects.equals(this.Variance, that.Variance);
    }
}