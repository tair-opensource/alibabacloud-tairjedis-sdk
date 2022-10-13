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
 * Implementation of min agg
 *
 */
public class InternalMin extends InternalNumericMetricsAggregation.SingleValue implements Min {
    private final double min;

    public InternalMin(String name, JsonObject in) {
        super(name);
        this.min = in.get("value").getAsDouble();
    }

    @Override
    public String getWriteableName() {
        return MinAggregationBuilder.NAME;
    }

    @Override
    public double value() {
        return min;
    }

    @Override
    public double getValue() {
        return min;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), min);
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
        InternalMin other = (InternalMin) obj;
        return Objects.equals(min, other.min);
    }
}

