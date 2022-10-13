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

import com.aliyun.tair.tairsearch.search.aggregations.InternalAggregation;

import java.util.List;

/**
 * Base aggregation to aggregate all docs into a single numeric metric
 *
 */
public abstract class InternalNumericMetricsAggregation extends InternalAggregation {

    /**
     * A single numeric metric value
     *
     */
    public abstract static class SingleValue extends InternalNumericMetricsAggregation implements NumericMetricsAggregation.SingleValue{
        protected SingleValue(String name) {
            super(name);
        }

        @Override
        public String getValueAsString() {
            return Double.toString(value());
        }

        @Override
        public Object getProperty(List<String> path) {
            if (path.isEmpty()) {
                return this;
            } else if (path.size() == 1 && "value".equals(path.get(0))) {
                return value();
            } else {
                throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
            }
        }

    }

    /**
     * Multe numeric metric values
     *
     */
    public abstract static class MultiValue extends InternalNumericMetricsAggregation implements NumericMetricsAggregation.MultiValue{
        protected MultiValue(String name) {
            super(name);
        }

        public abstract double value(String name);

        public String valueAsString(String name) {
            return Double.toString(value(name));
        }

        @Override
        public Object getProperty(List<String> path) {
            if (path.isEmpty()) {
                return this;
            } else if (path.size() == 1) {
                return value(path.get(0));
            } else {
                throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
            }
        }

    }

    private InternalNumericMetricsAggregation(String name) {
        super(name);
    }
    
}
