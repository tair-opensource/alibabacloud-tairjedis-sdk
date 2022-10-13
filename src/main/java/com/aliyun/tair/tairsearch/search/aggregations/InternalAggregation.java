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

import java.util.List;
import java.util.Objects;

import com.aliyun.tair.tairsearch.search.aggregations.support.AggregationPath;

/**
 * An internal implementation of {@link Aggregation}. Serves as a base class for all aggregation implementations.
 *
 */
public abstract class InternalAggregation implements Aggregation {
    protected final String name;

    /**
     * Constructs an aggregation result with a given name.
     *
     * @param name The name of the aggregation.
     */
    protected InternalAggregation(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    /**
     * Get the value of specified path in the aggregation.
     *
     * @param path
     *            the path to the property in the aggregation tree
     * @return the value of the property
     */
    public Object getProperty(String path) {
        AggregationPath aggPath = AggregationPath.parse(path);
        return getProperty(aggPath.getPathElementsAsStringList());
    }

    public abstract Object getProperty(List<String> path);


    @Override
    public String getType() {
        return getWriteableName();
    }

    protected abstract String getWriteableName();

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        if (obj == this) {
            return true;
        }

        InternalAggregation other = (InternalAggregation) obj;
        return Objects.equals(name, other.name);
    }
}
