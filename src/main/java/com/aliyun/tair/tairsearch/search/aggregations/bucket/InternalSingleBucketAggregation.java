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

package com.aliyun.tair.tairsearch.search.aggregations.bucket;

import com.aliyun.tair.tairsearch.factory.AggregationRegisterFactory;
import com.aliyun.tair.tairsearch.search.aggregations.InternalAggregation;
import com.aliyun.tair.tairsearch.search.aggregations.InternalAggregations;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A base class for all the single bucket aggregations.
 *
 */
public abstract class InternalSingleBucketAggregation extends InternalAggregation implements SingleBucketAggregation {

    private long docCount;
    private InternalAggregations aggregations;
    private static String TYPE = "type";

    protected InternalSingleBucketAggregation(String name, JsonObject in) {
        super(name);
        List<InternalAggregation> aggregations = new ArrayList<>();
        for (Map.Entry<String, JsonElement> entry : in.entrySet()) {
            if("doc_count".equals(entry.getKey()))
            {
                this.docCount = entry.getValue().getAsLong();
            }
            else {
                String key = entry.getKey();
                JsonElement value = entry.getValue();
                if (value.isJsonObject()) {
                    String type = ((JsonObject) value).get(TYPE).getAsString();
                    aggregations.add(AggregationRegisterFactory.AggregationRegister.get(type).apply(key, (JsonObject) value));
                }
            }
        }
        this.aggregations = InternalAggregations.from(aggregations);
    }

//    /**
//     * Creates a single bucket aggregation.
//     *
//     * @param name          The aggregation name.
//     * @param docCount      The document count in the single bucket.
//     * @param aggregations  The already built sub-aggregations that are associated with the bucket.
//     */
//    protected InternalSingleBucketAggregation(String name, long docCount, InternalAggregations aggregations) {
//        super(name);
//        this.docCount = docCount;
//        this.aggregations = aggregations;
//    }

    @Override
    public long getDocCount() {
        return docCount;
    }

    @Override
    public InternalAggregations getAggregations() {
        return aggregations;
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else {
            String aggName = path.get(0);
            if ("_count".equals(aggName)) {
                if (path.size() > 1) {
                    throw new IllegalArgumentException("_count must be the last element in the path");
                }
                return getDocCount();
            }
            InternalAggregation aggregation = aggregations.get(aggName);
            if (aggregation == null) {
                throw new IllegalArgumentException("Cannot find an aggregation named [" + aggName + "] in [" + getName() + "]");
            }
            return aggregation.getProperty(path.subList(1, path.size()));
        }
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

        InternalSingleBucketAggregation other = (InternalSingleBucketAggregation) obj;
        return Objects.equals(docCount, other.docCount) && Objects.equals(aggregations, other.aggregations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), docCount, aggregations);
    }

}
