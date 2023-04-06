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

import com.aliyun.tair.tairsearch.factory.AggregationRegisterFactory;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.*;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

/**
 * Represents a set of {@link Aggregation}s
 *
 */
public class Aggregations implements Iterable<Aggregation>{

    public static final String AGGREGATIONS_FIELD = "aggregations";
    public static final String TYPE_FIELD = "type";

    protected final List<? extends Aggregation> aggregations;
    private Map<String, Aggregation> aggregationsAsMap;
    private JsonObject aggregationsJsonObject;

    public Aggregations(List<? extends Aggregation> aggregations) {
        this.aggregations = aggregations;
        if (aggregations.isEmpty()) {
            aggregationsAsMap = emptyMap();
        }
    }

    public Aggregations(JsonObject in) {
        aggregationsJsonObject = in;
        List<Aggregation> aggregations = new ArrayList<>();
        for (Map.Entry<String, JsonElement> entry : in.entrySet()) {
            String key = entry.getKey();
            JsonElement value = entry.getValue();
            if(value.isJsonObject()){
                String type = ((JsonObject) value).get(TYPE_FIELD).getAsString();
                aggregations.add(AggregationRegisterFactory.AggregationRegister.get(type).apply(key, (JsonObject)value));
            }
        }
        this.aggregations = aggregations;
        if (aggregations.isEmpty()) {
            aggregationsAsMap = emptyMap();
        }
    }

    /**
     * Iterates over the {@link Aggregation}s.
     */
    @Override
    public final Iterator<Aggregation> iterator() {
        return aggregations.stream().map((p) -> (Aggregation) p).iterator();
    }

    /**
     * The list of {@link Aggregation}s.
     */
    public final List<Aggregation> asList() {
        return Collections.unmodifiableList(aggregations);
    }

    /**
     * Returns the {@link Aggregation}s keyed by aggregation name.
     */
    public final Map<String, Aggregation> asMap() {
        return getAsMap();
    }

    /**
     * Returns the {@link Aggregation}s keyed by aggregation name.
     */
    public final Map<String, Aggregation> getAsMap() {
        if (aggregationsAsMap == null) {
            Map<String, Aggregation> newAggregationsAsMap = new HashMap<>(aggregations.size());
            for (Aggregation aggregation : aggregations) {
                newAggregationsAsMap.put(aggregation.getName(), aggregation);
            }
            this.aggregationsAsMap = unmodifiableMap(newAggregationsAsMap);
        }
        return aggregationsAsMap;
    }

    /**
     * Returns the aggregation that is associated with the specified name.
     */
    @SuppressWarnings("unchecked")
    public final <A extends Aggregation> A get(String name) {
        return (A) asMap().get(name);
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        return aggregations.equals(((Aggregations) obj).aggregations);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(getClass(), aggregations);
    }

    @Override
    public String toString(){
        return aggregationsJsonObject.toString();
    }
}
