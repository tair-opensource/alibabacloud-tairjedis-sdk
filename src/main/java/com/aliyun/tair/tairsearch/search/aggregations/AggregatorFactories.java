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

import com.google.gson.JsonObject;

import java.util.*;

/**
 * An immutable collection of {@link AggregatorFactories}.
 *
 * @opensearch.internal
 */
public class AggregatorFactories {

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final Set<String> names = new HashSet<>();

        // Using LinkedHashSets to preserve the order of insertion, that makes the results
        // ordered nicely, although technically order does not matter
        private final Collection<AggregationBuilder> aggregationBuilders = new LinkedHashSet<>();

        /**
         * Create an empty builder.
         */
        public Builder() {}

        public Builder addAggregator(AggregationBuilder factory) {
            if (names.add(factory.getName()) == false) {
                throw new IllegalArgumentException(
                    "Two sibling aggregations cannot have the same name: [" + factory.getName() + "]");
            }
            aggregationBuilders.add(factory);
            return this;
        }

        public Collection<AggregationBuilder> getAggregatorFactories() {
            return Collections.unmodifiableCollection(aggregationBuilders);
        }

        public int count() {
            return aggregationBuilders.size();
        }

        /**
         * Construct JSON object for aggregations field
         */
        public JsonObject constructJSON()
        {
            JsonObject aggsObject = new JsonObject();
            if(aggregationBuilders != null){
                for (AggregationBuilder subAgg : aggregationBuilders) {
                    aggsObject.add(subAgg.getName(), subAgg.constructJSON());
                }
            }
            return aggsObject;
        }

        @Override
        public int hashCode() {
            return Objects.hash(aggregationBuilders);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Builder other = (Builder) obj;

            if (!Objects.equals(aggregationBuilders, other.aggregationBuilders)) {
                return false;
            }
            return true;
        }

        @Override
        public String toString(){
            return constructJSON().toString();
        }
    }
}
