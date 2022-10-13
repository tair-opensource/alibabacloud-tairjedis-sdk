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

package com.aliyun.tair.tairsearch.search.aggregations.bucket.terms;

import com.aliyun.tair.tairsearch.search.aggregations.Aggregations;
import com.aliyun.tair.tairsearch.search.aggregations.InternalAggregation;
import com.aliyun.tair.tairsearch.search.aggregations.InternalAggregations;
import com.aliyun.tair.tairsearch.search.aggregations.InternalMultiBucketAggregation;
import com.aliyun.tair.tairsearch.factory.AggregationRegisterFactory;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Implementation of terms
 *
 */
public abstract class InternalTerms<A extends InternalTerms<A, B>, B extends InternalTerms.AbstractInternalBucket> extends
        InternalMultiBucketAggregation<A, B>
        implements
        Terms {

    /**
     * Base internal multi bucket
     *
     */
    public abstract static class AbstractInternalBucket extends InternalMultiBucketAggregation.InternalBucket implements Terms.Bucket { }

    /**
     * Base bucket class
     *
     */
    public abstract static class Bucket<B extends Bucket<B>> extends AbstractInternalBucket{

        protected long docCount;
        protected InternalAggregations aggregations;
        private static String TYPE = "type";

        protected Bucket(JsonObject in){
            List<InternalAggregation> aggregations = new ArrayList<>();
            for (Map.Entry<String, JsonElement> entry : in.entrySet()) {
                if("doc_count".equals(entry.getKey()))
                {
                    this.docCount = entry.getValue().getAsLong();
                }
                else if("key".equals(entry.getKey())){
                    continue;
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


        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Bucket<?> that = (Bucket<?>) obj;
            // No need to take format and showDocCountError, they are attributes
            // of the parent terms aggregation object that are only copied here
            // for serialization purposes
            return Objects.equals(docCount, that.docCount)
                    && Objects.equals(aggregations, that.aggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), docCount, aggregations);
        }
    }

    protected InternalTerms(
            String name
    ) {
        super(name);
    }



    @Override
    public abstract List<B> getBuckets();

    @Override
    public abstract B getBucketByKey(String term);


}
