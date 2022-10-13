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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Common superclass for results of the terms aggregation on mapped fields.
 *
 */
public abstract class InternalMappedTerms<A extends InternalTerms<A, B>, B extends InternalTerms.Bucket<B>> extends InternalTerms<A, B> {
    protected final List<B> buckets;
    protected Map<String, B> bucketMap;

    protected InternalMappedTerms(
            String name,
            JsonObject in
    ) {
        super(name);
        List<B> buckets = new ArrayList<>();
        JsonArray bucketsArray = in.get("buckets").getAsJsonArray();
        for(int i = 0;i < bucketsArray.size();i++){
            buckets.add((B) new StringTerms.Bucket(bucketsArray.get(i).getAsJsonObject()));
        }

        this.buckets = buckets;
    }


    @Override
    public List<B> getBuckets() {
        return buckets;
    }

    @Override
    public B getBucketByKey(String term) {
        if (bucketMap == null) {
            bucketMap = buckets.stream().collect(Collectors.toMap(Bucket::getKeyAsString, Function.identity()));
        }
        return bucketMap.get(term);
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

        InternalMappedTerms<?, ?> that = (InternalMappedTerms<?, ?>) obj;
        return Objects.equals(buckets, that.buckets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), buckets);
    }
}
