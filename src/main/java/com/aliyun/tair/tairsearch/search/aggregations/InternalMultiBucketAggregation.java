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

import com.aliyun.tair.tairsearch.search.aggregations.bucket.MultiBucketsAggregation;
import com.aliyun.tair.tairsearch.search.aggregations.bucket.SingleBucketAggregation;

import java.util.List;

/**
 * Base class for internal aggregations that are comprised of multiple buckets
 *
 */
public abstract class InternalMultiBucketAggregation<
        A extends InternalMultiBucketAggregation,
        B extends InternalMultiBucketAggregation.InternalBucket> extends InternalAggregation implements MultiBucketsAggregation {

    private static String BUCKEY_COUNT = "_bucket_count";
    private static String COUNT = "_count";
    private static String KEY = "_key";

    public InternalMultiBucketAggregation(String name) {
        super(name);
    }

    @Override
    public abstract List<B> getBuckets();

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        }
        return resolvePropertyFromPath(path, getBuckets(), getName());
    }

    static Object resolvePropertyFromPath(List<String> path, List<? extends InternalBucket> buckets, String name) {
        String aggName = path.get(0);
        if (BUCKEY_COUNT.equals(aggName)) {
            return buckets.size();
        }

        // This is a bucket key, look through our buckets and see if we can find a match
        if (aggName.startsWith("'") && aggName.endsWith("'")) {
            for (InternalBucket bucket : buckets) {
                if (bucket.getKeyAsString().equals(aggName.substring(1, aggName.length() - 1))) {
                    return bucket.getProperty(name, path.subList(1, path.size()));
                }
            }
            // No key match, time to give up
            throw new RuntimeException("Cannot find an key [" + aggName + "] in [" + name + "]");
        }

        Object[] propertyArray = new Object[buckets.size()];
        for (int i = 0; i < buckets.size(); i++) {
            propertyArray[i] = buckets.get(i).getProperty(name, path);
        }
        return propertyArray;

    }

    /**
     * Counts the number of inner buckets inside the provided {@link InternalBucket}
     */
    public static int countInnerBucket(InternalBucket bucket) {
        int count = 0;
        for (Aggregation agg : bucket.getAggregations().asList()) {
            count += countInnerBucket(agg);
        }
        return count;
    }

    /**
     * Counts the number of inner buckets inside the provided {@link Aggregation}
     */
    public static int countInnerBucket(Aggregation agg) {
        int size = 0;
        if (agg instanceof MultiBucketsAggregation) {
            MultiBucketsAggregation multi = (MultiBucketsAggregation) agg;
            for (MultiBucketsAggregation.Bucket bucket : multi.getBuckets()) {
                ++size;
                for (Aggregation bucketAgg : bucket.getAggregations().asList()) {
                    size += countInnerBucket(bucketAgg);
                }
            }
        } else if (agg instanceof SingleBucketAggregation) {
            SingleBucketAggregation single = (SingleBucketAggregation) agg;
            for (Aggregation bucketAgg : single.getAggregations().asList()) {
                size += countInnerBucket(bucketAgg);
            }
        }
        return size;
    }

    public abstract static class InternalBucket implements Bucket{

        public Object getProperty(String containingAggName, List<String> path) {
            if (path.isEmpty()) {
                return this;
            }
            Aggregations aggregations = getAggregations();
            String aggName = path.get(0);
            if (COUNT.equals(aggName)) {
                if (path.size() > 1) {
                    throw new RuntimeException("_count must be the last element in the path");
                }
                return getDocCount();
            } else if (KEY.equals(aggName)) {
                if (path.size() > 1) {
                    throw new RuntimeException("_key must be the last element in the path");
                }
                return getKey();
            }
            InternalAggregation aggregation = aggregations.get(aggName);
            if (aggregation == null) {
                throw new RuntimeException(
                        "Cannot find an aggregation named [" + aggName + "] in [" + containingAggName + "]"
                );
            }
            return aggregation.getProperty(path.subList(1, path.size()));
        }
    }


}
