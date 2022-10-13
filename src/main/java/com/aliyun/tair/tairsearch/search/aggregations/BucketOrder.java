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

/**
 * Bucket ordering strategy. Buckets can be order either as
 *
 *
 */
public abstract class BucketOrder {
    /**
     * Creates a bucket ordering strategy that sorts buckets by their document counts (ascending or descending).
     *
     * @param asc direction to sort by: {@code true} for ascending, {@code false} for descending.
     */
    public static BucketOrder count(boolean asc) {
        return asc ? InternalOrder.COUNT_ASC : InternalOrder.COUNT_DESC;
    }

    /**
     * Creates a bucket ordering strategy that sorts buckets by their keys (ascending or descending). This may be
     * used as a tie-breaker to avoid non-deterministic ordering.
     *
     * @param asc direction to sort by: {@code true} for ascending, {@code false} for descending.
     */
    public static BucketOrder key(boolean asc) {
        return asc ? InternalOrder.KEY_ASC : InternalOrder.KEY_DESC;
    }

    /**
     * Construct JSON object for this BucketOrder
     */
    public abstract JsonObject constructJSON();

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);

}
