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

import com.aliyun.tair.tairsearch.search.sort.SortOrder;
import com.google.gson.JsonObject;

import java.util.Locale;
import java.util.Objects;

/**
 * Implementations for Bucket ordering strategies.
 *
 *
 */
public abstract class InternalOrder extends BucketOrder {
    /**
     * {@link BucketOrder} implementation for simple, fixed orders like
     * {@link InternalOrder#COUNT_ASC}. Complex implementations should not
     * use this.
     *
     *
     */
    private static class SimpleOrder extends InternalOrder {
        private final String key;
        private final SortOrder order;

        SimpleOrder(
                String key,
                SortOrder order
        ) {
            this.key = key;
            this.order = order;
        }

        @Override
        public JsonObject constructJSON()
        {
            JsonObject orderObject = new JsonObject();
            orderObject.addProperty(key, order.toString().toLowerCase(Locale.ROOT));
            return orderObject;
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, order);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            SimpleOrder other = (SimpleOrder) obj;
            return Objects.equals(key, other.key) && Objects.equals(order, other.order);
        }
    }

    /**
     * Order by the (higher) count of each bucket.
     */
    static final InternalOrder COUNT_DESC = new SimpleOrder(
            "_count",
            SortOrder.DESC
    );

    /**
     * Order by the (lower) count of each bucket.
     */
    static final InternalOrder COUNT_ASC = new SimpleOrder(
            "_count",
            SortOrder.ASC
    );

    /**
     * Order by the key of each bucket descending.
     */
    static final InternalOrder KEY_DESC = new SimpleOrder(
            "_key",
            SortOrder.DESC
    );

    /**
     * Order by the key of each bucket ascending.
     */
    static final InternalOrder KEY_ASC = new SimpleOrder("_key", SortOrder.ASC);

    /**
     * Determine if the ordering strategy is sorting on bucket count descending.
     *
     * @param order bucket ordering strategy to check.
     * @return {@code true} if the ordering strategy is sorting on bucket count descending, {@code false} otherwise.
     */
    public static boolean isCountDesc(BucketOrder order) {
        return isOrder(order, COUNT_DESC);
    }

    /**
     * Determine if the ordering strategy is sorting on bucket key (ascending or descending).
     *
     * @param order bucket ordering strategy to check.
     * @return {@code true} if the ordering strategy is sorting on bucket key, {@code false} otherwise.
     */
    public static boolean isKeyOrder(BucketOrder order) {
        return isOrder(order, KEY_ASC) || isOrder(order, KEY_DESC);
    }

    /**
     * Determine if the ordering strategy is sorting on bucket key ascending.
     *
     * @param order bucket ordering strategy to check.
     * @return {@code true} if the ordering strategy is sorting on bucket key ascending, {@code false} otherwise.
     */
    public static boolean isKeyAsc(BucketOrder order) {
        return isOrder(order, KEY_ASC);
    }

    /**
     * Determine if the ordering strategy is sorting on bucket key descending.
     *
     * @param order bucket ordering strategy to check.
     * @return {@code true} if the ordering strategy is sorting on bucket key descending, {@code false} otherwise.
     */
    public static boolean isKeyDesc(BucketOrder order) {
        return isOrder(order, KEY_DESC);
    }

    /**
     * Determine if the ordering strategy matches the expected one.
     *
     * @param order    bucket ordering strategy to check.
     * @param expected expected  bucket ordering strategy.
     * @return {@code true} if the order matches, {@code false} otherwise.
     */
    private static boolean isOrder(BucketOrder order, BucketOrder expected) {
        if (order == expected) {
            return true;
        }
        return false;
    }

}
