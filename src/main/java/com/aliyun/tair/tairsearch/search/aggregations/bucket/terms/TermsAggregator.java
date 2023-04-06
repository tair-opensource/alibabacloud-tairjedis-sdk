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

import java.util.Objects;

/**
 * Base aggregator class to aggregate documents by terms
 *
 *
 */
public abstract class TermsAggregator {
    /**
     * Bucket count thresholds
     *
     *
     */
    public static class BucketCountThresholds {
        private long minDocCount;
        private int requiredSize;

        public BucketCountThresholds(long minDocCount, int requiredSize) {
            this.minDocCount = minDocCount;
            this.requiredSize = requiredSize;
        }


        public BucketCountThresholds(BucketCountThresholds bucketCountThresholds) {
            this(
                    bucketCountThresholds.minDocCount,
                    bucketCountThresholds.requiredSize
            );
        }

        /**
         * The minimum numbers of documents a bucket must have in order to
         * survive the final reduction.
         */
        public long getMinDocCount() {
            return minDocCount;
        }

        public void setMinDocCount(long minDocCount) {
            this.minDocCount = minDocCount;
        }

        public int getRequiredSize() {
            return requiredSize;
        }

        public void setRequiredSize(int requiredSize) {
            this.requiredSize = requiredSize;
        }

        @Override
        public int hashCode() {
            return Objects.hash(requiredSize, minDocCount);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            BucketCountThresholds other = (BucketCountThresholds) obj;
            return Objects.equals(requiredSize, other.requiredSize)
                    && Objects.equals(minDocCount, other.minDocCount);
        }
    }
}
