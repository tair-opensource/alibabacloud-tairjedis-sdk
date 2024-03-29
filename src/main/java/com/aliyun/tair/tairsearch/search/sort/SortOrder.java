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

package com.aliyun.tair.tairsearch.search.sort;

import java.util.Comparator;
import java.util.Locale;

public enum SortOrder {
    /**
     * Ascending order.
     */
    ASC {
        @Override
        public String toString() {
            return "asc";
        }

        @Override
        public int reverseMul() {
            return 1;
        }

        @Override
        public <T> Comparator<T> wrap(Comparator<T> delegate) {
            return delegate;
        }
    },
    /**
     * Descending order.
     */
    DESC {
        @Override
        public String toString() {
            return "desc";
        }

        @Override
        public int reverseMul() {
            return -1;
        }

        @Override
        public <T> Comparator<T> wrap(Comparator<T> delegate) {
            return delegate.reversed();
        }
    };

    public static SortOrder fromString(String op) {
        return valueOf(op.toUpperCase(Locale.ROOT));
    }

    /**
     * -1 if the sort is reversed from the standard comparators, 1 otherwise.
     */
    public abstract int reverseMul();

    /**
     * Wrap a comparator in one for this direction.
     */
    public abstract <T> Comparator<T> wrap(Comparator<T> delegate);
}

