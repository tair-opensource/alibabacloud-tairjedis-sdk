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
import java.util.SortedSet;

public class IncludeExclude {
    public static final String INCLUDE_FIELD = "include";
    public static final String EXCLUDE_FIELD = "exclude";
    private final String include, exclude;
    private final SortedSet<String> includeValues, excludeValues;

    /**
     * @param include   The string or regular expression pattern for the terms to be included
     * @param exclude   The string or regular expression pattern for the terms to be excluded
     */
    public IncludeExclude(String include, String exclude) {
        this.include = include;
        this.exclude = exclude;
        this.includeValues = null;
        this.excludeValues = null;
    }

    /**
     * @param includeValues   The terms to be included
     * @param excludeValues   The terms to be excluded
     */
    public IncludeExclude(SortedSet<String> includeValues, SortedSet<String> excludeValues) {
        if (includeValues == null && excludeValues == null) {
            throw new IllegalArgumentException();
        }
        this.include = null;
        this.exclude = null;
        this.includeValues = includeValues;
        this.excludeValues = excludeValues;
    }

    /**
     * @param includeValues   The terms to be included
     * @param exclude   The string or regular expression pattern for the terms to be excluded
     */
    public IncludeExclude(SortedSet<String> includeValues, String exclude) {
        if (includeValues == null) {
            throw new IllegalArgumentException();
        }
        this.include = null;
        this.exclude = exclude;
        this.includeValues = includeValues;
        this.excludeValues = null;
    }

    /**
     * @param include   The string or regular expression pattern for the terms to be included
     * @param excludeValues   The terms to be excluded
     */
    public IncludeExclude(String include, SortedSet<String> excludeValues) {
        if (excludeValues == null) {
            throw new IllegalArgumentException();
        }
        this.include = include;
        this.exclude = null;
        this.includeValues = null;
        this.excludeValues = excludeValues;
    }

    public String include() {return include;}
    public String exclude() {return exclude;}
    public SortedSet<String> includeValues() {return includeValues;}
    public SortedSet<String> excludeValues() {return excludeValues;}

    @Override
    public int hashCode() {
        return Objects.hash(include, exclude, includeValues, excludeValues);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        IncludeExclude other = (IncludeExclude) obj;
        return Objects.equals(include, other.include)
                && Objects.equals(exclude, other.exclude)
                && Objects.equals(includeValues, other.includeValues)
                && Objects.equals(excludeValues, other.excludeValues);
    }
}
