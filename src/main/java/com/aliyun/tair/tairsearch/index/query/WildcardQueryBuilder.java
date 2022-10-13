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

package com.aliyun.tair.tairsearch.index.query;

import com.google.gson.JsonObject;

import java.util.Objects;

/**
 * Implements the wildcard search query. Supported wildcards are {@code *}, which
 * matches any character sequence (including the empty one), and {@code ?},
 * which matches any single character. Note this query can be slow, as it
 * needs to iterate over many terms. In order to prevent extremely slow WildcardQueries,
 * a Wildcard term should not start with one of the wildcards {@code *} or
 * {@code ?}.
 *
 */
public class WildcardQueryBuilder extends AbstractQueryBuilder<WildcardQueryBuilder> implements MultiTermQueryBuilder {
    private static final String VALUE= "value";
    public static final String NAME = "wildcard";

    protected final String fieldName;
    protected final String value;

    /**
     * Implements the wildcard search query. Supported wildcards are {@code *}, which
     * matches any character sequence (including the empty one), and {@code ?},
     * which matches any single character. Note this query can be slow, as it
     * needs to iterate over many terms. In order to prevent extremely slow WildcardQueries,
     * a Wildcard term should not start with one of the wildcards {@code *} or
     * {@code ?}.
     *
     * @param fieldName The field name
     * @param value The wildcard query string
     */
    public WildcardQueryBuilder(String fieldName, String value) {
        if (fieldName == null || fieldName.length() == 0) {
            throw new IllegalArgumentException("field name is null or empty");
        }
        if (value == null) {
            throw new IllegalArgumentException("value cannot be null");
        }
        this.fieldName = fieldName;
        this.value = value;
    }

    @Override
    public String fieldName() {
        return fieldName;
    }

    public String value() {
        return value;
    }

    @Override
    public JsonObject constructJSON() {
        JsonObject valueObject = new JsonObject();
        valueObject.addProperty(BOOST,boost);
        valueObject.addProperty(VALUE,value);
        JsonObject newQueryObject = new JsonObject();
        newQueryObject.add(fieldName,valueObject);
        JsonObject queryObject = new JsonObject();
        queryObject.add(NAME,newQueryObject);
        return queryObject;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, value);
    }

    @Override
    protected boolean doEquals(WildcardQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
                && Objects.equals(value, other.value);
    }
}
