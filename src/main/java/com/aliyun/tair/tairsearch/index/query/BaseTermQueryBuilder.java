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

import java.util.Objects;

/**
 * Base class for a TermQueryBuilder
 *
 *
 */
public abstract class BaseTermQueryBuilder<QB extends BaseTermQueryBuilder<QB>> extends AbstractQueryBuilder<QB> {
    public static final String VALUE_FIELD = "value";

    /** Name of field to match against. */
    protected final String fieldName;

    /** Value to find matches for. */
    protected final Object value;

    /**
     * Constructs a new base term query.
     *
     * @param fieldName  The name of the field
     * @param value The value of the term
     */
    public BaseTermQueryBuilder(String fieldName, String value) {
        this(fieldName, (Object) value);
    }

    /**
     * Constructs a new base term query.
     *
     * @param fieldName  The name of the field
     * @param value The value of the term
     */
    public BaseTermQueryBuilder(String fieldName, int value) {
        this(fieldName, (Object) value);
    }

    /**
     * Constructs a new base term query.
     *
     * @param fieldName  The name of the field
     * @param value The value of the term
     */
    public BaseTermQueryBuilder(String fieldName, long value) {
        this(fieldName, (Object) value);
    }

    /**
     * Constructs a new base term query.
     *
     * @param fieldName  The name of the field
     * @param value The value of the term
     */
    public BaseTermQueryBuilder(String fieldName, float value) {
        this(fieldName, (Object) value);
    }

    /**
     * Constructs a new base term query.
     *
     * @param fieldName  The name of the field
     * @param value The value of the term
     */
    public BaseTermQueryBuilder(String fieldName, double value) {
        this(fieldName, (Object) value);
    }

    /**
     * Constructs a new base term query.
     *
     * @param fieldName  The name of the field
     * @param value The value of the term
     */
    public BaseTermQueryBuilder(String fieldName, boolean value) {
        this(fieldName, (Object) value);
    }

    /**
     * Constructs a new base term query.
     *
     * @param fieldName  The name of the field
     * @param value The value of the term
     */
    public BaseTermQueryBuilder(String fieldName, Object value) {
        if (fieldName == null || fieldName.length() == 0) {
            throw new IllegalArgumentException("field name is null or empty");
        }
        if (value == null) {
            throw new IllegalArgumentException("value cannot be null");
        }
        this.fieldName = fieldName;
        this.value = value;
    }

    /** Returns the field name used in this query. */
    public String fieldName() {
        return this.fieldName;
    }

    /**
     *  Returns the value used in this query.
     *
     */
    public Object value() {
        return this.value;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, value);
    }

    @Override
    protected boolean doEquals(QB other) {
        return Objects.equals(fieldName, other.fieldName) && Objects.equals(value, other.value);
    }
}
