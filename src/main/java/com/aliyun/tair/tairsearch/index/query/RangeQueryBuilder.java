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

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.util.Objects;

/**
 * A Query that matches documents within an range of terms.
 *
 *
 */
public class RangeQueryBuilder extends AbstractQueryBuilder<RangeQueryBuilder> implements MultiTermQueryBuilder {
    public static final String NAME = "range";

    protected final String fieldName;

    private static final String GTE = "gte";
    private static final String GT = "gt";
    private static final String LTE = "lte";
    private static final String LT = "lt";

    private Object from = null;
    private boolean includeLower = true;
    private boolean includeUpper = true;
    private Object to = null;

    /**
     * A Query that matches documents within an range of terms.
     *
     * @param fieldName The field name
     */
    public RangeQueryBuilder(String fieldName) {
        if (fieldName == null || fieldName.length() == 0) {
            throw new IllegalArgumentException("field name is null or empty");
        }
        this.fieldName = fieldName;
    }

    /**
     * Get the field name for this query.
     */
    @Override
    public String fieldName() {
        return this.fieldName;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     *
     */
    public RangeQueryBuilder from(Object from, boolean includeLower) {
        this.from = from;
        this.includeLower = includeLower;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder from(Object from) {
        return from(from, this.includeLower);
    }

    /**
     * Gets the lower range value for this query.
     */
    public Object from() {
        return this.from;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder gt(Object from) {
        return from(from, false);
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder gte(Object from) {
        return from(from, true);
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder to(Object to, boolean includeUpper) {
        this.to = to;
        this.includeUpper = includeUpper;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder to(Object to) {
        return to(to, this.includeUpper);
    }

    /**
     * Gets the upper range value for this query.
     *
     */
    public Object to() {
        return this.to;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder lt(Object to) {
        return to(to, false);
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder lte(Object to) {
        return to(to, true);
    }

    /**
     * Should the lower bound be included or not. Defaults to {@code true}.
     */
    public RangeQueryBuilder includeLower(boolean includeLower) {
        this.includeLower = includeLower;
        return this;
    }

    /**
     * Gets the includeLower flag for this query.
     */
    public boolean includeLower() {
        return this.includeLower;
    }

    /**
     * Should the upper bound be included or not. Defaults to {@code true}.
     */
    public RangeQueryBuilder includeUpper(boolean includeUpper) {
        this.includeUpper = includeUpper;
        return this;
    }

    /**
     * Gets the includeUpper flag for this query.
     */
    public boolean includeUpper() {
        return this.includeUpper;
    }

    @Override
    public JsonObject constructJSON() {
        JsonObject valueObject = new JsonObject();
        Gson gson = new Gson();
        if(from != null){
            if(includeLower) {
                valueObject.add(GTE, gson.toJsonTree(from));
            }
            else{
                valueObject.add(GT, gson.toJsonTree(from));
            }
        }
        if(to != null){
            if(includeUpper) {
                valueObject.add(LTE, gson.toJsonTree(to));
            }
            else{
                valueObject.add(LT, gson.toJsonTree(to));
            }
        }
        valueObject.addProperty(BOOST,boost);
        JsonObject queryObject = new JsonObject();
        queryObject.add(fieldName,valueObject);
        JsonObject newQueryObject = new JsonObject();
        newQueryObject.add(NAME,queryObject);
        return newQueryObject;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, from, to, includeLower, includeUpper);
    }

    @Override
    protected boolean doEquals(RangeQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
                && Objects.equals(from, other.from)
                && Objects.equals(to, other.to)
                && Objects.equals(includeLower, other.includeLower)
                && Objects.equals(includeUpper, other.includeUpper);
    }
}
