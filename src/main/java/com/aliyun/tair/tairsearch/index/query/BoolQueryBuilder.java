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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 * A Query that matches documents matching boolean combinations of other queries.
 *
 *
 */
public class BoolQueryBuilder extends AbstractQueryBuilder<BoolQueryBuilder> {
    public static final String NAME = "bool";

    public static final int DEFAULT_MINIMUM_SHOULD_MATCH = 0;

    private static final String MUST_NOT = "must_not";
    private static final String SHOULD = "should";
    private static final String MUST = "must";
    private static final String MINIMUM_SHOULD_MATCH = "minimum_should_match";
    private final List<QueryBuilder> mustClauses = new ArrayList<>();
    private final List<QueryBuilder> mustNotClauses = new ArrayList<>();
    private final List<QueryBuilder> shouldClauses = new ArrayList<>();
    private int minimumShouldMatch = DEFAULT_MINIMUM_SHOULD_MATCH;

    /**
     * Build an empty bool query.
     */
    public BoolQueryBuilder() {}

    /**
     * Adds a query that <b>must</b> appear in the matching documents and will
     * contribute to scoring. No {@code null} value allowed.
     */
    public BoolQueryBuilder must(QueryBuilder queryBuilder) {
        if (queryBuilder == null) {
            throw new IllegalArgumentException("inner bool query clause cannot be null");
        }
        mustClauses.add(queryBuilder);
        return this;
    }

    /**
     * Gets the queries that <b>must</b> appear in the matching documents.
     */
    public List<QueryBuilder> must() {
        return this.mustClauses;
    }

    /**
     * Adds a query that <b>must not</b> appear in the matching documents.
     * No {@code null} value allowed.
     */
    public BoolQueryBuilder mustNot(QueryBuilder queryBuilder) {
        if (queryBuilder == null) {
            throw new IllegalArgumentException("inner bool query clause cannot be null");
        }
        mustNotClauses.add(queryBuilder);
        return this;
    }

    /**
     * Gets the queries that <b>must not</b> appear in the matching documents.
     */
    public List<QueryBuilder> mustNot() {
        return this.mustNotClauses;
    }

    /**
     * Adds a clause that <i>should</i> be matched by the returned documents. For a boolean query with no
     * {@code MUST} clauses one or more <code>SHOULD</code> clauses must match a document
     * for the BooleanQuery to match. No {@code null} value allowed.
     *
     * @see #minimumShouldMatch(int)
     */
    public BoolQueryBuilder should(QueryBuilder queryBuilder) {
        if (queryBuilder == null) {
            throw new IllegalArgumentException("inner bool query clause cannot be null");
        }
        shouldClauses.add(queryBuilder);
        return this;
    }

    /**
     * Gets the list of clauses that <b>should</b> be matched by the returned documents.
     *
     * @see #should(QueryBuilder)
     *  @see #minimumShouldMatch(int)
     */
    public List<QueryBuilder> should() {
        return this.shouldClauses;
    }

    /**
     * @return the number of the minimumShouldMatch settings for this query
     */
    public int minimumShouldMatch() {
        return this.minimumShouldMatch;
    }

    /**
     * Specifies a minimum number of the optional (should) boolean clauses which must be satisfied.
     * <p>
     * By default no optional clauses are necessary for a match
     * (unless there are no required clauses).  If this method is used,
     * then the specified number of clauses is required.
     * <p>
     * Use of this method is totally independent of specifying that
     * any specific clauses are required (or prohibited).  This number will
     * only be compared against the number of matching optional clauses.
     *
     * @param minimumShouldMatch the number of optional clauses that must match
     */
    public BoolQueryBuilder minimumShouldMatch(int minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
        return this;
    }

    /**
     * Returns <code>true</code> iff this query builder has at least one should, must, must not or filter clause.
     * Otherwise <code>false</code>.
     */
    public boolean hasClauses() {
        return !(mustClauses.isEmpty() && shouldClauses.isEmpty() && mustNotClauses.isEmpty());
    }

    @Override
    public JsonObject constructJSON() {
        JsonObject newQueryObject = new JsonObject();
        constructJSONArray(MUST, mustClauses, newQueryObject);
        constructJSONArray(MUST_NOT, mustNotClauses, newQueryObject);
        constructJSONArray(SHOULD, shouldClauses, newQueryObject);
        newQueryObject.addProperty(MINIMUM_SHOULD_MATCH, minimumShouldMatch);
        JsonObject queryObject = new JsonObject();
        queryObject.add(NAME, newQueryObject);
        return queryObject;
//        JSONObject newQueryObject = new JSONObject();
//        constructJSONArray(MUST, mustClauses, newQueryObject);
//        constructJSONArray(MUST_NOT, mustNotClauses, newQueryObject);
//        constructJSONArray(SHOULD, shouldClauses, newQueryObject);
//        newQueryObject.put(MINIMUM_SHOULD_MATCH, minimumShouldMatch);
//        JSONObject queryObject = new JSONObject();
//        queryObject.put(NAME, newQueryObject);
//        return queryObject;
    }

    private static void constructJSONArray(String field, List<QueryBuilder> queryBuilders, JsonObject builder){
        JsonArray queriesJSON = new JsonArray();
        for(QueryBuilder qb : queryBuilders)
        {
            queriesJSON.add(qb.constructJSON());
        }
        builder.add(field, queriesJSON);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(minimumShouldMatch, mustClauses, shouldClauses, mustNotClauses);
    }

    @Override
    protected boolean doEquals(BoolQueryBuilder other) {
        return Objects.equals(minimumShouldMatch, other.minimumShouldMatch)
                && Objects.equals(mustClauses, other.mustClauses)
                && Objects.equals(shouldClauses, other.shouldClauses)
                && Objects.equals(mustNotClauses, other.mustNotClauses);
    }
}
