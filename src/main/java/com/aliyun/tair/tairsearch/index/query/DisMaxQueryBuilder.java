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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


/**
 * A query that generates the union of documents produced by its sub-queries, and that scores each document
 * with the maximum score for that document as produced by any sub-query, plus a tie breaking increment for any
 * additional matching sub-queries.
 *
 *
 */
public class DisMaxQueryBuilder extends AbstractQueryBuilder<DisMaxQueryBuilder> {
    public static final String NAME = "dis_max";

    /** Default multiplication factor for breaking ties in document scores.*/
    public static final float DEFAULT_TIE_BREAKER = 0.0f;

    private static final String TIE_BREAKER_FIELD = "tie_breaker";
    private static final String QUERIES_FIELD = "queries";

    private final List<QueryBuilder> queries = new ArrayList<>();
    private float tieBreaker = DEFAULT_TIE_BREAKER;

    public DisMaxQueryBuilder() {}

    /**
     * Add a sub-query to this disjunction.
     */
    public DisMaxQueryBuilder add(QueryBuilder queryBuilder) {
        if (queryBuilder == null) {
            throw new IllegalArgumentException("inner dismax query clause cannot be null");
        }
        queries.add(queryBuilder);
        return this;
    }

    /**
     * @return an immutable list copy of the current sub-queries of this disjunction
     */
    public List<QueryBuilder> innerQueries() {
        return this.queries;
    }

    /**
     * The score of each non-maximum disjunct for a document is multiplied by this weight
     * and added into the final score.  If non-zero, the value should be small, on the order of 0.1, which says that
     * 10 occurrences of word in a lower-scored field that is also in a higher scored field is just as good as a unique
     * word in the lower scored field (i.e., one that is not in any higher scored field.
     */
    public DisMaxQueryBuilder tieBreaker(float tieBreaker) {
        this.tieBreaker = tieBreaker;
        return this;
    }

    /**
     * @return the tie breaker score
     * @see DisMaxQueryBuilder#tieBreaker(float)
     */
    public float tieBreaker() {
        return this.tieBreaker;
    }

    @Override
    public JsonObject constructJSON() {
        JsonObject newQueryObject = new JsonObject();
        newQueryObject.addProperty(TIE_BREAKER_FIELD,tieBreaker);
        JsonArray queriesJSON = new JsonArray();
        for(QueryBuilder qb : queries)
        {
            queriesJSON.add(qb.constructJSON());
        }
        newQueryObject.add(QUERIES_FIELD, queriesJSON);
        JsonObject queryObject = new JsonObject();
        queryObject.add(NAME, newQueryObject);
        return queryObject;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(queries, tieBreaker);
    }

    @Override
    protected boolean doEquals(DisMaxQueryBuilder other) {
        return Objects.equals(queries, other.queries) && Objects.equals(tieBreaker, other.tieBreaker);
    }
}
