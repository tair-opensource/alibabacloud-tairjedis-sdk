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

import java.util.Locale;
import java.util.Objects;


/**
 * Match query is a query that analyzes the text and constructs a query as the
 * result of the analysis.
 *
 *
 */
public class MatchQueryBuilder extends AbstractQueryBuilder<MatchQueryBuilder> {
    public static final String NAME = "match";
    public static final String OPERATOR = "operator";
    public static final String PREFIX_LENGTH = "prefix_length";
    public static final String ANALYZER = "analyzer";
    public static final String QUERY = "query";
    public static final String FUZZINESS = "fuzziness";
    public static final String MINIMUM_SHOULD_MATCH = "minimum_should_match";

    public static final Operator DEFAULT_OPERATOR = Operator.OR;
    public static final int DEFAULT_FUZZINESS = 0;
    public static final int DEFAULT_PREFIXLENGTH = 0;
    public static final int DEFAULT_MINIMUMSHOULDMATCH = 0;

    private final String fieldName;
    private final Object value;
    private Operator operator = DEFAULT_OPERATOR;
    private String analyzer = null;
    private int fuzziness = DEFAULT_FUZZINESS;
    private int prefixLength = DEFAULT_PREFIXLENGTH;
    private int minimumShouldMatch = DEFAULT_MINIMUMSHOULDMATCH;

    /**
     * Constructs a new match query.
     */
    public MatchQueryBuilder(String fieldName, Object value) {
        if (fieldName == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires fieldName");
        }
        if (value == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires query value");
        }
        this.fieldName = fieldName;
        this.value = value;
    }

    /** Returns the field name used in this query. */
    public String fieldName() {
        return this.fieldName;
    }

    /** Returns the value used in this query. */
    public Object value() {
        return this.value;
    }

    /** Sets the operator to use when using a boolean query. Defaults to {@code OR}. */
    public MatchQueryBuilder operator(Operator operator) {
        if (operator == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires operator to be non-null");
        }
        this.operator = operator;
        return this;
    }

    /** Returns the operator to use in a boolean query.*/
    public Operator operator() {
        return this.operator;
    }

    /**
     * Explicitly set the analyzer to use. Defaults to use explicit mapping config for the field, or, if not
     * set, the default search analyzer.
     */
    public MatchQueryBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    /** Get the analyzer to use, if previously set, otherwise {@code null} */
    public String analyzer() {
        return this.analyzer;
    }

    /** Sets the fuzziness used when evaluated to a fuzzy query type. Defaults to 0. */
    public MatchQueryBuilder fuzziness(int fuzziness) {
        this.fuzziness = fuzziness;
        return this;
    }

    /**  Gets the fuzziness used when evaluated to a fuzzy query type. */
    public int fuzziness() {
        return this.fuzziness;
    }

    /**
     * Sets the length of a length of common (non-fuzzy) prefix for fuzzy match queries
     * @param prefixLength non-negative length of prefix
     * @throws IllegalArgumentException in case the prefix is negative
     */
    public MatchQueryBuilder prefixLength(int prefixLength) {
        if (prefixLength < 0) {
            throw new IllegalArgumentException("[" + NAME + "] requires prefix length to be non-negative.");
        }
        this.prefixLength = prefixLength;
        return this;
    }

    /**
     * Gets the length of a length of common (non-fuzzy) prefix for fuzzy match queries
     */
    public int prefixLength() {
        return this.prefixLength;
    }

    /** Sets optional minimumShouldMatch value to apply to the query */
    public MatchQueryBuilder minimumShouldMatch(int minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
        return this;
    }

    /** Gets the minimumShouldMatch value */
    public int minimumShouldMatch() {
        return this.minimumShouldMatch;
    }


    @Override
    public JsonObject constructJSON() {
        JsonObject valueObject = new JsonObject();
        valueObject.add(QUERY,new Gson().toJsonTree(value));
        if(analyzer != null && analyzer.length() != 0){
            valueObject.addProperty(ANALYZER,analyzer);
        }
        if(fuzziness != DEFAULT_FUZZINESS)
        {
            valueObject.addProperty(FUZZINESS,fuzziness);
        }
        if(minimumShouldMatch != DEFAULT_MINIMUMSHOULDMATCH)
        {
            valueObject.addProperty(MINIMUM_SHOULD_MATCH,minimumShouldMatch);
        }
        if(prefixLength != DEFAULT_PREFIXLENGTH)
        {
            valueObject.addProperty(PREFIX_LENGTH,prefixLength);
        }
        if(operator != DEFAULT_OPERATOR)
        {
            valueObject.addProperty(OPERATOR,operator.toString().toLowerCase(Locale.ROOT));
        }
        JsonObject queryObject = new JsonObject();
        queryObject.add(fieldName,valueObject);
        JsonObject newQueryObject = new JsonObject();
        newQueryObject.add(NAME,queryObject);
        return newQueryObject;
    }

    @Override
    protected boolean doEquals(MatchQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
                && Objects.equals(value, other.value)
                && Objects.equals(operator, other.operator)
                && Objects.equals(analyzer, other.analyzer)
                && Objects.equals(fuzziness, other.fuzziness)
                && Objects.equals(prefixLength, other.prefixLength)
                && Objects.equals(minimumShouldMatch, other.minimumShouldMatch);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(
                fieldName,
                value,
                operator,
                analyzer,
                fuzziness,
                prefixLength,
                minimumShouldMatch
        );
    }


}
