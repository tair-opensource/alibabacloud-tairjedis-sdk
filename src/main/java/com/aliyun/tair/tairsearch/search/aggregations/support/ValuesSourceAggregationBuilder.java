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

package com.aliyun.tair.tairsearch.search.aggregations.support;

import com.aliyun.tair.tairsearch.search.aggregations.AbstractAggregationBuilder;
import com.aliyun.tair.tairsearch.search.aggregations.AggregatorFactories.Builder;
import com.google.gson.JsonObject;

import java.util.Objects;


/**
 * Base class for all values source agg builders
 *
 *
 */
public abstract class ValuesSourceAggregationBuilder<AB extends ValuesSourceAggregationBuilder<AB>> extends AbstractAggregationBuilder<AB> {

    /**
     * Base leaf only
     *
     *
     */
    public abstract static class LeafOnly<AB extends ValuesSourceAggregationBuilder<AB>> extends
            ValuesSourceAggregationBuilder<AB> {

        protected LeafOnly(String name) {
            super(name);
        }

        @Override
        public final AB subAggregations(Builder subFactories) {
            throw new IllegalArgumentException(
                "Aggregator [" + name + "] of type [" + getType() + "] cannot accept sub-aggregations"
            );
        }

    }

    protected String field = null;
    public static final String FIELD = "field";

    protected ValuesSourceAggregationBuilder(String name) {
        super(name);
    }

    /**
     * Sets the field to use for this aggregation.
     */
    @SuppressWarnings("unchecked")
    public AB field(String field) {
        if (field == null) {
            throw new IllegalArgumentException("[field] must not be null: [" + name + "]");
        }
        this.field = field;
        return (AB) this;
    }

    /**
     * Gets the field to use for this aggregation.
     */
    public String field() {
        return field;
    }

    @Override
    protected  JsonObject constructInternalJSON()
    {
        JsonObject fieldObject = new JsonObject();
        if(field != null){
            fieldObject.addProperty(FIELD, field);
        }
        return  fieldObject;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), field);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        if (super.equals(obj) == false) {
            return false;
        }
        ValuesSourceAggregationBuilder<?> other = (ValuesSourceAggregationBuilder<?>) obj;
        return Objects.equals(field, other.field);
    }

}
