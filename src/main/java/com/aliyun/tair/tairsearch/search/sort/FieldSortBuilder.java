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

import java.util.Locale;
import java.util.Objects;

import com.google.gson.JsonObject;

public class FieldSortBuilder extends SortBuilder<FieldSortBuilder> {
    public static final String DOC_FIELD_NAME = "_doc";

    private final String fieldName;

    /**
     * Constructs a new sort based on a document field.
     *
     * @param fieldName
     *            The field name.
     */
    public FieldSortBuilder(String fieldName) {
        if (fieldName == null) {
            throw new IllegalArgumentException("fieldName must not be null");
        }
        this.fieldName = fieldName;
    }

    /** Returns the document field this sort should be based on. */
    public String getFieldName() {
        return this.fieldName;
    }

    @Override
    public  JsonObject constructJSON(){
        JsonObject orderObject = new JsonObject();
        orderObject.addProperty(ORDER_FIELD, order.toString().toLowerCase(Locale.ROOT));
        JsonObject sortObject = new JsonObject();
        sortObject.add(fieldName, orderObject);
        return sortObject;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        FieldSortBuilder builder = (FieldSortBuilder) other;
        return (Objects.equals(this.fieldName, builder.fieldName)
                && Objects.equals(this.order, builder.order));
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                this.fieldName,
                this.order
        );
    }
}
