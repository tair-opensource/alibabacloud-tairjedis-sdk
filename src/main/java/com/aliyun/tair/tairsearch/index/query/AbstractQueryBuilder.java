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

public abstract class AbstractQueryBuilder<QB extends AbstractQueryBuilder<QB>> implements QueryBuilder {
    public static final float DEFAULT_BOOST = 1.0f;
    public static final String BOOST = "boost";
    protected float boost = DEFAULT_BOOST;

    /**
     * Returns the boost for this query.
     */
    @Override
    public final float boost() {
        return this.boost;
    }

    protected final void checkNegativeBoost(float boost) {
        if (Float.compare(boost, 0f) < 0) {
            throw new IllegalArgumentException(
                    "negative [boost] are not allowed in [" + toString() + "], " + "use a value between 0 and 1 to deboost"
            );
        }
    }

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    @SuppressWarnings("unchecked")
    @Override
    public final QB boost(float boost) {
        checkNegativeBoost(boost);
        this.boost = boost;
        return (QB) this;
    }

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        @SuppressWarnings("unchecked")
        QB other = (QB) obj;
        return Objects.equals(boost, other.boost) && doEquals(other);
    }

    /**
     * Indicates whether some other {@link QueryBuilder} object of the same type is "equal to" this one.
     */
    protected abstract boolean doEquals(QB other);

    @Override
    public final int hashCode() {
        return Objects.hash(getClass(), boost, doHashCode());
    }

    protected abstract int doHashCode();

    @Override
    public String toString(){
        return constructJSON().toString();
    }
}
