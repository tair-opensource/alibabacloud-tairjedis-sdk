/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.tair.tairsearch.search;

import com.google.gson.JsonObject;

import java.util.Objects;

public class TotalHits {
    private JsonObject totalHits;

    public enum Relation {
        /** The total hit count is equal to {@link TotalHits#value}. */
        EQUAL_TO,
        /** The total hit count is greater than or equal to {@link TotalHits#value}. */
        GREATER_THAN_OR_EQUAL_TO
    }

    public final long value;

    public final Relation relation;

    public TotalHits(JsonObject in){
        totalHits = in;
        long value = in.get("value").getAsLong();
        String relation = in.get("relation").getAsString();
        if (value < 0) {
            throw new IllegalArgumentException("value must be >= 0, got " + value);
        }
        this.value = value;
        if("eq".equals(relation)) {
            this.relation = Relation.EQUAL_TO;
        }
        else if("gte".equals(relation)){
            this.relation = Relation.GREATER_THAN_OR_EQUAL_TO;
        }
        else {
            throw new IllegalArgumentException("relation must be eq or gte, got " + value);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TotalHits totalHits = (TotalHits) o;
        return value == totalHits.value && relation == totalHits.relation;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, relation);
    }

    @Override
    public String toString(){
        return totalHits.toString();
    }
}
