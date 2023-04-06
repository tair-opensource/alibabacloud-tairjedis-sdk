package com.aliyun.tair.tairsearch.factory;

import com.aliyun.tair.tairsearch.search.aggregations.InternalAggregation;
import com.aliyun.tair.tairsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import com.aliyun.tair.tairsearch.search.aggregations.bucket.filter.InternalFilter;
import com.aliyun.tair.tairsearch.search.aggregations.bucket.terms.StringTerms;
import com.aliyun.tair.tairsearch.search.aggregations.metrics.*;
import com.google.gson.JsonObject;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Register classes to parse result for every aggregation
 */
public class AggregationRegisterFactory {
    public static final Map<String, BiFunction<String, JsonObject, InternalAggregation>> AggregationRegister =
        new HashMap<String, BiFunction<String, JsonObject, InternalAggregation>>(){{
            put(SumAggregationBuilder.NAME, InternalSum::new);
            put(MaxAggregationBuilder.NAME, InternalMax::new);
            put(AvgAggregationBuilder.NAME, InternalAvg::new);
            put(MinAggregationBuilder.NAME, InternalMin::new);
            put(SumOfSquaresAggregationBuilder.NAME, InternalSumOfSquares::new);
            put(VarianceAggregationBuilder.NAME, InternalVariance::new);
            put(StdDeviationAggregationBuilder.NAME, InternalStdDeviation::new);
            put(ExtendedStatsAggregationBuilder.NAME, InternalExtendedStats::new);
            put(ValueCountAggregationBuilder.NAME, InternalValueCount::new);
            put(FilterAggregationBuilder.NAME, InternalFilter::new);
            put(StringTerms.NAME, StringTerms::new);
        }};
}
