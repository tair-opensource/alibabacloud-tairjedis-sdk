package com.aliyun.tair.taircpc;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.taircpc.factory.CpcBuilderFactory;
import com.aliyun.tair.taircpc.params.*;
import com.aliyun.tair.taircpc.results.Update2JudResult;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.CommandArguments;
import redis.clients.jedis.CommandObject;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.util.JedisClusterCRC16;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static redis.clients.jedis.Protocol.toByteArray;

public class TairCpcPipeline extends Pipeline {
    public TairCpcPipeline(Jedis jedis) {
        super(jedis);
    }

    /**
     * Estimate the cpc.
     *
     * @param key the key
     * @return Success: double; Empty: 0; Fail: error.
     */
    public Response<Double> cpcEstimate(final String key) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCESTIMATE)
            .add(key), BuilderFactory.DOUBLE));
    }

    public Response<Double> cpcEstimate(final byte[] key) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCESTIMATE)
            .add(key), BuilderFactory.DOUBLE));
    }

    /**
     * MutiUpdate the cpc.
     *
     * @param keys {key item expStr exp} [key item expStr exp] ...
     * @return Success: OK; Fail: error.
     */
    public Response<String> cpcMUpdate(final ArrayList<CpcData> keys) {
        if (keys == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        for (CpcData key : keys) {
            if (key.getKey() == null) {
                throw new IllegalArgumentException(CommonResult.keyIsNull);
            }
            if (key.getItem() == null) {
                throw new IllegalArgumentException(CommonResult.valueIsNull);
            }
        }
        CpcMultiUpdateParams keyList = new CpcMultiUpdateParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCMUPDATE)
            .addObjects(keyList.getByteParams(keys)), BuilderFactory.STRING));
    }

    /**
     * MutiUpdate the cpc.
     *
     * @param keys {key item expStr exp} [key item expStr exp] ...
     * @return Success: List<Double>; Fail: error.
     */
    public Response<List<Double>> cpcMUpdate2Est(final ArrayList<CpcData> keys) {
        if (keys == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        for (CpcData key : keys) {
            if (key.getKey() == null) {
                throw new IllegalArgumentException(CommonResult.keyIsNull);
            }
            if (key.getItem() == null) {
                throw new IllegalArgumentException(CommonResult.valueIsNull);
            }
        }
        CpcMultiUpdateParams keyList = new CpcMultiUpdateParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCMUPDATE2EST)
            .addObjects(keyList.getByteParams(keys)), CpcBuilderFactory.CPCUPDATE2EST_MULTI_RESULT));
    }

    /**
     * MutiUpdate the cpc.
     *
     * @param keys {key item expStr exp} [key item expStr exp] ...
     * @return Success: HashMap<String, Double>; Fail: error.
     */
    public Response<HashMap<String, Double>> cpcMUpdate2EstWithKey(final ArrayList<CpcData> keys) {
        if (keys == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        for (CpcData key : keys) {
            if (key.getKey() == null) {
                throw new IllegalArgumentException(CommonResult.keyIsNull);
            }
        }
        for (CpcData key : keys) {
            if (key.getItem() == null) {
                throw new IllegalArgumentException(CommonResult.valueIsNull);
            }
        }
        CpcMultiUpdateParams keyList = new CpcMultiUpdateParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCMUPDATE2ESTWITHKEY)
            .addObjects(keyList.getByteParams(keys)), CpcBuilderFactory.CPCUPDATE2ESTWITHKEY_MULTI_RESULT));
    }

    /**
     * MutiUpdate the cpc.
     *
     * @param keys {key item expStr exp} [key item expStr exp] ...
     * @return Success: List<Update2judResult>; Fail: error.
     */
    public Response<List<Update2JudResult>> cpcMUpdate2Jud(final ArrayList<CpcData> keys) {
        if (keys == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        for (CpcData key : keys) {
            if (key.getKey() == null) {
                throw new IllegalArgumentException(CommonResult.keyIsNull);
            }
        }
        for (CpcData key : keys) {
            if (key.getItem() == null) {
                throw new IllegalArgumentException(CommonResult.valueIsNull);
            }
        }
        CpcMultiUpdateParams keyList = new CpcMultiUpdateParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCMUPDATE2JUD)
            .addObjects(keyList.getByteParams(keys)), CpcBuilderFactory.CPCUPDATE2JUD_MULTI_RESULT));
    }

    /**
     * Update the item of a cpc.
     *
     * @param key  the key
     * @param item the item
     * @return Success: OK; Fail: error.
     */
    public Response<String> cpcUpdate(final String key, final String item) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCUPDATE)
            .add(key)
            .add(item), BuilderFactory.STRING));
    }

    public Response<String> cpcUpdate(final byte[] key, final byte[] item) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCUPDATE)
            .add(key)
            .add(item), BuilderFactory.STRING));
    }

    /**
     * Update the item of a cpc.
     *
     * @param key    the key
     * @param item   the item
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     *               `EX` - Set expire time (seconds)
     *               `EXAT` - Set expire time as a UNIX timestamp (seconds)
     *               `PX` - Set expire time (milliseconds)
     *               `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: OK; Fail: error.
     */
    public Response<String> cpcUpdate(final String key, final String item, final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCUPDATE)
            .addObjects(params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(item))),
            BuilderFactory.STRING));
    }

    public Response<String> cpcUpdate(final byte[] key, final byte[] item, final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCUPDATE)
            .addObjects(params.getByteParams(key, item)), BuilderFactory.STRING));
    }

    /**
     * Update the item of a cpc.
     *
     * @param key  the key
     * @param item the item
     * @return Success: String List; Fail: error.
     */
    public Response<Update2JudResult> cpcUpdate2Jud(final String key, final String item) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCUPDATE2JUD)
            .add(key)
            .add(item), CpcBuilderFactory.CPCUPDATE2JUD_RESULT));
    }

    public Response<Update2JudResult> cpcUpdate2Jud(final byte[] key, final byte[] item) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCUPDATE2JUD)
            .add(key)
            .add(item), CpcBuilderFactory.CPCUPDATE2JUD_RESULT));
    }

    /**
     * Update the item of a cpc.
     *
     * @param key    the key
     * @param item   the item
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     *               `EX` - Set expire time (seconds)
     *               `EXAT` - Set expire time as a UNIX timestamp (seconds)
     *               `PX` - Set expire time (milliseconds)
     *               `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: String List; Fail: error.
     */
    public Response<Update2JudResult> cpcUpdate2Jud(final String key, final String item, final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCUPDATE2JUD)
            .addObjects(params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(item))),
            CpcBuilderFactory.CPCUPDATE2JUD_RESULT));
    }

    public Response<Update2JudResult> cpcUpdate2Jud(final byte[] key, final byte[] item, final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCUPDATE2JUD)
            .addObjects(params.getByteParams(key, item)), CpcBuilderFactory.CPCUPDATE2JUD_RESULT));
    }

    /**
     * Update the item of a cpc.
     *
     * @param key  the key
     * @param item the item
     * @return Success: Double value; Fail: error.
     */
    public Response<Double> cpcUpdate2Est(final String key, final String item) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCUPDATE2EST)
            .add(key)
            .add(item), BuilderFactory.DOUBLE));
    }

    public Response<Double> cpcUpdate2Est(final byte[] key, final byte[] item) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCUPDATE2EST)
            .add(key)
            .add(item), BuilderFactory.DOUBLE));
    }

    /**
     * Update the item of a cpc.
     *
     * @param key    the key
     * @param item   the item
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     *               `EX` - Set expire time (seconds)
     *               `EXAT` - Set expire time as a UNIX timestamp (seconds)
     *               `PX` - Set expire time (milliseconds)
     *               `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: Double value; Fail: error.
     */
    public Response<Double> cpcUpdate2Est(final String key, final String item, final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCUPDATE2EST)
            .addObjects(params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(item))),
            BuilderFactory.DOUBLE));
    }

    public Response<Double> cpcUpdate2Est(final byte[] key, final byte[] item, final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCUPDATE2EST)
            .addObjects(params.getByteParams(key, item)), BuilderFactory.DOUBLE));
    }

    /**
     * Update the item of a cpcArray.
     *
     * @param key       the key
     * @param timestamp the timestamp
     * @param item      the item
     * @return Success: OK; Fail: error.
     */
    public Response<String> cpcArrayUpdate(final String key, final long timestamp, final String item) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCARRAYUPDATE)
            .add(key)
            .add(String.valueOf(timestamp))
            .add(item), BuilderFactory.STRING));
    }

    public Response<String> cpcArrayUpdate(final byte[] key, final long timestamp, final byte[] item) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCARRAYUPDATE)
            .add(key)
            .add(toByteArray(timestamp))
            .add(item), BuilderFactory.STRING));
    }

    /**
     * Update the item of a cpcArray.
     *
     * @param key       the key
     * @param timestamp the timestamp
     * @param item      the item
     * @param params    the params: [EX time] [EXAT time] [PX time] [PXAT time]
     *                  `EX` - Set expire time (seconds)
     *                  `EXAT` - Set expire time as a UNIX timestamp (seconds)
     *                  `PX` - Set expire time (milliseconds)
     *                  `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: OK; Fail: error.
     */
    public Response<String> cpcArrayUpdate(final String key, final long timestamp, final String item,
        final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCARRAYUPDATE)
            .addObjects(
                params.getByteParams(SafeEncoder.encode(key), toByteArray(timestamp), SafeEncoder.encode(item))),
            BuilderFactory.STRING));
    }

    public Response<String> cpcArrayUpdate(final byte[] key, final long timestamp, final byte[] item,
        final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCARRAYUPDATE)
            .addObjects(params.getByteParams(key, toByteArray(timestamp), item)), BuilderFactory.STRING));
    }

    /**
     * Update the item of a cpcArray.
     *
     * @param key       the key
     * @param timestamp the timestamp
     * @param item      the item
     * @return Success: Double value; Fail: error.
     */
    public Response<Double> cpcArrayUpdate2Est(final String key, final long timestamp, final String item) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCARRAYUPDATE2EST)
            .add(key)
            .add(String.valueOf(timestamp))
            .add(item), BuilderFactory.DOUBLE));
    }

    public Response<Double> cpcArrayUpdate2Est(final byte[] key, final long timestamp, final byte[] item) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCARRAYUPDATE2EST)
            .add(key)
            .add(toByteArray(timestamp))
            .add(item), BuilderFactory.DOUBLE));
    }

    /**
     * Update the item of a cpcArray.
     *
     * @param key       the key
     * @param timestamp the timestamp
     * @param item      the item
     * @param params    the params: [EX time] [EXAT time] [PX time] [PXAT time]
     *                  `EX` - Set expire time (seconds)
     *                  `EXAT` - Set expire time as a UNIX timestamp (seconds)
     *                  `PX` - Set expire time (milliseconds)
     *                  `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: Double value; Fail: error.
     */
    public Response<Double> cpcArrayUpdate2Est(final String key, final long timestamp, final String item,
        final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCARRAYUPDATE2EST)
            .addObjects(
                params.getByteParams(SafeEncoder.encode(key), toByteArray(timestamp), SafeEncoder.encode(item))),
            BuilderFactory.DOUBLE));
    }

    public Response<Double> cpcArrayUpdate2Est(final byte[] key, final long timestamp, final byte[] item,
        final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCARRAYUPDATE2EST)
            .addObjects(params.getByteParams(key, toByteArray(timestamp), item)), BuilderFactory.DOUBLE));
    }

    /**
     * Update the item of a cpcArray.
     *
     * @param key       the key
     * @param timestamp the timestamp
     * @param item      the item
     * @return Success: Update2JudResult; Fail: error.
     */
    public Response<Update2JudResult> cpcArrayUpdate2Jud(final String key, final long timestamp, final String item) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCARRAYUPDATE2JUD)
            .add(key)
            .add(String.valueOf(timestamp))
            .add(item), CpcBuilderFactory.CPCUPDATE2JUD_RESULT));
    }

    public Response<Update2JudResult> cpcArrayUpdate2Jud(final byte[] key, final long timestamp, final byte[] item) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCARRAYUPDATE2JUD)
            .add(key)
            .add(toByteArray(timestamp))
            .add(item), CpcBuilderFactory.CPCUPDATE2JUD_RESULT));
    }

    /**
     * Update the item of a cpcArray.
     *
     * @param key       the key
     * @param timestamp the timestamp
     * @param item      the item
     * @param params    the params: [EX time] [EXAT time] [PX time] [PXAT time]
     *                  `EX` - Set expire time (seconds)
     *                  `EXAT` - Set expire time as a UNIX timestamp (seconds)
     *                  `PX` - Set expire time (milliseconds)
     *                  `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: Update2JudResult; Fail: error.
     */
    public Response<Update2JudResult> cpcArrayUpdate2Jud(final String key, final long timestamp, final String item,
        final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCARRAYUPDATE2JUD)
            .addObjects(
                params.getByteParams(SafeEncoder.encode(key), toByteArray(timestamp), SafeEncoder.encode(item))),
            CpcBuilderFactory.CPCUPDATE2JUD_RESULT));
    }

    public Response<Update2JudResult> cpcArrayUpdate2Jud(final byte[] key, final long timestamp, final byte[] item,
        final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCARRAYUPDATE2JUD)
            .addObjects(params.getByteParams(key, toByteArray(timestamp), item)),
            CpcBuilderFactory.CPCUPDATE2JUD_RESULT));
    }

    /**
     * Estimate the cpcArray.
     *
     * @param key       the key
     * @param timestamp the timestamp
     * @return Success: double; Empty: 0; Fail: error.
     */
    public Response<Double> cpcArrayEstimate(final String key, final long timestamp) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCARRAYESTIMATE)
            .add(key)
            .add(String.valueOf(timestamp)), BuilderFactory.DOUBLE));
    }

    public Response<Double> cpcArrayEstimate(final byte[] key, final long timestamp) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCARRAYESTIMATE)
            .add(key)
            .add(toByteArray(timestamp)), BuilderFactory.DOUBLE));
    }

    /**
     * Estimate the cpcArray for a range.
     *
     * @param key       the key
     * @param starttime the starttime
     * @param endtime   the endtime
     * @return Success: String List; Fail: error.
     */
    public Response<List<Double>> cpcArrayEstimateRange(final String key, final long starttime, final long endtime) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCARRAYESTIMATERANGE)
            .add(key)
            .add(String.valueOf(starttime))
            .add(String.valueOf(endtime)), CpcBuilderFactory.CPCARRAY_ESTIMATE_RANGE_RESULT));
    }

    public Response<List<Double>> cpcArrayEstimateRange(final byte[] key, final long starttime, final long endtime) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCARRAYESTIMATERANGE)
            .add(key)
            .add(toByteArray(starttime))
            .add(toByteArray(endtime)), CpcBuilderFactory.CPCARRAY_ESTIMATE_RANGE_RESULT));
    }

    /**
     * Estimate & sum the cpcArray for a range.
     *
     * @param key       the key
     * @param timestamp the timestamp
     * @param range     the range
     * @return Success: double; Empty: 0; Fail: error.
     */
    public Response<Double> cpcArrayEstimateRangeSum(final String key, final long timestamp, final long range) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCARRAYESTIMATERANGESUM)
            .add(key)
            .add(String.valueOf(timestamp))
            .add(String.valueOf(range)), BuilderFactory.DOUBLE));
    }

    public Response<Double> cpcArrayEstimateRangeSum(final byte[] key, final long timestamp, final long range) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCARRAYESTIMATERANGESUM)
            .add(key)
            .add(toByteArray(timestamp))
            .add(toByteArray(range)), BuilderFactory.DOUBLE));
    }

    /**
     * Estimate & merge the cpcArray for a range.
     *
     * @param key       the key
     * @param timestamp the timestamp
     * @param range     the range
     * @return Success: double; Empty: 0; Fail: error.
     */
    public Response<Double> cpcArrayEstimateRangeMerge(final String key, final long timestamp, final long range) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCARRAYESTIMATERANGEMERGE)
            .add(key)
            .add(String.valueOf(timestamp))
            .add(String.valueOf(range)), BuilderFactory.DOUBLE));
    }

    public Response<Double> cpcArrayEstimateRangeMerge(final byte[] key, final long timestamp, final long range) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCARRAYESTIMATERANGEMERGE)
            .add(key)
            .add(toByteArray(timestamp))
            .add(toByteArray(range)), BuilderFactory.DOUBLE));
    }

    public int getSlot(String key) {
        return JedisClusterCRC16.getSlot(key);
    }

    public Response<Double> cpcArrayEstimateTimeMerge(final String key, final long starttime, final long endtime) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCARRAYESTIMATETIMEMERGE)
            .add(SafeEncoder.encode(key))
            .add(toByteArray(starttime))
            .add(toByteArray(endtime)), BuilderFactory.DOUBLE));
    }

    public Response<Double> cpcArrayEstimateTimeMerge(final byte[] key, final long starttime, final long endtime) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.CPCARRAYESTIMATETIMEMERGE)
            .add(key)
            .add(toByteArray(starttime))
            .add(toByteArray(endtime)), BuilderFactory.DOUBLE));
    }

    // sum operation

    /**
     * Add the value of a sum key.
     *
     * @param key   the key
     * @param value the value
     * @return Success: sum value; Fail: error.
     */
    public Response<Double> sumAdd(final String key, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.SUMADD)
            .add(key)
            .add(String.valueOf(value)), BuilderFactory.DOUBLE));
    }

    public Response<Double> sumAdd(final byte[] key, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.SUMADD)
            .add(key)
            .add(toByteArray(value)), BuilderFactory.DOUBLE));
    }

    /**
     * Add the value of a sum key.
     *
     * @param key    the key
     * @param value  the value
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     *               `EX` - Set expire time (seconds)
     *               `EXAT` - Set expire time as a UNIX timestamp (seconds)
     *               `PX` - Set expire time (milliseconds)
     *               `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: sum value; Fail: error.
     */
    public Response<Double> sumAdd(final String key, final double value, final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.SUMADD)
            .addObjects(params.getByteParams(SafeEncoder.encode(key), toByteArray(value))), BuilderFactory.DOUBLE));
    }

    public Response<Double> sumAdd(final byte[] key, final double value, final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.SUMADD)
            .addObjects(params.getByteParams(key, toByteArray(value))), BuilderFactory.DOUBLE));
    }

    /**
     * Set the value of a sum key.
     *
     * @param key   the key
     * @param value the value
     * @return Success: sum value; Fail: error.
     */
    public Response<Double> sumSet(final String key, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.SUMSET)
            .add(key)
            .add(String.valueOf(value)), BuilderFactory.DOUBLE));
    }

    public Response<Double> sumSet(final byte[] key, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.SUMSET)
            .add(key)
            .add(toByteArray(value)), BuilderFactory.DOUBLE));
    }

    /**
     * Set the value of a sum key.
     *
     * @param key    the key
     * @param value  the value
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     *               `EX` - Set expire time (seconds)
     *               `EXAT` - Set expire time as a UNIX timestamp (seconds)
     *               `PX` - Set expire time (milliseconds)
     *               `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: sum value; Fail: error.
     */
    public Response<Double> sumSet(final String key, final double value, final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.SUMSET)
            .addObjects(params.getByteParams(SafeEncoder.encode(key), toByteArray(value))), BuilderFactory.DOUBLE));
    }

    public Response<Double> sumSet(final byte[] key, final double value, final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.SUMSET)
            .addObjects(params.getByteParams(key, toByteArray(value))), BuilderFactory.DOUBLE));
    }

    /**
     * Get the value of a sum key.
     *
     * @param key the key
     * @return Success: sum value; Fail: error.
     */
    public Response<Double> sumGet(final String key) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.SUMGET)
            .add(key), BuilderFactory.DOUBLE));
    }

    public Response<Double> sumGet(final byte[] key) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.SUMGET)
            .add(key), BuilderFactory.DOUBLE));
    }

    /**
     * Add the value of a sumArray key.
     *
     * @param key       the key
     * @param timestamp the timestamp
     * @param value     the value
     * @return Success: sum value of timestamp; Fail: error.
     */
    public Response<Double> sumArrayAdd(final String key, final long timestamp, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.SUMARRAYADD)
            .add(key)
            .add(String.valueOf(timestamp))
            .add(String.valueOf(value)), BuilderFactory.DOUBLE));
    }

    public Response<Double> sumArrayAdd(final byte[] key, final long timestamp, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.SUMARRAYADD)
            .add(key)
            .add(toByteArray(timestamp))
            .add(toByteArray(value)), BuilderFactory.DOUBLE));
    }

    /**
     * Add the value of a sumArray key.
     *
     * @param key       the key
     * @param timestamp the timestamp
     * @param value     the value
     * @param params    the params: [EX time] [EXAT time] [PX time] [PXAT time]
     *                  `EX` - Set expire time (seconds)
     *                  `EXAT` - Set expire time as a UNIX timestamp (seconds)
     *                  `PX` - Set expire time (milliseconds)
     *                  `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: sum value of timestamp; Fail: error.
     */
    public Response<Double> sumArrayAdd(final String key, final long timestamp, final double value,
        final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.SUMARRAYADD)
            .addObjects(params.getByteParams(SafeEncoder.encode(key), toByteArray(timestamp), toByteArray(value))),
            BuilderFactory.DOUBLE));
    }

    public Response<Double> sumArrayAdd(final byte[] key, final long timestamp, final double value,
        final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.SUMARRAYADD)
            .addObjects(params.getByteParams(key, toByteArray(timestamp), toByteArray(value))), BuilderFactory.DOUBLE));
    }

    /**
     * Get the value of a sum key.
     *
     * @param key       the key
     * @param timestamp the timestamp
     * @return Success: sum value; Fail: error.
     */
    public Response<Double> sumArrayGet(final String key, final long timestamp) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.SUMARRAYGET)
            .add(SafeEncoder.encode(key))
            .add(toByteArray(timestamp)), BuilderFactory.DOUBLE));
    }

    public Response<Double> sumArrayGet(final byte[] key, final long timestamp) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.SUMARRAYGET)
            .add(key)
            .add(toByteArray(timestamp)), BuilderFactory.DOUBLE));
    }

    /**
     * Get the values of an array sum key range.
     *
     * @param key       the key
     * @param starttime the starttime
     * @param endtime   the endtime
     * @return Success: sum value list; Fail: error.
     */
    public Response<List<Double>> sumArrayGetRange(final String key, final long starttime, final long endtime) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.SUMARRAYGETRANGE)
            .add(SafeEncoder.encode(key))
            .add(toByteArray(starttime))
            .add(toByteArray(endtime)), CpcBuilderFactory.CPCARRAY_RANGE_RESULT));
    }

    public Response<List<Double>> sumArrayGetRange(final byte[] key, final long starttime, final long endtime) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.SUMARRAYGETRANGE)
            .add(key)
            .add(toByteArray(starttime))
            .add(toByteArray(endtime)), CpcBuilderFactory.CPCARRAY_RANGE_RESULT));
    }

    public Response<Double> sumArrayGetRangeTimeMerge(final String key, final long starttime, final long endtime) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.SUMARRAYGETTIMEMERGE)
            .add(SafeEncoder.encode(key))
            .add(toByteArray(starttime))
            .add(toByteArray(endtime)), BuilderFactory.DOUBLE));
    }

    public Response<Double> sumArrayGetRangeTimeMerge(final byte[] key, final long starttime, final long endtime) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.SUMARRAYGETTIMEMERGE)
            .add(key)
            .add(toByteArray(starttime))
            .add(toByteArray(endtime)), BuilderFactory.DOUBLE));
    }

    /**
     * Get the merge value of an array sum key range.
     *
     * @param key the key
     * @return Success: merge of sum value; Fail: error.
     */
    public Response<Double> sumArrayGetRangeMerge(final String key, final long endtime, final long range) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.SUMARRAYGETRANGEMERGE)
            .add(SafeEncoder.encode(key))
            .add(toByteArray(endtime))
            .add(toByteArray(range)), BuilderFactory.DOUBLE));
    }

    public Response<Double> sumArrayGetRangeMerge(final byte[] key, final long endtime, final long range) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.SUMARRAYGETRANGEMERGE)
            .add(key)
            .add(toByteArray(endtime))
            .add(toByteArray(range)), BuilderFactory.DOUBLE));
    }

    // max operation

    /**
     * Add the value of a max key.
     *
     * @param key   the key
     * @param value the value
     * @return Success: max value; Fail: error.
     */
    public Response<Double> maxAdd(final String key, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MAXADD)
            .add(key)
            .add(String.valueOf(value)), BuilderFactory.DOUBLE));
    }

    public Response<Double> maxAdd(final byte[] key, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MAXADD)
            .add(key)
            .add(toByteArray(value)), BuilderFactory.DOUBLE));
    }

    /**
     * Add the value of a max key.
     *
     * @param key    the key
     * @param value  the value
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     *               `EX` - Set expire time (seconds)
     *               `EXAT` - Set expire time as a UNIX timestamp (seconds)
     *               `PX` - Set expire time (milliseconds)
     *               `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: max value; Fail: error.
     */
    public Response<Double> maxAdd(final String key, final double value, final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MAXADD)
            .addObjects(params.getByteParams(SafeEncoder.encode(key), toByteArray(value))), BuilderFactory.DOUBLE));
    }

    public Response<Double> maxAdd(final byte[] key, final double value, final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MAXADD)
            .addObjects(params.getByteParams(key, toByteArray(value))), BuilderFactory.DOUBLE));
    }

    /**
     * Set the value of a max key.
     *
     * @param key   the key
     * @param value the value
     * @return Success: max value; Fail: error.
     */
    public Response<Double> maxSet(final String key, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MAXSET)
            .add(key)
            .add(String.valueOf(value)), BuilderFactory.DOUBLE));
    }

    public Response<Double> maxSet(final byte[] key, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MAXSET)
            .add(key)
            .add(toByteArray(value)), BuilderFactory.DOUBLE));
    }

    /**
     * Set the value of a max key.
     *
     * @param key    the key
     * @param value  the value
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     *               `EX` - Set expire time (seconds)
     *               `EXAT` - Set expire time as a UNIX timestamp (seconds)
     *               `PX` - Set expire time (milliseconds)
     *               `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: max value; Fail: error.
     */
    public Response<Double> maxSet(final String key, final double value, final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MAXSET)
            .addObjects(params.getByteParams(SafeEncoder.encode(key), toByteArray(value))), BuilderFactory.DOUBLE));
    }

    public Response<Double> maxSet(final byte[] key, final double value, final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MAXSET)
            .addObjects(params.getByteParams(key, toByteArray(value))), BuilderFactory.DOUBLE));
    }

    /**
     * Get the value of a max key.
     *
     * @param key the key
     * @return Success: max value; Fail: error.
     */
    public Response<Double> maxGet(final String key) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MAXGET)
            .add(key), BuilderFactory.DOUBLE));
    }

    public Response<Double> maxGet(final byte[] key) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MAXGET)
            .add(key), BuilderFactory.DOUBLE));
    }

    /**
     * Add the value of a maxArray key.
     *
     * @param key       the key
     * @param timestamp the timestamp
     * @param value     the value
     * @return Success: max value of timestamp; Fail: error.
     */
    public Response<Double> maxArrayAdd(final String key, final long timestamp, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MAXARRAYADD)
            .add(key)
            .add(String.valueOf(timestamp))
            .add(String.valueOf(value)), BuilderFactory.DOUBLE));
    }

    public Response<Double> maxArrayAdd(final byte[] key, final long timestamp, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MAXARRAYADD)
            .add(key)
            .add(toByteArray(timestamp))
            .add(toByteArray(value)), BuilderFactory.DOUBLE));
    }

    /**
     * Add the value of a maxArray key.
     *
     * @param key       the key
     * @param timestamp the timestamp
     * @param value     the value
     * @param params    the params: [EX time] [EXAT time] [PX time] [PXAT time]
     *                  `EX` - Set expire time (seconds)
     *                  `EXAT` - Set expire time as a UNIX timestamp (seconds)
     *                  `PX` - Set expire time (milliseconds)
     *                  `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: max value of timestamp; Fail: error.
     */
    public Response<Double> maxArrayAdd(final String key, final long timestamp, final double value,
        final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MAXARRAYADD)
            .addObjects(params.getByteParams(SafeEncoder.encode(key), toByteArray(timestamp), toByteArray(value))),
            BuilderFactory.DOUBLE));
    }

    public Response<Double> maxArrayAdd(final byte[] key, final long timestamp, final double value,
        final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MAXARRAYADD)
            .addObjects(params.getByteParams(key, toByteArray(timestamp), toByteArray(value))), BuilderFactory.DOUBLE));
    }

    /**
     * Get the value of a max key.
     *
     * @param key       the key
     * @param timestamp the timestamp
     * @return Success: max value; Fail: error.
     */
    public Response<Double> maxArrayGet(final String key, final long timestamp) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MAXARRAYGET)
            .add(SafeEncoder.encode(key))
            .add(toByteArray(timestamp)), BuilderFactory.DOUBLE));
    }

    public Response<Double> maxArrayGet(final byte[] key, final long timestamp) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MAXARRAYGET)
            .add(key)
            .add(toByteArray(timestamp)), BuilderFactory.DOUBLE));
    }

    /**
     * Get the values of an array max key range.
     *
     * @param key       the key
     * @param starttime the starttime
     * @param endtime   the endtime
     * @return Success: max value list; Fail: error.
     */
    public Response<List<Double>> maxArrayGetRange(final String key, final long starttime, final long endtime) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MAXARRAYGETRANGE)
            .add(SafeEncoder.encode(key))
            .add(toByteArray(starttime))
            .add(toByteArray(endtime)), CpcBuilderFactory.CPCARRAY_RANGE_RESULT));
    }

    public Response<List<Double>> maxArrayGetRange(final byte[] key, final long starttime, final long endtime) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MAXARRAYGETRANGE)
            .add(key)
            .add(toByteArray(starttime))
            .add(toByteArray(endtime)), CpcBuilderFactory.CPCARRAY_RANGE_RESULT));
    }

    public Response<Double> maxArrayGetRangeTimeMerge(final String key, final long starttime, final long endtime) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MAXARRAYGETTIMEMERGE)
            .add(SafeEncoder.encode(key))
            .add(toByteArray(starttime))
            .add(toByteArray(endtime)), BuilderFactory.DOUBLE));
    }

    public Response<Double> maxArrayGetRangeTimeMerge(final byte[] key, final long starttime, final long endtime) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MAXARRAYGETTIMEMERGE)
            .add(key)
            .add(toByteArray(starttime))
            .add(toByteArray(endtime)), BuilderFactory.DOUBLE));
    }

    /**
     * Get the merge value of an array max key range.
     *
     * @param key the key
     * @return Success: merge of max value; Fail: error.
     */
    public Response<Double> maxArrayGetRangeMerge(final String key, final long endtime, final long range) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MAXARRAYGETRANGEMERGE)
            .add(SafeEncoder.encode(key))
            .add(toByteArray(endtime))
            .add(toByteArray(range)), BuilderFactory.DOUBLE));
    }

    public Response<Double> maxArrayGetRangeMerge(final byte[] key, final long endtime, final long range) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MAXARRAYGETRANGEMERGE)
            .add(key)
            .add(toByteArray(endtime))
            .add(toByteArray(range)), BuilderFactory.DOUBLE));
    }

    // min operation

    /**
     * Add the value of a min key.
     *
     * @param key   the key
     * @param value the value
     * @return Success: min value; Fail: error.
     */
    public Response<Double> minAdd(final String key, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MINADD)
            .add(key)
            .add(String.valueOf(value)), BuilderFactory.DOUBLE));
    }

    public Response<Double> minAdd(final byte[] key, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MINADD)
            .add(key)
            .add(toByteArray(value)), BuilderFactory.DOUBLE));
    }

    /**
     * Add the value of a min key.
     *
     * @param key    the key
     * @param value  the value
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     *               `EX` - Set expire time (seconds)
     *               `EXAT` - Set expire time as a UNIX timestamp (seconds)
     *               `PX` - Set expire time (milliseconds)
     *               `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: min value; Fail: error.
     */
    public Response<Double> minAdd(final String key, final double value, final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MINADD)
            .addObjects(params.getByteParams(SafeEncoder.encode(key), toByteArray(value))), BuilderFactory.DOUBLE));
    }

    public Response<Double> minAdd(final byte[] key, final double value, final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MINADD)
            .addObjects(params.getByteParams(key, toByteArray(value))), BuilderFactory.DOUBLE));
    }

    /**
     * Set the value of a min key.
     *
     * @param key   the key
     * @param value the value
     * @return Success: min value; Fail: error.
     */
    public Response<Double> minSet(final String key, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MINSET)
            .add(key)
            .add(String.valueOf(value)), BuilderFactory.DOUBLE));
    }

    public Response<Double> minSet(final byte[] key, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MINSET)
            .add(key)
            .add(toByteArray(value)), BuilderFactory.DOUBLE));
    }

    /**
     * Set the value of a min key.
     *
     * @param key    the key
     * @param value  the value
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     *               `EX` - Set expire time (seconds)
     *               `EXAT` - Set expire time as a UNIX timestamp (seconds)
     *               `PX` - Set expire time (milliseconds)
     *               `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: min value; Fail: error.
     */
    public Response<Double> minSet(final String key, final double value, final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MINSET)
            .addObjects(params.getByteParams(SafeEncoder.encode(key), toByteArray(value))), BuilderFactory.DOUBLE));
    }

    public Response<Double> minSet(final byte[] key, final double value, final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MINSET)
            .addObjects(params.getByteParams(key, toByteArray(value))), BuilderFactory.DOUBLE));
    }

    /**
     * Get the value of a min key.
     *
     * @param key the key
     * @return Success: min value; Fail: error.
     */
    public Response<Double> minGet(final String key) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MINGET)
            .add(key), BuilderFactory.DOUBLE));
    }

    public Response<Double> minGet(final byte[] key) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MINGET)
            .add(key), BuilderFactory.DOUBLE));
    }

    /**
     * Add the value of a minArray key.
     *
     * @param key       the key
     * @param timestamp the timestamp
     * @param value     the value
     * @return Success: min value of timestamp; Fail: error.
     */
    public Response<Double> minArrayAdd(final String key, final long timestamp, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MINARRAYADD)
            .add(key)
            .add(String.valueOf(timestamp))
            .add(String.valueOf(value)), BuilderFactory.DOUBLE));
    }

    public Response<Double> minArrayAdd(final byte[] key, final long timestamp, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MINARRAYADD)
            .add(key)
            .add(toByteArray(timestamp))
            .add(toByteArray(value)), BuilderFactory.DOUBLE));
    }

    /**
     * Add the value of a minArray key.
     *
     * @param key       the key
     * @param timestamp the timestamp
     * @param value     the value
     * @param params    the params: [EX time] [EXAT time] [PX time] [PXAT time]
     *                  `EX` - Set expire time (seconds)
     *                  `EXAT` - Set expire time as a UNIX timestamp (seconds)
     *                  `PX` - Set expire time (milliseconds)
     *                  `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: min value of timestamp; Fail: error.
     */
    public Response<Double> minArrayAdd(final String key, final long timestamp, final double value,
        final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MINARRAYADD)
            .addObjects(params.getByteParams(SafeEncoder.encode(key), toByteArray(timestamp), toByteArray(value))),
            BuilderFactory.DOUBLE));
    }

    public Response<Double> minArrayAdd(final byte[] key, final long timestamp, final double value,
        final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MINARRAYADD)
            .addObjects(params.getByteParams(key, toByteArray(timestamp), toByteArray(value))), BuilderFactory.DOUBLE));
    }

    /**
     * Get the value of a min key.
     *
     * @param key       the key
     * @param timestamp the timestamp
     * @return Success: min value; Fail: error.
     */
    public Response<Double> minArrayGet(final String key, final long timestamp) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MINARRAYGET)
            .add(SafeEncoder.encode(key))
            .add(toByteArray(timestamp)), BuilderFactory.DOUBLE));
    }

    public Response<Double> minArrayGet(final byte[] key, final long timestamp) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MINARRAYGET)
            .add(key)
            .add(toByteArray(timestamp)), BuilderFactory.DOUBLE));
    }

    /**
     * Get the values of an array min key range.
     *
     * @param key       the key
     * @param starttime the starttime
     * @param endtime   the endtime
     * @return Success: min value list; Fail: error.
     */
    public Response<List<Double>> minArrayGetRange(final String key, final long starttime, final long endtime) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MINARRAYGETRANGE)
            .add(SafeEncoder.encode(key))
            .add(toByteArray(starttime))
            .add(toByteArray(endtime)), CpcBuilderFactory.CPCARRAY_RANGE_RESULT));
    }

    public Response<List<Double>> minArrayGetRange(final byte[] key, final long starttime, final long endtime) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MINARRAYGETRANGE)
            .add(key)
            .add(toByteArray(starttime))
            .add(toByteArray(endtime)), CpcBuilderFactory.CPCARRAY_RANGE_RESULT));
    }

    public Response<Double> minArrayGetRangeTimeMerge(final String key, final long starttime, final long endtime) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MINARRAYGETTIMEMERGE)
            .add(SafeEncoder.encode(key))
            .add(toByteArray(starttime))
            .add(toByteArray(endtime)), BuilderFactory.DOUBLE));
    }

    public Response<Double> minArrayGetRangeTimeMerge(final byte[] key, final long starttime, final long endtime) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MINARRAYGETTIMEMERGE)
            .add(key)
            .add(toByteArray(starttime))
            .add(toByteArray(endtime)), BuilderFactory.DOUBLE));
    }

    /**
     * Get the merge value of an array min key range.
     *
     * @param key the key
     * @return Success: merge of min value; Fail: error.
     */
    public Response<Double> minArrayGetRangeMerge(final String key, final long endtime, final long range) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MINARRAYGETRANGEMERGE)
            .add(SafeEncoder.encode(key))
            .add(toByteArray(endtime))
            .add(toByteArray(range)), BuilderFactory.DOUBLE));
    }

    public Response<Double> minArrayGetRangeMerge(final byte[] key, final long endtime, final long range) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.MINARRAYGETRANGEMERGE)
            .add(key)
            .add(toByteArray(endtime))
            .add(toByteArray(range)), BuilderFactory.DOUBLE));
    }

    // first operation

    /**
     * Add the value of a first key.
     *
     * @param key     the key
     * @param content the content
     * @param value   the value
     * @return Success: first value; Fail: error.
     */
    public Response<String> firstAdd(final String key, final String content, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.FIRSTADD)
            .add(key)
            .add(content)
            .add(String.valueOf(value)), BuilderFactory.STRING));
    }

    public Response<String> firstAdd(final byte[] key, final byte[] content, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.FIRSTADD)
            .add(key)
            .add(content)
            .add(toByteArray(value)), BuilderFactory.STRING));
    }

    /**
     * Add the value of a first key.
     *
     * @param key     the key
     * @param content the content
     * @param value   the value
     * @param params  the params: [EX time] [EXAT time] [PX time] [PXAT time]
     *                `EX` - Set expire time (seconds)
     *                `EXAT` - Set expire time as a UNIX timestamp (seconds)
     *                `PX` - Set expire time (milliseconds)
     *                `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: first value; Fail: error.
     */
    public Response<String> firstAdd(final String key, final String content, final double value,
        final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.FIRSTADD)
            .addObjects(params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(content), toByteArray(value))),
            BuilderFactory.STRING));
    }

    public Response<String> firstAdd(final byte[] key, final byte[] content, final double value,
        final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.FIRSTADD)
            .addObjects(params.getByteParams(key, content, toByteArray(value))), BuilderFactory.STRING));
    }

    /**
     * Set the value of a first key.
     *
     * @param key     the key
     * @param content the content
     * @param value   the value
     * @return Success: first value; Fail: error.
     */
    public Response<String> firstSet(final String key, final String content, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.FIRSTSET)
            .add(key)
            .add(content)
            .add(String.valueOf(value)), BuilderFactory.STRING));
    }

    public Response<String> firstSet(final byte[] key, final byte[] content, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.FIRSTSET)
            .add(key)
            .add(content)
            .add(toByteArray(value)), BuilderFactory.STRING));
    }

    /**
     * Set the value of a first key.
     *
     * @param key     the key
     * @param content the content
     * @param value   the value
     * @param params  the params: [EX time] [EXAT time] [PX time] [PXAT time]
     *                `EX` - Set expire time (seconds)
     *                `EXAT` - Set expire time as a UNIX timestamp (seconds)
     *                `PX` - Set expire time (milliseconds)
     *                `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: first value; Fail: error.
     */
    public Response<String> firstSet(final String key, final String content, final double value,
        final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.FIRSTSET)
            .addObjects(params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(content), toByteArray(value))),
            BuilderFactory.STRING));
    }

    public Response<String> firstSet(final byte[] key, final byte[] content, final double value,
        final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.FIRSTSET)
            .addObjects(params.getByteParams(key, content, toByteArray(value))), BuilderFactory.STRING));
    }

    /**
     * Get the value of a first key.
     *
     * @param key the key
     * @return Success: first value; Fail: error.
     */
    public Response<String> firstGet(final String key) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.FIRSTGET)
            .add(key), BuilderFactory.STRING));
    }

    public Response<String> firstGet(final byte[] key) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.FIRSTGET)
            .add(key), BuilderFactory.STRING));
    }

    /**
     * Add the value of a firstArray key.
     *
     * @param key       the key
     * @param timestamp the timestamp
     * @param content   the content
     * @param value     the value
     * @return Success: first value of timestamp; Fail: error.
     */
    public Response<String> firstArrayAdd(final String key, final long timestamp, final String content,
        final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.FIRSTARRAYADD)
            .add(key)
            .add(String.valueOf(timestamp))
            .add(content)
            .add(String.valueOf(value)), BuilderFactory.STRING));
    }

    public Response<String> firstArrayAdd(final byte[] key, final long timestamp, final byte[] content,
        final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.FIRSTARRAYADD)
            .add(key)
            .add(toByteArray(timestamp))
            .add(content)
            .add(toByteArray(value)), BuilderFactory.STRING));
    }

    /**
     * Add the value of a firstArray key.
     *
     * @param key       the key
     * @param timestamp the timestamp
     * @param content   the content
     * @param value     the value
     * @param params    the params: [EX time] [EXAT time] [PX time] [PXAT time]
     *                  `EX` - Set expire time (seconds)
     *                  `EXAT` - Set expire time as a UNIX timestamp (seconds)
     *                  `PX` - Set expire time (milliseconds)
     *                  `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: first value of timestamp; Fail: error.
     */
    public Response<String> firstArrayAdd(final String key, final long timestamp, final String content,
        final double value, final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.FIRSTARRAYADD)
            .addObjects(
                params.getByteParams(SafeEncoder.encode(key), toByteArray(timestamp), SafeEncoder.encode(content),
                    toByteArray(value))), BuilderFactory.STRING));
    }

    public Response<String> firstArrayAdd(final byte[] key, final long timestamp, final byte[] content,
        final double value, final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.FIRSTARRAYADD)
            .addObjects(params.getByteParams(key, toByteArray(timestamp), content, toByteArray(value))),
            BuilderFactory.STRING));
    }

    /**
     * Get the value of a first key.
     *
     * @param key       the key
     * @param timestamp the timestamp
     * @return Success: first value; Fail: error.
     */
    public Response<String> firstArrayGet(final String key, final long timestamp) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.FIRSTARRAYGET)
            .add(SafeEncoder.encode(key))
            .add(toByteArray(timestamp)), BuilderFactory.STRING));
    }

    public Response<String> firstArrayGet(final byte[] key, final long timestamp) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.FIRSTARRAYGET)
            .add(key)
            .add(toByteArray(timestamp)), BuilderFactory.STRING));
    }

    public Response<String> firstArrayGetRangeTimeMerge(final String key, final long starttime, final long endtime) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.FIRSTARRAYGETTIMEMERGE)
            .add(SafeEncoder.encode(key))
            .add(toByteArray(starttime))
            .add(toByteArray(endtime)), BuilderFactory.STRING));
    }

    public Response<String> firstArrayGetRangeTimeMerge(final byte[] key, final long starttime, final long endtime) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.FIRSTARRAYGETTIMEMERGE)
            .add(key)
            .add(toByteArray(starttime))
            .add(toByteArray(endtime)), BuilderFactory.STRING));
    }

    /**
     * Get the merge value of an array first key range.
     *
     * @param key the key
     * @return Success: merge of first value; Fail: error.
     */
    public Response<String> firstArrayGetRangeMerge(final String key, final long endtime, final long range) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.FIRSTARRAYGETRANGEMERGE)
            .add(SafeEncoder.encode(key))
            .add(toByteArray(endtime))
            .add(toByteArray(range)), BuilderFactory.STRING));
    }

    public Response<String> firstArrayGetRangeMerge(final byte[] key, final long endtime, final long range) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.FIRSTARRAYGETRANGEMERGE)
            .add(key)
            .add(toByteArray(endtime))
            .add(toByteArray(range)), BuilderFactory.STRING));
    }

    // last operation

    /**
     * Add the value of a last key.
     *
     * @param key     the key
     * @param content the content
     * @param value   the value
     * @return Success: last value; Fail: error.
     */
    public Response<String> lastAdd(final String key, final String content, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.LASTADD)
            .add(key)
            .add(content)
            .add(String.valueOf(value)), BuilderFactory.STRING));
    }

    public Response<String> lastAdd(final byte[] key, final byte[] content, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.LASTADD)
            .add(key)
            .add(content)
            .add(toByteArray(value)), BuilderFactory.STRING));
    }

    /**
     * Add the value of a last key.
     *
     * @param key     the key
     * @param content the content
     * @param value   the value
     * @param params  the params: [EX time] [EXAT time] [PX time] [PXAT time]
     *                `EX` - Set expire time (seconds)
     *                `EXAT` - Set expire time as a UNIX timestamp (seconds)
     *                `PX` - Set expire time (milliseconds)
     *                `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: last value; Fail: error.
     */
    public Response<String> lastAdd(final String key, final String content, final double value,
        final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.LASTADD)
            .addObjects(params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(content), toByteArray(value))),
            BuilderFactory.STRING));
    }

    public Response<String> lastAdd(final byte[] key, final byte[] content, final double value,
        final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.LASTADD)
            .addObjects(params.getByteParams(key, content, toByteArray(value))), BuilderFactory.STRING));
    }

    /**
     * Set the value of a last key.
     *
     * @param key     the key
     * @param content the content
     * @param value   the value
     * @return Success: last value; Fail: error.
     */
    public Response<String> lastSet(final String key, final String content, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.LASTSET)
            .add(key)
            .add(content)
            .add(String.valueOf(value)), BuilderFactory.STRING));
    }

    public Response<String> lastSet(final byte[] key, final byte[] content, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.LASTSET)
            .add(key)
            .add(content)
            .add(toByteArray(value)), BuilderFactory.STRING));
    }

    /**
     * Set the value of a last key.
     *
     * @param key     the key
     * @param content the content
     * @param value   the value
     * @param params  the params: [EX time] [EXAT time] [PX time] [PXAT time]
     *                `EX` - Set expire time (seconds)
     *                `EXAT` - Set expire time as a UNIX timestamp (seconds)
     *                `PX` - Set expire time (milliseconds)
     *                `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: last value; Fail: error.
     */
    public Response<String> lastSet(final String key, final String content, final double value,
        final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.LASTSET)
            .addObjects(params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(content), toByteArray(value))),
            BuilderFactory.STRING));
    }

    public Response<String> lastSet(final byte[] key, final byte[] content, final double value,
        final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.LASTSET)
            .addObjects(params.getByteParams(key, content, toByteArray(value))), BuilderFactory.STRING));
    }

    /**
     * Get the value of a last key.
     *
     * @param key the key
     * @return Success: last value; Fail: error.
     */
    public Response<String> lastGet(final String key) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.LASTGET)
            .add(key), BuilderFactory.STRING));
    }

    public Response<String> lastGet(final byte[] key) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.LASTGET)
            .add(key), BuilderFactory.STRING));
    }

    /**
     * Add the value of a lastArray key.
     *
     * @param key       the key
     * @param timestamp the timestamp
     * @param content   the content
     * @param value     the value
     * @return Success: last value of timestamp; Fail: error.
     */
    public Response<String> lastArrayAdd(final String key, final long timestamp, final String content,
        final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.LASTARRAYADD)
            .add(key)
            .add(String.valueOf(timestamp))
            .add(content)
            .add(String.valueOf(value)), BuilderFactory.STRING));
    }

    public Response<String> lastArrayAdd(final byte[] key, final long timestamp, final byte[] content,
        final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.LASTARRAYADD)
            .add(key)
            .add(toByteArray(timestamp))
            .add(content)
            .add(toByteArray(value)), BuilderFactory.STRING));
    }

    /**
     * Add the value of a lastArray key.
     *
     * @param key       the key
     * @param timestamp the timestamp
     * @param content   the content
     * @param value     the value
     * @param params    the params: [EX time] [EXAT time] [PX time] [PXAT time]
     *                  `EX` - Set expire time (seconds)
     *                  `EXAT` - Set expire time as a UNIX timestamp (seconds)
     *                  `PX` - Set expire time (milliseconds)
     *                  `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: last value of timestamp; Fail: error.
     */
    public Response<String> lastArrayAdd(final String key, final long timestamp, final String content,
        final double value, final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.LASTARRAYADD)
            .addObjects(
                params.getByteParams(SafeEncoder.encode(key), toByteArray(timestamp), SafeEncoder.encode(content),
                    toByteArray(value))), BuilderFactory.STRING));
    }

    public Response<String> lastArrayAdd(final byte[] key, final long timestamp, final byte[] content,
        final double value, final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.LASTARRAYADD)
            .addObjects(params.getByteParams(key, toByteArray(timestamp), content, toByteArray(value))),
            BuilderFactory.STRING));
    }

    /**
     * Get the value of a last key.
     *
     * @param key       the key
     * @param timestamp the timestamp
     * @return Success: last value; Fail: error.
     */
    public Response<String> lastArrayGet(final String key, final long timestamp) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.LASTARRAYGET)
            .add(SafeEncoder.encode(key))
            .add(toByteArray(timestamp)), BuilderFactory.STRING));
    }

    public Response<String> lastArrayGet(final byte[] key, final long timestamp) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.LASTARRAYGET)
            .add(key)
            .add(toByteArray(timestamp)), BuilderFactory.STRING));
    }

    public Response<String> lastArrayGetRangeTimeMerge(final String key, final long starttime, final long endtime) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.LASTARRAYGETTIMEEMERGE)
            .add(SafeEncoder.encode(key))
            .add(toByteArray(starttime))
            .add(toByteArray(endtime)), BuilderFactory.STRING));
    }

    public Response<String> lastArrayGetRangeTimeMerge(final byte[] key, final long starttime, final long endtime) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.LASTARRAYGETTIMEEMERGE)
            .add(key)
            .add(toByteArray(starttime))
            .add(toByteArray(endtime)), BuilderFactory.STRING));
    }

    /**
     * Get the merge value of an array last key range.
     *
     * @param key the key
     * @return Success: merge of last value; Fail: error.
     */
    public Response<String> lastArrayGetRangeMerge(final String key, final long endtime, final long range) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.LASTARRAYGETRANGEMERGE)
            .add(SafeEncoder.encode(key))
            .add(toByteArray(endtime))
            .add(toByteArray(range)), BuilderFactory.STRING));
    }

    public Response<String> lastArrayGetRangeMerge(final byte[] key, final long endtime, final long range) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.LASTARRAYGETRANGEMERGE)
            .add(key)
            .add(toByteArray(endtime))
            .add(toByteArray(range)), BuilderFactory.STRING));
    }

    // avg operation

    /**
     * Add the value of a avg key.
     *
     * @param key   the key
     * @param count the count
     * @param value the value
     * @return Success: avg value; Fail: error.
     */
    public Response<Double> avgAdd(final String key, final long count, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.AVGADD)
            .add(key)
            .add(String.valueOf(count))
            .add(String.valueOf(value)), BuilderFactory.DOUBLE));
    }

    public Response<Double> avgAdd(final byte[] key, final long count, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.AVGADD)
            .add(key)
            .add(toByteArray(count))
            .add(toByteArray(value)), BuilderFactory.DOUBLE));
    }

    /**
     * Add the value of a avg key.
     *
     * @param key    the key
     * @param count  the count
     * @param value  the value
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     *               `EX` - Set expire time (seconds)
     *               `EXAT` - Set expire time as a UNIX timestamp (seconds)
     *               `PX` - Set expire time (milliseconds)
     *               `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: avg value; Fail: error.
     */
    public Response<Double> avgAdd(final String key, final long count, final double value,
        final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.AVGADD)
            .addObjects(params.getByteParams(SafeEncoder.encode(key), toByteArray(count), toByteArray(value))),
            BuilderFactory.DOUBLE));
    }

    public Response<Double> avgAdd(final byte[] key, final long count, final double value,
        final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.AVGADD)
            .addObjects(params.getByteParams(key, toByteArray(count), toByteArray(value))), BuilderFactory.DOUBLE));
    }

    /**
     * Set the value of a avg key.
     *
     * @param key   the key
     * @param count the count
     * @param value the value
     * @return Success: avg value; Fail: error.
     */
    public Response<Double> avgSet(final String key, final long count, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.AVGSET)
            .add(key)
            .add(String.valueOf(count))
            .add(String.valueOf(value)), BuilderFactory.DOUBLE));
    }

    public Response<Double> avgSet(final byte[] key, final long count, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.AVGSET)
            .add(key)
            .add(toByteArray(count))
            .add(toByteArray(value)), BuilderFactory.DOUBLE));
    }

    /**
     * Set the value of a avg key.
     *
     * @param key    the key
     * @param count  the count
     * @param value  the value
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     *               `EX` - Set expire time (seconds)
     *               `EXAT` - Set expire time as a UNIX timestamp (seconds)
     *               `PX` - Set expire time (milliseconds)
     *               `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: avg value; Fail: error.
     */
    public Response<Double> avgSet(final String key, final long count, final double value,
        final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.AVGSET)
            .addObjects(params.getByteParams(SafeEncoder.encode(key), toByteArray(count), toByteArray(value))),
            BuilderFactory.DOUBLE));
    }

    public Response<Double> avgSet(final byte[] key, final long count, final double value,
        final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.AVGSET)
            .addObjects(params.getByteParams(key, toByteArray(count), toByteArray(value))), BuilderFactory.DOUBLE));
    }

    /**
     * Get the value of a avg key.
     *
     * @param key the key
     * @return Success: avg value; Fail: error.
     */
    public Response<Double> avgGet(final String key) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.AVGGET)
            .add(key), BuilderFactory.DOUBLE));
    }

    public Response<Double> avgGet(final byte[] key) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.AVGGET)
            .add(key), BuilderFactory.DOUBLE));
    }

    /**
     * Add the value of a avgArray key.
     *
     * @param key       the key
     * @param timestamp the timestamp
     * @param count     the count
     * @param value     the value
     * @return Success: avg value of timestamp; Fail: error.
     */
    public Response<Double> avgArrayAdd(final String key, final long timestamp, final long count, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.AVGARRAYADD)
            .add(key)
            .add(String.valueOf(timestamp))
            .add(String.valueOf(count))
            .add(String.valueOf(value)), BuilderFactory.DOUBLE));
    }

    public Response<Double> avgArrayAdd(final byte[] key, final long timestamp, final long count, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.AVGARRAYADD)
            .add(key)
            .add(toByteArray(timestamp))
            .add(toByteArray(count))
            .add(toByteArray(value)), BuilderFactory.DOUBLE));
    }

    /**
     * Add the value of a avgArray key.
     *
     * @param key       the key
     * @param timestamp the timestamp
     * @param count     the count
     * @param value     the value
     * @param params    the params: [EX time] [EXAT time] [PX time] [PXAT time]
     *                  `EX` - Set expire time (seconds)
     *                  `EXAT` - Set expire time as a UNIX timestamp (seconds)
     *                  `PX` - Set expire time (milliseconds)
     *                  `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: avg value of timestamp; Fail: error.
     */
    public Response<Double> avgArrayAdd(final String key, final long timestamp, final long count, final double value,
        final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.AVGARRAYADD)
            .addObjects(params.getByteParams(SafeEncoder.encode(key), toByteArray(timestamp), toByteArray(count),
                toByteArray(value))), BuilderFactory.DOUBLE));
    }

    public Response<Double> avgArrayAdd(final byte[] key, final long timestamp, final long count, final double value,
        final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.AVGARRAYADD)
            .addObjects(params.getByteParams(key, toByteArray(timestamp), toByteArray(count), toByteArray(value))),
            BuilderFactory.DOUBLE));
    }

    /**
     * Get the value of a avg key.
     *
     * @param key       the key
     * @param timestamp the timestamp
     * @return Success: avg value; Fail: error.
     */
    public Response<Double> avgArrayGet(final String key, final long timestamp) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.AVGARRAYGET)
            .add(SafeEncoder.encode(key))
            .add(toByteArray(timestamp)), BuilderFactory.DOUBLE));
    }

    public Response<Double> avgArrayGet(final byte[] key, final long timestamp) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.AVGARRAYGET)
            .add(key)
            .add(toByteArray(timestamp)), BuilderFactory.DOUBLE));
    }

    /**
     * Get the values of an array avg key range.
     *
     * @param key       the key
     * @param starttime the starttime
     * @param endtime   the endtime
     * @return Success: avg value list; Fail: error.
     */
    public Response<List<Double>> avgArrayGetRange(final String key, final long starttime, final long endtime) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.AVGARRAYGETRANGE)
            .add(SafeEncoder.encode(key))
            .add(toByteArray(starttime))
            .add(toByteArray(endtime)), CpcBuilderFactory.CPCARRAY_RANGE_RESULT));
    }

    public Response<List<Double>> avgArrayGetRange(final byte[] key, final long starttime, final long endtime) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.AVGARRAYGETRANGE)
            .add(key)
            .add(toByteArray(starttime))
            .add(toByteArray(endtime)), CpcBuilderFactory.CPCARRAY_RANGE_RESULT));
    }

    public Response<Double> avgArrayGetRangeTimeMerge(final String key, final long starttime, final long endtime) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.AVGARRAYGETTIMEMERGE)
            .add(SafeEncoder.encode(key))
            .add(toByteArray(starttime))
            .add(toByteArray(endtime)), BuilderFactory.DOUBLE));
    }

    public Response<Double> avgArrayGetRangeTimeMerge(final byte[] key, final long starttime, final long endtime) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.AVGARRAYGETTIMEMERGE)
            .add(key)
            .add(toByteArray(starttime))
            .add(toByteArray(endtime)), BuilderFactory.DOUBLE));
    }

    /**
     * Get the merge value of an array avg key range.
     *
     * @param key the key
     * @return Success: merge of avg value; Fail: error.
     */
    public Response<Double> avgArrayGetRangeMerge(final String key, final long endtime, final long range) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.AVGARRAYGETRANGEMERGE)
            .add(SafeEncoder.encode(key))
            .add(toByteArray(endtime))
            .add(toByteArray(range)), BuilderFactory.DOUBLE));
    }

    public Response<Double> avgArrayGetRangeMerge(final byte[] key, final long endtime, final long range) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.AVGARRAYGETRANGEMERGE)
            .add(key)
            .add(toByteArray(endtime))
            .add(toByteArray(range)), BuilderFactory.DOUBLE));
    }

    // stddev operation

    /**
     * Add the value of a stddev key.
     *
     * @param key   the key
     * @param value the value
     * @return Success: stddev value; Fail: error.
     */
    public Response<Double> stddevAdd(final String key, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.STDDEVADD)
            .add(key)
            .add(String.valueOf(value)), BuilderFactory.DOUBLE));
    }

    public Response<Double> stddevAdd(final byte[] key, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.STDDEVADD)
            .add(key)
            .add(toByteArray(value)), BuilderFactory.DOUBLE));
    }

    /**
     * Add the value of a stddev key.
     *
     * @param key    the key
     * @param value  the value
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     *               `EX` - Set expire time (seconds)
     *               `EXAT` - Set expire time as a UNIX timestamp (seconds)
     *               `PX` - Set expire time (milliseconds)
     *               `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: stddev value; Fail: error.
     */
    public Response<Double> stddevAdd(final String key, final double value, final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.STDDEVADD)
            .addObjects(params.getByteParams(SafeEncoder.encode(key), toByteArray(value))), BuilderFactory.DOUBLE));
    }

    public Response<Double> stddevAdd(final byte[] key, final double value, final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.STDDEVADD)
            .addObjects(params.getByteParams(key, toByteArray(value))), BuilderFactory.DOUBLE));
    }

    /**
     * Set the value of a stddev key.
     *
     * @param key   the key
     * @param value the value
     * @return Success: stddev value; Fail: error.
     */
    public Response<Double> stddevSet(final String key, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.STDDEVSET)
            .add(key)
            .add(String.valueOf(value)), BuilderFactory.DOUBLE));
    }

    public Response<Double> stddevSet(final byte[] key, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.STDDEVSET)
            .add(key)
            .add(toByteArray(value)), BuilderFactory.DOUBLE));
    }

    /**
     * Set the value of a stddev key.
     *
     * @param key    the key
     * @param value  the value
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     *               `EX` - Set expire time (seconds)
     *               `EXAT` - Set expire time as a UNIX timestamp (seconds)
     *               `PX` - Set expire time (milliseconds)
     *               `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: stddev value; Fail: error.
     */
    public Response<Double> stddevSet(final String key, final double value, final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.STDDEVSET)
            .addObjects(params.getByteParams(SafeEncoder.encode(key), toByteArray(value))), BuilderFactory.DOUBLE));
    }

    public Response<Double> stddevSet(final byte[] key, final double value, final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.STDDEVSET)
            .addObjects(params.getByteParams(key, toByteArray(value))), BuilderFactory.DOUBLE));
    }

    /**
     * Get the value of a stddev key.
     *
     * @param key the key
     * @return Success: stddev value; Fail: error.
     */
    public Response<Double> stddevGet(final String key) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.STDDEVGET)
            .add(key), BuilderFactory.DOUBLE));
    }

    public Response<Double> stddevGet(final byte[] key) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.STDDEVGET)
            .add(key), BuilderFactory.DOUBLE));
    }

    /**
     * Add the value of a stddevArray key.
     *
     * @param key       the key
     * @param timestamp the timestamp
     * @param value     the value
     * @return Success: stddev value of timestamp; Fail: error.
     */
    public Response<Double> stddevArrayAdd(final String key, final long timestamp, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.STDDEVARRAYADD)
            .add(key)
            .add(String.valueOf(timestamp))
            .add(String.valueOf(value)), BuilderFactory.DOUBLE));
    }

    public Response<Double> stddevArrayAdd(final byte[] key, final long timestamp, final double value) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.STDDEVARRAYADD)
            .add(key)
            .add(toByteArray(timestamp))
            .add(toByteArray(value)), BuilderFactory.DOUBLE));
    }

    /**
     * Add the value of a stddevArray key.
     *
     * @param key       the key
     * @param timestamp the timestamp
     * @param value     the value
     * @param params    the params: [EX time] [EXAT time] [PX time] [PXAT time]
     *                  `EX` - Set expire time (seconds)
     *                  `EXAT` - Set expire time as a UNIX timestamp (seconds)
     *                  `PX` - Set expire time (milliseconds)
     *                  `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: stddev value of timestamp; Fail: error.
     */
    public Response<Double> stddevArrayAdd(final String key, final long timestamp, final double value,
        final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.STDDEVARRAYADD)
            .addObjects(params.getByteParams(SafeEncoder.encode(key), toByteArray(timestamp), toByteArray(value))),
            BuilderFactory.DOUBLE));
    }

    public Response<Double> stddevArrayAdd(final byte[] key, final long timestamp, final double value,
        final CpcUpdateParams params) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.STDDEVARRAYADD)
            .addObjects(params.getByteParams(key, toByteArray(timestamp), toByteArray(value))), BuilderFactory.DOUBLE));
    }

    /**
     * Get the value of a stddev key.
     *
     * @param key       the key
     * @param timestamp the timestamp
     * @return Success: stddev value; Fail: error.
     */
    public Response<Double> stddevArrayGet(final String key, final long timestamp) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.STDDEVARRAYGET)
            .add(SafeEncoder.encode(key))
            .add(toByteArray(timestamp)), BuilderFactory.DOUBLE));
    }

    public Response<Double> stddevArrayGet(final byte[] key, final long timestamp) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.STDDEVARRAYGET)
            .add(key)
            .add(toByteArray(timestamp)), BuilderFactory.DOUBLE));
    }

    /**
     * Get the values of an array stddev key range.
     *
     * @param key       the key
     * @param starttime the starttime
     * @param endtime   the endtime
     * @return Success: stddev value list; Fail: error.
     */
    public Response<List<Double>> stddevArrayGetRange(final String key, final long starttime, final long endtime) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.STDDEVARRAYGETRANGE)
            .add(SafeEncoder.encode(key))
            .add(toByteArray(starttime))
            .add(toByteArray(endtime)), CpcBuilderFactory.CPCARRAY_RANGE_RESULT));
    }

    public Response<List<Double>> stddevArrayGetRange(final byte[] key, final long starttime, final long endtime) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.STDDEVARRAYGETRANGE)
            .add(key)
            .add(toByteArray(starttime))
            .add(toByteArray(endtime)), CpcBuilderFactory.CPCARRAY_RANGE_RESULT));
    }

    public Response<Double> stddevArrayGetRangeTimeMerge(final String key, final long starttime, final long endtime) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.STDDEVARRAYGETTIMEMERGE)
            .add(SafeEncoder.encode(key))
            .add(toByteArray(starttime))
            .add(toByteArray(endtime)), BuilderFactory.DOUBLE));
    }

    public Response<Double> stddevArrayGetRangeTimeMerge(final byte[] key, final long starttime, final long endtime) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.STDDEVARRAYGETTIMEMERGE)
            .add(key)
            .add(toByteArray(starttime))
            .add(toByteArray(endtime)), BuilderFactory.DOUBLE));
    }

    /**
     * Get the merge value of an array stddev key range.
     *
     * @param key the key
     * @return Success: merge of stddev value; Fail: error.
     */
    public Response<Double> stddevArrayGetRangeMerge(final String key, final long endtime, final long range) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.STDDEVARRAYGETRANGEMERGE)
            .add(SafeEncoder.encode(key))
            .add(toByteArray(endtime))
            .add(toByteArray(range)), BuilderFactory.DOUBLE));
    }

    public Response<Double> stddevArrayGetRangeMerge(final byte[] key, final long endtime, final long range) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.STDDEVARRAYGETRANGEMERGE)
            .add(key)
            .add(toByteArray(endtime))
            .add(toByteArray(range)), BuilderFactory.DOUBLE));
    }

    /**
     * Get the value of a key.
     *
     * @param key       the key
     * @param timestamp the timestamp
     * @return getResponse(Success : sum value ; Fail : error.
     */
    public Response<Object> sketchesGet(final String key, final long timestamp) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.SKETCHESGET)
            .add(SafeEncoder.encode(key))
            .add(toByteArray(timestamp)), CpcBuilderFactory.SKETCHES_GET_RESULT));
    }

    public Response<Object> sketchesGet(final byte[] key, final long timestamp) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.SKETCHESGET)
            .add(key)
            .add(toByteArray(timestamp)), CpcBuilderFactory.SKETCHES_GET_RESULT));
    }

    /**
     * Get the merge value of an array key range.
     *
     * @param key the key
     * @return getResponse(Success : merge of sum value ; Fail : error.
     */
    public Response<Object> sketchesGetRangeMerge(final String key, final long endtime, final long range) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.SKETCHESRANGEMERGE)
            .add(SafeEncoder.encode(key))
            .add(toByteArray(endtime))
            .add(toByteArray(range)), CpcBuilderFactory.SKETCHES_GET_RESULT));
    }

    public Response<Object> sketchesGetRangeMerge(final byte[] key, final long endtime, final long range) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.SKETCHESRANGEMERGE)
            .add(key)
            .add(toByteArray(endtime))
            .add(toByteArray(range)), CpcBuilderFactory.SKETCHES_GET_RESULT));
    }

    /**
     * Get the values of an array sum key range.
     *
     * @param key       the key
     * @param starttime the starttime
     * @param endtime   the endtime
     * @return getResponse(Success : sum value list ; Fail : error.
     */
    public Response<List<Double>> sketchesGetRange(final String key, final long starttime, final long endtime) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.SKETCHESRANGE)
            .add(SafeEncoder.encode(key))
            .add(toByteArray(starttime))
            .add(toByteArray(endtime)), CpcBuilderFactory.CPCARRAY_RANGE_RESULT));
    }

    public Response<List<Double>> sketchesGetRange(final byte[] key, final long starttime, final long endtime) {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.SKETCHESRANGE)
            .add(key)
            .add(toByteArray(starttime))
            .add(toByteArray(endtime)), CpcBuilderFactory.CPCARRAY_RANGE_RESULT));
    }

    /**
     * MutiUpdate the item of a cpcArray.
     *
     * @param keys {key timestamp item size expStr exp} [key timestamp item size expStr exp] ...
     * @return Success: OK; Fail: error.
     */
    public Response<String> sketchesBatchWrite(final ArrayList<CpcArrayMultiData> keys) {
        if (keys == null || keys.isEmpty()) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        for (CpcArrayMultiData key : keys) {
            if (key.getKey() == null) {
                throw new IllegalArgumentException(CommonResult.keyIsNull);
            }
        }

        CpcMultiArrayUpdateParams keyList = new CpcMultiArrayUpdateParams();
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.SKETCHESBATCHWRITE)
            .addObjects(keyList.getByteMultiParams(keys)), BuilderFactory.STRING));
    }
}
