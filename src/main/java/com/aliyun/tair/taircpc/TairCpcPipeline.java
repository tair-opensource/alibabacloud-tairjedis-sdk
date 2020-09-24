package com.aliyun.tair.taircpc;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.taircpc.factory.CpcBuilderFactory;
import com.aliyun.tair.taircpc.params.*;
import com.aliyun.tair.taircpc.results.Update2JudResult;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.util.JedisClusterCRC16;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static redis.clients.jedis.Protocol.toByteArray;

public class TairCpcPipeline extends Pipeline {

    /**
     * Estimate the cpc.
     *
     * @param key   the key
     * @return Success: double; Empty: 0; Fail: error.
     */
    public Response<Double> cpcEstimate(final String key) throws JedisConnectionException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCESTIMATE, key);
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> cpcEstimate(final byte[] key) throws JedisConnectionException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCESTIMATE, key);
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * MutiUpdate the cpc.
     *
     * @param keys    {key item expStr exp} [key item expStr exp] ...
     * @return Success: OK; Fail: error.
     */
    public Response<String> cpcMUpdate(final ArrayList<CpcData> keys) throws JedisConnectionException,IllegalArgumentException,
            JedisDataException {
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
        getClient("").sendCommand(ModuleCommand.CPCMUPDATE, keyList.getByteParams(keys));
        return getResponse(BuilderFactory.STRING);
    }

    /**
     * MutiUpdate the cpc.
     *
     * @param keys    {key item expStr exp} [key item expStr exp] ...
     * @return Success: List<Double>; Fail: error.
     */
    public Response<List<Double>> cpcMUpdate2Est(final ArrayList<CpcData> keys) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
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
        getClient("").sendCommand(ModuleCommand.CPCMUPDATE2EST, keyList.getByteParams(keys));
        return getResponse(CpcBuilderFactory.CPCUPDATE2EST_MULTI_RESULT);
    }

    /**
     * MutiUpdate the cpc.
     *
     * @param keys    {key item expStr exp} [key item expStr exp] ...
     * @return Success: HashMap<String, Double>; Fail: error.
     */
    public Response<HashMap<String, Double>> cpcMUpdate2EstWithKey(final ArrayList<CpcData> keys) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
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
        getClient("").sendCommand(ModuleCommand.CPCMUPDATE2ESTWITHKEY, keyList.getByteParams(keys));
        return getResponse(CpcBuilderFactory.CPCUPDATE2ESTWITHKEY_MULTI_RESULT);
    }

    /**
     * MutiUpdate the cpc.
     *
     * @param keys    {key item expStr exp} [key item expStr exp] ...
     * @return Success: List<Update2judResult>; Fail: error.
     */
    public Response<List<Update2JudResult>> cpcMUpdate2Jud(final ArrayList<CpcData> keys) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
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
        getClient("").sendCommand(ModuleCommand.CPCMUPDATE2JUD, keyList.getByteParams(keys));
        return getResponse(CpcBuilderFactory.CPCUPDATE2JUD_MULTI_RESULT);
    }

    /**
     * Update the item of a cpc.
     *
     * @param key   the key
     * @param item the item
     * @return Success: OK; Fail: error.
     */
    public Response<String> cpcUpdate(final String key, final String item) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCUPDATE, key, item);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> cpcUpdate(final byte[] key, final byte[] item) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCUPDATE, key, item);
        return getResponse(BuilderFactory.STRING);
    }

    /**
     * Update the item of a cpc.
     *
     * @param key   the key
     * @param item the item
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: OK; Fail: error.
     */
    public Response<String> cpcUpdate(final String key, final String item, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCUPDATE,
                params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(item)));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> cpcUpdate(final byte[] key, final byte[] item, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCUPDATE, params.getByteParams(key, item));
        return getResponse(BuilderFactory.STRING);
    }

    /**
     * Update the item of a cpc.
     *
     * @param key   the key
     * @param item the item
     * @return Success: String List; Fail: error.
     */
    public Response<Update2JudResult> cpcUpdate2Jud(final String key, final String item) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCUPDATE2JUD, key, item);
        return getResponse(CpcBuilderFactory.CPCUPDATE2JUD_RESULT);
    }

    public Response<Update2JudResult> cpcUpdate2Jud(final byte[] key, final byte[] item) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCUPDATE2JUD, key, item);
        return getResponse(CpcBuilderFactory.CPCUPDATE2JUD_RESULT);
    }

    /**
     * Update the item of a cpc.
     *
     * @param key   the key
     * @param item the item
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: String List; Fail: error.
     */
    public Response<Update2JudResult> cpcUpdate2Jud(final String key, final String item, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCUPDATE2JUD,
                params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(item)));
        return getResponse(CpcBuilderFactory.CPCUPDATE2JUD_RESULT);
    }

    public Response<Update2JudResult> cpcUpdate2Jud(final byte[] key, final byte[] item, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCUPDATE2JUD, params.getByteParams(key, item));
        return getResponse(CpcBuilderFactory.CPCUPDATE2JUD_RESULT);
    }

    /**
     * Update the item of a cpc.
     *
     * @param key   the key
     * @param item the item
     * @return Success: Double value; Fail: error.
     */
    public Response<Double> cpcUpdate2Est(final String key, final String item) throws JedisConnectionException,IllegalArgumentException,
            JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCUPDATE2EST, key, item);
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> cpcUpdate2Est(final byte[] key, final byte[] item) throws JedisConnectionException,IllegalArgumentException,
            JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCUPDATE2EST, key, item);
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Update the item of a cpc.
     *
     * @param key   the key
     * @param item the item
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: Double value; Fail: error.
     */
    public Response<Double> cpcUpdate2Est(final String key, final String item, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCUPDATE2EST,
                params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(item)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> cpcUpdate2Est(final byte[] key, final byte[] item, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCUPDATE2EST, params.getByteParams(key, item));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Update the item of a cpcArray.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @param item the item
     * @return Success: OK; Fail: error.
     */
    public Response<String> cpcArrayUpdate(final String key, final long timestamp, final String item) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYUPDATE, key, String.valueOf(timestamp), item);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> cpcArrayUpdate(final byte[] key, final long timestamp, final byte[] item) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYUPDATE, key, toByteArray(timestamp), item);
        return getResponse(BuilderFactory.STRING);
    }

    /**
     * Update the item of a cpcArray.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @param item the item
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: OK; Fail: error.
     */
    public Response<String> cpcArrayUpdate(final String key, final long timestamp, final String item, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYUPDATE, params.getByteParams(SafeEncoder.encode(key), toByteArray(timestamp), SafeEncoder.encode(item)));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> cpcArrayUpdate(final byte[] key, final long timestamp, final byte[] item, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYUPDATE, params.getByteParams(key, toByteArray(timestamp), item));
        return getResponse(BuilderFactory.STRING);
    }

    /**
     * Update the item of a cpcArray.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @param item the item
     * @return Success: Double value; Fail: error.
     */
    public Response<Double> cpcArrayUpdate2Est(final String key, final long timestamp, final String item) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYUPDATE2EST, key, String.valueOf(timestamp), item);
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> cpcArrayUpdate2Est(final byte[] key, final long timestamp, final byte[] item) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYUPDATE2EST, key, toByteArray(timestamp), item);
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Update the item of a cpcArray.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @param item the item
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: Double value; Fail: error.
     */
    public Response<Double> cpcArrayUpdate2Est(final String key, final long timestamp, final String item, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYUPDATE2EST, params.getByteParams(SafeEncoder.encode(key),
                toByteArray(timestamp), SafeEncoder.encode(item)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> cpcArrayUpdate2Est(final byte[] key, final long timestamp, final byte[] item, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYUPDATE2EST, params.getByteParams(key, toByteArray(timestamp),
                item));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Update the item of a cpcArray.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @param item the item
     * @return Success: Update2JudResult; Fail: error.
     */
    public Response<Update2JudResult> cpcArrayUpdate2Jud(final String key, final long timestamp, final String item) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYUPDATE2JUD, key, String.valueOf(timestamp), item);
        return getResponse(CpcBuilderFactory.CPCUPDATE2JUD_RESULT);
    }

    public Response<Update2JudResult> cpcArrayUpdate2Jud(final byte[] key, final long timestamp, final byte[] item) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYUPDATE2JUD, key, toByteArray(timestamp), item);
        return getResponse(CpcBuilderFactory.CPCUPDATE2JUD_RESULT);
    }

    /**
     * Update the item of a cpcArray.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @param item the item
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: Update2JudResult; Fail: error.
     */
    public Response<Update2JudResult> cpcArrayUpdate2Jud(final String key, final long timestamp, final String item, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYUPDATE2JUD, params.getByteParams(SafeEncoder.encode(key),
                toByteArray(timestamp), SafeEncoder.encode(item)));
        return getResponse(CpcBuilderFactory.CPCUPDATE2JUD_RESULT);
    }

    public Response<Update2JudResult> cpcArrayUpdate2Jud(final byte[] key, final long timestamp, final byte[] item, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYUPDATE2JUD, params.getByteParams(key, toByteArray(timestamp),
                item));
        return getResponse(CpcBuilderFactory.CPCUPDATE2JUD_RESULT);
    }

    /**
     * MutiUpdate the item of a cpcArray.
     *
     * @param keys    {key timestamp item size expStr exp} [key timestamp item size expStr exp] ...
     * @return Success: OK; Fail: error.
     */
    public Response<String> cpcArrayMUpdate(final ArrayList<CpcArrayData> keys) throws JedisConnectionException,IllegalArgumentException,
            JedisDataException {
        if (keys == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        for (CpcArrayData key : keys) {
            if (key.getKey() == null) {
                throw new IllegalArgumentException(CommonResult.keyIsNull);
            }
        }
        for (CpcArrayData key : keys) {
            if (key.getItem() == null) {
                throw new IllegalArgumentException(CommonResult.valueIsNull);
            }
        }
        CpcMultiArrayUpdateParams keyList = new CpcMultiArrayUpdateParams();
        getClient("").sendCommand(ModuleCommand.CPCMARRAYUPDATE, keyList.getByteParams(keys));
        return getResponse(BuilderFactory.STRING);
    }

    /**
     * MutiUpdate the item of a cpcArray.
     *
     * @param keys    {key timestamp item size expStr exp} [key timestamp item size expStr exp] ...
     * @return Success: List<Double>; Fail: error.
     */
    public Response<List<Double>> cpcArrayMUpdate2Est(final ArrayList<CpcArrayData> keys) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (keys == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        for (CpcArrayData key : keys) {
            if (key.getKey() == null) {
                throw new IllegalArgumentException(CommonResult.keyIsNull);
            }
        }
        for (CpcArrayData key : keys) {
            if (key.getItem() == null) {
                throw new IllegalArgumentException(CommonResult.valueIsNull);
            }
        }
        CpcMultiArrayUpdateParams keyList = new CpcMultiArrayUpdateParams();
        getClient("").sendCommand(ModuleCommand.CPCMARRAYUPDATE2EST, keyList.getByteParams(keys));
        return getResponse(CpcBuilderFactory.CPCARRAYUPDATE2EST_MULTI_RESULT);
    }

//    /**
//     * MutiUpdate the item of a cpcArray.
//     *
//     * @param keys    {key timestamp item size expStr exp} [key timestamp item size expStr exp] ...
//     * @return Success: HashMap<String, Double>; Fail: error.
//     */
//    public Response<HashMap<String, Double>> cpcArrayMUpdate2EstWithKey(final ArrayList<CpcArrayData> keys) throws JedisConnectionException,
//            IllegalArgumentException, JedisDataException {
//        if (keys == null) {
//            throw new IllegalArgumentException(CommonResult.keyIsNull);
//        }
//        for (CpcArrayData key : keys) {
//            if (key.getKey() == null) {
//                throw new IllegalArgumentException(CommonResult.keyIsNull);
//            }
//        }
//        for (CpcArrayData key : keys) {
//            if (key.getItem() == null) {
//                throw new IllegalArgumentException(CommonResult.valueIsNull);
//            }
//        }
//        CpcMultiArrayUpdateParams keyList = new CpcMultiArrayUpdateParams();
//        getClient("").sendCommand(ModuleCommand.CPCMARRAYUPDATE2ESTWITHKEY, keyList.getByteParams(keys));
//        return getResponse(CpcBuilderFactory.CPCUPDATE2ESTWITHKEY_MULTI_RESULT);
//    }

    /**
     * MutiUpdate the item of a cpcArray.
     *
     * @param keys    {key timestamp item size expStr exp} [key timestamp item size expStr exp] ...
     * @return Success: List<Update2JudResult>; Fail: error.
     */
    public Response<List<Update2JudResult>> cpcArrayMUpdate2Jud(final ArrayList<CpcArrayData> keys) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (keys == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        for (CpcArrayData key : keys) {
            if (key.getKey() == null) {
                throw new IllegalArgumentException(CommonResult.keyIsNull);
            }
        }
        for (CpcArrayData key : keys) {
            if (key.getItem() == null) {
                throw new IllegalArgumentException(CommonResult.valueIsNull);
            }
        }
        CpcMultiArrayUpdateParams keyList = new CpcMultiArrayUpdateParams();
        getClient("").sendCommand(ModuleCommand.CPCMARRAYUPDATE2JUD, keyList.getByteParams(keys));
        return getResponse(CpcBuilderFactory.CPCARRAYUPDATE2JUD_MULTI_RESULT);
    }

    /**
     * MutiUpdate the item of a cpcArray.
     *
     * @param keys    {key timestamp item size expStr exp} [key timestamp item size expStr exp] ...
     * @return Success: HashMap<String, Update2JudResult>; Fail: error.
     */
    public Response<HashMap<String, Update2JudResult>> cpcArrayMUpdate2JudWithKey(final ArrayList<CpcArrayData> keys) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (keys == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        for (CpcArrayData key : keys) {
            if (key.getKey() == null) {
                throw new IllegalArgumentException(CommonResult.keyIsNull);
            }
        }
        for (CpcArrayData key : keys) {
            if (key.getItem() == null) {
                throw new IllegalArgumentException(CommonResult.valueIsNull);
            }
        }
        CpcMultiArrayUpdateParams keyList = new CpcMultiArrayUpdateParams();
        getClient("").sendCommand(ModuleCommand.CPCMARRAYUPDATE2JUDWITHKEY, keyList.getByteParams(keys));
        return getResponse(CpcBuilderFactory.CPCUPDATE2JUDWITHKEY_MULTI_RESULT);
    }

    /**
     * Estimate the cpcArray.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @return Success: double; Empty: 0; Fail: error.
     */
    public Response<Double> cpcArrayEstimate(final String key, final long timestamp) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYESTIMATE, key, String.valueOf(timestamp));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> cpcArrayEstimate(final byte[] key, final long timestamp) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYESTIMATE, key, toByteArray(timestamp));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Estimate the cpcArray for a range.
     *
     * @param key   the key
     * @param starttime the starttime
     * @param endtime the endtime
     * @return Success: String List; Fail: error.
     */
    public Response<List<Double>> cpcArrayEstimateRange(final String key, final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYESTIMATERANGE, key, String.valueOf(starttime), String.valueOf(endtime));
        return getResponse(CpcBuilderFactory.CPCARRAY_ESTIMATE_RANGE_RESULT);
    }

    public Response<List<Double>> cpcArrayEstimateRange(final byte[] key, final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYESTIMATERANGE, key, toByteArray(starttime), toByteArray(endtime));
        return getResponse(CpcBuilderFactory.CPCARRAY_ESTIMATE_RANGE_RESULT);
    }

    /**
     * Estimate & sum the cpcArray for a range.
     *
     * @param key   the key
     * @param starttime the starttime
     * @param endtime the endtime
     * @return Success: double; Empty: 0; Fail: error.
     */
    public Response<Double> cpcArrayEstimateRangeSum(final String key, final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYESTIMATERANGESUM, key, String.valueOf(starttime), String.valueOf(endtime));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> cpcArrayEstimateRangeSum(final byte[] key, final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYESTIMATERANGESUM, key, toByteArray(starttime), toByteArray(endtime));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Estimate & merge the cpcArray for a range.
     *
     * @param key   the key
     * @param endtime the endtime
     * @param range the range
     * @return Success: double; Empty: 0; Fail: error.
     */
    public Response<Double> cpcArrayEstimateRangeMerge(final String key, final long endtime, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYESTIMATERANGEMERGE, key, String.valueOf(endtime), String.valueOf(range));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> cpcArrayEstimateRangeMerge(final byte[] key, final long endtime, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYESTIMATERANGEMERGE, key, toByteArray(endtime), toByteArray(range));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public int getSlot (String key) throws JedisConnectionException {
        return JedisClusterCRC16.getSlot(key);
    }

    // sum operation

    /**
     * Add the value of a sum key.
     *
     * @param key   the key
     * @param value the value
     * @return Success: sum value; Fail: error.
     */
    public Response<Double> sumAdd(final String key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.SUMADD, key, String.valueOf(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> sumAdd(final byte[] key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.SUMADD, key, toByteArray(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Add the value of a sum key.
     *
     * @param key   the key
     * @param value the value
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: sum value; Fail: error.
     */
    public Response<Double> sumAdd(final String key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.SUMADD,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(value)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> sumAdd(final byte[] key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.SUMADD, params.getByteParams(key, toByteArray(value)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Set the value of a sum key.
     *
     * @param key   the key
     * @param value the value
     * @return Success: sum value; Fail: error.
     */
    public Response<Double> sumSet(final String key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.SUMSET, key, String.valueOf(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> sumSet(final byte[] key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.SUMSET, key, toByteArray(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Set the value of a sum key.
     *
     * @param key   the key
     * @param value the value
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: sum value; Fail: error.
     */
    public Response<Double> sumSet(final String key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.SUMSET,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(value)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> sumSet(final byte[] key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.SUMSET, params.getByteParams(key, toByteArray(value)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Get the value of a sum key.
     *
     * @param key   the key
     * @return Success: sum value; Fail: error.
     */
    public Response<Double> sumGet(final String key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.SUMGET, key);
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> sumGet(final byte[] key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.SUMGET, key);
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Add the value of a sumArray key.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @param value the value
     * @return Success: sum value of timestamp; Fail: error.
     */
    public Response<Double> sumArrayAdd(final String key, final long timestamp, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.SUMARRAYADD, key, String.valueOf(timestamp), String.valueOf(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> sumArrayAdd(final byte[] key, final long timestamp, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.SUMARRAYADD, key, toByteArray(timestamp), toByteArray(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Add the value of a sumArray key.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @param value the value
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: sum value of timestamp; Fail: error.
     */
    public Response<Double> sumArrayAdd(final String key, final long timestamp, final double value, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.SUMARRAYADD, params.getByteParams(SafeEncoder.encode(key), toByteArray(timestamp), toByteArray(value)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> sumArrayAdd(final byte[] key, final long timestamp, final double value, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.SUMARRAYADD, params.getByteParams(key, toByteArray(timestamp), toByteArray(value)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Get the value of a sum key.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @return Success: sum value; Fail: error.
     */
    public Response<Double> sumArrayGet(final String key, final long timestamp) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.SUMARRAYGET, SafeEncoder.encode(key), toByteArray(timestamp));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> sumArrayGet(final byte[] key, final long timestamp) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.SUMARRAYGET, key, toByteArray(timestamp));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Get the values of an array sum key range.
     *
     * @param key   the key
     * @param starttime the starttime
     * @param endtime the endtime
     * @return Success: sum value list; Fail: error.
     */
    public Response<List<Double>> sumArrayGetRange(final String key, final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.SUMARRAYGETRANGE, SafeEncoder.encode(key), toByteArray(starttime), toByteArray(endtime));
        return getResponse(CpcBuilderFactory.CPCARRAY_RANGE_RESULT);
    }

    public Response<List<Double>> sumArrayGetRange(final byte[] key, final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.SUMARRAYGETRANGE, key, toByteArray(starttime), toByteArray(endtime));
        return getResponse(CpcBuilderFactory.CPCARRAY_RANGE_RESULT);
    }

    public Response<Double> sumArrayGetRangeTimeMerge(final String key,  final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.SUMARRAYGETTIMEMERGE, SafeEncoder.encode(key), toByteArray(starttime), toByteArray(endtime));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> sumArrayGetRangeTimeMerge(final byte[] key,  final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.SUMARRAYGETTIMEMERGE, key, toByteArray(starttime), toByteArray(endtime));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Get the merge value of an array sum key range.
     *
     * @param key   the key
     * @return Success: merge of sum value; Fail: error.
     */
    public Response<Double> sumArrayGetRangeMerge(final String key,  final long endtime, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.SUMARRAYGETRANGEMERGE, SafeEncoder.encode(key), toByteArray(endtime), toByteArray(range));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> sumArrayGetRangeMerge(final byte[] key,  final long endtime, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.SUMARRAYGETRANGEMERGE, key, toByteArray(endtime), toByteArray(range));
        return getResponse(BuilderFactory.DOUBLE);
    }


    // max operation

    /**
     * Add the value of a max key.
     *
     * @param key   the key
     * @param value the value
     * @return Success: max value; Fail: error.
     */
    public Response<Double> maxAdd(final String key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MAXADD, key, String.valueOf(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> maxAdd(final byte[] key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MAXADD, key, toByteArray(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Add the value of a max key.
     *
     * @param key   the key
     * @param value the value
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: max value; Fail: error.
     */
    public Response<Double> maxAdd(final String key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MAXADD,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(value)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> maxAdd(final byte[] key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MAXADD, params.getByteParams(key, toByteArray(value)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Set the value of a max key.
     *
     * @param key   the key
     * @param value the value
     * @return Success: max value; Fail: error.
     */
    public Response<Double> maxSet(final String key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MAXSET, key, String.valueOf(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> maxSet(final byte[] key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MAXSET, key, toByteArray(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Set the value of a max key.
     *
     * @param key   the key
     * @param value the value
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: max value; Fail: error.
     */
    public Response<Double> maxSet(final String key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MAXSET,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(value)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> maxSet(final byte[] key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MAXSET, params.getByteParams(key, toByteArray(value)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Get the value of a max key.
     *
     * @param key   the key
     * @return Success: max value; Fail: error.
     */
    public Response<Double> maxGet(final String key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MAXGET, key);
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> maxGet(final byte[] key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MAXGET, key);
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Add the value of a maxArray key.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @param value the value
     * @return Success: max value of timestamp; Fail: error.
     */
    public Response<Double> maxArrayAdd(final String key, final long timestamp, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MAXARRAYADD, key, String.valueOf(timestamp), String.valueOf(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> maxArrayAdd(final byte[] key, final long timestamp, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MAXARRAYADD, key, toByteArray(timestamp), toByteArray(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Add the value of a maxArray key.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @param value the value
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: max value of timestamp; Fail: error.
     */
    public Response<Double> maxArrayAdd(final String key, final long timestamp, final double value, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MAXARRAYADD, params.getByteParams(SafeEncoder.encode(key), toByteArray(timestamp), toByteArray(value)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> maxArrayAdd(final byte[] key, final long timestamp, final double value, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MAXARRAYADD, params.getByteParams(key, toByteArray(timestamp), toByteArray(value)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Get the value of a max key.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @return Success: max value; Fail: error.
     */
    public Response<Double> maxArrayGet(final String key, final long timestamp) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MAXARRAYGET, SafeEncoder.encode(key), toByteArray(timestamp));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> maxArrayGet(final byte[] key, final long timestamp) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MAXARRAYGET, key, toByteArray(timestamp));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Get the values of an array max key range.
     *
     * @param key   the key
     * @param starttime the starttime
     * @param endtime the endtime
     * @return Success: max value list; Fail: error.
     */
    public Response<List<Double>> maxArrayGetRange(final String key, final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MAXARRAYGETRANGE, SafeEncoder.encode(key), toByteArray(starttime), toByteArray(endtime));
        return getResponse(CpcBuilderFactory.CPCARRAY_RANGE_RESULT);
    }

    public Response<List<Double>> maxArrayGetRange(final byte[] key, final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MAXARRAYGETRANGE, key, toByteArray(starttime), toByteArray(endtime));
        return getResponse(CpcBuilderFactory.CPCARRAY_RANGE_RESULT);
    }

    public Response<Double> maxArrayGetRangeTimeMerge(final String key,  final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MAXARRAYGETTIMEMERGE, SafeEncoder.encode(key), toByteArray(starttime), toByteArray(endtime));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> maxArrayGetRangeTimeMerge(final byte[] key,  final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MAXARRAYGETTIMEMERGE, key, toByteArray(starttime), toByteArray(endtime));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Get the merge value of an array max key range.
     *
     * @param key   the key
     * @return Success: merge of max value; Fail: error.
     */
    public Response<Double> maxArrayGetRangeMerge(final String key,  final long endtime, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MAXARRAYGETRANGEMERGE, SafeEncoder.encode(key), toByteArray(endtime), toByteArray(range));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> maxArrayGetRangeMerge(final byte[] key,  final long endtime, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MAXARRAYGETRANGEMERGE, key, toByteArray(endtime), toByteArray(range));
        return getResponse(BuilderFactory.DOUBLE);
    }

    // min operation

    /**
     * Add the value of a min key.
     *
     * @param key   the key
     * @param value the value
     * @return Success: min value; Fail: error.
     */
    public Response<Double> minAdd(final String key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MINADD, key, String.valueOf(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> minAdd(final byte[] key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MINADD, key, toByteArray(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Add the value of a min key.
     *
     * @param key   the key
     * @param value the value
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: min value; Fail: error.
     */
    public Response<Double> minAdd(final String key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MINADD,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(value)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> minAdd(final byte[] key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MINADD, params.getByteParams(key, toByteArray(value)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Set the value of a min key.
     *
     * @param key   the key
     * @param value the value
     * @return Success: min value; Fail: error.
     */
    public Response<Double> minSet(final String key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MINSET, key, String.valueOf(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> minSet(final byte[] key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MINSET, key, toByteArray(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Set the value of a min key.
     *
     * @param key   the key
     * @param value the value
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: min value; Fail: error.
     */
    public Response<Double> minSet(final String key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MINSET,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(value)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> minSet(final byte[] key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MINSET, params.getByteParams(key, toByteArray(value)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Get the value of a min key.
     *
     * @param key   the key
     * @return Success: min value; Fail: error.
     */
    public Response<Double> minGet(final String key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MINGET, key);
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> minGet(final byte[] key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MINGET, key);
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Add the value of a minArray key.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @param value the value
     * @return Success: min value of timestamp; Fail: error.
     */
    public Response<Double> minArrayAdd(final String key, final long timestamp, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MINARRAYADD, key, String.valueOf(timestamp), String.valueOf(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> minArrayAdd(final byte[] key, final long timestamp, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MINARRAYADD, key, toByteArray(timestamp), toByteArray(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Add the value of a minArray key.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @param value the value
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: min value of timestamp; Fail: error.
     */
    public Response<Double> minArrayAdd(final String key, final long timestamp, final double value, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MINARRAYADD, params.getByteParams(SafeEncoder.encode(key), toByteArray(timestamp), toByteArray(value)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> minArrayAdd(final byte[] key, final long timestamp, final double value, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MINARRAYADD, params.getByteParams(key, toByteArray(timestamp), toByteArray(value)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Get the value of a min key.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @return Success: min value; Fail: error.
     */
    public Response<Double> minArrayGet(final String key, final long timestamp) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MINARRAYGET, SafeEncoder.encode(key), toByteArray(timestamp));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> minArrayGet(final byte[] key, final long timestamp) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MINARRAYGET, key, toByteArray(timestamp));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Get the values of an array min key range.
     *
     * @param key   the key
     * @param starttime the starttime
     * @param endtime the endtime
     * @return Success: min value list; Fail: error.
     */
    public Response<List<Double>> minArrayGetRange(final String key, final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MINARRAYGETRANGE, SafeEncoder.encode(key), toByteArray(starttime), toByteArray(endtime));
        return getResponse(CpcBuilderFactory.CPCARRAY_RANGE_RESULT);
    }

    public Response<List<Double>> minArrayGetRange(final byte[] key, final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MINARRAYGETRANGE, key, toByteArray(starttime), toByteArray(endtime));
        return getResponse(CpcBuilderFactory.CPCARRAY_RANGE_RESULT);
    }

    public Response<Double> minArrayGetRangeTimeMerge(final String key,  final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MINARRAYGETTIMEMERGE, SafeEncoder.encode(key), toByteArray(starttime), toByteArray(endtime));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> minArrayGetRangeTimeMerge(final byte[] key,  final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MINARRAYGETTIMEMERGE, key, toByteArray(starttime), toByteArray(endtime));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Get the merge value of an array min key range.
     *
     * @param key   the key
     * @return Success: merge of min value; Fail: error.
     */
    public Response<Double> minArrayGetRangeMerge(final String key,  final long endtime, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MINARRAYGETRANGEMERGE, SafeEncoder.encode(key), toByteArray(endtime), toByteArray(range));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> minArrayGetRangeMerge(final byte[] key,  final long endtime, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.MINARRAYGETRANGEMERGE, key, toByteArray(endtime), toByteArray(range));
        return getResponse(BuilderFactory.DOUBLE);
    }

    // first operation

    /**
     * Add the value of a first key.
     *
     * @param key   the key
     * @param content the content
     * @param value the value
     * @return Success: first value; Fail: error.
     */
    public Response<String> firstAdd(final String key, final String content, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.FIRSTADD, key, content, String.valueOf(value));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> firstAdd(final byte[] key, final byte[] content, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.FIRSTADD, key, content, toByteArray(value));
        return getResponse(BuilderFactory.STRING);
    }

    /**
     * Add the value of a first key.
     *
     * @param key   the key
     * @param content the content
     * @param value the value
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: first value; Fail: error.
     */
    public Response<String> firstAdd(final String key, final String content, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.FIRSTADD,
                params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(content), toByteArray(value)));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> firstAdd(final byte[] key, final byte[] content, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.FIRSTADD, params.getByteParams(key, content, toByteArray(value)));
        return getResponse(BuilderFactory.STRING);
    }

    /**
     * Set the value of a first key.
     *
     * @param key   the key
     * @param content the content
     * @param value the value
     * @return Success: first value; Fail: error.
     */
    public Response<String> firstSet(final String key, final String content, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.FIRSTSET, key, content, String.valueOf(value));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> firstSet(final byte[] key, final byte[] content, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.FIRSTSET, key, content, toByteArray(value));
        return getResponse(BuilderFactory.STRING);
    }

    /**
     * Set the value of a first key.
     *
     * @param key   the key
     * @param content the content
     * @param value the value
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: first value; Fail: error.
     */
    public Response<String> firstSet(final String key, final String content, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.FIRSTSET,
                params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(content), toByteArray(value)));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> firstSet(final byte[] key, final byte[] content, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.FIRSTSET, params.getByteParams(key, content, toByteArray(value)));
        return getResponse(BuilderFactory.STRING);
    }

    /**
     * Get the value of a first key.
     *
     * @param key   the key
     * @return Success: first value; Fail: error.
     */
    public Response<String> firstGet(final String key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.FIRSTGET, key);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> firstGet(final byte[] key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.FIRSTGET, key);
        return getResponse(BuilderFactory.STRING);
    }

    /**
     * Add the value of a firstArray key.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @param content the content
     * @param value the value
     * @return Success: first value of timestamp; Fail: error.
     */
    public Response<String> firstArrayAdd(final String key, final long timestamp, final String content, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.FIRSTARRAYADD, key, String.valueOf(timestamp), content, String.valueOf(value));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> firstArrayAdd(final byte[] key, final long timestamp, final byte[] content, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.FIRSTARRAYADD, key, toByteArray(timestamp), content, toByteArray(value));
        return getResponse(BuilderFactory.STRING);
    }

    /**
     * Add the value of a firstArray key.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @param content the content
     * @param value the value
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: first value of timestamp; Fail: error.
     */
    public Response<String> firstArrayAdd(final String key, final long timestamp, final String content, final double value, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.FIRSTARRAYADD, params.getByteParams(SafeEncoder.encode(key), toByteArray(timestamp), SafeEncoder.encode(content), toByteArray(value)));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> firstArrayAdd(final byte[] key, final long timestamp, final byte[] content, final double value, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.FIRSTARRAYADD, params.getByteParams(key, toByteArray(timestamp), content, toByteArray(value)));
        return getResponse(BuilderFactory.STRING);
    }

    /**
     * Get the value of a first key.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @return Success: first value; Fail: error.
     */
    public Response<String> firstArrayGet(final String key, final long timestamp) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.FIRSTARRAYGET, SafeEncoder.encode(key), toByteArray(timestamp));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> firstArrayGet(final byte[] key, final long timestamp) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.FIRSTARRAYGET, key, toByteArray(timestamp));
        return getResponse(BuilderFactory.STRING);
    }

//    /**
//     * Get the values of an array first key range.
//     *
//     * @param key   the key
//     * @param starttime the starttime
//     * @param endtime the endtime
//     * @return Success: first value list; Fail: error.
//     */
//    public Response<List<String>> firstArrayGetRange(final String key, final long starttime, final long endtime) throws JedisConnectionException,
//            IllegalArgumentException, JedisDataException {
//        if (key == null) {
//            throw new IllegalArgumentException(CommonResult.keyIsNull);
//        }
//        getClient("").sendCommand(ModuleCommand.FIRSTARRAYGETRANGE, SafeEncoder.encode(key), toByteArray(starttime), toByteArray(endtime));
//        return getResponse(BuilderFactory.STRING_LIST);
//    }
//
//    public Response<List<String>> firstArrayGetRange(final byte[] key, final long starttime, final long endtime) throws JedisConnectionException,
//            IllegalArgumentException, JedisDataException {
//        if (key == null) {
//            throw new IllegalArgumentException(CommonResult.keyIsNull);
//        }
//        getClient("").sendCommand(ModuleCommand.FIRSTARRAYGETRANGE, key, toByteArray(starttime), toByteArray(endtime));
//        return getResponse(BuilderFactory.STRING_LIST);
//    }

    public Response<String> firstArrayGetRangeTimeMerge(final String key,  final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.FIRSTARRAYGETTIMEMERGE, SafeEncoder.encode(key), toByteArray(starttime), toByteArray(endtime));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> firstArrayGetRangeTimeMerge(final byte[] key,  final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.FIRSTARRAYGETTIMEMERGE, key, toByteArray(starttime), toByteArray(endtime));
        return getResponse(BuilderFactory.STRING);
    }

    /**
     * Get the merge value of an array first key range.
     *
     * @param key   the key
     * @return Success: merge of first value; Fail: error.
     */
    public Response<String> firstArrayGetRangeMerge(final String key,  final long endtime, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.FIRSTARRAYGETRANGEMERGE, SafeEncoder.encode(key), toByteArray(endtime), toByteArray(range));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> firstArrayGetRangeMerge(final byte[] key,  final long endtime, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.FIRSTARRAYGETRANGEMERGE, key, toByteArray(endtime), toByteArray(range));
        return getResponse(BuilderFactory.STRING);
    }

    // last operation

    /**
     * Add the value of a last key.
     *
     * @param key   the key
     * @param content the content
     * @param value the value
     * @return Success: last value; Fail: error.
     */
    public Response<String> lastAdd(final String key, final String content, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.LASTADD, key, content, String.valueOf(value));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> lastAdd(final byte[] key, final byte[] content, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.LASTADD, key, content, toByteArray(value));
        return getResponse(BuilderFactory.STRING);
    }

    /**
     * Add the value of a last key.
     *
     * @param key   the key
     * @param content the content
     * @param value the value
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: last value; Fail: error.
     */
    public Response<String> lastAdd(final String key, final String content, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.LASTADD,
                params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(content), toByteArray(value)));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> lastAdd(final byte[] key, final byte[] content, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.LASTADD, params.getByteParams(key, content, toByteArray(value)));
        return getResponse(BuilderFactory.STRING);
    }

    /**
     * Set the value of a last key.
     *
     * @param key   the key
     * @param content the content
     * @param value the value
     * @return Success: last value; Fail: error.
     */
    public Response<String> lastSet(final String key, final String content, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.LASTSET, key, content, String.valueOf(value));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> lastSet(final byte[] key, final byte[] content, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.LASTSET, key, content, toByteArray(value));
        return getResponse(BuilderFactory.STRING);
    }

    /**
     * Set the value of a last key.
     *
     * @param key   the key
     * @param content the content
     * @param value the value
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: last value; Fail: error.
     */
    public Response<String> lastSet(final String key, final String content, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.LASTSET,
                params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(content), toByteArray(value)));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> lastSet(final byte[] key, final byte[] content, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.LASTSET, params.getByteParams(key, content, toByteArray(value)));
        return getResponse(BuilderFactory.STRING);
    }

    /**
     * Get the value of a last key.
     *
     * @param key   the key
     * @return Success: last value; Fail: error.
     */
    public Response<String> lastGet(final String key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.LASTGET, key);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> lastGet(final byte[] key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.LASTGET, key);
        return getResponse(BuilderFactory.STRING);
    }

    /**
     * Add the value of a lastArray key.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @param content the content
     * @param value the value
     * @return Success: last value of timestamp; Fail: error.
     */
    public Response<String> lastArrayAdd(final String key, final long timestamp, final String content, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.LASTARRAYADD, key, String.valueOf(timestamp), content, String.valueOf(value));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> lastArrayAdd(final byte[] key, final long timestamp, final byte[] content, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.LASTARRAYADD, key, toByteArray(timestamp), content, toByteArray(value));
        return getResponse(BuilderFactory.STRING);
    }

    /**
     * Add the value of a lastArray key.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @param content the content
     * @param value the value
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: last value of timestamp; Fail: error.
     */
    public Response<String> lastArrayAdd(final String key, final long timestamp, final String content, final double value, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.LASTARRAYADD, params.getByteParams(SafeEncoder.encode(key), toByteArray(timestamp), SafeEncoder.encode(content), toByteArray(value)));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> lastArrayAdd(final byte[] key, final long timestamp, final byte[] content, final double value, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.LASTARRAYADD, params.getByteParams(key, toByteArray(timestamp), content, toByteArray(value)));
        return getResponse(BuilderFactory.STRING);
    }

    /**
     * Get the value of a last key.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @return Success: last value; Fail: error.
     */
    public Response<String> lastArrayGet(final String key, final long timestamp) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.LASTARRAYGET, SafeEncoder.encode(key), toByteArray(timestamp));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> lastArrayGet(final byte[] key, final long timestamp) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.LASTARRAYGET, key, toByteArray(timestamp));
        return getResponse(BuilderFactory.STRING);
    }

//    /**
//     * Get the values of an array last key range.
//     *
//     * @param key   the key
//     * @param starttime the starttime
//     * @param endtime the endtime
//     * @return Success: last value list; Fail: error.
//     */
//    public Response<List<String>> lastArrayGetRange(final String key, final long starttime, final long endtime) throws JedisConnectionException,
//            IllegalArgumentException, JedisDataException {
//        if (key == null) {
//            throw new IllegalArgumentException(CommonResult.keyIsNull);
//        }
//        getClient("").sendCommand(ModuleCommand.LASTARRAYGETRANGE, SafeEncoder.encode(key), toByteArray(starttime), toByteArray(endtime));
//        return getResponse(BuilderFactory.STRING_LIST);
//    }
//
//    public Response<List<String>> lastArrayGetRange(final byte[] key, final long starttime, final long endtime) throws JedisConnectionException,
//            IllegalArgumentException, JedisDataException {
//        if (key == null) {
//            throw new IllegalArgumentException(CommonResult.keyIsNull);
//        }
//        getClient("").sendCommand(ModuleCommand.LASTARRAYGETRANGE, key, toByteArray(starttime), toByteArray(endtime));
//        return getResponse(BuilderFactory.STRING_LIST);
//    }

    public Response<String> lastArrayGetRangeTimeMerge(final String key,  final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.LASTARRAYGETTIMEEMERGE, SafeEncoder.encode(key), toByteArray(starttime), toByteArray(endtime));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> lastArrayGetRangeTimeMerge(final byte[] key,  final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.LASTARRAYGETTIMEEMERGE, key, toByteArray(starttime), toByteArray(endtime));
        return getResponse(BuilderFactory.STRING);
    }

    /**
     * Get the merge value of an array last key range.
     *
     * @param key   the key
     * @return Success: merge of last value; Fail: error.
     */
    public Response<String> lastArrayGetRangeMerge(final String key,  final long endtime, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.LASTARRAYGETRANGEMERGE, SafeEncoder.encode(key), toByteArray(endtime), toByteArray(range));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> lastArrayGetRangeMerge(final byte[] key,  final long endtime, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.LASTARRAYGETRANGEMERGE, key, toByteArray(endtime), toByteArray(range));
        return getResponse(BuilderFactory.STRING);
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
    public Response<Double> avgAdd(final String key, final long count, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.AVGADD, key, String.valueOf(count), String.valueOf(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> avgAdd(final byte[] key, final long count, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.AVGADD, key, toByteArray(count), toByteArray(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Add the value of a avg key.
     *
     * @param key   the key
     * @param count the count
     * @param value the value
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: avg value; Fail: error.
     */
    public Response<Double> avgAdd(final String key, final long count, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.AVGADD,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(count), toByteArray(value)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> avgAdd(final byte[] key, final long count, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.AVGADD, params.getByteParams(key, toByteArray(count), toByteArray(value)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Set the value of a avg key.
     *
     * @param key   the key
     * @param count the count
     * @param value the value
     * @return Success: avg value; Fail: error.
     */
    public Response<Double> avgSet(final String key, final long count, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.AVGSET, key, String.valueOf(count), String.valueOf(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> avgSet(final byte[] key, final long count, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.AVGSET, key, toByteArray(count), toByteArray(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Set the value of a avg key.
     *
     * @param key   the key
     * @param count the count
     * @param value the value
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: avg value; Fail: error.
     */
    public Response<Double> avgSet(final String key, final long count, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.AVGSET,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(count), toByteArray(value)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> avgSet(final byte[] key, final long count, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.AVGSET, params.getByteParams(key, toByteArray(count), toByteArray(value)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Get the value of a avg key.
     *
     * @param key   the key
     * @return Success: avg value; Fail: error.
     */
    public Response<Double> avgGet(final String key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.AVGGET, key);
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> avgGet(final byte[] key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.AVGGET, key);
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Add the value of a avgArray key.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @param count the count
     * @param value the value
     * @return Success: avg value of timestamp; Fail: error.
     */
    public Response<Double> avgArrayAdd(final String key, final long timestamp, final long count, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.AVGARRAYADD, key, String.valueOf(timestamp), String.valueOf(count), String.valueOf(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> avgArrayAdd(final byte[] key, final long timestamp, final long count, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.AVGARRAYADD, key, toByteArray(timestamp), toByteArray(count), toByteArray(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Add the value of a avgArray key.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @param count the count
     * @param value the value
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: avg value of timestamp; Fail: error.
     */
    public Response<Double> avgArrayAdd(final String key, final long timestamp, final long count, final double value, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.AVGARRAYADD, params.getByteParams(SafeEncoder.encode(key), toByteArray(timestamp), toByteArray(count), toByteArray(value)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> avgArrayAdd(final byte[] key, final long timestamp, final long count, final double value, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.AVGARRAYADD, params.getByteParams(key, toByteArray(timestamp), toByteArray(count), toByteArray(value)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Get the value of a avg key.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @return Success: avg value; Fail: error.
     */
    public Response<Double> avgArrayGet(final String key, final long timestamp) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.AVGARRAYGET, SafeEncoder.encode(key), toByteArray(timestamp));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> avgArrayGet(final byte[] key, final long timestamp) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.AVGARRAYGET, key, toByteArray(timestamp));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Get the values of an array avg key range.
     *
     * @param key   the key
     * @param starttime the starttime
     * @param endtime the endtime
     * @return Success: avg value list; Fail: error.
     */
    public Response<List<Double>> avgArrayGetRange(final String key, final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.AVGARRAYGETRANGE, SafeEncoder.encode(key), toByteArray(starttime), toByteArray(endtime));
        return getResponse(CpcBuilderFactory.CPCARRAY_RANGE_RESULT);
    }

    public Response<List<Double>> avgArrayGetRange(final byte[] key, final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.AVGARRAYGETRANGE, key, toByteArray(starttime), toByteArray(endtime));
        return getResponse(CpcBuilderFactory.CPCARRAY_RANGE_RESULT);
    }


    public Response<Double> avgArrayGetRangeTimeMerge(final String key,  final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.AVGARRAYGETTIMEMERGE, SafeEncoder.encode(key), toByteArray(starttime), toByteArray(endtime));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> avgArrayGetRangeTimeMerge(final byte[] key,  final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.AVGARRAYGETTIMEMERGE, key, toByteArray(starttime), toByteArray(endtime));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Get the merge value of an array avg key range.
     *
     * @param key   the key
     * @return Success: merge of avg value; Fail: error.
     */
    public Response<Double> avgArrayGetRangeMerge(final String key,  final long endtime, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.AVGARRAYGETRANGEMERGE, SafeEncoder.encode(key), toByteArray(endtime), toByteArray(range));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> avgArrayGetRangeMerge(final byte[] key,  final long endtime, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.AVGARRAYGETRANGEMERGE, key, toByteArray(endtime), toByteArray(range));
        return getResponse(BuilderFactory.DOUBLE);
    }

    // stddev operation

    /**
     * Add the value of a stddev key.
     *
     * @param key   the key
     * @param value the value
     * @return Success: stddev value; Fail: error.
     */
    public Response<Double> stddevAdd(final String key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.STDDEVADD, key, String.valueOf(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> stddevAdd(final byte[] key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.STDDEVADD, key, toByteArray(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Add the value of a stddev key.
     *
     * @param key   the key
     * @param value the value
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: stddev value; Fail: error.
     */
    public Response<Double> stddevAdd(final String key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.STDDEVADD,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(value)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> stddevAdd(final byte[] key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.STDDEVADD, params.getByteParams(key, toByteArray(value)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Set the value of a stddev key.
     *
     * @param key   the key
     * @param value the value
     * @return Success: stddev value; Fail: error.
     */
    public Response<Double> stddevSet(final String key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.STDDEVSET, key, String.valueOf(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> stddevSet(final byte[] key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.STDDEVSET, key, toByteArray(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Set the value of a stddev key.
     *
     * @param key   the key
     * @param value the value
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: stddev value; Fail: error.
     */
    public Response<Double> stddevSet(final String key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.STDDEVSET,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(value)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> stddevSet(final byte[] key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.STDDEVSET, params.getByteParams(key, toByteArray(value)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Get the value of a stddev key.
     *
     * @param key   the key
     * @return Success: stddev value; Fail: error.
     */
    public Response<Double> stddevGet(final String key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.STDDEVGET, key);
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> stddevGet(final byte[] key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.STDDEVGET, key);
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Add the value of a stddevArray key.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @param value the value
     * @return Success: stddev value of timestamp; Fail: error.
     */
    public Response<Double> stddevArrayAdd(final String key, final long timestamp, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.STDDEVARRAYADD, key, String.valueOf(timestamp), String.valueOf(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> stddevArrayAdd(final byte[] key, final long timestamp, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.STDDEVARRAYADD, key, toByteArray(timestamp), toByteArray(value));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Add the value of a stddevArray key.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @param value the value
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: stddev value of timestamp; Fail: error.
     */
    public Response<Double> stddevArrayAdd(final String key, final long timestamp, final double value, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.STDDEVARRAYADD, params.getByteParams(SafeEncoder.encode(key), toByteArray(timestamp), toByteArray(value)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> stddevArrayAdd(final byte[] key, final long timestamp, final double value, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.STDDEVARRAYADD, params.getByteParams(key, toByteArray(timestamp), toByteArray(value)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Get the value of a stddev key.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @return Success: stddev value; Fail: error.
     */
    public Response<Double> stddevArrayGet(final String key, final long timestamp) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.STDDEVARRAYGET, SafeEncoder.encode(key), toByteArray(timestamp));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> stddevArrayGet(final byte[] key, final long timestamp) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.STDDEVARRAYGET, key, toByteArray(timestamp));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Get the values of an array stddev key range.
     *
     * @param key   the key
     * @param starttime the starttime
     * @param endtime the endtime
     * @return Success: stddev value list; Fail: error.
     */
    public Response<List<Double>> stddevArrayGetRange(final String key, final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.STDDEVARRAYGETRANGE, SafeEncoder.encode(key), toByteArray(starttime), toByteArray(endtime));
        return getResponse(CpcBuilderFactory.CPCARRAY_RANGE_RESULT);
    }

    public Response<List<Double>> stddevArrayGetRange(final byte[] key, final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.STDDEVARRAYGETRANGE, key, toByteArray(starttime), toByteArray(endtime));
        return getResponse(CpcBuilderFactory.CPCARRAY_RANGE_RESULT);
    }

    public Response<Double> stddevArrayGetRangeTimeMerge(final String key,  final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.STDDEVARRAYGETTIMEMERGE, SafeEncoder.encode(key), toByteArray(starttime), toByteArray(endtime));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> stddevArrayGetRangeTimeMerge(final byte[] key,  final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.STDDEVARRAYGETTIMEMERGE, key, toByteArray(starttime), toByteArray(endtime));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Get the merge value of an array stddev key range.
     *
     * @param key   the key
     * @return Success: merge of stddev value; Fail: error.
     */
    public Response<Double> stddevArrayGetRangeMerge(final String key,  final long endtime, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.STDDEVARRAYGETRANGEMERGE, SafeEncoder.encode(key), toByteArray(endtime), toByteArray(range));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> stddevArrayGetRangeMerge(final byte[] key,  final long endtime, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.STDDEVARRAYGETRANGEMERGE, key, toByteArray(endtime), toByteArray(range));
        return getResponse(BuilderFactory.DOUBLE);
    }
}
