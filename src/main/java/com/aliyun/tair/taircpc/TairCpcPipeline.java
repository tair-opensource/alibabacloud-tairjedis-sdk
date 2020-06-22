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
     * @param offset the offset
     * @param item the item
     * @param size the size
     * @return Success: OK; Fail: error.
     */
    public Response<String> cpcArrayUpdate(final String key, final long offset, final String item, final long size) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYUPDATE, key, String.valueOf(offset), item, String.valueOf(size));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> cpcArrayUpdate(final byte[] key, final long offset, final byte[] item, final long size) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYUPDATE, key, toByteArray(offset), item, toByteArray(size));
        return getResponse(BuilderFactory.STRING);
    }

    /**
     * Update the item of a cpcArray.
     *
     * @param key   the key
     * @param offset the offset
     * @param item the item
     * @param size the size
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: OK; Fail: error.
     */
    public Response<String> cpcArrayUpdate(final String key, final long offset, final String item, final long size, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYUPDATE, params.getByteParams(SafeEncoder.encode(key), toByteArray(offset), SafeEncoder.encode(item), toByteArray(size)));
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> cpcArrayUpdate(final byte[] key, final long offset, final byte[] item, final long size, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYUPDATE, params.getByteParams(key, toByteArray(offset), item, toByteArray(size)));
        return getResponse(BuilderFactory.STRING);
    }

    /**
     * Update the item of a cpcArray.
     *
     * @param key   the key
     * @param offset the offset
     * @param item the item
     * @param size the size
     * @return Success: Double value; Fail: error.
     */
    public Response<Double> cpcArrayUpdate2Est(final String key, final long offset, final String item, final long size) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYUPDATE2EST, key, String.valueOf(offset), item, String.valueOf(size));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> cpcArrayUpdate2Est(final byte[] key, final long offset, final byte[] item, final long size) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYUPDATE2EST, key, toByteArray(offset), item, toByteArray(size));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Update the item of a cpcArray.
     *
     * @param key   the key
     * @param offset the offset
     * @param item the item
     * @param size the size
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: Double value; Fail: error.
     */
    public Response<Double> cpcArrayUpdate2Est(final String key, final long offset, final String item, final long size, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYUPDATE2EST, params.getByteParams(SafeEncoder.encode(key),
                toByteArray(offset), SafeEncoder.encode(item), toByteArray(size)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> cpcArrayUpdate2Est(final byte[] key, final long offset, final byte[] item, final long size, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYUPDATE2EST, params.getByteParams(key, toByteArray(offset),
                item, toByteArray(size)));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Update the item of a cpcArray.
     *
     * @param key   the key
     * @param offset the offset
     * @param item the item
     * @param size the size
     * @return Success: Update2JudResult; Fail: error.
     */
    public Response<Update2JudResult> cpcArrayUpdate2Jud(final String key, final long offset, final String item, final long size) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYUPDATE2JUD, key, String.valueOf(offset), item, String.valueOf(size));
        return getResponse(CpcBuilderFactory.CPCUPDATE2JUD_RESULT);
    }

    public Response<Update2JudResult> cpcArrayUpdate2Jud(final byte[] key, final long offset, final byte[] item, final long size) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYUPDATE2JUD, key, toByteArray(offset), item, toByteArray(size));
        return getResponse(CpcBuilderFactory.CPCUPDATE2JUD_RESULT);
    }

    /**
     * Update the item of a cpcArray.
     *
     * @param key   the key
     * @param offset the offset
     * @param item the item
     * @param size the size
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: Update2JudResult; Fail: error.
     */
    public Response<Update2JudResult> cpcArrayUpdate2Jud(final String key, final long offset, final String item, final long size, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYUPDATE2JUD, params.getByteParams(SafeEncoder.encode(key),
                toByteArray(offset), SafeEncoder.encode(item), toByteArray(size)));
        return getResponse(CpcBuilderFactory.CPCUPDATE2JUD_RESULT);
    }

    public Response<Update2JudResult> cpcArrayUpdate2Jud(final byte[] key, final long offset, final byte[] item, final long size, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYUPDATE2JUD, params.getByteParams(key, toByteArray(offset),
                item, toByteArray(size)));
        return getResponse(CpcBuilderFactory.CPCUPDATE2JUD_RESULT);
    }

    /**
     * MutiUpdate the item of a cpcArray.
     *
     * @param keys    {key offset item size expStr exp} [key offset item size expStr exp] ...
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
     * @param keys    {key offset item size expStr exp} [key offset item size expStr exp] ...
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
//     * @param keys    {key offset item size expStr exp} [key offset item size expStr exp] ...
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
     * @param keys    {key offset item size expStr exp} [key offset item size expStr exp] ...
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
     * @param keys    {key offset item size expStr exp} [key offset item size expStr exp] ...
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
     * @param offset the offset
     * @return Success: double; Empty: 0; Fail: error.
     */
    public Response<Double> cpcArrayEstimate(final String key, final long offset) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYESTIMATE, key, String.valueOf(offset));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> cpcArrayEstimate(final byte[] key, final long offset) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYESTIMATE, key, toByteArray(offset));
        return getResponse(BuilderFactory.DOUBLE);
    }

    /**
     * Estimate the cpcArray for a range.
     *
     * @param key   the key
     * @param offset the offset
     * @param range the range
     * @return Success: String List; Fail: error.
     */
    public Response<List<Double>> cpcArrayEstimateRange(final String key, final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYESTIMATERANGE, key, String.valueOf(offset), String.valueOf(range));
        return getResponse(CpcBuilderFactory.CPCARRAY_ESTIMATE_RANGE_RESULT);
    }

    public Response<List<Double>> cpcArrayEstimateRange(final byte[] key, final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYESTIMATERANGE, key, toByteArray(offset), toByteArray(range));
        return getResponse(CpcBuilderFactory.CPCARRAY_ESTIMATE_RANGE_RESULT);
    }

    /**
     * Estimate & merge the cpcArray for a range.
     *
     * @param key   the key
     * @param offset the offset
     * @param range the range
     * @return Success: double; Empty: 0; Fail: error.
     */
    public Response<Double> cpcArrayEstimateRangeMerge(final String key, final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYESTIMATERANGEMERGE, key, String.valueOf(offset), String.valueOf(range));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> cpcArrayEstimateRangeMerge(final byte[] key, final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        getClient("").sendCommand(ModuleCommand.CPCARRAYESTIMATERANGEMERGE, key, toByteArray(offset), toByteArray(range));
        return getResponse(BuilderFactory.DOUBLE);
    }

    public int getSlot (String key) throws JedisConnectionException {
        return JedisClusterCRC16.getSlot(key);
    }
}
