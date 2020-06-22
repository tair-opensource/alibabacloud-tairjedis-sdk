package com.aliyun.tair.taircpc;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.taircpc.factory.CpcBuilderFactory;
import com.aliyun.tair.taircpc.params.*;
import com.aliyun.tair.taircpc.results.Update2EstWithKeyResult;
import com.aliyun.tair.taircpc.results.Update2JudResult;
import com.aliyun.tair.taircpc.results.Update2JudWithKeyResult;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.util.JedisClusterCRC16;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static redis.clients.jedis.Protocol.toByteArray;

public class TairCpc {

    private Jedis jedis;

    public TairCpc(Jedis jedis) {
        this.jedis = jedis;
    }

    private Jedis getJedis() {
        return jedis;
    }


    /**
     * Estimate the cpc.
     *
     * @param key   the key
     * @return Success: double; Empty: 0; Fail: error.
     */
    public Double cpcEstimate(final String key) throws JedisConnectionException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.CPCESTIMATE, key);
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double cpcEstimate(final byte[] key) throws JedisConnectionException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.CPCESTIMATE, key);
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * MutiUpdate the cpc.
     *
     * @param keys    {key item expStr exp} [key item expStr exp] ...
     * @return Success: OK; Fail: error.
     */
    public String cpcMUpdate(final ArrayList<CpcData> keys) throws JedisConnectionException,IllegalArgumentException,
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
        Object obj = getJedis().sendCommand(ModuleCommand.CPCMUPDATE, keyList.getByteParams(keys));
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * MutiUpdate the cpc.
     *
     * @param keys    {key item expStr exp} [key item expStr exp] ...
     * @return Success: List<Double>; Fail: error.
     */
    public List<Double> cpcMUpdate2Est(final ArrayList<CpcData> keys) throws JedisConnectionException,
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
        Object obj = getJedis().sendCommand(ModuleCommand.CPCMUPDATE2EST, keyList.getByteParams(keys));
        return CpcBuilderFactory.CPCUPDATE2EST_MULTI_RESULT.build(obj);
    }

//    /**
//     * MutiUpdate the cpc.
//     *
//     * @param keys    {key item expStr exp} [key item expStr exp] ...
//     * @return Success: HashMap<String, Double>; Fail: error.
//     */
//    public HashMap<String, Double> cpcMUpdate2EstWithKey(final ArrayList<CpcData> keys) throws JedisConnectionException,
//            IllegalArgumentException, JedisDataException {
//        if (keys == null) {
//            throw new IllegalArgumentException(CommonResult.keyIsNull);
//        }
//        for (CpcData key : keys) {
//            if (key.getKey() == null) {
//                throw new IllegalArgumentException(CommonResult.keyIsNull);
//            }
//        }
//        for (CpcData key : keys) {
//            if (key.getItem() == null) {
//                throw new IllegalArgumentException(CommonResult.valueIsNull);
//            }
//        }
//        CpcMultiUpdateParams keyList = new CpcMultiUpdateParams();
//        Object obj = getJedis().sendCommand(ModuleCommand.CPCMUPDATE2ESTWITHKEY, keyList.getByteParams(keys));
//        return CpcBuilderFactory.CPCUPDATE2ESTWITHKEY_MULTI_RESULT.build(obj);
//    }

    /**
     * MutiUpdate the cpc.
     *
     * @param keys    {key item expStr exp} [key item expStr exp] ...
     * @return Success: List<Update2judResult>; Fail: error.
     */
    public List<Update2JudResult> cpcMUpdate2Jud(final ArrayList<CpcData> keys) throws JedisConnectionException,
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
        Object obj = getJedis().sendCommand(ModuleCommand.CPCMUPDATE2JUD, keyList.getByteParams(keys));
        return CpcBuilderFactory.CPCUPDATE2JUD_MULTI_RESULT.build(obj);
    }

    /**
     * Update the item of a cpc.
     *
     * @param key   the key
     * @param item the item
     * @return Success: OK; Fail: error.
     */
    public String cpcUpdate(final String key, final String item) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.CPCUPDATE, key, item);
        return BuilderFactory.STRING.build(obj);
    }

    public String cpcUpdate(final byte[] key, final byte[] item) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.CPCUPDATE, key, item);
        return BuilderFactory.STRING.build(obj);
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
    public String cpcUpdate(final String key, final String item, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.CPCUPDATE,
                params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(item)));
        return BuilderFactory.STRING.build(obj);
    }

    public String cpcUpdate(final byte[] key, final byte[] item, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.CPCUPDATE, params.getByteParams(key, item));
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Update the item of a cpc.
     *
     * @param key   the key
     * @param item the item
     * @return Success: Update2JudResult; Fail: error.
     */
    public Update2JudResult cpcUpdate2Jud(final String key, final String item) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.CPCUPDATE2JUD, key, item);
        return CpcBuilderFactory.CPCUPDATE2JUD_RESULT.build(obj);
    }

    public Update2JudResult cpcUpdate2Jud(final byte[] key, final byte[] item) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.CPCUPDATE2JUD, key, item);
        return CpcBuilderFactory.CPCUPDATE2JUD_RESULT.build(obj);
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
     * @return Success: Update2JudResult; Fail: error.
     */
    public Update2JudResult cpcUpdate2Jud(final String key, final String item, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.CPCUPDATE2JUD,
                params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(item)));
        return CpcBuilderFactory.CPCUPDATE2JUD_RESULT.build(obj);
    }

    public Update2JudResult cpcUpdate2Jud(final byte[] key, final byte[] item, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.CPCUPDATE2JUD, params.getByteParams(key, item));
        return CpcBuilderFactory.CPCUPDATE2JUD_RESULT.build(obj);
    }

    /**
     * Update the item of a cpc.
     *
     * @param key   the key
     * @param item the item
     * @return Success: Double value; Fail: error.
     */
    public Double cpcUpdate2Est(final String key, final String item) throws JedisConnectionException,IllegalArgumentException,
            JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.CPCUPDATE2EST, key, item);
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double cpcUpdate2Est(final byte[] key, final byte[] item) throws JedisConnectionException,IllegalArgumentException,
            JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.CPCUPDATE2EST, key, item);
        return BuilderFactory.DOUBLE.build(obj);
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
    public Double cpcUpdate2Est(final String key, final String item, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.CPCUPDATE2EST,
                params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(item)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double cpcUpdate2Est(final byte[] key, final byte[] item, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.CPCUPDATE2EST, params.getByteParams(key, item));
        return BuilderFactory.DOUBLE.build(obj);
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
    public String cpcArrayUpdate(final String key, final long offset, final String item, final long size) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.CPCARRAYUPDATE, key, String.valueOf(offset), item, String.valueOf(size));
        return BuilderFactory.STRING.build(obj);
    }

    public String cpcArrayUpdate(final byte[] key, final long offset, final byte[] item, final long size) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.CPCARRAYUPDATE, key, toByteArray(offset), item, toByteArray(size));
        return BuilderFactory.STRING.build(obj);
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
    public String cpcArrayUpdate(final String key, final long offset, final String item, final long size, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.CPCARRAYUPDATE, params.getByteParams(SafeEncoder.encode(key), toByteArray(offset), SafeEncoder.encode(item), toByteArray(size)));
        return BuilderFactory.STRING.build(obj);
    }

    public String cpcArrayUpdate(final byte[] key, final long offset, final byte[] item, final long size, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.CPCARRAYUPDATE, params.getByteParams(key, toByteArray(offset), item, toByteArray(size)));
        return BuilderFactory.STRING.build(obj);
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
    public Double cpcArrayUpdate2Est(final String key, final long offset, final String item, final long size) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.CPCARRAYUPDATE2EST, key, String.valueOf(offset), item, String.valueOf(size));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double cpcArrayUpdate2Est(final byte[] key, final long offset, final byte[] item, final long size) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.CPCARRAYUPDATE2EST, key, toByteArray(offset), item, toByteArray(size));
        return BuilderFactory.DOUBLE.build(obj);
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
    public Double cpcArrayUpdate2Est(final String key, final long offset, final String item, final long size, final CpcUpdateParams params)
            throws JedisConnectionException, IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.CPCARRAYUPDATE2EST, params.getByteParams(SafeEncoder.encode(key),
                toByteArray(offset), SafeEncoder.encode(item), toByteArray(size)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double cpcArrayUpdate2Est(final byte[] key, final long offset, final byte[] item, final long size, final CpcUpdateParams params)
            throws JedisConnectionException, IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.CPCARRAYUPDATE2EST, params.getByteParams(key, toByteArray(offset),
                item, toByteArray(size)));
        return BuilderFactory.DOUBLE.build(obj);
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
    public Update2JudResult cpcArrayUpdate2Jud(final String key, final long offset, final String item, final long size)
            throws JedisConnectionException, IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.CPCARRAYUPDATE2JUD, key, String.valueOf(offset), item, String.valueOf(size));
        return CpcBuilderFactory.CPCUPDATE2JUD_RESULT.build(obj);
    }

    public Update2JudResult cpcArrayUpdate2Jud(final byte[] key, final long offset, final byte[] item, final long size)
            throws JedisConnectionException, IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.CPCARRAYUPDATE2JUD, key, toByteArray(offset), item, toByteArray(size));
        return CpcBuilderFactory.CPCUPDATE2JUD_RESULT.build(obj);
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
    public Update2JudResult cpcArrayUpdate2Jud(final String key, final long offset, final String item, final long size, final CpcUpdateParams params)
            throws JedisConnectionException, IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.CPCARRAYUPDATE2JUD, params.getByteParams(SafeEncoder.encode(key),
                toByteArray(offset), SafeEncoder.encode(item), toByteArray(size)));
        return CpcBuilderFactory.CPCUPDATE2JUD_RESULT.build(obj);
    }

    public Update2JudResult cpcArrayUpdate2Jud(final byte[] key, final long offset, final byte[] item, final long size, final CpcUpdateParams params)
            throws JedisConnectionException, IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.CPCARRAYUPDATE2JUD, params.getByteParams(key, toByteArray(offset),
                item, toByteArray(size)));
        return CpcBuilderFactory.CPCUPDATE2JUD_RESULT.build(obj);
    }

    /**
     * MutiUpdate the item of a cpcArray.
     *
     * @param keys    {key offset item size expStr exp} [key offset item size expStr exp] ...
     * @return Success: OK; Fail: error.
     */
    public String cpcArrayMUpdate(final ArrayList<CpcArrayData> keys) throws JedisConnectionException,IllegalArgumentException,
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
        Object obj = getJedis().sendCommand(ModuleCommand.CPCMARRAYUPDATE, keyList.getByteParams(keys));
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * MutiUpdate the item of a cpcArray.
     *
     * @param keys    {key offset item size expStr exp} [key offset item size expStr exp] ...
     * @return Success: List<Double>; Fail: error.
     */
    public List<Double> cpcArrayMUpdate2Est(final ArrayList<CpcArrayData> keys) throws JedisConnectionException,
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
        Object obj = getJedis().sendCommand(ModuleCommand.CPCMARRAYUPDATE2EST, keyList.getByteParams(keys));
        return CpcBuilderFactory.CPCARRAYUPDATE2EST_MULTI_RESULT.build(obj);
    }

    /**
     * MutiUpdate the item of a cpcArray.
     *
     * @param keys    {key offset item size expStr exp} [key offset item size expStr exp] ...
     * @return Success: HashMap<String, Double>; Fail: error.
     */
    public HashMap<String, Double> cpcArrayMUpdate2EstWithKey(final ArrayList<CpcArrayData> keys) throws JedisConnectionException,
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
        Object obj = getJedis().sendCommand(ModuleCommand.CPCMARRAYUPDATE2ESTWITHKEY, keyList.getByteParams(keys));
        return CpcBuilderFactory.CPCUPDATE2ESTWITHKEY_MULTI_RESULT.build(obj);
    }

    /**
     * MutiUpdate the item of a cpcArray.
     *
     * @param keys    {key offset item size expStr exp} [key offset item size expStr exp] ...
     * @return Success: List<Update2JudResult>; Fail: error.
     */
    public List<Update2JudResult> cpcArrayMUpdate2Jud(final ArrayList<CpcArrayData> keys) throws JedisConnectionException,
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
        Object obj = getJedis().sendCommand(ModuleCommand.CPCMARRAYUPDATE2JUD, keyList.getByteParams(keys));
        return CpcBuilderFactory.CPCARRAYUPDATE2JUD_MULTI_RESULT.build(obj);
    }

    /**
     * MutiUpdate the item of a cpcArray.
     *
     * @param keys    {key offset item size expStr exp} [key offset item size expStr exp] ...
     * @return Success: HashMap<String, Update2JudResult>; Fail: error.
     */
    public HashMap<String, Update2JudResult> cpcArrayMUpdate2JudWithKey(final ArrayList<CpcArrayData> keys) throws JedisConnectionException,
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
        Object obj = getJedis().sendCommand(ModuleCommand.CPCMARRAYUPDATE2JUDWITHKEY, keyList.getByteParams(keys));
        return CpcBuilderFactory.CPCUPDATE2JUDWITHKEY_MULTI_RESULT.build(obj);
    }

    /**
     * Estimate the cpcArray.
     *
     * @param key   the key
     * @param offset the offset
     * @return Success: double; Empty: 0; Fail: error.
     */
    public Double cpcArrayEstimate(final String key, final long offset) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.CPCARRAYESTIMATE, key, String.valueOf(offset));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double cpcArrayEstimate(final byte[] key, final long offset) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.CPCARRAYESTIMATE, key, toByteArray(offset));
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * Estimate the cpcArray for a range.
     *
     * @param key   the key
     * @param offset the offset
     * @param range the range
     * @return Success: String List; Fail: error.
     */
    public List<Double> cpcArrayEstimateRange(final String key, final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.CPCARRAYESTIMATERANGE, key, String.valueOf(offset), String.valueOf(range));
        return CpcBuilderFactory.CPCARRAY_ESTIMATE_RANGE_RESULT.build(obj);
    }

    public List<Double> cpcArrayEstimateRange(final byte[] key, final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.CPCARRAYESTIMATERANGE, key, toByteArray(offset), toByteArray(range));
        return CpcBuilderFactory.CPCARRAY_ESTIMATE_RANGE_RESULT.build(obj);
    }

    /**
     * Estimate & merge the cpcArray for a range.
     *
     * @param key   the key
     * @param offset the offset
     * @param range the range
     * @return Success: double; Empty: 0; Fail: error.
     */
    public Double cpcArrayEstimateRangeMerge(final String key, final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.CPCARRAYESTIMATERANGEMERGE, key, String.valueOf(offset), String.valueOf(range));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double cpcArrayEstimateRangeMerge(final byte[] key, final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.CPCARRAYESTIMATERANGEMERGE, key, toByteArray(offset), toByteArray(range));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public int getSlot (String key) throws JedisConnectionException {
        return JedisClusterCRC16.getSlot(key);
    }



}
