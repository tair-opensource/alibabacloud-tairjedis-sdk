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


    // sum operation

    /**
     * Add the value of a sum key.
     *
     * @param key   the key
     * @param value the value
     * @return Success: sum value; Fail: error.
     */
    public Double sumAdd(final String key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.SUMADD, key, String.valueOf(value));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double sumAdd(final byte[] key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.SUMADD, key, toByteArray(value));
        return BuilderFactory.DOUBLE.build(obj);
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
    public Double sumAdd(final String key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.SUMADD,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(value)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double sumAdd(final byte[] key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.SUMADD, params.getByteParams(key, toByteArray(value)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * Set the value of a sum key.
     *
     * @param key   the key
     * @param value the value
     * @return Success: sum value; Fail: error.
     */
    public Double sumSet(final String key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.SUMSET, key, String.valueOf(value));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double sumSet(final byte[] key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.SUMSET, key, toByteArray(value));
        return BuilderFactory.DOUBLE.build(obj);
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
    public Double sumSet(final String key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.SUMSET,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(value)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double sumSet(final byte[] key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.SUMSET, params.getByteParams(key, toByteArray(value)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * Get the value of a sum key.
     *
     * @param key   the key
     * @return Success: sum value; Fail: error.
     */
    public Double sumGet(final String key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.SUMGET, key);
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double sumGet(final byte[] key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.SUMGET, key);
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * Add the value of a sumArray key.
     *
     * @param key   the key
     * @param offset the offset
     * @param value the value
     * @param size the size
     * @return Success: sum value of offset; Fail: error.
     */
    public Double sumArrayAdd(final String key, final long offset, final double value, final long size) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.SUMARRAYADD, key, String.valueOf(offset), String.valueOf(value), String.valueOf(size));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double sumArrayAdd(final byte[] key, final long offset, final double value, final long size) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.SUMARRAYADD, key, toByteArray(offset), toByteArray(value), toByteArray(size));
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * Add the value of a sumArray key.
     *
     * @param key   the key
     * @param offset the offset
     * @param value the value
     * @param size the size
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: sum value of offset; Fail: error.
     */
    public Double sumArrayAdd(final String key, final long offset, final double value, final long size, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.SUMARRAYADD, params.getByteParams(SafeEncoder.encode(key), toByteArray(offset), toByteArray(value), toByteArray(size)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double sumArrayAdd(final byte[] key, final long offset, final double value, final long size, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.SUMARRAYADD, params.getByteParams(key, toByteArray(offset), toByteArray(value), toByteArray(size)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * Get the value of a sum key.
     *
     * @param key   the key
     * @param offset the offset
     * @return Success: sum value; Fail: error.
     */
    public Double sumArrayGet(final String key, final long offset) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.SUMARRAYGET, SafeEncoder.encode(key), toByteArray(offset));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double sumArrayGet(final byte[] key, final long offset) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.SUMARRAYGET, key, toByteArray(offset));
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * Get the values of an array sum key range.
     *
     * @param key   the key
     * @param offset the offset
     * @param range the range
     * @return Success: sum value list; Fail: error.
     */
    public List<Double> sumArrayGetRange(final String key, final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.SUMARRAYGETRANGE, SafeEncoder.encode(key), toByteArray(offset), toByteArray(range));
        return CpcBuilderFactory.CPCARRAY_RANGE_RESULT.build(obj);
    }

    public List<Double> sumArrayGetRange(final byte[] key, final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.SUMARRAYGETRANGE, key, toByteArray(offset), toByteArray(range));
        return CpcBuilderFactory.CPCARRAY_RANGE_RESULT.build(obj);
    }

    /**
     * Get the merge value of an array sum key range.
     *
     * @param key   the key
     * @return Success: merge of sum value; Fail: error.
     */
    public Double sumArrayGetRangeMerge(final String key,  final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.SUMARRAYGETRANGEMERGE, SafeEncoder.encode(key), toByteArray(offset), toByteArray(range));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double sumArrayGetRangeMerge(final byte[] key,  final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.SUMARRAYGETRANGEMERGE, key, toByteArray(offset), toByteArray(range));
        return BuilderFactory.DOUBLE.build(obj);
    }


    // max operation

    /**
     * Add the value of a max key.
     *
     * @param key   the key
     * @param value the value
     * @return Success: max value; Fail: error.
     */
    public Double maxAdd(final String key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MAXADD, key, String.valueOf(value));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double maxAdd(final byte[] key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MAXADD, key, toByteArray(value));
        return BuilderFactory.DOUBLE.build(obj);
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
    public Double maxAdd(final String key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MAXADD,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(value)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double maxAdd(final byte[] key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MAXADD, params.getByteParams(key, toByteArray(value)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * Set the value of a max key.
     *
     * @param key   the key
     * @param value the value
     * @return Success: max value; Fail: error.
     */
    public Double maxSet(final String key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MAXSET, key, String.valueOf(value));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double maxSet(final byte[] key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MAXSET, key, toByteArray(value));
        return BuilderFactory.DOUBLE.build(obj);
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
    public Double maxSet(final String key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MAXSET,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(value)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double maxSet(final byte[] key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MAXSET, params.getByteParams(key, toByteArray(value)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * Get the value of a max key.
     *
     * @param key   the key
     * @return Success: max value; Fail: error.
     */
    public Double maxGet(final String key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MAXGET, key);
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double maxGet(final byte[] key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MAXGET, key);
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * Add the value of a maxArray key.
     *
     * @param key   the key
     * @param offset the offset
     * @param value the value
     * @param size the size
     * @return Success: max value of offset; Fail: error.
     */
    public Double maxArrayAdd(final String key, final long offset, final double value, final long size) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MAXARRAYADD, key, String.valueOf(offset), String.valueOf(value), String.valueOf(size));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double maxArrayAdd(final byte[] key, final long offset, final double value, final long size) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MAXARRAYADD, key, toByteArray(offset), toByteArray(value), toByteArray(size));
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * Add the value of a maxArray key.
     *
     * @param key   the key
     * @param offset the offset
     * @param value the value
     * @param size the size
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: max value of offset; Fail: error.
     */
    public Double maxArrayAdd(final String key, final long offset, final double value, final long size, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MAXARRAYADD, params.getByteParams(SafeEncoder.encode(key), toByteArray(offset), toByteArray(value), toByteArray(size)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double maxArrayAdd(final byte[] key, final long offset, final double value, final long size, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MAXARRAYADD, params.getByteParams(key, toByteArray(offset), toByteArray(value), toByteArray(size)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * Get the value of a max key.
     *
     * @param key   the key
     * @param offset the offset
     * @return Success: max value; Fail: error.
     */
    public Double maxArrayGet(final String key, final long offset) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MAXARRAYGET, SafeEncoder.encode(key), toByteArray(offset));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double maxArrayGet(final byte[] key, final long offset) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MAXARRAYGET, key, toByteArray(offset));
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * Get the values of an array max key range.
     *
     * @param key   the key
     * @param offset the offset
     * @param range the range
     * @return Success: max value list; Fail: error.
     */
    public List<Double> maxArrayGetRange(final String key, final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MAXARRAYGETRANGE, SafeEncoder.encode(key), toByteArray(offset), toByteArray(range));
        return CpcBuilderFactory.CPCARRAY_RANGE_RESULT.build(obj);
    }

    public List<Double> maxArrayGetRange(final byte[] key, final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MAXARRAYGETRANGE, key, toByteArray(offset), toByteArray(range));
        return CpcBuilderFactory.CPCARRAY_RANGE_RESULT.build(obj);
    }

    /**
     * Get the merge value of an array max key range.
     *
     * @param key   the key
     * @return Success: merge of max value; Fail: error.
     */
    public Double maxArrayGetRangeMerge(final String key,  final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MAXARRAYGETRANGEMERGE, SafeEncoder.encode(key), toByteArray(offset), toByteArray(range));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double maxArrayGetRangeMerge(final byte[] key,  final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MAXARRAYGETRANGEMERGE, key, toByteArray(offset), toByteArray(range));
        return BuilderFactory.DOUBLE.build(obj);
    }

    // min operation

    /**
     * Add the value of a min key.
     *
     * @param key   the key
     * @param value the value
     * @return Success: min value; Fail: error.
     */
    public Double minAdd(final String key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MINADD, key, String.valueOf(value));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double minAdd(final byte[] key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MINADD, key, toByteArray(value));
        return BuilderFactory.DOUBLE.build(obj);
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
    public Double minAdd(final String key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MINADD,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(value)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double minAdd(final byte[] key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MINADD, params.getByteParams(key, toByteArray(value)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * Set the value of a min key.
     *
     * @param key   the key
     * @param value the value
     * @return Success: min value; Fail: error.
     */
    public Double minSet(final String key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MINSET, key, String.valueOf(value));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double minSet(final byte[] key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MINSET, key, toByteArray(value));
        return BuilderFactory.DOUBLE.build(obj);
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
    public Double minSet(final String key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MINSET,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(value)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double minSet(final byte[] key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MINSET, params.getByteParams(key, toByteArray(value)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * Get the value of a min key.
     *
     * @param key   the key
     * @return Success: min value; Fail: error.
     */
    public Double minGet(final String key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MINGET, key);
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double minGet(final byte[] key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MINGET, key);
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * Add the value of a minArray key.
     *
     * @param key   the key
     * @param offset the offset
     * @param value the value
     * @param size the size
     * @return Success: min value of offset; Fail: error.
     */
    public Double minArrayAdd(final String key, final long offset, final double value, final long size) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MINARRAYADD, key, String.valueOf(offset), String.valueOf(value), String.valueOf(size));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double minArrayAdd(final byte[] key, final long offset, final double value, final long size) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MINARRAYADD, key, toByteArray(offset), toByteArray(value), toByteArray(size));
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * Add the value of a minArray key.
     *
     * @param key   the key
     * @param offset the offset
     * @param value the value
     * @param size the size
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: min value of offset; Fail: error.
     */
    public Double minArrayAdd(final String key, final long offset, final double value, final long size, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MINARRAYADD, params.getByteParams(SafeEncoder.encode(key), toByteArray(offset), toByteArray(value), toByteArray(size)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double minArrayAdd(final byte[] key, final long offset, final double value, final long size, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MINARRAYADD, params.getByteParams(key, toByteArray(offset), toByteArray(value), toByteArray(size)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * Get the value of a min key.
     *
     * @param key   the key
     * @param offset the offset
     * @return Success: min value; Fail: error.
     */
    public Double minArrayGet(final String key, final long offset) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MINARRAYGET, SafeEncoder.encode(key), toByteArray(offset));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double minArrayGet(final byte[] key, final long offset) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MINARRAYGET, key, toByteArray(offset));
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * Get the values of an array min key range.
     *
     * @param key   the key
     * @param offset the offset
     * @param range the range
     * @return Success: min value list; Fail: error.
     */
    public List<Double> minArrayGetRange(final String key, final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MINARRAYGETRANGE, SafeEncoder.encode(key), toByteArray(offset), toByteArray(range));
        return CpcBuilderFactory.CPCARRAY_RANGE_RESULT.build(obj);
    }

    public List<Double> minArrayGetRange(final byte[] key, final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MINARRAYGETRANGE, key, toByteArray(offset), toByteArray(range));
        return CpcBuilderFactory.CPCARRAY_RANGE_RESULT.build(obj);
    }

    /**
     * Get the merge value of an array min key range.
     *
     * @param key   the key
     * @return Success: merge of min value; Fail: error.
     */
    public Double minArrayGetRangeMerge(final String key,  final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MINARRAYGETRANGEMERGE, SafeEncoder.encode(key), toByteArray(offset), toByteArray(range));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double minArrayGetRangeMerge(final byte[] key,  final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.MINARRAYGETRANGEMERGE, key, toByteArray(offset), toByteArray(range));
        return BuilderFactory.DOUBLE.build(obj);
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
    public String firstAdd(final String key, final String content, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.FIRSTADD, key, content, String.valueOf(value));
        return BuilderFactory.STRING.build(obj);
    }

    public String firstAdd(final byte[] key, final byte[] content, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.FIRSTADD, key, content, toByteArray(value));
        return BuilderFactory.STRING.build(obj);
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
    public String firstAdd(final String key, final String content, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.FIRSTADD,
                params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(content), toByteArray(value)));
        return BuilderFactory.STRING.build(obj);
    }

    public String firstAdd(final byte[] key, final byte[] content, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.FIRSTADD, params.getByteParams(key, content, toByteArray(value)));
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Set the value of a first key.
     *
     * @param key   the key
     * @param content the content
     * @param value the value
     * @return Success: first value; Fail: error.
     */
    public String firstSet(final String key, final String content, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.FIRSTSET, key, content, String.valueOf(value));
        return BuilderFactory.STRING.build(obj);
    }

    public String firstSet(final byte[] key, final byte[] content, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.FIRSTSET, key, content, toByteArray(value));
        return BuilderFactory.STRING.build(obj);
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
    public String firstSet(final String key, final String content, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.FIRSTSET,
                params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(content), toByteArray(value)));
        return BuilderFactory.STRING.build(obj);
    }

    public String firstSet(final byte[] key, final byte[] content, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.FIRSTSET, params.getByteParams(key, content, toByteArray(value)));
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Get the value of a first key.
     *
     * @param key   the key
     * @return Success: first value; Fail: error.
     */
    public String firstGet(final String key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.FIRSTGET, key);
        return BuilderFactory.STRING.build(obj);
    }

    public String firstGet(final byte[] key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.FIRSTGET, key);
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Add the value of a firstArray key.
     *
     * @param key   the key
     * @param offset the offset
     * @param content the content
     * @param value the value
     * @param size the size
     * @return Success: first value of offset; Fail: error.
     */
    public String firstArrayAdd(final String key, final long offset, final String content, final double value, final long size) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.FIRSTARRAYADD, key, String.valueOf(offset), content, String.valueOf(value), String.valueOf(size));
        return BuilderFactory.STRING.build(obj);
    }

    public String firstArrayAdd(final byte[] key, final long offset, final byte[] content, final double value, final long size) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.FIRSTARRAYADD, key, toByteArray(offset), content, toByteArray(value), toByteArray(size));
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Add the value of a firstArray key.
     *
     * @param key   the key
     * @param offset the offset
     * @param content the content
     * @param value the value
     * @param size the size
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: first value of offset; Fail: error.
     */
    public String firstArrayAdd(final String key, final long offset, final String content, final double value, final long size, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.FIRSTARRAYADD, params.getByteParams(SafeEncoder.encode(key), toByteArray(offset), SafeEncoder.encode(content), toByteArray(value), toByteArray(size)));
        return BuilderFactory.STRING.build(obj);
    }

    public String firstArrayAdd(final byte[] key, final long offset, final byte[] content, final double value, final long size, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.FIRSTARRAYADD, params.getByteParams(key, toByteArray(offset), content, toByteArray(value), toByteArray(size)));
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Get the value of a first key.
     *
     * @param key   the key
     * @param offset the offset
     * @return Success: first value; Fail: error.
     */
    public String firstArrayGet(final String key, final long offset) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.FIRSTARRAYGET, SafeEncoder.encode(key), toByteArray(offset));
        return BuilderFactory.STRING.build(obj);
    }

    public String firstArrayGet(final byte[] key, final long offset) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.FIRSTARRAYGET, key, toByteArray(offset));
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Get the values of an array first key range.
     *
     * @param key   the key
     * @param offset the offset
     * @param range the range
     * @return Success: first value list; Fail: error.
     */
    public List<String> firstArrayGetRange(final String key, final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.FIRSTARRAYGETRANGE, SafeEncoder.encode(key), toByteArray(offset), toByteArray(range));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<String> firstArrayGetRange(final byte[] key, final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.FIRSTARRAYGETRANGE, key, toByteArray(offset), toByteArray(range));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    /**
     * Get the merge value of an array first key range.
     *
     * @param key   the key
     * @return Success: merge of first value; Fail: error.
     */
    public String firstArrayGetRangeMerge(final String key,  final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.FIRSTARRAYGETRANGEMERGE, SafeEncoder.encode(key), toByteArray(offset), toByteArray(range));
        return BuilderFactory.STRING.build(obj);
    }

    public String firstArrayGetRangeMerge(final byte[] key,  final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = getJedis().sendCommand(ModuleCommand.FIRSTARRAYGETRANGEMERGE, key, toByteArray(offset), toByteArray(range));
        return BuilderFactory.STRING.build(obj);
    }

}
