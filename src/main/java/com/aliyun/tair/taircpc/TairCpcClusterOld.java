package com.aliyun.tair.taircpc;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.taircpc.factory.CpcBuilderFactory;
import com.aliyun.tair.taircpc.params.*;
import com.aliyun.tair.taircpc.results.Update2JudResult;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.util.JedisClusterCRC16;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static redis.clients.jedis.Protocol.toByteArray;

public class TairCpcClusterOld {
    private JedisCluster jc;

    public TairCpcClusterOld(JedisCluster jc) {
        this.jc = jc;
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
        Object obj = jc.sendCommand(key, ModuleCommand.CPCESTIMATE, key);
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double cpcEstimate(final byte[] key) throws JedisConnectionException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.CPCESTIMATE, key);
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
        Object obj = jc.sendCommand(SafeEncoder.encode(keys.get(0).getKey()), ModuleCommand.CPCMUPDATE, keyList.getByteParams(keys));
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
        Object obj = jc.sendCommand(SafeEncoder.encode(keys.get(0).getKey()), ModuleCommand.CPCMUPDATE2EST, keyList.getByteParams(keys));
        return CpcBuilderFactory.CPCUPDATE2EST_MULTI_RESULT.build(obj);
    }

    /**
     * MutiUpdate the cpc.
     *
     * @param keys    {key item expStr exp} [key item expStr exp] ...
     * @return Success: HashMap<String, Double>; Fail: error.
     */
    public HashMap<String, Double> cpcMUpdate2EstWithKey(final ArrayList<CpcData> keys) throws JedisConnectionException,
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
        Object obj = jc.sendCommand(SafeEncoder.encode(keys.get(0).getKey()), ModuleCommand.CPCMUPDATE2ESTWITHKEY, keyList.getByteParams(keys));
        return CpcBuilderFactory.CPCUPDATE2ESTWITHKEY_MULTI_RESULT.build(obj);
    }

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
        Object obj = jc.sendCommand(SafeEncoder.encode(keys.get(0).getKey()), ModuleCommand.CPCMUPDATE2JUD, keyList.getByteParams(keys));
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
        Object obj = jc.sendCommand(key, ModuleCommand.CPCUPDATE, key, item);
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
        Object obj = jc.sendCommand(key, ModuleCommand.CPCUPDATE, key, item);
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
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.CPCUPDATE,
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
        Object obj = jc.sendCommand(key, ModuleCommand.CPCUPDATE, params.getByteParams(key, item));
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Update the item of a cpc.
     *
     * @param key   the key
     * @param item the item
     * @return Success: String List; Fail: error.
     */
    public Update2JudResult cpcUpdate2Jud(final String key, final String item) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.CPCUPDATE2JUD, key, item);
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
        Object obj = jc.sendCommand(key, ModuleCommand.CPCUPDATE2JUD, key, item);
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
     * @return Success: String List; Fail: error.
     */
    public Update2JudResult cpcUpdate2Jud(final String key, final String item, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.CPCUPDATE2JUD,
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
        Object obj = jc.sendCommand(key, ModuleCommand.CPCUPDATE2JUD, params.getByteParams(key, item));
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
        Object obj = jc.sendCommand(key, ModuleCommand.CPCUPDATE2EST, key, item);
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
        Object obj = jc.sendCommand(key, ModuleCommand.CPCUPDATE2EST, key, item);
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
    public Double cpcUpdate2Est(final String key, final String item, final CpcUpdateParams params) throws JedisConnectionException,IllegalArgumentException,
            JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.CPCUPDATE2EST,
                params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(item)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double cpcUpdate2Est(final byte[] key, final byte[] item, final CpcUpdateParams params) throws JedisConnectionException,IllegalArgumentException,
            JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.CPCUPDATE2EST, params.getByteParams(key, item));
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
        Object obj = jc.sendCommand(key, ModuleCommand.CPCARRAYUPDATE, key, String.valueOf(offset), item, String.valueOf(size));
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
        Object obj = jc.sendCommand(key, ModuleCommand.CPCARRAYUPDATE, key, toByteArray(offset), item, toByteArray(size));
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
    public String cpcArrayUpdate(final String key, final long offset, final String item, final long size, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.CPCARRAYUPDATE, params.getByteParams(SafeEncoder.encode(key), toByteArray(offset), SafeEncoder.encode(item), toByteArray(size)));
        return BuilderFactory.STRING.build(obj);
    }

    public String cpcArrayUpdate(final byte[] key, final long offset, final byte[] item, final long size, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.CPCARRAYUPDATE, params.getByteParams(key, toByteArray(offset), item, toByteArray(size)));
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
        Object obj = jc.sendCommand(key, ModuleCommand.CPCARRAYUPDATE2EST, key, String.valueOf(offset), item, String.valueOf(size));
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
        Object obj = jc.sendCommand(key, ModuleCommand.CPCARRAYUPDATE2EST, key, toByteArray(offset), item, toByteArray(size));
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
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.CPCARRAYUPDATE2EST, params.getByteParams(SafeEncoder.encode(key),
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
        Object obj = jc.sendCommand(key, ModuleCommand.CPCARRAYUPDATE2EST, params.getByteParams(key, toByteArray(offset),
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
        Object obj = jc.sendCommand(key, ModuleCommand.CPCARRAYUPDATE2JUD, key, String.valueOf(offset), item, String.valueOf(size));
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
        Object obj = jc.sendCommand(key, ModuleCommand.CPCARRAYUPDATE2JUD, key, toByteArray(offset), item, toByteArray(size));
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
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.CPCARRAYUPDATE2JUD, params.getByteParams(SafeEncoder.encode(key),
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
        Object obj = jc.sendCommand(key, ModuleCommand.CPCARRAYUPDATE2JUD, params.getByteParams(key, toByteArray(offset),
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
        Object obj = jc.sendCommand(SafeEncoder.encode(keys.get(0).getKey()), ModuleCommand.CPCMARRAYUPDATE, keyList.getByteParams(keys));
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
        Object obj = jc.sendCommand(SafeEncoder.encode(keys.get(0).getKey()), ModuleCommand.CPCMARRAYUPDATE2EST, keyList.getByteParams(keys));
        return CpcBuilderFactory.CPCARRAYUPDATE2EST_MULTI_RESULT.build(obj);
    }

//    /**
//     * MutiUpdate the item of a cpcArray.
//     *
//     * @param keys    {key offset item size expStr exp} [key offset item size expStr exp] ...
//     * @return Success: HashMap<String, Double>; Fail: error.
//     */
//    public HashMap<String, Double> cpcArrayMUpdate2EstWithKey(final ArrayList<CpcArrayData> keys) throws JedisConnectionException,
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
//        Object obj = jc.sendCommand(SafeEncoder.encode(keys.get(0).getKey()), ModuleCommand.CPCMARRAYUPDATE2ESTWITHKEY, keyList.getByteParams(keys));
//        return CpcBuilderFactory.CPCUPDATE2ESTWITHKEY_MULTI_RESULT.build(obj);
//    }

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
        Object obj = jc.sendCommand(SafeEncoder.encode(keys.get(0).getKey()), ModuleCommand.CPCMARRAYUPDATE2JUD, keyList.getByteParams(keys));
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
        Object obj = jc.sendCommand(SafeEncoder.encode(keys.get(0).getKey()), ModuleCommand.CPCMARRAYUPDATE2JUDWITHKEY, keyList.getByteParams(keys));
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
        Object obj = jc.sendCommand(key, ModuleCommand.CPCARRAYESTIMATE, key, String.valueOf(offset));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double cpcArrayEstimate(final byte[] key, final long offset) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.CPCARRAYESTIMATE, key, toByteArray(offset));
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
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.CPCARRAYESTIMATERANGE, key, String.valueOf(offset), String.valueOf(range));
        return CpcBuilderFactory.CPCARRAY_ESTIMATE_RANGE_RESULT.build(obj);
    }

    public List<Double> cpcArrayEstimateRange(final byte[] key, final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.CPCARRAYESTIMATERANGE, key, toByteArray(offset), toByteArray(range));
        return CpcBuilderFactory.CPCARRAY_ESTIMATE_RANGE_RESULT.build(obj);
    }

    /**
     * Estimate & sum the cpcArray for a range.
     *
     * @param key   the key
     * @param offset the offset
     * @param range the range
     * @return Success: double; Empty: 0; Fail: error.
     */
    public Double cpcArrayEstimateRangeSum(final String key, final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.CPCARRAYESTIMATERANGESUM, key, String.valueOf(offset), String.valueOf(range));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double cpcArrayEstimateRangeSum(final byte[] key, final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.CPCARRAYESTIMATERANGESUM, key, toByteArray(offset), toByteArray(range));
        return BuilderFactory.DOUBLE.build(obj);
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
        Object obj = jc.sendCommand(key, ModuleCommand.CPCARRAYESTIMATERANGEMERGE, key, String.valueOf(offset), String.valueOf(range));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double cpcArrayEstimateRangeMerge(final byte[] key, final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.CPCARRAYESTIMATERANGEMERGE, key, toByteArray(offset), toByteArray(range));
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
        Object obj = jc.sendCommand(key, ModuleCommand.SUMADD, key, String.valueOf(value));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double sumAdd(final byte[] key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.SUMADD, key, toByteArray(value));
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
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.SUMADD,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(value)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double sumAdd(final byte[] key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.SUMADD, params.getByteParams(key, toByteArray(value)));
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
        Object obj = jc.sendCommand(key, ModuleCommand.SUMSET, key, String.valueOf(value));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double sumSet(final byte[] key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.SUMSET, key, toByteArray(value));
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
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.SUMSET,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(value)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double sumSet(final byte[] key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.SUMSET, params.getByteParams(key, toByteArray(value)));
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
        Object obj = jc.sendCommand(key, ModuleCommand.SUMGET, key);
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double sumGet(final byte[] key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.SUMGET, key);
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
        Object obj = jc.sendCommand(key, ModuleCommand.SUMARRAYADD, key, String.valueOf(offset), String.valueOf(value), String.valueOf(size));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double sumArrayAdd(final byte[] key, final long offset, final double value, final long size) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.SUMARRAYADD, key, toByteArray(offset), toByteArray(value), toByteArray(size));
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
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.SUMARRAYADD, params.getByteParams(SafeEncoder.encode(key), toByteArray(offset), toByteArray(value), toByteArray(size)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double sumArrayAdd(final byte[] key, final long offset, final double value, final long size, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.SUMARRAYADD, params.getByteParams(key, toByteArray(offset), toByteArray(value), toByteArray(size)));
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
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.SUMARRAYGET, SafeEncoder.encode(key), toByteArray(offset));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double sumArrayGet(final byte[] key, final long offset) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.SUMARRAYGET, key, toByteArray(offset));
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
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.SUMARRAYGETRANGE, SafeEncoder.encode(key), toByteArray(offset), toByteArray(range));
        return CpcBuilderFactory.CPCARRAY_RANGE_RESULT.build(obj);
    }

    public List<Double> sumArrayGetRange(final byte[] key, final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.SUMARRAYGETRANGE, key, toByteArray(offset), toByteArray(range));
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
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.SUMARRAYGETRANGEMERGE, SafeEncoder.encode(key), toByteArray(offset), toByteArray(range));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double sumArrayGetRangeMerge(final byte[] key,  final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.SUMARRAYGETRANGEMERGE, key, toByteArray(offset), toByteArray(range));
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
        Object obj = jc.sendCommand(key, ModuleCommand.MAXADD, key, String.valueOf(value));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double maxAdd(final byte[] key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.MAXADD, key, toByteArray(value));
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
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.MAXADD,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(value)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double maxAdd(final byte[] key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.MAXADD, params.getByteParams(key, toByteArray(value)));
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
        Object obj = jc.sendCommand(key, ModuleCommand.MAXSET, key, String.valueOf(value));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double maxSet(final byte[] key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.MAXSET, key, toByteArray(value));
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
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.MAXSET,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(value)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double maxSet(final byte[] key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.MAXSET, params.getByteParams(key, toByteArray(value)));
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
        Object obj = jc.sendCommand(key, ModuleCommand.MAXGET, key);
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double maxGet(final byte[] key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.MAXGET, key);
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
        Object obj = jc.sendCommand(key, ModuleCommand.MAXARRAYADD, key, String.valueOf(offset), String.valueOf(value), String.valueOf(size));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double maxArrayAdd(final byte[] key, final long offset, final double value, final long size) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.MAXARRAYADD, key, toByteArray(offset), toByteArray(value), toByteArray(size));
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
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.MAXARRAYADD, params.getByteParams(SafeEncoder.encode(key), toByteArray(offset), toByteArray(value), toByteArray(size)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double maxArrayAdd(final byte[] key, final long offset, final double value, final long size, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.MAXARRAYADD, params.getByteParams(key, toByteArray(offset), toByteArray(value), toByteArray(size)));
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
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.MAXARRAYGET, SafeEncoder.encode(key), toByteArray(offset));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double maxArrayGet(final byte[] key, final long offset) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.MAXARRAYGET, key, toByteArray(offset));
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
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.MAXARRAYGETRANGE, SafeEncoder.encode(key), toByteArray(offset), toByteArray(range));
        return CpcBuilderFactory.CPCARRAY_RANGE_RESULT.build(obj);
    }

    public List<Double> maxArrayGetRange(final byte[] key, final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.MAXARRAYGETRANGE, key, toByteArray(offset), toByteArray(range));
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
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.MAXARRAYGETRANGEMERGE, SafeEncoder.encode(key), toByteArray(offset), toByteArray(range));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double maxArrayGetRangeMerge(final byte[] key,  final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.MAXARRAYGETRANGEMERGE, key, toByteArray(offset), toByteArray(range));
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
        Object obj = jc.sendCommand(key, ModuleCommand.MINADD, key, String.valueOf(value));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double minAdd(final byte[] key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.MINADD, key, toByteArray(value));
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
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.MINADD,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(value)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double minAdd(final byte[] key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.MINADD, params.getByteParams(key, toByteArray(value)));
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
        Object obj = jc.sendCommand(key, ModuleCommand.MINSET, key, String.valueOf(value));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double minSet(final byte[] key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.MINSET, key, toByteArray(value));
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
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.MINSET,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(value)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double minSet(final byte[] key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.MINSET, params.getByteParams(key, toByteArray(value)));
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
        Object obj = jc.sendCommand(key, ModuleCommand.MINGET, key);
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double minGet(final byte[] key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.MINGET, key);
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
        Object obj = jc.sendCommand(key, ModuleCommand.MINARRAYADD, key, String.valueOf(offset), String.valueOf(value), String.valueOf(size));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double minArrayAdd(final byte[] key, final long offset, final double value, final long size) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.MINARRAYADD, key, toByteArray(offset), toByteArray(value), toByteArray(size));
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
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.MINARRAYADD, params.getByteParams(SafeEncoder.encode(key), toByteArray(offset), toByteArray(value), toByteArray(size)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double minArrayAdd(final byte[] key, final long offset, final double value, final long size, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.MINARRAYADD, params.getByteParams(key, toByteArray(offset), toByteArray(value), toByteArray(size)));
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
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.MINARRAYGET, SafeEncoder.encode(key), toByteArray(offset));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double minArrayGet(final byte[] key, final long offset) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.MINARRAYGET, key, toByteArray(offset));
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
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.MINARRAYGETRANGE, SafeEncoder.encode(key), toByteArray(offset), toByteArray(range));
        return CpcBuilderFactory.CPCARRAY_RANGE_RESULT.build(obj);
    }

    public List<Double> minArrayGetRange(final byte[] key, final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.MINARRAYGETRANGE, key, toByteArray(offset), toByteArray(range));
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
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.MINARRAYGETRANGEMERGE, SafeEncoder.encode(key), toByteArray(offset), toByteArray(range));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double minArrayGetRangeMerge(final byte[] key,  final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.MINARRAYGETRANGEMERGE, key, toByteArray(offset), toByteArray(range));
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
        Object obj = jc.sendCommand(key, ModuleCommand.FIRSTADD, key, content, String.valueOf(value));
        return BuilderFactory.STRING.build(obj);
    }

    public String firstAdd(final byte[] key, final byte[] content, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.FIRSTADD, key, content, toByteArray(value));
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
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.FIRSTADD,
                params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(content), toByteArray(value)));
        return BuilderFactory.STRING.build(obj);
    }

    public String firstAdd(final byte[] key, final byte[] content, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.FIRSTADD, params.getByteParams(key, content, toByteArray(value)));
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
        Object obj = jc.sendCommand(key, ModuleCommand.FIRSTSET, key, content, String.valueOf(value));
        return BuilderFactory.STRING.build(obj);
    }

    public String firstSet(final byte[] key, final byte[] content, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.FIRSTSET, key, content, toByteArray(value));
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
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.FIRSTSET,
                params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(content), toByteArray(value)));
        return BuilderFactory.STRING.build(obj);
    }

    public String firstSet(final byte[] key, final byte[] content, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.FIRSTSET, params.getByteParams(key, content, toByteArray(value)));
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
        Object obj = jc.sendCommand(key, ModuleCommand.FIRSTGET, key);
        return BuilderFactory.STRING.build(obj);
    }

    public String firstGet(final byte[] key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.FIRSTGET, key);
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
        Object obj = jc.sendCommand(key, ModuleCommand.FIRSTARRAYADD, key, String.valueOf(offset), content, String.valueOf(value), String.valueOf(size));
        return BuilderFactory.STRING.build(obj);
    }

    public String firstArrayAdd(final byte[] key, final long offset, final byte[] content, final double value, final long size) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.FIRSTARRAYADD, key, toByteArray(offset), content, toByteArray(value), toByteArray(size));
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
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.FIRSTARRAYADD, params.getByteParams(SafeEncoder.encode(key), toByteArray(offset), SafeEncoder.encode(content), toByteArray(value), toByteArray(size)));
        return BuilderFactory.STRING.build(obj);
    }

    public String firstArrayAdd(final byte[] key, final long offset, final byte[] content, final double value, final long size, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.FIRSTARRAYADD, params.getByteParams(key, toByteArray(offset), content, toByteArray(value), toByteArray(size)));
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
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.FIRSTARRAYGET, SafeEncoder.encode(key), toByteArray(offset));
        return BuilderFactory.STRING.build(obj);
    }

    public String firstArrayGet(final byte[] key, final long offset) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.FIRSTARRAYGET, key, toByteArray(offset));
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
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.FIRSTARRAYGETRANGE, SafeEncoder.encode(key), toByteArray(offset), toByteArray(range));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<String> firstArrayGetRange(final byte[] key, final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.FIRSTARRAYGETRANGE, key, toByteArray(offset), toByteArray(range));
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
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.FIRSTARRAYGETRANGEMERGE, SafeEncoder.encode(key), toByteArray(offset), toByteArray(range));
        return BuilderFactory.STRING.build(obj);
    }

    public String firstArrayGetRangeMerge(final byte[] key,  final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.FIRSTARRAYGETRANGEMERGE, key, toByteArray(offset), toByteArray(range));
        return BuilderFactory.STRING.build(obj);
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
    public String lastAdd(final String key, final String content, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.LASTADD, key, content, String.valueOf(value));
        return BuilderFactory.STRING.build(obj);
    }

    public String lastAdd(final byte[] key, final byte[] content, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.LASTADD, key, content, toByteArray(value));
        return BuilderFactory.STRING.build(obj);
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
    public String lastAdd(final String key, final String content, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.LASTADD,
                params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(content), toByteArray(value)));
        return BuilderFactory.STRING.build(obj);
    }

    public String lastAdd(final byte[] key, final byte[] content, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.LASTADD, params.getByteParams(key, content, toByteArray(value)));
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Set the value of a last key.
     *
     * @param key   the key
     * @param content the content
     * @param value the value
     * @return Success: last value; Fail: error.
     */
    public String lastSet(final String key, final String content, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.LASTSET, key, content, String.valueOf(value));
        return BuilderFactory.STRING.build(obj);
    }

    public String lastSet(final byte[] key, final byte[] content, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.LASTSET, key, content, toByteArray(value));
        return BuilderFactory.STRING.build(obj);
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
    public String lastSet(final String key, final String content, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.LASTSET,
                params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(content), toByteArray(value)));
        return BuilderFactory.STRING.build(obj);
    }

    public String lastSet(final byte[] key, final byte[] content, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.LASTSET, params.getByteParams(key, content, toByteArray(value)));
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Get the value of a last key.
     *
     * @param key   the key
     * @return Success: last value; Fail: error.
     */
    public String lastGet(final String key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.LASTGET, key);
        return BuilderFactory.STRING.build(obj);
    }

    public String lastGet(final byte[] key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.LASTGET, key);
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Add the value of a lastArray key.
     *
     * @param key   the key
     * @param offset the offset
     * @param content the content
     * @param value the value
     * @param size the size
     * @return Success: last value of offset; Fail: error.
     */
    public String lastArrayAdd(final String key, final long offset, final String content, final double value, final long size) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.LASTARRAYADD, key, String.valueOf(offset), content, String.valueOf(value), String.valueOf(size));
        return BuilderFactory.STRING.build(obj);
    }

    public String lastArrayAdd(final byte[] key, final long offset, final byte[] content, final double value, final long size) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.LASTARRAYADD, key, toByteArray(offset), content, toByteArray(value), toByteArray(size));
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Add the value of a lastArray key.
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
     * @return Success: last value of offset; Fail: error.
     */
    public String lastArrayAdd(final String key, final long offset, final String content, final double value, final long size, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.LASTARRAYADD, params.getByteParams(SafeEncoder.encode(key), toByteArray(offset), SafeEncoder.encode(content), toByteArray(value), toByteArray(size)));
        return BuilderFactory.STRING.build(obj);
    }

    public String lastArrayAdd(final byte[] key, final long offset, final byte[] content, final double value, final long size, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.LASTARRAYADD, params.getByteParams(key, toByteArray(offset), content, toByteArray(value), toByteArray(size)));
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Get the value of a last key.
     *
     * @param key   the key
     * @param offset the offset
     * @return Success: last value; Fail: error.
     */
    public String lastArrayGet(final String key, final long offset) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.LASTARRAYGET, SafeEncoder.encode(key), toByteArray(offset));
        return BuilderFactory.STRING.build(obj);
    }

    public String lastArrayGet(final byte[] key, final long offset) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.LASTARRAYGET, key, toByteArray(offset));
        return BuilderFactory.STRING.build(obj);
    }

    /**
     * Get the values of an array last key range.
     *
     * @param key   the key
     * @param offset the offset
     * @param range the range
     * @return Success: last value list; Fail: error.
     */
    public List<String> lastArrayGetRange(final String key, final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.LASTARRAYGETRANGE, SafeEncoder.encode(key), toByteArray(offset), toByteArray(range));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<String> lastArrayGetRange(final byte[] key, final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.LASTARRAYGETRANGE, key, toByteArray(offset), toByteArray(range));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    /**
     * Get the merge value of an array last key range.
     *
     * @param key   the key
     * @return Success: merge of last value; Fail: error.
     */
    public String lastArrayGetRangeMerge(final String key,  final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.LASTARRAYGETRANGEMERGE, SafeEncoder.encode(key), toByteArray(offset), toByteArray(range));
        return BuilderFactory.STRING.build(obj);
    }

    public String lastArrayGetRangeMerge(final byte[] key,  final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.LASTARRAYGETRANGEMERGE, key, toByteArray(offset), toByteArray(range));
        return BuilderFactory.STRING.build(obj);
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
    public Double avgAdd(final String key, final long count, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.AVGADD, key, String.valueOf(count), String.valueOf(value));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double avgAdd(final byte[] key, final long count, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.AVGADD, key, toByteArray(count), toByteArray(value));
        return BuilderFactory.DOUBLE.build(obj);
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
    public Double avgAdd(final String key, final long count, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.AVGADD,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(count), toByteArray(value)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double avgAdd(final byte[] key, final long count, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.AVGADD, params.getByteParams(key, toByteArray(count), toByteArray(value)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * Set the value of a avg key.
     *
     * @param key   the key
     * @param count the count
     * @param value the value
     * @return Success: avg value; Fail: error.
     */
    public Double avgSet(final String key, final long count, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.AVGSET, key, String.valueOf(count), String.valueOf(value));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double avgSet(final byte[] key, final long count, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.AVGSET, key, toByteArray(count), toByteArray(value));
        return BuilderFactory.DOUBLE.build(obj);
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
    public Double avgSet(final String key, final long count, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.AVGSET,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(count), toByteArray(value)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double avgSet(final byte[] key, final long count, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.AVGSET, params.getByteParams(key, toByteArray(count), toByteArray(value)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * Get the value of a avg key.
     *
     * @param key   the key
     * @return Success: avg value; Fail: error.
     */
    public Double avgGet(final String key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.AVGGET, key);
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double avgGet(final byte[] key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.AVGGET, key);
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * Add the value of a avgArray key.
     *
     * @param key   the key
     * @param offset the offset
     * @param count the count
     * @param value the value
     * @param size the size
     * @return Success: avg value of offset; Fail: error.
     */
    public Double avgArrayAdd(final String key, final long offset, final long count, final double value, final long size) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.AVGARRAYADD, key, String.valueOf(offset), String.valueOf(count), String.valueOf(value), String.valueOf(size));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double avgArrayAdd(final byte[] key, final long offset, final long count, final double value, final long size) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.AVGARRAYADD, key, toByteArray(offset), toByteArray(count), toByteArray(value), toByteArray(size));
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * Add the value of a avgArray key.
     *
     * @param key   the key
     * @param offset the offset
     * @param count the count
     * @param value the value
     * @param size the size
     * @param params the params: [EX time] [EXAT time] [PX time] [PXAT time]
     * `EX` - Set expire time (seconds)
     * `EXAT` - Set expire time as a UNIX timestamp (seconds)
     * `PX` - Set expire time (milliseconds)
     * `PXAT` - Set expire time as a UNIX timestamp (milliseconds)
     * @return Success: avg value of offset; Fail: error.
     */
    public Double avgArrayAdd(final String key, final long offset, final long count, final double value, final long size, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.AVGARRAYADD, params.getByteParams(SafeEncoder.encode(key), toByteArray(offset), toByteArray(count), toByteArray(value), toByteArray(size)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double avgArrayAdd(final byte[] key, final long offset, final long count, final double value, final long size, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.AVGARRAYADD, params.getByteParams(key, toByteArray(offset), toByteArray(count), toByteArray(value), toByteArray(size)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * Get the value of a avg key.
     *
     * @param key   the key
     * @param offset the offset
     * @return Success: avg value; Fail: error.
     */
    public Double avgArrayGet(final String key, final long offset) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.AVGARRAYGET, SafeEncoder.encode(key), toByteArray(offset));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double avgArrayGet(final byte[] key, final long offset) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.AVGARRAYGET, key, toByteArray(offset));
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * Get the values of an array avg key range.
     *
     * @param key   the key
     * @param offset the offset
     * @param range the range
     * @return Success: avg value list; Fail: error.
     */
    public List<Double> avgArrayGetRange(final String key, final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.AVGARRAYGETRANGE, SafeEncoder.encode(key), toByteArray(offset), toByteArray(range));
        return CpcBuilderFactory.CPCARRAY_RANGE_RESULT.build(obj);
    }

    public List<Double> avgArrayGetRange(final byte[] key, final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.AVGARRAYGETRANGE, key, toByteArray(offset), toByteArray(range));
        return CpcBuilderFactory.CPCARRAY_RANGE_RESULT.build(obj);
    }

    /**
     * Get the merge value of an array avg key range.
     *
     * @param key   the key
     * @return Success: merge of avg value; Fail: error.
     */
    public Double avgArrayGetRangeMerge(final String key,  final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.AVGARRAYGETRANGEMERGE, SafeEncoder.encode(key), toByteArray(offset), toByteArray(range));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double avgArrayGetRangeMerge(final byte[] key,  final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.AVGARRAYGETRANGEMERGE, key, toByteArray(offset), toByteArray(range));
        return BuilderFactory.DOUBLE.build(obj);
    }

    // stddev operation

    /**
     * Add the value of a stddev key.
     *
     * @param key   the key
     * @param value the value
     * @return Success: stddev value; Fail: error.
     */
    public Double stddevAdd(final String key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.STDDEVADD, key, String.valueOf(value));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double stddevAdd(final byte[] key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.STDDEVADD, key, toByteArray(value));
        return BuilderFactory.DOUBLE.build(obj);
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
    public Double stddevAdd(final String key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.STDDEVADD,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(value)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double stddevAdd(final byte[] key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.STDDEVADD, params.getByteParams(key, toByteArray(value)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * Set the value of a stddev key.
     *
     * @param key   the key
     * @param value the value
     * @return Success: stddev value; Fail: error.
     */
    public Double stddevSet(final String key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.STDDEVSET, key, String.valueOf(value));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double stddevSet(final byte[] key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.STDDEVSET, key, toByteArray(value));
        return BuilderFactory.DOUBLE.build(obj);
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
    public Double stddevSet(final String key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.STDDEVSET,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(value)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double stddevSet(final byte[] key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.STDDEVSET, params.getByteParams(key, toByteArray(value)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * Get the value of a stddev key.
     *
     * @param key   the key
     * @return Success: stddev value; Fail: error.
     */
    public Double stddevGet(final String key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.STDDEVGET, key);
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double stddevGet(final byte[] key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.STDDEVGET, key);
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * Add the value of a stddevArray key.
     *
     * @param key   the key
     * @param offset the offset
     * @param value the value
     * @param size the size
     * @return Success: stddev value of offset; Fail: error.
     */
    public Double stddevArrayAdd(final String key, final long offset, final double value, final long size) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.STDDEVARRAYADD, key, String.valueOf(offset), String.valueOf(value), String.valueOf(size));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double stddevArrayAdd(final byte[] key, final long offset, final double value, final long size) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.STDDEVARRAYADD, key, toByteArray(offset), toByteArray(value), toByteArray(size));
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * Add the value of a stddevArray key.
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
     * @return Success: stddev value of offset; Fail: error.
     */
    public Double stddevArrayAdd(final String key, final long offset, final double value, final long size, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.STDDEVARRAYADD, params.getByteParams(SafeEncoder.encode(key), toByteArray(offset), toByteArray(value), toByteArray(size)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double stddevArrayAdd(final byte[] key, final long offset, final double value, final long size, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.STDDEVARRAYADD, params.getByteParams(key, toByteArray(offset), toByteArray(value), toByteArray(size)));
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * Get the value of a stddev key.
     *
     * @param key   the key
     * @param offset the offset
     * @return Success: stddev value; Fail: error.
     */
    public Double stddevArrayGet(final String key, final long offset) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.STDDEVARRAYGET, SafeEncoder.encode(key), toByteArray(offset));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double stddevArrayGet(final byte[] key, final long offset) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.STDDEVARRAYGET, key, toByteArray(offset));
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * Get the values of an array stddev key range.
     *
     * @param key   the key
     * @param offset the offset
     * @param range the range
     * @return Success: stddev value list; Fail: error.
     */
    public List<Double> stddevArrayGetRange(final String key, final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.STDDEVARRAYGETRANGE, SafeEncoder.encode(key), toByteArray(offset), toByteArray(range));
        return CpcBuilderFactory.CPCARRAY_RANGE_RESULT.build(obj);
    }

    public List<Double> stddevArrayGetRange(final byte[] key, final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.STDDEVARRAYGETRANGE, key, toByteArray(offset), toByteArray(range));
        return CpcBuilderFactory.CPCARRAY_RANGE_RESULT.build(obj);
    }

    /**
     * Get the merge value of an array stddev key range.
     *
     * @param key   the key
     * @return Success: merge of stddev value; Fail: error.
     */
    public Double stddevArrayGetRangeMerge(final String key,  final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.STDDEVARRAYGETRANGEMERGE, SafeEncoder.encode(key), toByteArray(offset), toByteArray(range));
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double stddevArrayGetRangeMerge(final byte[] key,  final long offset, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jc.sendCommand(key, ModuleCommand.STDDEVARRAYGETRANGEMERGE, key, toByteArray(offset), toByteArray(range));
        return BuilderFactory.DOUBLE.build(obj);
    }
}
