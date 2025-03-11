package com.aliyun.tair.taircpc;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.taircpc.factory.CpcBuilderFactory;
import com.aliyun.tair.taircpc.params.*;
import com.aliyun.tair.taircpc.results.Update2JudResult;
import io.valkey.BuilderFactory;
import io.valkey.Jedis;
import io.valkey.JedisPool;
import io.valkey.exceptions.JedisConnectionException;
import io.valkey.exceptions.JedisDataException;
import io.valkey.util.JedisClusterCRC16;
import io.valkey.util.SafeEncoder;

import java.util.ArrayList;
import java.util.List;

import static io.valkey.Protocol.toByteArray;

public class TairCpc {

    private Jedis jedis;
    private JedisPool jedisPool;

    public TairCpc(Jedis jedis) {
        this.jedis = jedis;
    }
    
    public TairCpc(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    private Jedis getJedis() {
        if (jedisPool != null) {
            return jedisPool.getResource();
        }
        return jedis;
    }

    private void releaseJedis(Jedis jedis) {
        if (jedisPool != null) {
            jedis.close();
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCESTIMATE, key);
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double cpcEstimate(final byte[] key) throws JedisConnectionException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCESTIMATE, key);
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            CpcMultiUpdateParams keyList = new CpcMultiUpdateParams();
            Object obj = jedis.sendCommand(ModuleCommand.CPCMUPDATE, keyList.getByteParams(keys));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            CpcMultiUpdateParams keyList = new CpcMultiUpdateParams();
            Object obj = jedis.sendCommand(ModuleCommand.CPCMUPDATE2EST, keyList.getByteParams(keys));
            return CpcBuilderFactory.CPCUPDATE2EST_MULTI_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
     * MultiUpdate the cpc.
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
        Jedis jedis = getJedis();
        try {
            CpcMultiUpdateParams keyList = new CpcMultiUpdateParams();
            Object obj = jedis.sendCommand(ModuleCommand.CPCMUPDATE2JUD, keyList.getByteParams(keys));
            return CpcBuilderFactory.CPCUPDATE2JUD_MULTI_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCUPDATE, key, item);
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public String cpcUpdate(final byte[] key, final byte[] item) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCUPDATE, key, item);
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCUPDATE,
                params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(item)));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public String cpcUpdate(final byte[] key, final byte[] item, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCUPDATE, params.getByteParams(key, item));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCUPDATE2JUD, key, item);
            return CpcBuilderFactory.CPCUPDATE2JUD_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Update2JudResult cpcUpdate2Jud(final byte[] key, final byte[] item) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCUPDATE2JUD, key, item);
            return CpcBuilderFactory.CPCUPDATE2JUD_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCUPDATE2JUD,
                params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(item)));
            return CpcBuilderFactory.CPCUPDATE2JUD_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Update2JudResult cpcUpdate2Jud(final byte[] key, final byte[] item, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCUPDATE2JUD, params.getByteParams(key, item));
            return CpcBuilderFactory.CPCUPDATE2JUD_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCUPDATE2EST, key, item);
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double cpcUpdate2Est(final byte[] key, final byte[] item) throws JedisConnectionException,IllegalArgumentException,
            JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCUPDATE2EST, key, item);
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCUPDATE2EST,
                params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(item)));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double cpcUpdate2Est(final byte[] key, final byte[] item, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCUPDATE2EST, params.getByteParams(key, item));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Update the item of a cpcArray.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @param item the item
     * @return Success: OK; Fail: error.
     */
    public String cpcArrayUpdate(final String key, final long timestamp, final String item) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCARRAYUPDATE, key, String.valueOf(timestamp), item);
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public String cpcArrayUpdate(final byte[] key, final long timestamp, final byte[] item) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCARRAYUPDATE, key, toByteArray(timestamp), item);
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
    public String cpcArrayUpdate(final String key, final long timestamp, final String item, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCARRAYUPDATE, params.getByteParams(SafeEncoder.encode(key), toByteArray(timestamp), SafeEncoder.encode(item)));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public String cpcArrayUpdate(final byte[] key, final long timestamp, final byte[] item, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCARRAYUPDATE, params.getByteParams(key, toByteArray(timestamp), item));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Update the item of a cpcArray.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @param item the item
     * @return Success: Double value; Fail: error.
     */
    public Double cpcArrayUpdate2Est(final String key, final long timestamp, final String item) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCARRAYUPDATE2EST, key, String.valueOf(timestamp), item);
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double cpcArrayUpdate2Est(final byte[] key, final long timestamp, final byte[] item) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCARRAYUPDATE2EST, key, toByteArray(timestamp), item);
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
    public Double cpcArrayUpdate2Est(final String key, final long timestamp, final String item, final CpcUpdateParams params)
            throws JedisConnectionException, IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCARRAYUPDATE2EST, params.getByteParams(SafeEncoder.encode(key),
                toByteArray(timestamp), SafeEncoder.encode(item)));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double cpcArrayUpdate2Est(final byte[] key, final long timestamp, final byte[] item, final CpcUpdateParams params)
            throws JedisConnectionException, IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCARRAYUPDATE2EST, params.getByteParams(key, toByteArray(timestamp),
                item));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Update the item of a cpcArray.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @param item the item
     * @return Success: Update2JudResult; Fail: error.
     */
    public Update2JudResult cpcArrayUpdate2Jud(final String key, final long timestamp, final String item)
            throws JedisConnectionException, IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCARRAYUPDATE2JUD, key, String.valueOf(timestamp), item);
            return CpcBuilderFactory.CPCUPDATE2JUD_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Update2JudResult cpcArrayUpdate2Jud(final byte[] key, final long timestamp, final byte[] item)
            throws JedisConnectionException, IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCARRAYUPDATE2JUD, key, toByteArray(timestamp), item);
            return CpcBuilderFactory.CPCUPDATE2JUD_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
    public Update2JudResult cpcArrayUpdate2Jud(final String key, final long timestamp, final String item, final CpcUpdateParams params)
            throws JedisConnectionException, IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCARRAYUPDATE2JUD, params.getByteParams(SafeEncoder.encode(key),
                toByteArray(timestamp), SafeEncoder.encode(item)));
            return CpcBuilderFactory.CPCUPDATE2JUD_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Update2JudResult cpcArrayUpdate2Jud(final byte[] key, final long timestamp, final byte[] item, final CpcUpdateParams params)
            throws JedisConnectionException, IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        if (item == null) {
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCARRAYUPDATE2JUD, params.getByteParams(key, toByteArray(timestamp),
                item));
            return CpcBuilderFactory.CPCUPDATE2JUD_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

//    /**
//     * MutiUpdate the item of a cpcArray.
//     *
//     * @param keys    {key timestamp item size expStr exp} [key timestamp item size expStr exp] ...
//     * @return Success: OK; Fail: error.
//     */
//    public String cpcArrayMUpdate(final ArrayList<CpcArrayData> keys) throws JedisConnectionException,IllegalArgumentException,
//            JedisDataException {
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
//        Object obj = getJedis().sendCommand(ModuleCommand.CPCMARRAYUPDATE, keyList.getByteParams(keys));
//        return BuilderFactory.STRING.build(obj);
//    }
//
//    /**
//     * MutiUpdate the item of a cpcArray.
//     *
//     * @param keys    {key timestamp item size expStr exp} [key timestamp item size expStr exp] ...
//     * @return Success: List<Double>; Fail: error.
//     */
//    public List<Double> cpcArrayMUpdate2Est(final ArrayList<CpcArrayData> keys) throws JedisConnectionException,
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
//        Object obj = getJedis().sendCommand(ModuleCommand.CPCMARRAYUPDATE2EST, keyList.getByteParams(keys));
//        return CpcBuilderFactory.CPCARRAYUPDATE2EST_MULTI_RESULT.build(obj);
//    }
//
//    /**
//     * MutiUpdate the item of a cpcArray.
//     *
//     * @param keys    {key timestamp item size expStr exp} [key timestamp item size expStr exp] ...
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
//        Object obj = getJedis().sendCommand(ModuleCommand.CPCMARRAYUPDATE2ESTWITHKEY, keyList.getByteParams(keys));
//        return CpcBuilderFactory.CPCUPDATE2ESTWITHKEY_MULTI_RESULT.build(obj);
//    }
//
//    /**
//     * MutiUpdate the item of a cpcArray.
//     *
//     * @param keys    {key timestamp item size expStr exp} [key timestamp item size expStr exp] ...
//     * @return Success: List<Update2JudResult>; Fail: error.
//     */
//    public List<Update2JudResult> cpcArrayMUpdate2Jud(final ArrayList<CpcArrayData> keys) throws JedisConnectionException,
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
//        Object obj = getJedis().sendCommand(ModuleCommand.CPCMARRAYUPDATE2JUD, keyList.getByteParams(keys));
//        return CpcBuilderFactory.CPCARRAYUPDATE2JUD_MULTI_RESULT.build(obj);
//    }
//
//    /**
//     * MutiUpdate the item of a cpcArray.
//     *
//     * @param keys    {key timestamp item size expStr exp} [key timestamp item size expStr exp] ...
//     * @return Success: HashMap<String, Update2JudResult>; Fail: error.
//     */
//    public HashMap<String, Update2JudResult> cpcArrayMUpdate2JudWithKey(final ArrayList<CpcArrayData> keys) throws JedisConnectionException,
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
//        Object obj = getJedis().sendCommand(ModuleCommand.CPCMARRAYUPDATE2JUDWITHKEY, keyList.getByteParams(keys));
//        return CpcBuilderFactory.CPCUPDATE2JUDWITHKEY_MULTI_RESULT.build(obj);
//    }

    /**
     * Estimate the cpcArray.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @return Success: double; Empty: 0; Fail: error.
     */
    public Double cpcArrayEstimate(final String key, final long timestamp) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCARRAYESTIMATE, key, String.valueOf(timestamp));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double cpcArrayEstimate(final byte[] key, final long timestamp) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCARRAYESTIMATE, key, toByteArray(timestamp));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Estimate the cpcArray for a range.
     *
     * @param key   the key
     * @param starttime the starttime
     * @param endtime the endtime
     * @return Success: String List; Fail: error.
     */
    public List<Double> cpcArrayEstimateRange(final String key, final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCARRAYESTIMATERANGE, key, String.valueOf(starttime), String.valueOf(endtime));
            return CpcBuilderFactory.CPCARRAY_ESTIMATE_RANGE_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public List<Double> cpcArrayEstimateRange(final byte[] key, final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCARRAYESTIMATERANGE, key, toByteArray(starttime), toByteArray(endtime));
            return CpcBuilderFactory.CPCARRAY_ESTIMATE_RANGE_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Estimate & sum the cpcArray for a range.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @param range the range
     * @return Success: double; Empty: 0; Fail: error.
     */
    public Double cpcArrayEstimateRangeSum(final String key, final long timestamp, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCARRAYESTIMATERANGESUM, key, String.valueOf(timestamp), String.valueOf(range));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double cpcArrayEstimateRangeSum(final byte[] key, final long timestamp, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCARRAYESTIMATERANGESUM, key, toByteArray(timestamp), toByteArray(range));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Estimate & merge the cpcArray for a range.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @param range the range
     * @return Success: double; Empty: 0; Fail: error.
     */
    public Double cpcArrayEstimateRangeMerge(final String key, final long timestamp, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCARRAYESTIMATERANGEMERGE, key, String.valueOf(timestamp), String.valueOf(range));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double cpcArrayEstimateRangeMerge(final byte[] key, final long timestamp, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException{
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCARRAYESTIMATERANGEMERGE, key, toByteArray(timestamp), toByteArray(range));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public int getSlot (String key) throws JedisConnectionException {
        return JedisClusterCRC16.getSlot(key);
    }


    public Double cpcArrayEstimateTimeMerge(final String key,  final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCARRAYESTIMATETIMEMERGE, SafeEncoder.encode(key), toByteArray(starttime), toByteArray(endtime));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double cpcArrayEstimateTimeMerge(final byte[] key,  final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.CPCARRAYESTIMATETIMEMERGE, key, toByteArray(starttime), toByteArray(endtime));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.SUMADD, key, String.valueOf(value));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double sumAdd(final byte[] key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.SUMADD, key, toByteArray(value));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.SUMADD,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(value)));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double sumAdd(final byte[] key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.SUMADD, params.getByteParams(key, toByteArray(value)));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.SUMSET, key, String.valueOf(value));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double sumSet(final byte[] key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.SUMSET, key, toByteArray(value));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.SUMSET,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(value)));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double sumSet(final byte[] key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.SUMSET, params.getByteParams(key, toByteArray(value)));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.SUMGET, key);
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double sumGet(final byte[] key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.SUMGET, key);
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Add the value of a sumArray key.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @param value the value
     * @return Success: sum value of timestamp; Fail: error.
     */
    public Double sumArrayAdd(final String key, final long timestamp, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.SUMARRAYADD, key, String.valueOf(timestamp), String.valueOf(value));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double sumArrayAdd(final byte[] key, final long timestamp, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.SUMARRAYADD, key, toByteArray(timestamp), toByteArray(value));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
    public Double sumArrayAdd(final String key, final long timestamp, final double value, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.SUMARRAYADD, params.getByteParams(SafeEncoder.encode(key), toByteArray(timestamp), toByteArray(value)));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double sumArrayAdd(final byte[] key, final long timestamp, final double value, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.SUMARRAYADD, params.getByteParams(key, toByteArray(timestamp), toByteArray(value)));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Get the value of a sum key.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @return Success: sum value; Fail: error.
     */
    public Double sumArrayGet(final String key, final long timestamp) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.SUMARRAYGET, SafeEncoder.encode(key), toByteArray(timestamp));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double sumArrayGet(final byte[] key, final long timestamp) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.SUMARRAYGET, key, toByteArray(timestamp));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Get the values of an array sum key range.
     *
     * @param key   the key
     * @param starttime the starttime
     * @param endtime the endtime
     * @return Success: sum value list; Fail: error.
     */
    public List<Double> sumArrayGetRange(final String key, final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.SUMARRAYGETRANGE, SafeEncoder.encode(key), toByteArray(starttime), toByteArray(endtime));
            return CpcBuilderFactory.CPCARRAY_RANGE_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public List<Double> sumArrayGetRange(final byte[] key, final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.SUMARRAYGETRANGE, key, toByteArray(starttime), toByteArray(endtime));
            return CpcBuilderFactory.CPCARRAY_RANGE_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Get the merge value of an array sum key range.
     *
     * @param key   the key
     * @return Success: merge of sum value; Fail: error.
     */
    public Double sumArrayGetRangeTimeMerge(final String key,  final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.SUMARRAYGETTIMEMERGE, SafeEncoder.encode(key), toByteArray(starttime), toByteArray(endtime));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double sumArrayGetRangeTimeMerge(final byte[] key,  final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.SUMARRAYGETTIMEMERGE, key, toByteArray(starttime), toByteArray(endtime));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Get the merge value of an array sum key range.
     *
     * @param key   the key
     * @return Success: merge of sum value; Fail: error.
     */
    public Double sumArrayGetRangeMerge(final String key,  final long endtime, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.SUMARRAYGETRANGEMERGE, SafeEncoder.encode(key), toByteArray(endtime), toByteArray(range));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double sumArrayGetRangeMerge(final byte[] key,  final long endtime, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.SUMARRAYGETRANGEMERGE, key, toByteArray(endtime), toByteArray(range));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MAXADD, key, String.valueOf(value));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double maxAdd(final byte[] key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MAXADD, key, toByteArray(value));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MAXADD,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(value)));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double maxAdd(final byte[] key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MAXADD, params.getByteParams(key, toByteArray(value)));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MAXSET, key, String.valueOf(value));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double maxSet(final byte[] key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MAXSET, key, toByteArray(value));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MAXSET,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(value)));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double maxSet(final byte[] key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MAXSET, params.getByteParams(key, toByteArray(value)));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MAXGET, key);
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double maxGet(final byte[] key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MAXGET, key);
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Add the value of a maxArray key.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @param value the value
     * @return Success: max value of timestamp; Fail: error.
     */
    public Double maxArrayAdd(final String key, final long timestamp, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MAXARRAYADD, key, String.valueOf(timestamp), String.valueOf(value));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double maxArrayAdd(final byte[] key, final long timestamp, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MAXARRAYADD, key, toByteArray(timestamp), toByteArray(value));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
    public Double maxArrayAdd(final String key, final long timestamp, final double value, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MAXARRAYADD, params.getByteParams(SafeEncoder.encode(key), toByteArray(timestamp), toByteArray(value)));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double maxArrayAdd(final byte[] key, final long timestamp, final double value, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MAXARRAYADD, params.getByteParams(key, toByteArray(timestamp), toByteArray(value)));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Get the value of a max key.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @return Success: max value; Fail: error.
     */
    public Double maxArrayGet(final String key, final long timestamp) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MAXARRAYGET, SafeEncoder.encode(key), toByteArray(timestamp));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double maxArrayGet(final byte[] key, final long timestamp) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Object obj = jedis.sendCommand(ModuleCommand.MAXARRAYGET, key, toByteArray(timestamp));
        return BuilderFactory.DOUBLE.build(obj);
    }

    /**
     * Get the values of an array max key range.
     *
     * @param key   the key
     * @param starttime the starttime
     * @param endtime the endtime
     * @return Success: max value list; Fail: error.
     */
    public List<Double> maxArrayGetRange(final String key, final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MAXARRAYGETRANGE, SafeEncoder.encode(key), toByteArray(starttime), toByteArray(endtime));
            return CpcBuilderFactory.CPCARRAY_RANGE_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public List<Double> maxArrayGetRange(final byte[] key, final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MAXARRAYGETRANGE, key, toByteArray(starttime), toByteArray(endtime));
            return CpcBuilderFactory.CPCARRAY_RANGE_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Get the merge value of an array max key range.
     *
     * @param key   the key
     * @return Success: merge of max value; Fail: error.
     */
    public Double maxArrayGetRangeTimeMerge(final String key,  final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MAXARRAYGETTIMEMERGE, SafeEncoder.encode(key), toByteArray(starttime), toByteArray(endtime));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double maxArrayGetRangeTimeMerge(final byte[] key,  final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MAXARRAYGETTIMEMERGE, key, toByteArray(starttime), toByteArray(endtime));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Get the merge value of an array max key range.
     *
     * @param key   the key
     * @return Success: merge of max value; Fail: error.
     */
    public Double maxArrayGetRangeMerge(final String key,  final long endtime, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MAXARRAYGETRANGEMERGE, SafeEncoder.encode(key), toByteArray(endtime), toByteArray(range));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double maxArrayGetRangeMerge(final byte[] key,  final long endtime, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MAXARRAYGETRANGEMERGE, key, toByteArray(endtime), toByteArray(range));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MINADD, key, String.valueOf(value));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double minAdd(final byte[] key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MINADD, key, toByteArray(value));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MINADD,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(value)));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double minAdd(final byte[] key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MINADD, params.getByteParams(key, toByteArray(value)));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MINSET, key, String.valueOf(value));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double minSet(final byte[] key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MINSET, key, toByteArray(value));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MINSET,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(value)));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double minSet(final byte[] key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MINSET, params.getByteParams(key, toByteArray(value)));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MINGET, key);
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double minGet(final byte[] key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MINGET, key);
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Add the value of a minArray key.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @param value the value
     * @return Success: min value of timestamp; Fail: error.
     */
    public Double minArrayAdd(final String key, final long timestamp, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MINARRAYADD, key, String.valueOf(timestamp), String.valueOf(value));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double minArrayAdd(final byte[] key, final long timestamp, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MINARRAYADD, key, toByteArray(timestamp), toByteArray(value));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
    public Double minArrayAdd(final String key, final long timestamp, final double value, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MINARRAYADD, params.getByteParams(SafeEncoder.encode(key), toByteArray(timestamp), toByteArray(value)));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double minArrayAdd(final byte[] key, final long timestamp, final double value, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MINARRAYADD, params.getByteParams(key, toByteArray(timestamp), toByteArray(value)));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Get the value of a min key.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @return Success: min value; Fail: error.
     */
    public Double minArrayGet(final String key, final long timestamp) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MINARRAYGET, SafeEncoder.encode(key), toByteArray(timestamp));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double minArrayGet(final byte[] key, final long timestamp) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MINARRAYGET, key, toByteArray(timestamp));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Get the values of an array min key range.
     *
     * @param key   the key
     * @param starttime the starttime
     * @param endtime the endtime
     * @return Success: min value list; Fail: error.
     */
    public List<Double> minArrayGetRange(final String key, final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MINARRAYGETRANGE, SafeEncoder.encode(key), toByteArray(starttime), toByteArray(endtime));
            return CpcBuilderFactory.CPCARRAY_RANGE_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public List<Double> minArrayGetRange(final byte[] key, final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MINARRAYGETRANGE, key, toByteArray(starttime), toByteArray(endtime));
            return CpcBuilderFactory.CPCARRAY_RANGE_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double minArrayGetRangeTimeMerge(final String key,  final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MINARRAYGETTIMEMERGE, SafeEncoder.encode(key), toByteArray(starttime), toByteArray(endtime));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double minArrayGetRangeTimeMerge(final byte[] key,  final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MINARRAYGETTIMEMERGE, key, toByteArray(starttime), toByteArray(endtime));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Get the merge value of an array min key range.
     *
     * @param key   the key
     * @return Success: merge of min value; Fail: error.
     */
    public Double minArrayGetRangeMerge(final String key,  final long endtime, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MINARRAYGETRANGEMERGE, SafeEncoder.encode(key), toByteArray(endtime), toByteArray(range));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double minArrayGetRangeMerge(final byte[] key,  final long endtime, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.MINARRAYGETRANGEMERGE, key, toByteArray(endtime), toByteArray(range));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.FIRSTADD, key, content, String.valueOf(value));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public String firstAdd(final byte[] key, final byte[] content, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.FIRSTADD, key, content, toByteArray(value));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.FIRSTADD,
                params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(content), toByteArray(value)));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public String firstAdd(final byte[] key, final byte[] content, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.FIRSTADD, params.getByteParams(key, content, toByteArray(value)));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.FIRSTSET, key, content, String.valueOf(value));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public String firstSet(final byte[] key, final byte[] content, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.FIRSTSET, key, content, toByteArray(value));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.FIRSTSET,
                params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(content), toByteArray(value)));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public String firstSet(final byte[] key, final byte[] content, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.FIRSTSET, params.getByteParams(key, content, toByteArray(value)));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.FIRSTGET, key);
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public String firstGet(final byte[] key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.FIRSTGET, key);
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
    public String firstArrayAdd(final String key, final long timestamp, final String content, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.FIRSTARRAYADD, key, String.valueOf(timestamp), content, String.valueOf(value));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public String firstArrayAdd(final byte[] key, final long timestamp, final byte[] content, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.FIRSTARRAYADD, key, toByteArray(timestamp), content, toByteArray(value));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
    public String firstArrayAdd(final String key, final long timestamp, final String content, final double value, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.FIRSTARRAYADD, params.getByteParams(SafeEncoder.encode(key), toByteArray(timestamp), SafeEncoder.encode(content), toByteArray(value)));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public String firstArrayAdd(final byte[] key, final long timestamp, final byte[] content, final double value, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.FIRSTARRAYADD, params.getByteParams(key, toByteArray(timestamp), content, toByteArray(value)));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Get the value of a first key.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @return Success: first value; Fail: error.
     */
    public String firstArrayGet(final String key, final long timestamp) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.FIRSTARRAYGET, SafeEncoder.encode(key), toByteArray(timestamp));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public String firstArrayGet(final byte[] key, final long timestamp) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.FIRSTARRAYGET, key, toByteArray(timestamp));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

//    /**
//     * Get the values of an array first key range.
//     *
//     * @param key   the key
//     * @param starttime the starttime
//     * @param endtime the endtime
//     * @return Success: first value list; Fail: error.
//     */
//    public List<String> firstArrayGetRange(final String key, final long starttime, final long endtime) throws JedisConnectionException,
//            IllegalArgumentException, JedisDataException {
//        if (key == null) {
//            throw new IllegalArgumentException(CommonResult.keyIsNull);
//        }
//        Object obj = getJedis().sendCommand(ModuleCommand.FIRSTARRAYGETRANGE, SafeEncoder.encode(key), toByteArray(starttime), toByteArray(endtime));
//        return BuilderFactory.STRING_LIST.build(obj);
//    }
//
//    public List<String> firstArrayGetRange(final byte[] key, final long starttime, final long endtime) throws JedisConnectionException,
//            IllegalArgumentException, JedisDataException {
//        if (key == null) {
//            throw new IllegalArgumentException(CommonResult.keyIsNull);
//        }
//        Object obj = getJedis().sendCommand(ModuleCommand.FIRSTARRAYGETRANGE, key, toByteArray(starttime), toByteArray(endtime));
//        return BuilderFactory.STRING_LIST.build(obj);
//    }

    public String firstArrayGetRangeTimeMerge(final String key,  final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.FIRSTARRAYGETTIMEMERGE, SafeEncoder.encode(key), toByteArray(starttime), toByteArray(endtime));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public String firstArrayGetRangeTimeMerge(final byte[] key,  final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.FIRSTARRAYGETTIMEMERGE, key, toByteArray(starttime), toByteArray(endtime));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Get the merge value of an array first key range.
     *
     * @param key   the key
     * @return Success: merge of first value; Fail: error.
     */
    public String firstArrayGetRangeMerge(final String key,  final long endtime, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.FIRSTARRAYGETRANGEMERGE, SafeEncoder.encode(key), toByteArray(endtime), toByteArray(range));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public String firstArrayGetRangeMerge(final byte[] key,  final long endtime, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.FIRSTARRAYGETRANGEMERGE, key, toByteArray(endtime), toByteArray(range));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.LASTADD, key, content, String.valueOf(value));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public String lastAdd(final byte[] key, final byte[] content, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.LASTADD, key, content, toByteArray(value));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.LASTADD,
                params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(content), toByteArray(value)));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public String lastAdd(final byte[] key, final byte[] content, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.LASTADD, params.getByteParams(key, content, toByteArray(value)));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.LASTSET, key, content, String.valueOf(value));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public String lastSet(final byte[] key, final byte[] content, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.LASTSET, key, content, toByteArray(value));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.LASTSET,
                params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(content), toByteArray(value)));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public String lastSet(final byte[] key, final byte[] content, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.LASTSET, params.getByteParams(key, content, toByteArray(value)));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.LASTGET, key);
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public String lastGet(final byte[] key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.LASTGET, key);
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
    public String lastArrayAdd(final String key, final long timestamp, final String content, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.LASTARRAYADD, key, String.valueOf(timestamp), content, String.valueOf(value));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public String lastArrayAdd(final byte[] key, final long timestamp, final byte[] content, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.LASTARRAYADD, key, toByteArray(timestamp), content, toByteArray(value));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
    public String lastArrayAdd(final String key, final long timestamp, final String content, final double value, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.LASTARRAYADD, params.getByteParams(SafeEncoder.encode(key), toByteArray(timestamp), SafeEncoder.encode(content), toByteArray(value)));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public String lastArrayAdd(final byte[] key, final long timestamp, final byte[] content, final double value, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.LASTARRAYADD, params.getByteParams(key, toByteArray(timestamp), content, toByteArray(value)));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Get the value of a last key.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @return Success: last value; Fail: error.
     */
    public String lastArrayGet(final String key, final long timestamp) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.LASTARRAYGET, SafeEncoder.encode(key), toByteArray(timestamp));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public String lastArrayGet(final byte[] key, final long timestamp) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.LASTARRAYGET, key, toByteArray(timestamp));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

//    /**
//     * Get the values of an array last key range.
//     *
//     * @param key   the key
//     * @param starttime the starttime
//     * @param endtime the endtime
//     * @return Success: last value list; Fail: error.
//     */
//    public List<String> lastArrayGetRange(final String key, final long starttime, final long endtime) throws JedisConnectionException,
//            IllegalArgumentException, JedisDataException {
//        if (key == null) {
//            throw new IllegalArgumentException(CommonResult.keyIsNull);
//        }
//        Object obj = getJedis().sendCommand(ModuleCommand.LASTARRAYGETRANGE, SafeEncoder.encode(key), toByteArray(starttime), toByteArray(endtime));
//        return BuilderFactory.STRING_LIST.build(obj);
//    }
//
//    public List<String> lastArrayGetRange(final byte[] key, final long starttime, final long endtime) throws JedisConnectionException,
//            IllegalArgumentException, JedisDataException {
//        if (key == null) {
//            throw new IllegalArgumentException(CommonResult.keyIsNull);
//        }
//        Object obj = getJedis().sendCommand(ModuleCommand.LASTARRAYGETRANGE, key, toByteArray(starttime), toByteArray(endtime));
//        return BuilderFactory.STRING_LIST.build(obj);
//    }

    public String lastArrayGetRangeTimeMerge(final String key,  final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.LASTARRAYGETTIMEEMERGE, SafeEncoder.encode(key), toByteArray(starttime), toByteArray(endtime));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }

    }

    public String lastArrayGetRangeTimeMerge(final byte[] key,  final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.LASTARRAYGETTIMEEMERGE, key, toByteArray(starttime), toByteArray(endtime));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Get the merge value of an array last key range.
     *
     * @param key   the key
     * @return Success: merge of last value; Fail: error.
     */
    public String lastArrayGetRangeMerge(final String key,  final long endtime, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.LASTARRAYGETRANGEMERGE, SafeEncoder.encode(key), toByteArray(endtime), toByteArray(range));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public String lastArrayGetRangeMerge(final byte[] key,  final long endtime, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.LASTARRAYGETRANGEMERGE, key, toByteArray(endtime), toByteArray(range));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.AVGADD, key, String.valueOf(count), String.valueOf(value));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double avgAdd(final byte[] key, final long count, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.AVGADD, key, toByteArray(count), toByteArray(value));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.AVGADD,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(count), toByteArray(value)));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double avgAdd(final byte[] key, final long count, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.AVGADD, params.getByteParams(key, toByteArray(count), toByteArray(value)));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.AVGSET, key, String.valueOf(count), String.valueOf(value));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double avgSet(final byte[] key, final long count, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.AVGSET, key, toByteArray(count), toByteArray(value));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.AVGSET,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(count), toByteArray(value)));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double avgSet(final byte[] key, final long count, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.AVGSET, params.getByteParams(key, toByteArray(count), toByteArray(value)));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.AVGGET, key);
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double avgGet(final byte[] key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.AVGGET, key);
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
    public Double avgArrayAdd(final String key, final long timestamp, final long count, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.AVGARRAYADD, key, String.valueOf(timestamp), String.valueOf(count), String.valueOf(value));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double avgArrayAdd(final byte[] key, final long timestamp, final long count, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.AVGARRAYADD, key, toByteArray(timestamp), toByteArray(count), toByteArray(value));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
    public Double avgArrayAdd(final String key, final long timestamp, final long count, final double value, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.AVGARRAYADD, params.getByteParams(SafeEncoder.encode(key), toByteArray(timestamp), toByteArray(count), toByteArray(value)));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double avgArrayAdd(final byte[] key, final long timestamp, final long count, final double value, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.AVGARRAYADD, params.getByteParams(key, toByteArray(timestamp), toByteArray(count), toByteArray(value)));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Get the value of a avg key.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @return Success: avg value; Fail: error.
     */
    public Double avgArrayGet(final String key, final long timestamp) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.AVGARRAYGET, SafeEncoder.encode(key), toByteArray(timestamp));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double avgArrayGet(final byte[] key, final long timestamp) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.AVGARRAYGET, key, toByteArray(timestamp));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Get the values of an array avg key range.
     *
     * @param key   the key
     * @param starttime the starttime
     * @param endtime the endtime
     * @return Success: avg value list; Fail: error.
     */
    public List<Double> avgArrayGetRange(final String key, final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.AVGARRAYGETRANGE, SafeEncoder.encode(key), toByteArray(starttime), toByteArray(endtime));
            return CpcBuilderFactory.CPCARRAY_RANGE_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public List<Double> avgArrayGetRange(final byte[] key, final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.AVGARRAYGETRANGE, key, toByteArray(starttime), toByteArray(endtime));
            return CpcBuilderFactory.CPCARRAY_RANGE_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }


    public Double avgArrayGetRangeTimeMerge(final String key,  final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.AVGARRAYGETTIMEMERGE, SafeEncoder.encode(key), toByteArray(starttime), toByteArray(endtime));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double avgArrayGetRangeTimeMerge(final byte[] key,  final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.AVGARRAYGETTIMEMERGE, key, toByteArray(starttime), toByteArray(endtime));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Get the merge value of an array avg key range.
     *
     * @param key   the key
     * @return Success: merge of avg value; Fail: error.
     */
    public Double avgArrayGetRangeMerge(final String key,  final long endtime, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.AVGARRAYGETRANGEMERGE, SafeEncoder.encode(key), toByteArray(endtime), toByteArray(range));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double avgArrayGetRangeMerge(final byte[] key,  final long endtime, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.AVGARRAYGETRANGEMERGE, key, toByteArray(endtime), toByteArray(range));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.STDDEVADD, key, String.valueOf(value));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double stddevAdd(final byte[] key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.STDDEVADD, key, toByteArray(value));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.STDDEVADD,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(value)));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double stddevAdd(final byte[] key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.STDDEVADD, params.getByteParams(key, toByteArray(value)));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.STDDEVSET, key, String.valueOf(value));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double stddevSet(final byte[] key, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.STDDEVSET, key, toByteArray(value));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.STDDEVSET,
                params.getByteParams(SafeEncoder.encode(key), toByteArray(value)));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double stddevSet(final byte[] key, final double value, final CpcUpdateParams params) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.STDDEVSET, params.getByteParams(key, toByteArray(value)));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.STDDEVGET, key);
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double stddevGet(final byte[] key) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.STDDEVGET, key);
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Add the value of a stddevArray key.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @param value the value
     * @return Success: stddev value of timestamp; Fail: error.
     */
    public Double stddevArrayAdd(final String key, final long timestamp, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.STDDEVARRAYADD, key, String.valueOf(timestamp), String.valueOf(value));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double stddevArrayAdd(final byte[] key, final long timestamp, final double value) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.STDDEVARRAYADD, key, toByteArray(timestamp), toByteArray(value));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
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
    public Double stddevArrayAdd(final String key, final long timestamp, final double value, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.STDDEVARRAYADD, params.getByteParams(SafeEncoder.encode(key), toByteArray(timestamp), toByteArray(value)));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double stddevArrayAdd(final byte[] key, final long timestamp, final double value, final CpcUpdateParams params)
            throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.STDDEVARRAYADD, params.getByteParams(key, toByteArray(timestamp), toByteArray(value)));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Get the value of a stddev key.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @return Success: stddev value; Fail: error.
     */
    public Double stddevArrayGet(final String key, final long timestamp) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.STDDEVARRAYGET, SafeEncoder.encode(key), toByteArray(timestamp));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double stddevArrayGet(final byte[] key, final long timestamp) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.STDDEVARRAYGET, key, toByteArray(timestamp));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Get the values of an array stddev key range.
     *
     * @param key   the key
     * @param starttime the starttime
     * @param endtime the endtime
     * @return Success: stddev value list; Fail: error.
     */
    public List<Double> stddevArrayGetRange(final String key, final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.STDDEVARRAYGETRANGE, SafeEncoder.encode(key), toByteArray(starttime), toByteArray(endtime));
            return CpcBuilderFactory.CPCARRAY_RANGE_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public List<Double> stddevArrayGetRange(final byte[] key, final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.STDDEVARRAYGETRANGE, key, toByteArray(starttime), toByteArray(endtime));
            return CpcBuilderFactory.CPCARRAY_RANGE_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double stddevArrayGetRangeTimeMerge(final String key,  final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.STDDEVARRAYGETTIMEMERGE, SafeEncoder.encode(key), toByteArray(starttime), toByteArray(endtime));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double stddevArrayGetRangeTimeMerge(final byte[] key,  final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.STDDEVARRAYGETTIMEMERGE, key, toByteArray(starttime), toByteArray(endtime));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Get the merge value of an array stddev key range.
     *
     * @param key   the key
     * @return Success: merge of stddev value; Fail: error.
     */
    public Double stddevArrayGetRangeMerge(final String key,  final long endtime, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.STDDEVARRAYGETRANGEMERGE, SafeEncoder.encode(key), toByteArray(endtime), toByteArray(range));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Double stddevArrayGetRangeMerge(final byte[] key,  final long endtime, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.STDDEVARRAYGETRANGEMERGE, key, toByteArray(endtime), toByteArray(range));
            return BuilderFactory.DOUBLE.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * MutiUpdate the item of a cpcArray.
     *
     * @param keys    {key timestamp item size expStr exp} [key timestamp item size expStr exp] ...
     * @return Success: OK; Fail: error.
     */
    public String sketchesBatchWrite(final ArrayList<CpcArrayMultiData> keys) throws JedisConnectionException,IllegalArgumentException, JedisDataException {
        if (keys == null || keys.isEmpty()) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        for (CpcArrayMultiData key : keys) {
            if (key.getKey() == null) {
                throw new IllegalArgumentException(CommonResult.keyIsNull);
            }
        }
        Jedis jedis = getJedis();
        try {
            CpcMultiArrayUpdateParams keyList = new CpcMultiArrayUpdateParams();
            Object obj = jedis.sendCommand(ModuleCommand.SKETCHESBATCHWRITE, keyList.getByteMultiParams(keys));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Get the value of a key.
     *
     * @param key   the key
     * @param timestamp the timestamp
     * @return Success: sum value; Fail: error.
     */
    public Object sketchesGet(final String key, final long timestamp) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.SKETCHESGET, SafeEncoder.encode(key), toByteArray(timestamp));
            return CpcBuilderFactory.SKETCHES_GET_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Object sketchesGet(final byte[] key, final long timestamp) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.SKETCHESGET, key, toByteArray(timestamp));
            return CpcBuilderFactory.SKETCHES_GET_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Get the merge value of an array key range.
     *
     * @param key   the key
     * @return Success: merge of sum value; Fail: error.
     */
    public Object sketchesGetRangeMerge(final String key,  final long endtime, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.SKETCHESRANGEMERGE, SafeEncoder.encode(key), toByteArray(endtime), toByteArray(range));
            return CpcBuilderFactory.SKETCHES_GET_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Object sketchesGetRangeMerge(final byte[] key,  final long endtime, final long range) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.SKETCHESRANGEMERGE, key, toByteArray(endtime), toByteArray(range));
            return CpcBuilderFactory.SKETCHES_GET_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Get the values of an array sum key range.
     *
     * @param key   the key
     * @param starttime the starttime
     * @param endtime the endtime
     * @return Success: sum value list; Fail: error.
     */
    public List<Double> sketchesGetRange(final String key, final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.SKETCHESRANGE, SafeEncoder.encode(key), toByteArray(starttime), toByteArray(endtime));
            return CpcBuilderFactory.CPCARRAY_RANGE_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public List<Double> sketchesGetRange(final byte[] key, final long starttime, final long endtime) throws JedisConnectionException,
            IllegalArgumentException, JedisDataException {
        if (key == null) {
            throw new IllegalArgumentException(CommonResult.keyIsNull);
        }
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.SKETCHESRANGE, key, toByteArray(starttime), toByteArray(endtime));
            return CpcBuilderFactory.CPCARRAY_RANGE_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

}
