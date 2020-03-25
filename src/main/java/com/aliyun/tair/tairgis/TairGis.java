package com.aliyun.tair.tairgis;

import com.aliyun.tair.ModuleCommand;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TairGis {
    private Jedis jedis;

    public TairGis(Jedis jedis) {
        this.jedis = jedis;
    }

    private Jedis getJedis() {
        return jedis;
    }

    /**
     * Add a polygon named polygonName in key.
     *
     * @param key            the key
     * @param polygonName    the polygonName
     * @param polygonWktText the polygonWktText
     *                       example for polygonWktText: 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'
     * @return Success: 1; Cover old value: 0.
     */
    public Long gisadd(final String key, final String polygonName, final String polygonWktText) {

        Object obj = getJedis().sendCommand(ModuleCommand.GISADD, key, polygonName, polygonWktText);
        return BuilderFactory.LONG.build(obj);
    }

    public Long gisadd(final byte[] key, final byte[] polygonName, final byte[] polygonWktText) {

        Object obj = getJedis().sendCommand(ModuleCommand.GISADD, key, polygonName, polygonWktText);
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * Get a polygon named polygonName in key.
     *
     * @param key         the key
     * @param polygonName the polygonName
     * @return Success: polygonWktText; Not exist: null; Fail: error.
     */
    public String gisget(final String key, final String polygonName) {
        Object obj = getJedis().sendCommand(ModuleCommand.GISGET, key, polygonName);
        return BuilderFactory.STRING.build(obj);
    }

    public byte[] gisget(final byte[] key, final byte[] polygonName) {
        Object obj = getJedis().sendCommand(ModuleCommand.GISGET, key, polygonName);
        return BuilderFactory.BYTE_ARRAY.build(obj);
    }

    /**
     * Find a polygon named polygonName in key.
     *
     * @param key          the key
     * @param pointWktText the pointWktText
     * @return Success: polygonWktText; Not find: null; Fail: error.
     */
    public Map<String, String> gissearch(final String key, final String pointWktText) {
        Object obj = getJedis().sendCommand(ModuleCommand.GISSEARCH, key, pointWktText);
        List<Object> result = (List<Object>)obj;
        if (null == result || 0 == result.size()) {
            return new HashMap<String, String>();
        } else {
            List<byte[]> rawResults = (List)result.get(1);
            return (Map)BuilderFactory.STRING_MAP.build(rawResults);
        }
    }

    public Map<byte[], byte[]> gissearch(final byte[] key, final byte[] pointWktText) {
        Object obj = getJedis().sendCommand(ModuleCommand.GISSEARCH, key, pointWktText);
        List<Object> result = (List<Object>)obj;
        if (null == result || 0 == result.size()) {
            return new HashMap<byte[], byte[]>();
        } else {
            List<byte[]> rawResults = (List)result.get(1);
            return (Map)BuilderFactory.BYTE_ARRAY_MAP.build(rawResults);
        }
    }

    /**
     * Judge the contain relationship for the pointWktText (point or linestring or polygonname) and the key.
     *
     * @param key          the key
     * @param pointWktText the pointWktText: POINT/LINESTRING/POLYGONNAME
     * @return Success: polygonWktText; Not find: null; Fail: error.
     */
    public Map<String, String> giscontains(final String key, final String pointWktText) {
        Object obj = getJedis().sendCommand(ModuleCommand.GISCONTAINS, key, pointWktText);
        List<Object> result = (List<Object>)obj;
        if (null == result || 0 == result.size()) {
            return new HashMap<String, String>();
        } else {
            List<byte[]> rawResults = (List)result.get(1);
            return (Map)BuilderFactory.STRING_MAP.build(rawResults);
        }
    }

    public Map<byte[], byte[]> giscontains(final byte[] key, final byte[] pointWktText) {
        Object obj = getJedis().sendCommand(ModuleCommand.GISCONTAINS, key, pointWktText);
        List<Object> result = (List<Object>)obj;
        if (null == result || 0 == result.size()) {
            return new HashMap<byte[], byte[]>();
        } else {
            List<byte[]> rawResults = (List)result.get(1);
            return (Map)BuilderFactory.BYTE_ARRAY_MAP.build(rawResults);
        }
    }

    /**
     * Judge the intersect relationship for the pointWktText (point or linestring or polygonname) and the key.
     *
     * @param key          the key
     * @param pointWktText the pointWktText: POINT/LINESTRING/POLYGONNAME
     * @return Success: polygonWktText; Not find: null; Fail: error.
     */
    public Map<String, String> gisintersects(final String key, final String pointWktText) {
        Object obj = getJedis().sendCommand(ModuleCommand.GISINTERSECTS, key, pointWktText);
        List<Object> result = (List<Object>)obj;
        if (null == result || 0 == result.size()) {
            return new HashMap<String, String>();
        } else {
            List<byte[]> rawResults = (List)result.get(1);
            return (Map)BuilderFactory.STRING_MAP.build(rawResults);
        }
    }

    public Map<byte[], byte[]> gisintersects(final byte[] key, final byte[] pointWktText) {
        Object obj = getJedis().sendCommand(ModuleCommand.GISINTERSECTS, key, pointWktText);
        List<Object> result = (List<Object>)obj;
        if (null == result || 0 == result.size()) {
            return new HashMap<byte[], byte[]>();
        } else {
            List<byte[]> rawResults = (List)result.get(1);
            return (Map)BuilderFactory.BYTE_ARRAY_MAP.build(rawResults);
        }
    }

    /**
     * Delete a polygon named polygonName in key.
     *
     * @param key         the key
     * @param polygonName the pointWktText
     * @return Success: OK; Not exist: null; Fail: error.
     */
    public String gisdel(final String key, final String polygonName) {
        Object obj = getJedis().sendCommand(ModuleCommand.GISDEL, key, polygonName);
        return BuilderFactory.STRING.build(obj);
    }

    public byte[] gisdel(final byte[] key, final byte[] polygonName) {
        Object obj = getJedis().sendCommand(ModuleCommand.GISDEL, key, polygonName);
        return BuilderFactory.BYTE_ARRAY.build(obj);
    }
}
