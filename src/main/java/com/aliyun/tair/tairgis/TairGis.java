package com.aliyun.tair.tairgis;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairgis.factory.GisBuilderFactory;
import com.aliyun.tair.tairgis.params.GisParams;
import com.aliyun.tair.tairgis.params.GisSearchResponse;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.GeoUnit;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;
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
     * @param key            the key
     * @param polygonName    the polygonName
     * @param polygonWktText the polygonWktText
     * example for polygonWktText: 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'
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
     * @param key          the key
     * @param pointWktText the pointWktText
     * @return Success: polygonWktText; Not find: null; Fail: error.
     */
    public Map<String, String> gissearch(final String key, final String pointWktText) {
        Object obj = getJedis().sendCommand(ModuleCommand.GISSEARCH, key, pointWktText);
        List<Object> result = (List<Object>) obj;
        if (null == result || 0 == result.size()) {
            return new HashMap<String, String>();
        } else {
            List<byte[]> rawResults = (List) result.get(1);
            return (Map) BuilderFactory.STRING_MAP.build(rawResults);
        }
    }

    public Map<byte[], byte[]> gissearch(final byte[] key, final byte[] pointWktText) {
        Object obj = getJedis().sendCommand(ModuleCommand.GISSEARCH, key, pointWktText);
        List<Object> result = (List<Object>) obj;
        if (null == result || 0 == result.size()) {
            return new HashMap<byte[], byte[]>();
        } else {
            List<byte[]> rawResults = (List) result.get(1);
            return (Map) BuilderFactory.BYTE_ARRAY_MAP.build(rawResults);
        }
    }

    public List<GisSearchResponse> gissearch(final String key, final double longitude, final double latitude,
        final double radius, final GeoUnit unit, final GisParams gisParams) {
        Object obj = getJedis().sendCommand(ModuleCommand.GISSEARCH,
            gisParams.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(GisParams.RADIUS),
                Protocol.toByteArray(longitude), Protocol.toByteArray(latitude),
                Protocol.toByteArray(radius), unit.raw));
        return GisBuilderFactory.GISSEARCH_WITH_PARAMS_RESULT.build(obj);
    }

    public List<GisSearchResponse> gissearch(final byte[] key, final double longitude, final double latitude,
        final double radius, final GeoUnit unit, final GisParams gisParams) {
        Object obj = getJedis().sendCommand(ModuleCommand.GISSEARCH,
            gisParams.getByteParams(key, SafeEncoder.encode(GisParams.RADIUS),
                Protocol.toByteArray(longitude), Protocol.toByteArray(latitude),
                Protocol.toByteArray(radius), unit.raw));
        return GisBuilderFactory.GISSEARCH_WITH_PARAMS_RESULT.build(obj);
    }

    public List<GisSearchResponse> gissearchByMember(final String key, String member, final double radius,
        final GeoUnit unit, final GisParams gisParams) {
        Object obj = getJedis().sendCommand(ModuleCommand.GISSEARCH,
            gisParams.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(GisParams.MEMBER),
                SafeEncoder.encode(member), Protocol.toByteArray(radius), unit.raw));
        return GisBuilderFactory.GISSEARCH_WITH_PARAMS_RESULT.build(obj);
    }

    public List<GisSearchResponse> gissearchByMember(final byte[] key, byte[] member, final double radius,
        final GeoUnit unit, final GisParams gisParams) {
        Object obj = getJedis().sendCommand(ModuleCommand.GISSEARCH,
            gisParams.getByteParams(key, SafeEncoder.encode(GisParams.MEMBER), member,
                Protocol.toByteArray(radius), unit.raw));
        return GisBuilderFactory.GISSEARCH_WITH_PARAMS_RESULT.build(obj);
    }

    /**
     * Judge the contain relationship for the pointWktText (point or linestring or polygonname) and the key.
     * @param key          the key
     * @param pointWktText the pointWktText: <POINT/LINESTRING/POLYGONNAME>
     * @return Success: polygonWktText; Not find: null; Fail: error.
     */
    public Map<String, String> giscontains(final String key, final String pointWktText) {
        Object obj = getJedis().sendCommand(ModuleCommand.GISCONTAINS, key, pointWktText);
        List<Object> result = (List<Object>) obj;
        if (null == result || 0 == result.size()) {
            return new HashMap<String, String>();
        } else {
            List<byte[]> rawResults = (List) result.get(1);
            return (Map) BuilderFactory.STRING_MAP.build(rawResults);
        }
    }

    public List<String> giscontains(final String key, final String pointWktText, final GisParams gisParams) {
        Object obj = getJedis().sendCommand(ModuleCommand.GISCONTAINS,
            gisParams.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(pointWktText)));
        List<Object> result = (List<Object>) obj;
        if (null == result || 0 == result.size()) {
            return new ArrayList<String>();
        } else {
            List<byte[]> rawResults = (List) result.get(1);
            return BuilderFactory.STRING_LIST.build(rawResults);
        }
    }

    public Map<byte[], byte[]> giscontains(final byte[] key, final byte[] pointWktText) {
        Object obj = getJedis().sendCommand(ModuleCommand.GISCONTAINS, key, pointWktText);
        List<Object> result = (List<Object>) obj;
        if (null == result || 0 == result.size()) {
            return new HashMap<byte[], byte[]>();
        } else {
            List<byte[]> rawResults = (List) result.get(1);
            return (Map) BuilderFactory.BYTE_ARRAY_MAP.build(rawResults);
        }
    }

    public List<byte[]> giscontains(final byte[] key, final byte[] pointWktText, final GisParams gisParams) {
        Object obj = getJedis().sendCommand(ModuleCommand.GISCONTAINS, gisParams.getByteParams(key, pointWktText));
        List<Object> result = (List<Object>) obj;
        if (null == result || 0 == result.size()) {
            return new ArrayList<byte[]>();
        } else {
            List<byte[]> rawResults = (List) result.get(1);
            return BuilderFactory.BYTE_ARRAY_LIST.build(rawResults);
        }
    }

    public Map<String, String> giswithin(final String key, final String pointWktText) {
        Object obj = getJedis().sendCommand(ModuleCommand.GISWITHIN, key, pointWktText);
        List<Object> result = (List<Object>) obj;
        if (null == result || 0 == result.size()) {
            return new HashMap<String, String>();
        } else {
            List<byte[]> rawResults = (List) result.get(1);
            return (Map) BuilderFactory.STRING_MAP.build(rawResults);
        }
    }

    public List<String> giswithin(final String key, final String pointWktText, final GisParams gisParams) {
        Object obj = getJedis().sendCommand(ModuleCommand.GISWITHIN,
            gisParams.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(pointWktText)));
        List<Object> result = (List<Object>) obj;
        if (null == result || 0 == result.size()) {
            return new ArrayList<String>();
        } else {
            List<byte[]> rawResults = (List) result.get(1);
            return BuilderFactory.STRING_LIST.build(rawResults);
        }
    }

    public Map<byte[], byte[]> giswithin(final byte[] key, final byte[] pointWktText) {
        Object obj = getJedis().sendCommand(ModuleCommand.GISWITHIN, key, pointWktText);
        List<Object> result = (List<Object>) obj;
        if (null == result || 0 == result.size()) {
            return new HashMap<byte[], byte[]>();
        } else {
            List<byte[]> rawResults = (List) result.get(1);
            return (Map) BuilderFactory.BYTE_ARRAY_MAP.build(rawResults);
        }
    }

    public List<byte[]> giswithin(final byte[] key, final byte[] pointWktText, final GisParams gisParams) {
        Object obj = getJedis().sendCommand(ModuleCommand.GISWITHIN, gisParams.getByteParams(key, pointWktText));
        List<Object> result = (List<Object>) obj;
        if (null == result || 0 == result.size()) {
            return new ArrayList<byte[]>();
        } else {
            List<byte[]> rawResults = (List) result.get(1);
            return BuilderFactory.BYTE_ARRAY_LIST.build(rawResults);
        }
    }

    /**
     * Judge the intersect relationship for the pointWktText (point or linestring or polygonname) and the key.
     * @param key          the key
     * @param pointWktText the pointWktText: <POINT/LINESTRING/POLYGONNAME>
     * @return Success: polygonWktText; Not find: null; Fail: error.
     */
    public Map<String, String> gisintersects(final String key, final String pointWktText) {
        Object obj = getJedis().sendCommand(ModuleCommand.GISINTERSECTS, key, pointWktText);
        List<Object> result =  (List<Object>) obj;
        if (null == result || 0 == result.size()) {
            return new HashMap<String, String>();
        } else {
            List<byte[]> rawResults = (List) result.get(1);
            return (Map) BuilderFactory.STRING_MAP.build(rawResults);
        }
    }

    public Map<byte[], byte[]> gisintersects(final byte[] key, final byte[] pointWktText) {
        Object obj = getJedis().sendCommand(ModuleCommand.GISINTERSECTS, key, pointWktText);
        List<Object> result =  (List<Object>) obj;
        if (null == result || 0 == result.size()) {
            return new HashMap<byte[], byte[]>();
        } else {
            List<byte[]> rawResults = (List) result.get(1);
            return (Map) BuilderFactory.BYTE_ARRAY_MAP.build(rawResults);
        }
    }

    /**
     * Delete a polygon named polygonName in key.
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

    /**
     * Get all polygon in key
     * @param key the key
     * @return
     */
    public Map<String, String> gisgetall(final String key) {
        Object obj = getJedis().sendCommand(ModuleCommand.GISGETALL, key);
        if (obj == null) {
            return new HashMap<>();
        }
        return BuilderFactory.STRING_MAP.build(obj);
    }

    public List<String> gisgetall(final String key, final GisParams gisParams) {
        Object obj = getJedis().sendCommand(ModuleCommand.GISGETALL,
            gisParams.getByteParams(SafeEncoder.encode(key)));
        if (obj == null) {
            return new ArrayList<>();
        }
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public Map<byte[], byte[]> gisgetall(final byte[] key) {
        Object obj = getJedis().sendCommand(ModuleCommand.GISGETALL, key);
        if (obj == null) {
            return new HashMap<>();
        }
        return BuilderFactory.BYTE_ARRAY_MAP.build(obj);
    }

    public List<byte[]> gisgetall(final byte[] key, final GisParams gisParams) {
        Object obj = getJedis().sendCommand(ModuleCommand.GISGETALL, gisParams.getByteParams(key));
        if (obj == null) {
            return new ArrayList<>();
        }
        return BuilderFactory.BYTE_ARRAY_LIST.build(obj);
    }

}
