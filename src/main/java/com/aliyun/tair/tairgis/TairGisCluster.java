package com.aliyun.tair.tairgis;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.jedis3.Jedis3BuilderFactory;
import com.aliyun.tair.tairgis.factory.GisBuilderFactory;
import com.aliyun.tair.tairgis.params.GisParams;
import com.aliyun.tair.tairgis.params.GisSearchResponse;
import io.valkey.BuilderFactory;
import io.valkey.JedisCluster;
import io.valkey.Protocol;
import io.valkey.args.GeoUnit;
import io.valkey.util.SafeEncoder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TairGisCluster {
    private JedisCluster jc;

    public TairGisCluster(JedisCluster jc) {
        this.jc = jc;
    }

    public Long gisadd(final String key, final String polygonName, final String polygonWktText) {

        Object obj = jc.sendCommand(key, ModuleCommand.GISADD, key, polygonName, polygonWktText);
        return BuilderFactory.LONG.build(obj);
    }

    public Long gisadd(final byte[] key, final byte[] polygonName, final byte[] polygonWktText) {

        Object obj = jc.sendCommand(key, ModuleCommand.GISADD, key, polygonName, polygonWktText);
        return BuilderFactory.LONG.build(obj);
    }

    public String gisget(final String key, final String polygonName) {
        Object obj = jc.sendCommand(key, ModuleCommand.GISGET, key, polygonName);
        return BuilderFactory.STRING.build(obj);
    }

    public byte[] gisget(final byte[] key, final byte[] polygonName) {
        Object obj = jc.sendCommand(key, ModuleCommand.GISGET, key, polygonName);
        return Jedis3BuilderFactory.BYTE_ARRAY.build(obj);
    }

    public Map<String, String> gissearch(final String key, final String pointWktText) {
        Object obj = jc.sendCommand(key, ModuleCommand.GISSEARCH, key, pointWktText);
        List<Object> result = (List<Object>) obj;
        if (null == result || 0 == result.size()) {
            return new HashMap<String, String>();
        } else {
            List<byte[]> rawResults = (List) result.get(1);
            return (Map) BuilderFactory.STRING_MAP.build(rawResults);
        }
    }

    public Map<byte[], byte[]> gissearch(final byte[] key, final byte[] pointWktText) {
        Object obj = jc.sendCommand(key, ModuleCommand.GISSEARCH, key, pointWktText);
        List<Object> result = (List<Object>) obj;
        if (null == result || 0 == result.size()) {
            return new HashMap<byte[], byte[]>();
        } else {
            List<byte[]> rawResults = (List) result.get(1);
            return (Map) Jedis3BuilderFactory.BYTE_ARRAY_MAP.build(rawResults);
        }
    }

    public List<GisSearchResponse> gissearch(final String key, final double longitude, final double latitude,
        final double radius, final GeoUnit unit, final GisParams gisParams) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.GISSEARCH,
            gisParams.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(GisParams.RADIUS),
                Protocol.toByteArray(longitude), Protocol.toByteArray(latitude),
                Protocol.toByteArray(radius), unit.getRaw()));
        return GisBuilderFactory.GISSEARCH_WITH_PARAMS_RESULT.build(obj);
    }

    public List<GisSearchResponse> gissearch(final byte[] key, final double longitude, final double latitude,
        final double radius, final GeoUnit unit, final GisParams gisParams) {
        Object obj = jc.sendCommand(key, ModuleCommand.GISSEARCH,
            gisParams.getByteParams(key, SafeEncoder.encode(GisParams.RADIUS),
                Protocol.toByteArray(longitude), Protocol.toByteArray(latitude),
                Protocol.toByteArray(radius), unit.getRaw()));
        return GisBuilderFactory.GISSEARCH_WITH_PARAMS_RESULT.build(obj);
    }

    public List<GisSearchResponse> gissearchByMember(final String key, String member, final double radius,
        final GeoUnit unit, final GisParams gisParams) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.GISSEARCH,
            gisParams.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(GisParams.MEMBER),
                SafeEncoder.encode(member), Protocol.toByteArray(radius), unit.getRaw()));
        return GisBuilderFactory.GISSEARCH_WITH_PARAMS_RESULT.build(obj);
    }

    public List<GisSearchResponse> gissearchByMember(final byte[] key, byte[] member, final double radius,
        final GeoUnit unit, final GisParams gisParams) {
        Object obj = jc.sendCommand(key, ModuleCommand.GISSEARCH,
            gisParams.getByteParams(key, SafeEncoder.encode(GisParams.MEMBER), member,
                Protocol.toByteArray(radius), unit.getRaw()));
        return GisBuilderFactory.GISSEARCH_WITH_PARAMS_RESULT.build(obj);
    }

    public Map<String, String> giscontains(final String key, final String pointWktText) {
        Object obj = jc.sendCommand(key, ModuleCommand.GISCONTAINS, key, pointWktText);
        List<Object> result = (List<Object>) obj;
        if (null == result || 0 == result.size()) {
            return new HashMap<String, String>();
        } else {
            List<byte[]> rawResults = (List) result.get(1);
            return (Map) BuilderFactory.STRING_MAP.build(rawResults);
        }
    }

    public List<String> giscontains(final String key, final String pointWktText, final GisParams gisParams) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.GISCONTAINS,
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
        Object obj = jc.sendCommand(key, ModuleCommand.GISCONTAINS, key, pointWktText);
        List<Object> result = (List<Object>) obj;
        if (null == result || 0 == result.size()) {
            return new HashMap<byte[], byte[]>();
        } else {
            List<byte[]> rawResults = (List) result.get(1);
            return (Map) Jedis3BuilderFactory.BYTE_ARRAY_MAP.build(rawResults);
        }
    }

    public List<byte[]> giscontains(final byte[] key, final byte[] pointWktText, final GisParams gisParams) {
        Object obj = jc.sendCommand(key, ModuleCommand.GISCONTAINS, gisParams.getByteParams(key, pointWktText));
        List<Object> result = (List<Object>) obj;
        if (null == result || 0 == result.size()) {
            return new ArrayList<byte[]>();
        } else {
            List<byte[]> rawResults = (List) result.get(1);
            return Jedis3BuilderFactory.BYTE_ARRAY_LIST.build(rawResults);
        }
    }

    public Map<String, String> giswithin(final String key, final String pointWktText) {
        Object obj = jc.sendCommand(key, ModuleCommand.GISWITHIN, key, pointWktText);
        List<Object> result = (List<Object>) obj;
        if (null == result || 0 == result.size()) {
            return new HashMap<String, String>();
        } else {
            List<byte[]> rawResults = (List) result.get(1);
            return (Map) BuilderFactory.STRING_MAP.build(rawResults);
        }
    }

    public List<String> giswithin(final String key, final String pointWktText, final GisParams gisParams) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.GISWITHIN,
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
        Object obj = jc.sendCommand(key, ModuleCommand.GISWITHIN, key, pointWktText);
        List<Object> result = (List<Object>) obj;
        if (null == result || 0 == result.size()) {
            return new HashMap<byte[], byte[]>();
        } else {
            List<byte[]> rawResults = (List) result.get(1);
            return (Map) Jedis3BuilderFactory.BYTE_ARRAY_MAP.build(rawResults);
        }
    }

    public List<byte[]> giswithin(final byte[] key, final byte[] pointWktText, final GisParams gisParams) {
        Object obj = jc.sendCommand(key, ModuleCommand.GISWITHIN, gisParams.getByteParams(key, pointWktText));
        List<Object> result = (List<Object>) obj;
        if (null == result || 0 == result.size()) {
            return new ArrayList<byte[]>();
        } else {
            List<byte[]> rawResults = (List) result.get(1);
            return Jedis3BuilderFactory.BYTE_ARRAY_LIST.build(rawResults);
        }
    }

    public Map<String, String> gisintersects(final String key, final String pointWktText) {
        Object obj = jc.sendCommand(key, ModuleCommand.GISINTERSECTS, key, pointWktText);
        List<Object> result =  (List<Object>) obj;
        if (null == result || 0 == result.size()) {
            return new HashMap<String, String>();
        } else {
            List<byte[]> rawResults = (List) result.get(1);
            return (Map) BuilderFactory.STRING_MAP.build(rawResults);
        }
    }

    public Map<byte[], byte[]> gisintersects(final byte[] key, final byte[] pointWktText) {
        Object obj = jc.sendCommand(key, ModuleCommand.GISINTERSECTS, key, pointWktText);
        List<Object> result =  (List<Object>) obj;
        if (null == result || 0 == result.size()) {
            return new HashMap<byte[], byte[]>();
        } else {
            List<byte[]> rawResults = (List) result.get(1);
            return (Map) Jedis3BuilderFactory.BYTE_ARRAY_MAP.build(rawResults);
        }
    }

    public String gisdel(final String key, final String polygonName) {
        Object obj = jc.sendCommand(key, ModuleCommand.GISDEL, key, polygonName);
        return BuilderFactory.STRING.build(obj);
    }

    public byte[] gisdel(final byte[] key, final byte[] polygonName) {
        Object obj = jc.sendCommand(key, ModuleCommand.GISDEL, key, polygonName);
        return Jedis3BuilderFactory.BYTE_ARRAY.build(obj);
    }

    public Map<String, String> gisgetall(final String key) {
        Object obj = jc.sendCommand(key, ModuleCommand.GISGETALL, key);
        return BuilderFactory.STRING_MAP.build(obj);
    }

    public List<String> gisgetall(final String key, final GisParams gisParams) {
        Object obj = jc.sendCommand(SafeEncoder.encode(key), ModuleCommand.GISGETALL,
            gisParams.getByteParams(SafeEncoder.encode(key)));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public Map<byte[], byte[]> gisgetall(final byte[] key) {
        Object obj = jc.sendCommand(key, ModuleCommand.GISGETALL, key);
        return Jedis3BuilderFactory.BYTE_ARRAY_MAP.build(obj);
    }

    public List<byte[]> gisgetall(final byte[] key, final GisParams gisParams) {
        Object obj =jc.sendCommand(key, ModuleCommand.GISGETALL, gisParams.getByteParams(key));
        return Jedis3BuilderFactory.BYTE_ARRAY_LIST.build(obj);
    }
}
