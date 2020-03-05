package com.aliyun.tair.tairgis;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairgis.params.GisParams;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.util.SafeEncoder;

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
        return BuilderFactory.BYTE_ARRAY.build(obj);
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
            return (Map) BuilderFactory.BYTE_ARRAY_MAP.build(rawResults);
        }
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
            return (Map) BuilderFactory.BYTE_ARRAY_MAP.build(rawResults);
        }
    }

    public List<byte[]> giscontains(final byte[] key, final byte[] pointWktText, final GisParams gisParams) {
        Object obj = jc.sendCommand(key, ModuleCommand.GISCONTAINS, gisParams.getByteParams(key, pointWktText));
        List<Object> result = (List<Object>) obj;
        if (null == result || 0 == result.size()) {
            return new ArrayList<byte[]>();
        } else {
            List<byte[]> rawResults = (List) result.get(1);
            return BuilderFactory.BYTE_ARRAY_LIST.build(rawResults);
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
            return (Map) BuilderFactory.BYTE_ARRAY_MAP.build(rawResults);
        }
    }

    public String gisdel(final String key, final String polygonName) {
        Object obj = jc.sendCommand(key, ModuleCommand.GISDEL, key, polygonName);
        return BuilderFactory.STRING.build(obj);
    }

    public byte[] gisdel(final byte[] key, final byte[] polygonName) {
        Object obj = jc.sendCommand(key, ModuleCommand.GISDEL, key, polygonName);
        return BuilderFactory.BYTE_ARRAY.build(obj);
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
        return BuilderFactory.BYTE_ARRAY_MAP.build(obj);
    }

    public List<byte[]> gisgetall(final byte[] key, final GisParams gisParams) {
        Object obj =jc.sendCommand(key, ModuleCommand.GISGETALL, gisParams.getByteParams(key));
        return BuilderFactory.BYTE_ARRAY_LIST.build(obj);
    }
}
