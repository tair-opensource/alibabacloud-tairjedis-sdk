package com.kvstore.jedis.tairgis;

import com.kvstore.jedis.ModuleCommand;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.JedisCluster;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author dwan
 * @date 2019/12/26
 */
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
}
