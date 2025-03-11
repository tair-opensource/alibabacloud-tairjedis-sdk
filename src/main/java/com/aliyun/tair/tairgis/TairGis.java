package com.aliyun.tair.tairgis;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.jedis3.Jedis3BuilderFactory;
import com.aliyun.tair.tairgis.factory.GisBuilderFactory;
import com.aliyun.tair.tairgis.params.GisParams;
import com.aliyun.tair.tairgis.params.GisSearchResponse;
import io.valkey.BuilderFactory;
import io.valkey.Jedis;
import io.valkey.JedisPool;
import io.valkey.Protocol;
import io.valkey.args.GeoUnit;
import io.valkey.util.SafeEncoder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TairGis {
    private Jedis jedis;
    private JedisPool jedisPool;

    public TairGis(Jedis jedis) {
        this.jedis = jedis;
    }

    public TairGis(JedisPool jedisPool) {
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
     * Add a polygon named polygonName in key.
     * @param key            the key
     * @param polygonName    the polygonName
     * @param polygonWktText the polygonWktText
     * example for polygonWktText: 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'
     * @return Success: 1; Cover old value: 0.
     */
    public Long gisadd(final String key, final String polygonName, final String polygonWktText) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.GISADD, key, polygonName, polygonWktText);
            return BuilderFactory.LONG.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Long gisadd(final byte[] key, final byte[] polygonName, final byte[] polygonWktText) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.GISADD, key, polygonName, polygonWktText);
            return BuilderFactory.LONG.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Get a polygon named polygonName in key.
     * @param key         the key
     * @param polygonName the polygonName
     * @return Success: polygonWktText; Not exist: null; Fail: error.
     */
    public String gisget(final String key, final String polygonName) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.GISGET, key, polygonName);
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public byte[] gisget(final byte[] key, final byte[] polygonName) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.GISGET, key, polygonName);
            return Jedis3BuilderFactory.BYTE_ARRAY.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Find a polygon named polygonName in key.
     * @param key          the key
     * @param pointWktText the pointWktText
     * @return Success: polygonWktText; Not find: null; Fail: error.
     */
    public Map<String, String> gissearch(final String key, final String pointWktText) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.GISSEARCH, key, pointWktText);
            List<Object> result = (List<Object>) obj;
            if (null == result || 0 == result.size()) {
                return new HashMap<String, String>();
            } else {
                List<byte[]> rawResults = (List) result.get(1);
                return (Map) BuilderFactory.STRING_MAP.build(rawResults);
            }
        } finally {
            releaseJedis(jedis);
        }
        
    }

    public Map<byte[], byte[]> gissearch(final byte[] key, final byte[] pointWktText) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.GISSEARCH, key, pointWktText);
            List<Object> result = (List<Object>) obj;
            if (null == result || 0 == result.size()) {
                return new HashMap<byte[], byte[]>();
            } else {
                List<byte[]> rawResults = (List) result.get(1);
                return (Map) Jedis3BuilderFactory.BYTE_ARRAY_MAP.build(rawResults);
            }
        } finally {
            releaseJedis(jedis);
        }
    }

    public List<GisSearchResponse> gissearch(final String key, final double longitude, final double latitude,
        final double radius, final GeoUnit unit, final GisParams gisParams) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.GISSEARCH,
                gisParams.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(GisParams.RADIUS),
                    Protocol.toByteArray(longitude), Protocol.toByteArray(latitude),
                    Protocol.toByteArray(radius), unit.getRaw()));
            return GisBuilderFactory.GISSEARCH_WITH_PARAMS_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public List<GisSearchResponse> gissearch(final byte[] key, final double longitude, final double latitude,
        final double radius, final GeoUnit unit, final GisParams gisParams) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.GISSEARCH,
                gisParams.getByteParams(key, SafeEncoder.encode(GisParams.RADIUS),
                    Protocol.toByteArray(longitude), Protocol.toByteArray(latitude),
                    Protocol.toByteArray(radius), unit.getRaw()));
            return GisBuilderFactory.GISSEARCH_WITH_PARAMS_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public List<GisSearchResponse> gissearchByMember(final String key, String member, final double radius,
        final GeoUnit unit, final GisParams gisParams) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.GISSEARCH,
                gisParams.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(GisParams.MEMBER),
                    SafeEncoder.encode(member), Protocol.toByteArray(radius), unit.getRaw()));
            return GisBuilderFactory.GISSEARCH_WITH_PARAMS_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public List<GisSearchResponse> gissearchByMember(final byte[] key, byte[] member, final double radius,
        final GeoUnit unit, final GisParams gisParams) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.GISSEARCH,
                gisParams.getByteParams(key, SafeEncoder.encode(GisParams.MEMBER), member,
                    Protocol.toByteArray(radius), unit.getRaw()));
            return GisBuilderFactory.GISSEARCH_WITH_PARAMS_RESULT.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Judge the contain relationship for the pointWktText (point or linestring or polygonname) and the key.
     * @param key          the key
     * @param pointWktText the pointWktText: <POINT/LINESTRING/POLYGONNAME>
     * @return Success: polygonWktText; Not find: null; Fail: error.
     */
    public Map<String, String> giscontains(final String key, final String pointWktText) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.GISCONTAINS, key, pointWktText);
            List<Object> result = (List<Object>) obj;
            if (null == result || 0 == result.size()) {
                return new HashMap<String, String>();
            } else {
                List<byte[]> rawResults = (List) result.get(1);
                return (Map) BuilderFactory.STRING_MAP.build(rawResults);
            }
        } finally {
            releaseJedis(jedis);
        }
    }

    public List<String> giscontains(final String key, final String pointWktText, final GisParams gisParams) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.GISCONTAINS,
                gisParams.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(pointWktText)));
            List<Object> result = (List<Object>) obj;
            if (null == result || 0 == result.size()) {
                return new ArrayList<String>();
            } else {
                List<byte[]> rawResults = (List) result.get(1);
                return BuilderFactory.STRING_LIST.build(rawResults);
            }
        } finally {
            releaseJedis(jedis);
        }
    }

    public Map<byte[], byte[]> giscontains(final byte[] key, final byte[] pointWktText) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.GISCONTAINS, key, pointWktText);
            List<Object> result = (List<Object>) obj;
            if (null == result || 0 == result.size()) {
                return new HashMap<byte[], byte[]>();
            } else {
                List<byte[]> rawResults = (List) result.get(1);
                return (Map) Jedis3BuilderFactory.BYTE_ARRAY_MAP.build(rawResults);
            }
        } finally {
            releaseJedis(jedis);
        }
    }

    public List<byte[]> giscontains(final byte[] key, final byte[] pointWktText, final GisParams gisParams) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.GISCONTAINS, gisParams.getByteParams(key, pointWktText));
            List<Object> result = (List<Object>) obj;
            if (null == result || 0 == result.size()) {
                return new ArrayList<byte[]>();
            } else {
                List<byte[]> rawResults = (List) result.get(1);
                return Jedis3BuilderFactory.BYTE_ARRAY_LIST.build(rawResults);
            }
        } finally {
            releaseJedis(jedis);
        }
    }

    public Map<String, String> giswithin(final String key, final String pointWktText) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.GISWITHIN, key, pointWktText);
            List<Object> result = (List<Object>) obj;
            if (null == result || 0 == result.size()) {
                return new HashMap<String, String>();
            } else {
                List<byte[]> rawResults = (List) result.get(1);
                return (Map) BuilderFactory.STRING_MAP.build(rawResults);
            }
        } finally {
            releaseJedis(jedis);
        }
    }

    public List<String> giswithin(final String key, final String pointWktText, final GisParams gisParams) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.GISWITHIN,
                gisParams.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(pointWktText)));
            List<Object> result = (List<Object>) obj;
            if (null == result || 0 == result.size()) {
                return new ArrayList<String>();
            } else {
                List<byte[]> rawResults = (List) result.get(1);
                return BuilderFactory.STRING_LIST.build(rawResults);
            }
        } finally {
            releaseJedis(jedis);
        }
    }

    public Map<byte[], byte[]> giswithin(final byte[] key, final byte[] pointWktText) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.GISWITHIN, key, pointWktText);
            List<Object> result = (List<Object>) obj;
            if (null == result || 0 == result.size()) {
                return new HashMap<byte[], byte[]>();
            } else {
                List<byte[]> rawResults = (List) result.get(1);
                return (Map) Jedis3BuilderFactory.BYTE_ARRAY_MAP.build(rawResults);
            }
        } finally {
            releaseJedis(jedis);
        }
    }

    public List<byte[]> giswithin(final byte[] key, final byte[] pointWktText, final GisParams gisParams) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.GISWITHIN, gisParams.getByteParams(key, pointWktText));
            List<Object> result = (List<Object>) obj;
            if (null == result || 0 == result.size()) {
                return new ArrayList<byte[]>();
            } else {
                List<byte[]> rawResults = (List) result.get(1);
                return Jedis3BuilderFactory.BYTE_ARRAY_LIST.build(rawResults);
            }
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Judge the intersect relationship for the pointWktText (point or linestring or polygonname) and the key.
     * @param key          the key
     * @param pointWktText the pointWktText: <POINT/LINESTRING/POLYGONNAME>
     * @return Success: polygonWktText; Not find: null; Fail: error.
     */
    public Map<String, String> gisintersects(final String key, final String pointWktText) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.GISINTERSECTS, key, pointWktText);
            List<Object> result =  (List<Object>) obj;
            if (null == result || 0 == result.size()) {
                return new HashMap<String, String>();
            } else {
                List<byte[]> rawResults = (List) result.get(1);
                return (Map) BuilderFactory.STRING_MAP.build(rawResults);
            }
        } finally {
            releaseJedis(jedis);
        }
    }

    public Map<byte[], byte[]> gisintersects(final byte[] key, final byte[] pointWktText) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.GISINTERSECTS, key, pointWktText);
            List<Object> result =  (List<Object>) obj;
            if (null == result || 0 == result.size()) {
                return new HashMap<byte[], byte[]>();
            } else {
                List<byte[]> rawResults = (List) result.get(1);
                return (Map) Jedis3BuilderFactory.BYTE_ARRAY_MAP.build(rawResults);
            }
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Delete a polygon named polygonName in key.
     * @param key         the key
     * @param polygonName the pointWktText
     * @return Success: OK; Not exist: null; Fail: error.
     */
    public String gisdel(final String key, final String polygonName) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.GISDEL, key, polygonName);
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public byte[] gisdel(final byte[] key, final byte[] polygonName) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.GISDEL, key, polygonName);
            return Jedis3BuilderFactory.BYTE_ARRAY.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Get all polygon in key
     * @param key the key
     * @return
     */
    public Map<String, String> gisgetall(final String key) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.GISGETALL, key);
            if (obj == null) {
                return new HashMap<>();
            }
            return BuilderFactory.STRING_MAP.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public List<String> gisgetall(final String key, final GisParams gisParams) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.GISGETALL,
                gisParams.getByteParams(SafeEncoder.encode(key)));
            if (obj == null) {
                return new ArrayList<>();
            }
            return BuilderFactory.STRING_LIST.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public Map<byte[], byte[]> gisgetall(final byte[] key) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.GISGETALL, key);
            if (obj == null) {
                return new HashMap<>();
            }
            return Jedis3BuilderFactory.BYTE_ARRAY_MAP.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public List<byte[]> gisgetall(final byte[] key, final GisParams gisParams) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.GISGETALL, gisParams.getByteParams(key));
            if (obj == null) {
                return new ArrayList<>();
            }
            return Jedis3BuilderFactory.BYTE_ARRAY_LIST.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

}
