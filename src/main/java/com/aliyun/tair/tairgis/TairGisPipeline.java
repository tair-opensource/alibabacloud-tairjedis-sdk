package com.aliyun.tair.tairgis;

import com.aliyun.tair.tairgis.factory.GisBuilderFactory;
import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairgis.params.GisParams;
import com.aliyun.tair.tairgis.params.GisSearchResponse;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.GeoUnit;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Response;
import redis.clients.jedis.util.SafeEncoder;

import java.util.List;
import java.util.Map;

public class TairGisPipeline extends Pipeline {

    public Response<Long> gisadd(final String key, final String polygonName, final String polygonWktText) {

        getClient("").sendCommand(ModuleCommand.GISADD, key, polygonName, polygonWktText);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> gisadd(final byte[] key, final byte[] polygonName, final byte[] polygonWktText) {

        getClient("").sendCommand(ModuleCommand.GISADD, key, polygonName, polygonWktText);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<String> gisget(final String key, final String polygonName) {
        getClient("").sendCommand(ModuleCommand.GISGET, key, polygonName);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<byte[]> gisget(final byte[] key, final byte[] polygonName) {
        getClient("").sendCommand(ModuleCommand.GISGET, key, polygonName);
        return getResponse(BuilderFactory.BYTE_ARRAY);
    }

    public Response<Map<String, String>> gissearch(final String key, final String pointWktText) {
        getClient("").sendCommand(ModuleCommand.GISSEARCH, key, pointWktText);
        return getResponse(GisBuilderFactory.GISSEARCH_RESULT_MAP_STRING);
    }

    public Response<Map<byte[], byte[]>> gissearch(final byte[] key, final byte[] pointWktText) {
        getClient("").sendCommand(ModuleCommand.GISSEARCH, key, pointWktText);
        return getResponse(GisBuilderFactory.GISSEARCH_RESULT_MAP_BYTE);
    }

    public Response<List<GisSearchResponse>> gissearch(final String key, final double longitude, final double latitude,
        final double radius, final GeoUnit unit, final GisParams gisParams) {
        getClient("").sendCommand(ModuleCommand.GISSEARCH,
            gisParams.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(GisParams.RADIUS),
                Protocol.toByteArray(longitude), Protocol.toByteArray(latitude),
                Protocol.toByteArray(radius), unit.raw));
        return getResponse(GisBuilderFactory.GISSEARCH_WITH_PARAMS_RESULT);
    }

    public Response<List<GisSearchResponse>> gissearch(final byte[] key, final double longitude, final double latitude,
        final double radius, final GeoUnit unit, final GisParams gisParams) {
        getClient("").sendCommand(ModuleCommand.GISSEARCH,
            gisParams.getByteParams(key, SafeEncoder.encode(GisParams.RADIUS),
                Protocol.toByteArray(longitude), Protocol.toByteArray(latitude),
                Protocol.toByteArray(radius), unit.raw));
        return getResponse(GisBuilderFactory.GISSEARCH_WITH_PARAMS_RESULT);
    }

    public Response<List<GisSearchResponse>> gissearchByMember(final String key, String member, final double radius,
        final GeoUnit unit, final GisParams gisParams) {
        getClient("").sendCommand(ModuleCommand.GISSEARCH,
            gisParams.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(GisParams.MEMBER),
                SafeEncoder.encode(member), Protocol.toByteArray(radius), unit.raw));
        return getResponse(GisBuilderFactory.GISSEARCH_WITH_PARAMS_RESULT);
    }

    public Response<List<GisSearchResponse>> gissearchByMember(final byte[] key, byte[] member, final double radius,
        final GeoUnit unit, final GisParams gisParams) {
        getClient("").sendCommand(ModuleCommand.GISSEARCH,
            gisParams.getByteParams(key, SafeEncoder.encode(GisParams.MEMBER),
                member, Protocol.toByteArray(radius), unit.raw));
        return getResponse(GisBuilderFactory.GISSEARCH_WITH_PARAMS_RESULT);
    }

    public Response<Map<String, String>> giscontains(final String key, final String pointWktText) {
        getClient("").sendCommand(ModuleCommand.GISCONTAINS, key, pointWktText);
        return getResponse(GisBuilderFactory.GISSEARCH_RESULT_MAP_STRING);
    }

    public Response<List<String>> giscontains(final String key, final String pointWktText, final GisParams gisParams) {
        getClient("").sendCommand(ModuleCommand.GISCONTAINS,
            gisParams.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(pointWktText)));
        return getResponse(GisBuilderFactory.GISSEARCH_RESULT_LIST_STRING);
    }

    public Response<Map<byte[], byte[]>> giscontains(final byte[] key, final byte[] pointWktText) {
        getClient("").sendCommand(ModuleCommand.GISCONTAINS, key, pointWktText);
        return getResponse(GisBuilderFactory.GISSEARCH_RESULT_MAP_BYTE);
    }

    public Response<List<byte[]>> giscontains(final byte[] key, final byte[] pointWktText, final GisParams gisParams) {
        getClient("").sendCommand(ModuleCommand.GISCONTAINS, gisParams.getByteParams(key, pointWktText));
        return getResponse(GisBuilderFactory.GISSEARCH_RESULT_BYTE_ARRAY_LIST);
    }

    public Response<Map<String, String>> giswithin(final String key, final String pointWktText) {
        getClient("").sendCommand(ModuleCommand.GISWITHIN, key, pointWktText);
        return getResponse(GisBuilderFactory.GISSEARCH_RESULT_MAP_STRING);
    }

    public Response<List<String>> giswithin(final String key, final String pointWktText, final GisParams gisParams) {
        getClient("").sendCommand(ModuleCommand.GISWITHIN,
            gisParams.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(pointWktText)));
        return getResponse(GisBuilderFactory.GISSEARCH_RESULT_LIST_STRING);
    }

    public Response<Map<byte[], byte[]>> giswithin(final byte[] key, final byte[] pointWktText) {
        getClient("").sendCommand(ModuleCommand.GISWITHIN, key, pointWktText);
        return getResponse(GisBuilderFactory.GISSEARCH_RESULT_MAP_BYTE);
    }

    public Response<List<byte[]>> giswithin(final byte[] key, final byte[] pointWktText, final GisParams gisParams) {
        getClient("").sendCommand(ModuleCommand.GISWITHIN, gisParams.getByteParams(key, pointWktText));
        return getResponse(GisBuilderFactory.GISSEARCH_RESULT_BYTE_ARRAY_LIST);
    }

    public Response<Map<String, String>> gisintersects(final String key, final String pointWktText) {
        getClient("").sendCommand(ModuleCommand.GISINTERSECTS, key, pointWktText);
        return getResponse(GisBuilderFactory.GISSEARCH_RESULT_MAP_STRING);
    }

    public Response<Map<byte[], byte[]>> gisintersects(final byte[] key, final byte[] pointWktText) {
        getClient("").sendCommand(ModuleCommand.GISINTERSECTS, key, pointWktText);
        return getResponse(GisBuilderFactory.GISSEARCH_RESULT_MAP_BYTE);
    }

    public Response<String> gisdel(final String key, final String polygonName) {
        getClient("").sendCommand(ModuleCommand.GISDEL, key, polygonName);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<byte[]> gisdel(final byte[] key, final byte[] polygonName) {
        getClient("").sendCommand(ModuleCommand.GISDEL, key, polygonName);
        return getResponse(BuilderFactory.BYTE_ARRAY);
    }

    public Response<Map<String, String>> gisgetall(final String key) {
        getClient("").sendCommand(ModuleCommand.GISGETALL, key);
        return getResponse(BuilderFactory.STRING_MAP);
    }

    public Response<List<String>> gisgetall(final String key, final GisParams gisParams) {
        getClient("").sendCommand(ModuleCommand.GISGETALL,
            gisParams.getByteParams(SafeEncoder.encode(key)));
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<Map<byte[], byte[]>> gisgetall(final byte[] key) {
        getClient("").sendCommand(ModuleCommand.GISGETALL, key);
        return getResponse(BuilderFactory.BYTE_ARRAY_MAP);
    }

    public Response<List<byte[]>> gisgetall(final byte[] key, final GisParams gisParams) {
        getClient("").sendCommand(ModuleCommand.GISGETALL, gisParams.getByteParams(key));
        return getResponse(BuilderFactory.BYTE_ARRAY_LIST);
    }

}
