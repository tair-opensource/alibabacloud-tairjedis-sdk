package com.aliyun.tair.tairgis;

import com.aliyun.tair.tairgis.factory.GisBuilderFactory;
import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairgis.params.GisParams;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Pipeline;
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
