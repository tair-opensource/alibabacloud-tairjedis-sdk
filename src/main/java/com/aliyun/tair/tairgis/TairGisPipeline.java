package com.aliyun.tair.tairgis;

import com.aliyun.tair.tairgis.factory.GisBuilderFactory;
import com.aliyun.tair.ModuleCommand;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

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

    public Response<Map<byte[], byte[]>> giscontains(final byte[] key, final byte[] pointWktText) {
        getClient("").sendCommand(ModuleCommand.GISCONTAINS, key, pointWktText);
        return getResponse(GisBuilderFactory.GISSEARCH_RESULT_MAP_BYTE);
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
}
