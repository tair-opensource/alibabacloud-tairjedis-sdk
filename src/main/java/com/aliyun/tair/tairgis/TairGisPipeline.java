package com.aliyun.tair.tairgis;

import com.aliyun.tair.jedis3.Jedis3BuilderFactory;
import com.aliyun.tair.tairgis.factory.GisBuilderFactory;
import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairgis.params.GisParams;
import com.aliyun.tair.tairgis.params.GisSearchResponse;
import io.valkey.BuilderFactory;
import io.valkey.CommandArguments;
import io.valkey.CommandObject;
import io.valkey.Jedis;
import io.valkey.args.GeoUnit;
import io.valkey.Pipeline;
import io.valkey.Protocol;
import io.valkey.Response;
import io.valkey.util.SafeEncoder;

import java.util.List;
import java.util.Map;

public class TairGisPipeline extends Pipeline {
    public TairGisPipeline(Jedis jedis) {
        super(jedis);
    }

    public Response<Long> gisadd(final String key, final String polygonName, final String polygonWktText) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.GISADD)
            .key(key)
            .add(polygonName)
            .add(polygonWktText), BuilderFactory.LONG));
    }

    public Response<Long> gisadd(final byte[] key, final byte[] polygonName, final byte[] polygonWktText) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.GISADD)
            .key(key)
            .add(polygonName)
            .add(polygonWktText), BuilderFactory.LONG));
    }

    public Response<String> gisget(final String key, final String polygonName) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.GISGET)
            .key(key)
            .add(polygonName), BuilderFactory.STRING));
    }

    public Response<byte[]> gisget(final byte[] key, final byte[] polygonName) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.GISGET)
            .key(key)
            .add(polygonName), Jedis3BuilderFactory.BYTE_ARRAY));
    }

    public Response<Map<String, String>> gissearch(final String key, final String pointWktText) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.GISSEARCH)
            .key(key)
            .add(pointWktText), GisBuilderFactory.GISSEARCH_RESULT_MAP_STRING));
    }

    public Response<Map<byte[], byte[]>> gissearch(final byte[] key, final byte[] pointWktText) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.GISSEARCH)
            .key(key)
            .add(pointWktText), GisBuilderFactory.GISSEARCH_RESULT_MAP_BYTE));
    }

    public Response<List<GisSearchResponse>> gissearch(final String key, final double longitude, final double latitude,
            final double radius, final GeoUnit unit, final GisParams gisParams) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.GISSEARCH)
            .addObjects(gisParams.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(GisParams.RADIUS),
                Protocol.toByteArray(longitude), Protocol.toByteArray(latitude),
                Protocol.toByteArray(radius), unit.getRaw())),
            GisBuilderFactory.GISSEARCH_WITH_PARAMS_RESULT));
    }

    public Response<List<GisSearchResponse>> gissearch(final byte[] key, final double longitude, final double latitude,
            final double radius, final GeoUnit unit, final GisParams gisParams) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.GISSEARCH)
            .addObjects(gisParams.getByteParams(key, SafeEncoder.encode(GisParams.RADIUS),
                Protocol.toByteArray(longitude), Protocol.toByteArray(latitude),
                Protocol.toByteArray(radius), unit.getRaw())),
            GisBuilderFactory.GISSEARCH_WITH_PARAMS_RESULT));
    }

    public Response<List<GisSearchResponse>> gissearchByMember(final String key, String member, final double radius,
            final GeoUnit unit, final GisParams gisParams) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.GISSEARCH)
            .addObjects(gisParams.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(GisParams.MEMBER),
                SafeEncoder.encode(member), Protocol.toByteArray(radius), unit.getRaw())),
            GisBuilderFactory.GISSEARCH_WITH_PARAMS_RESULT));
    }

    public Response<List<GisSearchResponse>> gissearchByMember(final byte[] key, byte[] member, final double radius,
            final GeoUnit unit, final GisParams gisParams) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.GISSEARCH)
            .addObjects(gisParams.getByteParams(key, SafeEncoder.encode(GisParams.MEMBER),
                member, Protocol.toByteArray(radius), unit.getRaw())),
            GisBuilderFactory.GISSEARCH_WITH_PARAMS_RESULT));
    }

    public Response<Map<String, String>> giscontains(final String key, final String pointWktText) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.GISCONTAINS)
            .key(key)
            .add(pointWktText), GisBuilderFactory.GISSEARCH_RESULT_MAP_STRING));
    }

    public Response<List<String>> giscontains(final String key, final String pointWktText, final GisParams gisParams) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.GISCONTAINS)
            .addObjects(gisParams.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(pointWktText))),
            GisBuilderFactory.GISSEARCH_RESULT_LIST_STRING));
    }

    public Response<Map<byte[], byte[]>> giscontains(final byte[] key, final byte[] pointWktText) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.GISCONTAINS)
            .key(key)
            .add(pointWktText), GisBuilderFactory.GISSEARCH_RESULT_MAP_BYTE));
    }

    public Response<List<byte[]>> giscontains(final byte[] key, final byte[] pointWktText, final GisParams gisParams) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.GISCONTAINS)
            .addObjects(gisParams.getByteParams(key, pointWktText)),
            GisBuilderFactory.GISSEARCH_RESULT_BYTE_ARRAY_LIST));
    }

    public Response<Map<String, String>> giswithin(final String key, final String pointWktText) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.GISWITHIN)
            .key(key)
            .add(pointWktText), GisBuilderFactory.GISSEARCH_RESULT_MAP_STRING));
    }

    public Response<List<String>> giswithin(final String key, final String pointWktText, final GisParams gisParams) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.GISWITHIN)
            .addObjects(gisParams.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(pointWktText))),
            GisBuilderFactory.GISSEARCH_RESULT_LIST_STRING));
    }

    public Response<Map<byte[], byte[]>> giswithin(final byte[] key, final byte[] pointWktText) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.GISWITHIN)
            .key(key)
            .add(pointWktText), GisBuilderFactory.GISSEARCH_RESULT_MAP_BYTE));
    }

    public Response<List<byte[]>> giswithin(final byte[] key, final byte[] pointWktText, final GisParams gisParams) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.GISWITHIN)
            .addObjects(gisParams.getByteParams(key, pointWktText)),
            GisBuilderFactory.GISSEARCH_RESULT_BYTE_ARRAY_LIST));
    }

    public Response<Map<String, String>> gisintersects(final String key, final String pointWktText) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.GISINTERSECTS)
            .key(key)
            .add(pointWktText), GisBuilderFactory.GISSEARCH_RESULT_MAP_STRING));
    }

    public Response<Map<byte[], byte[]>> gisintersects(final byte[] key, final byte[] pointWktText) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.GISINTERSECTS)
            .key(key)
            .add(pointWktText), GisBuilderFactory.GISSEARCH_RESULT_MAP_BYTE));
    }

    public Response<String> gisdel(final String key, final String polygonName) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.GISDEL)
            .key(key)
            .add(polygonName), BuilderFactory.STRING));
    }

    public Response<byte[]> gisdel(final byte[] key, final byte[] polygonName) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.GISDEL)
            .key(key)
            .add(polygonName), Jedis3BuilderFactory.BYTE_ARRAY));
    }

    public Response<Map<String, String>> gisgetall(final String key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.GISGETALL)
            .key(key), BuilderFactory.STRING_MAP));
    }

    public Response<List<String>> gisgetall(final String key, final GisParams gisParams) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.GISGETALL)
            .addObjects(gisParams.getByteParams(SafeEncoder.encode(key))),
            BuilderFactory.STRING_LIST));
    }

    public Response<Map<byte[], byte[]>> gisgetall(final byte[] key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.GISGETALL)
            .key(key), Jedis3BuilderFactory.BYTE_ARRAY_MAP));
    }

    public Response<List<byte[]>> gisgetall(final byte[] key, final GisParams gisParams) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.GISGETALL)
            .addObjects(gisParams.getByteParams(key)),
            Jedis3BuilderFactory.BYTE_ARRAY_LIST));
    }
}
