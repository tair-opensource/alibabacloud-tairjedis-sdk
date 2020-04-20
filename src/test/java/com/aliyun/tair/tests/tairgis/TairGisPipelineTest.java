package com.aliyun.tair.tests.tairgis;

import com.aliyun.tair.tairgis.params.GisParams;
import com.aliyun.tair.tairgis.params.GisSearchResponse;
import com.aliyun.tair.tests.AssertUtil;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKTReader;
import redis.clients.jedis.GeoUnit;
import redis.clients.jedis.Response;

import java.util.*;
import java.util.function.BiConsumer;

import static org.junit.Assert.assertTrue;

public class TairGisPipelineTest extends TairGisTestBase {

    String area;
    byte[] barea;
    private String randomkey_;
    private byte[] randomKeyBinary_;

    public TairGisPipelineTest() {
        randomkey_ = "randomkey_" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        randomKeyBinary_ = ("randomkey_" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        area = "area" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        barea = ("barea" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
    }

    @Test
    public void gissearchTest() throws Exception {
        int i = 0;
        long updated = 0;
        String retWktText = "";
        byte[] bretWktText = "".getBytes();
        Polygon polygon = null;
        Polygon retPolygon = null;
        WKTReader reader = new WKTReader(new GeometryFactory());
        String polygonName = "alibaba-xixi-campus",
            polygonWktText = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))", pointWktText = "POINT (30 11)";

        // String
        tairGisPipeline.gisadd(area, polygonName, polygonWktText);
        tairGisPipeline.gisget(area, polygonName);
        tairGisPipeline.gissearch(area, pointWktText);
        List<Object> objs = tairGisPipeline.syncAndReturnAll();

        i = 0;
        AssertUtil.assertEquals((long)1, objs.get(i++));
        polygon = (Polygon)reader.read(polygonWktText);
        retWktText = objs.get(i++).toString();
        retPolygon = (Polygon)reader.read(retWktText);
        AssertUtil.assertTrue(polygon.equals(retPolygon));

        AssertUtil.assertEquals(1, Map.class.cast(objs.get(i)).size());
        AssertUtil.assertTrue(Map.class.cast(objs.get(i)).containsKey(polygonName));
        AssertUtil.assertEquals(retWktText, Map.class.cast(objs.get(i++)).get(polygonName));

        // binary
        tairGisPipeline.gisadd(barea, polygonName.getBytes(), polygonWktText.getBytes());
        tairGisPipeline.gisget(barea, polygonName.getBytes());
        tairGisPipeline.gissearch(barea, pointWktText.getBytes());
        objs = tairGisPipeline.syncAndReturnAll();

        i = 0;
        AssertUtil.assertEquals((long)1, objs.get(i++));
        polygon = (Polygon)reader.read(polygonWktText);
        bretWktText = (byte[])objs.get(i++);
        retPolygon = (Polygon)reader.read(new String(bretWktText));
        AssertUtil.assertTrue(polygon.equals(retPolygon));

        AssertUtil.assertEquals(1, Map.class.cast(objs.get(i)).size());
    }

    @Test
    public void gisdelTest() throws Exception {
        String uuid = UUID.randomUUID().toString();
        String key = "hangzhou" + uuid;
        String polygonName = "alibaba-xixi-campus";
        String polygonWktText = "POLYGON((30 10,40 40,20 40,10 20,30 10))";
        String polygonName1 = "alibaba-aliyun";
        String polygonWktText1 = "POLYGON((30 10,40 40))";

        tairGisPipeline.gisadd(key, polygonName, polygonWktText);
        tairGisPipeline.gisadd(key, polygonName1, polygonWktText1);
        tairGisPipeline.gisget(key, polygonName);
        tairGisPipeline.gisdel(key, polygonName);
        tairGisPipeline.gisget(key, polygonName1);
        tairGisPipeline.gissearch(key, polygonWktText1);
        tairGisPipeline.giscontains(key, polygonWktText1, GisParams.gisParams().withoutWkt());
        tairGisPipeline.gisgetall(key);
        List<Object> objs = tairGisPipeline.syncAndReturnAll();

        int i = 0;
        AssertUtil.assertEquals((long)1, objs.get(i++));
        AssertUtil.assertEquals((long)1, objs.get(i++));
        AssertUtil.assertEquals(polygonWktText, objs.get(i++));
        AssertUtil.assertEquals("OK", objs.get(i++));
        AssertUtil.assertEquals(polygonWktText1, objs.get(i++));

        AssertUtil.assertEquals(1, Map.class.cast(objs.get(i)).size());
        AssertUtil.assertTrue(Map.class.cast(objs.get(i)).containsKey(polygonName1));
        AssertUtil.assertEquals(polygonWktText1, Map.class.cast(objs.get(i)).get(polygonName1));

        i += 1;
        AssertUtil.assertEquals(1, List.class.cast(objs.get(i)).size());
        AssertUtil.assertEquals(polygonName1, List.class.cast(objs.get(i)).get(0));

        i += 1;
        AssertUtil.assertEquals(1, Map.class.cast(objs.get(i)).size());
        AssertUtil.assertTrue(Map.class.cast(objs.get(i)).containsKey(polygonName1));
        AssertUtil.assertEquals(polygonWktText1, Map.class.cast(objs.get(i)).get(polygonName1));
    }

    @Test
    public void gisdelValueNotExistsTest() throws Exception {
        int i = 0;
        String uuid = UUID.randomUUID().toString();
        String key = "hangzhou" + uuid;
        String polygonName = "alibaba-xixi-campus";
        String polygonWktText = "POLYGON((30 10,40 40,20 40,10 20,30 10))";

        tairGisPipeline.gisadd(key, polygonName, polygonWktText);
        tairGisPipeline.gisdel(key.getBytes(), "not-exists-polygon".getBytes());
        List<Object> objs = tairGisPipeline.syncAndReturnAll();

        i = 0;
        AssertUtil.assertEquals((long)1, objs.get(i++));
        AssertUtil.assertEquals(null, objs.get(i++));
    }

    @Test
    public void gisSearchPipelineTest() {
        String uuid = UUID.randomUUID().toString();
        String key = "hangzhou" + uuid;

        tairGis.gisadd(key, "Palermo", "POINT (13.361389 38.115556)");
        tairGis.gisadd(key, "Catania", "POINT (15.087269 37.502669)");
        tairGis.gisadd(key, "Agrigento", "POINT (13.583333 37.316667)");

        // withoutvalue
        Response<List<GisSearchResponse>> r1 = tairGisPipeline.gissearch(key, 15, 37, 200,
            GeoUnit.KM, new GisParams().withoutValue());
        Response<List<GisSearchResponse>> r2 = tairGisPipeline.gissearchByMember(key, "Palermo", 200,
            GeoUnit.KM, new GisParams().withoutValue());

        tairGisPipeline.sync();

        List<GisSearchResponse> responses = r1.get();
        AssertUtil.assertEquals(3, responses.size());
        AssertUtil.assertEquals("Palermo", responses.get(0).getFieldByString());
        AssertUtil.assertNull(responses.get(0).getValueByString());

        responses = r2.get();
        AssertUtil.assertEquals(3, responses.size());
        AssertUtil.assertEquals("Palermo", responses.get(0).getFieldByString());
        AssertUtil.assertNull(responses.get(0).getValueByString());
    }

    @Test
    public void gisSearchPipelineTestBinary() {
        String uuid = UUID.randomUUID().toString();
        String key = "hangzhou" + uuid;

        tairGis.gisadd(key, "Palermo", "POINT (13.361389 38.115556)");
        tairGis.gisadd(key, "Catania", "POINT (15.087269 37.502669)");
        tairGis.gisadd(key, "Agrigento", "POINT (13.583333 37.316667)");

        // withoutvalue
        Response<List<GisSearchResponse>> r1 = tairGisPipeline.gissearch(key.getBytes(), 15, 37, 200,
            GeoUnit.KM, new GisParams().withoutValue());
        Response<List<GisSearchResponse>> r2 = tairGisPipeline.gissearchByMember(key.getBytes(), "Palermo".getBytes(), 200,
            GeoUnit.KM, new GisParams().withoutValue());

        tairGisPipeline.sync();

        List<GisSearchResponse> responses = r1.get();
        AssertUtil.assertEquals(3, responses.size());
        AssertUtil.assertEquals("Palermo", responses.get(0).getFieldByString());
        AssertUtil.assertNull(responses.get(0).getValueByString());

        responses = r2.get();
        AssertUtil.assertEquals(3, responses.size());
        AssertUtil.assertEquals("Palermo", responses.get(0).getFieldByString());
        AssertUtil.assertNull(responses.get(0).getValueByString());
    }

    @Test
    public void gisGetallPipelineTest() {
        String uuid = UUID.randomUUID().toString();
        String key = "hangzhou" + uuid;
        String polygonName = "alibaba-xixi-campus";
        String polygonWktText = "POLYGON((30 10,40 40,20 40,10 20,30 10))";

        long l = tairGis.gisadd(key, polygonName, polygonWktText);
        AssertUtil.assertEquals(l, 1);


        Response<Map<String, String>> r1 = tairGisPipeline.gisgetall(key);
        Response<List<String>> r2 = tairGisPipeline.gisgetall(key, GisParams.gisParams().withoutWkt());
        Response<Map<byte[], byte[]>> r3 = tairGisPipeline.gisgetall(key.getBytes());
        Response<List<byte[]>> r4 = tairGisPipeline.gisgetall(key.getBytes(),
            GisParams.gisParams().withoutWkt());

        tairGisPipeline.sync();

        // gisgetall
        Map<String, String> retMap = r1.get();
        AssertUtil.assertEquals(1, retMap.size());
        assertTrue(retMap.containsKey(polygonName));
        AssertUtil.assertEquals(polygonWktText, retMap.get(polygonName));

        // gisgetall withoutwkt
        List<String> retList = r2.get();
        AssertUtil.assertEquals(1, retList.size());
        AssertUtil.assertEquals(polygonName, retList.get(0));

        // gisgetall binary
        Map<byte[], byte[]> bretMap = r3.get();
        AssertUtil.assertEquals(1, bretMap.size());
        assertTrue(bretMap.containsKey(polygonName.getBytes()));
        assertTrue(Arrays.equals(polygonWktText.getBytes(), bretMap.get(polygonName.getBytes())));

        // gisgetall withoutwkt binary
        List<byte[]> bretList = r4.get();
        AssertUtil.assertEquals(1, bretList.size());
        assertTrue(Arrays.equals(polygonName.getBytes(), bretList.get(0)));
    }

    @Test
    public void gisContainsTest2() {
        String polygonName = "alibaba-xixi-campus",
            polygonWktText = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))", pointWktText = "POINT (30 11)";

        // String
        AssertUtil.assertEquals(1, (long)tairGis.gisadd(area, polygonName, polygonWktText));

        Response<Map<String, String>> r1 = tairGisPipeline.gissearch(area, pointWktText);
        Response<Map<String, String>> r2 = tairGisPipeline.giscontains(area, pointWktText);
        Response<Map<String, String>> r3 = tairGisPipeline.gisintersects(area, pointWktText);

        tairGisPipeline.sync();

        Map<String, String> searchResults = r1.get();
        AssertUtil.assertEquals(1, searchResults.size());
        assertTrue(searchResults.containsKey(polygonName));

        searchResults = r2.get();
        AssertUtil.assertEquals(1, searchResults.size());
        assertTrue(searchResults.containsKey(polygonName));

        searchResults = r3.get();
        AssertUtil.assertEquals(1, searchResults.size());
        assertTrue(searchResults.containsKey(polygonName));
    }

    @Test
    public void gisContainsTest() {
        String polygonName = "alibaba-xixi-campus",
            polygonWktText = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))", pointWktText = "POINT (30 11)";

        // String
        AssertUtil.assertEquals(1, (long)tairGis.gisadd(area, polygonName, polygonWktText));

        Response<Map<byte[], byte[]>> r1 = tairGisPipeline.gissearch(area.getBytes(), pointWktText.getBytes());
        Response<Map<byte[], byte[]>> r2 = tairGisPipeline.giscontains(area.getBytes(), pointWktText.getBytes());
        Response<Map<byte[], byte[]>> r3 = tairGisPipeline.gisintersects(area.getBytes(), pointWktText.getBytes());
        Response<List<byte[]>> r4 = tairGisPipeline.giscontains(area.getBytes(), pointWktText.getBytes(),
            GisParams.gisParams().withoutWkt());

        tairGisPipeline.sync();

        Map<byte[], byte[]> searchResults = r1.get();
        AssertUtil.assertEquals(1, searchResults.size());

        searchResults = r2.get();
        AssertUtil.assertEquals(1, searchResults.size());

        searchResults = r3.get();
        AssertUtil.assertEquals(1, searchResults.size());

        AssertUtil.assertEquals(2, r4.get().size());
    }
}
