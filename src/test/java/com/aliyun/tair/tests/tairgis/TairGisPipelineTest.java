package com.aliyun.tair.tests.tairgis;

import com.aliyun.tair.tairgis.params.GisParams;
import com.aliyun.tair.tests.AssertUtil;
import org.junit.Test;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKTReader;

import java.util.*;

public class TairGisPipelineTest extends TairGisTestBase{

    String area;
    byte[] barea;

    public TairGisPipelineTest() {
        area = "area" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        barea = ("barea" +Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
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
        bretWktText = (byte[]) objs.get(i++);
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
        tairGisPipeline.gisdel(key, "not-exists-polygon");
        List<Object> objs = tairGisPipeline.syncAndReturnAll();

        i = 0;
        AssertUtil.assertEquals((long)1, objs.get(i++));
        AssertUtil.assertEquals(null, objs.get(i++));
    }
}
