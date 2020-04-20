package com.aliyun.tair.tests.tairgis;

import com.aliyun.tair.tairgis.params.GisParams;
import com.aliyun.tair.tairgis.params.GisSearchResponse;
import com.aliyun.tair.tests.AssertUtil;
import com.taobao.eagleeye.EagleEye;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKTReader;
import redis.clients.jedis.GeoUnit;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class TairGisClusterTest extends TairGisTestBase {

    String area;
    byte[] barea;
    private String randomkey_;
    private byte[] randomKeyBinary_;

    private static final String EXGIS_BIGKEY = "EXGIS_BIGKEY";

    public TairGisClusterTest() {
        randomkey_ = "randomkey_" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        randomKeyBinary_ = ("randomkey_" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        area = "area" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        barea = ("barea" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
    }

    @Before
    public void startUp() throws Exception {
        EagleEye.startTrace(null, "test123", 0);
    }

    @After
    public void tearDown() throws Exception {
        EagleEye.endTrace("00");
    }

    @Test
    public void gissearchTest() throws Exception {
        long updated = 0;
        String retWktText = "";
        byte[] bretWktText = "".getBytes();
        Polygon polygon = null;
        Polygon retPolygon = null;
        WKTReader reader = new WKTReader(new GeometryFactory());
        String polygonName = "alibaba-xixi-campus",
            polygonWktText = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))", pointWktText = "POINT (30 11)";

        // String
        updated = tairGisCluster.gisadd(area, polygonName, polygonWktText);
        AssertUtil.assertEquals(1, updated);

        polygon = (Polygon)reader.read(polygonWktText);
        retWktText = tairGisCluster.gisget(area, polygonName);
        retPolygon = (Polygon)reader.read(retWktText);
        AssertUtil.assertTrue(polygon.equals(retPolygon));

        Map<String, String> searchResults = tairGisCluster.gissearch(area, pointWktText);
        AssertUtil.assertEquals(1, searchResults.size());
        AssertUtil.assertTrue(searchResults.containsKey(polygonName));
        AssertUtil.assertEquals(retWktText, searchResults.get(polygonName));

        searchResults = tairGisCluster.giscontains(area, pointWktText);
        AssertUtil.assertEquals(1, searchResults.size());
        AssertUtil.assertTrue(searchResults.containsKey(polygonName));
        AssertUtil.assertEquals(retWktText, searchResults.get(polygonName));

        searchResults = tairGisCluster.gisintersects(area, pointWktText);
        AssertUtil.assertEquals(1, searchResults.size());
        AssertUtil.assertTrue(searchResults.containsKey(polygonName));
        AssertUtil.assertEquals(retWktText, searchResults.get(polygonName));

        // binary
        updated = tairGisCluster.gisadd(barea, polygonName.getBytes(), polygonWktText.getBytes());
        AssertUtil.assertEquals(1, updated);

        polygon = (Polygon)reader.read(polygonWktText);
        bretWktText = tairGisCluster.gisget(barea, polygonName.getBytes());
        retPolygon = (Polygon)reader.read(new String(bretWktText));
        AssertUtil.assertTrue(polygon.equals(retPolygon));

        Map<byte[], byte[]> bsearchResults = tairGisCluster.gissearch(barea, pointWktText.getBytes());
        AssertUtil.assertEquals(1, bsearchResults.size());
        AssertUtil.assertTrue(bsearchResults.containsKey(polygonName.getBytes()));
        AssertUtil.assertEquals(true, Arrays.equals(retWktText.getBytes(), bsearchResults.get(polygonName.getBytes())));

        bsearchResults = tairGisCluster.giscontains(barea, pointWktText.getBytes());
        AssertUtil.assertEquals(1, bsearchResults.size());
        AssertUtil.assertTrue(bsearchResults.containsKey(polygonName.getBytes()));
        AssertUtil.assertEquals(true, Arrays.equals(retWktText.getBytes(), bsearchResults.get(polygonName.getBytes())));

        bsearchResults = tairGisCluster.gisintersects(barea, pointWktText.getBytes());
        AssertUtil.assertEquals(1, bsearchResults.size());
        AssertUtil.assertTrue(bsearchResults.containsKey(polygonName.getBytes()));
        AssertUtil.assertEquals(true, Arrays.equals(retWktText.getBytes(), bsearchResults.get(polygonName.getBytes())));
    }

    @Test
    public void gisdelTest() throws Exception {
        String uuid = UUID.randomUUID().toString();
        String key = "hangzhou" + uuid;
        String polygonName = "alibaba-xixi-campus";
        String polygonWktText = "POLYGON((30 10,40 40,20 40,10 20,30 10))";
        String polygonName1 = "alibaba-aliyun";
        String polygonWktText1 = "POLYGON((30 10,40 40))";

        long l = tairGisCluster.gisadd(key, polygonName, polygonWktText);
        AssertUtil.assertEquals(l, 1);
        l = tairGisCluster.gisadd(key, polygonName1, polygonWktText1);
        AssertUtil.assertEquals(l, 1);

        String retWktText = tairGisCluster.gisget(key, polygonName);
        AssertUtil.assertEquals(polygonWktText, retWktText);

        byte[] ret = tairGisCluster.gisdel(key.getBytes(), polygonName.getBytes());
        AssertUtil.assertEquals("OK", new String(ret));

        String retWktText1 = tairGisCluster.gisget(key, polygonName1);
        AssertUtil.assertEquals(polygonWktText1, retWktText1);

        Map<String, String> retMap = tairGisCluster.gissearch(key, polygonWktText1);
        AssertUtil.assertEquals(1, retMap.size());
        AssertUtil.assertEquals(retMap.containsKey(polygonName1), true);
        AssertUtil.assertEquals(polygonWktText1, retMap.get(polygonName1));
    }

    @Test
    public void gisdelKeyNotExistTest() throws Exception {
        String uuid = UUID.randomUUID().toString();
        String key = "hangzhou" + uuid;
        String polygonName = "alibaba-xixi-campus";

        AssertUtil.assertEquals(null, tairGisCluster.gisdel(key, polygonName));
    }

    @Test
    public void gisdelValueNotExistsTest() throws Exception {
        String uuid = UUID.randomUUID().toString();
        String key = "hangzhou" + uuid;
        String polygonName = "alibaba-xixi-campus";
        String polygonWktText = "POLYGON((30 10,40 40,20 40,10 20,30 10))";

        long l = tairGisCluster.gisadd(key, polygonName, polygonWktText);
        AssertUtil.assertEquals(l, 1);

        String ret = tairGisCluster.gisdel(key, "not-exists-polygon");
        AssertUtil.assertEquals(null, ret);
    }

    @Test
    public void gisdelKeyTypeErrorTest() throws Exception {
        String uuid = UUID.randomUUID().toString();
        String key = "hangzhou" + uuid;
        String polygonName = "alibaba-xixi-campus";
        String polygonWktText = "POLYGON((30 10,40 40,20 40,10 20,30 10))";

        try {
            tairGisCluster.gisdel(key, "not-exists-polygon");
        } catch (Exception e) {
            if (!e.getMessage().contains("WRONGTYPE")) {
                AssertUtil.fail("incorrect exception message: " + e.getMessage());
            }
        }
    }

    @Test
    public void gisBigKeyTest() {
        String polygonWktText;
        Random random = new Random();
        StringBuilder sb = new StringBuilder();
        sb.append("POLYGON ((");
        for (int i = 0; i < System.currentTimeMillis() % 1024; i++) {
            int x = random.nextInt(1024);
            int y = random.nextInt(1024);
            sb.append(x);
            sb.append(" ");
            sb.append(y);
            sb.append(",");
        }
        polygonWktText = sb.substring(0, sb.length() - 1);
        polygonWktText += "))";

        String polygonName = UUID.randomUUID().toString();
        long updated = tairGisCluster.gisadd(EXGIS_BIGKEY, polygonName, polygonWktText);
        AssertUtil.assertEquals(1, updated);
    }

    @Test
    public void gisSearchBigKeyTest() {
        String pointWktText = "POINT (30 11)";
        String linestringWktText = "LINESTRING (10 10, 40 40)";
        String polygonWktText = "POLYGON ((31 20, 29 20, 29 21, 31 31))";

        tairGisCluster.gissearch(EXGIS_BIGKEY, pointWktText);
        tairGisCluster.giscontains(EXGIS_BIGKEY, pointWktText);
        tairGisCluster.gisintersects(EXGIS_BIGKEY, pointWktText);

        tairGisCluster.gissearch(EXGIS_BIGKEY, linestringWktText);
        tairGisCluster.giscontains(EXGIS_BIGKEY, linestringWktText);
        tairGisCluster.gisintersects(EXGIS_BIGKEY, linestringWktText);

        tairGisCluster.gissearch(EXGIS_BIGKEY, polygonWktText);
        tairGisCluster.giscontains(EXGIS_BIGKEY, polygonWktText);
        tairGisCluster.gisintersects(EXGIS_BIGKEY, polygonWktText);
    }

    @Test
    public void gisContainsTest() {
        String uuid = UUID.randomUUID().toString();
        String key = "hangzhou" + uuid;
        String polygonName = "alibaba-xixi-campus";
        String polygonWktText = "POLYGON((30 10,40 40,20 40,10 20,30 10))";
        String pointWkt = "POINT (30 11)";

        long l = tairGisCluster.gisadd(key, polygonName, polygonWktText);
        AssertUtil.assertEquals(l, 1);

        // giscontains
        Map<String, String> retMap = tairGisCluster.giscontains(key, pointWkt);
        AssertUtil.assertEquals(1, retMap.size());
        AssertUtil.assertTrue(retMap.containsKey(polygonName));
        AssertUtil.assertEquals(polygonWktText, retMap.get(polygonName));

        // giscontains withoutwkt
        List<String> retList = tairGisCluster.giscontains(key, pointWkt, GisParams.gisParams().withoutWkt());
        AssertUtil.assertEquals(1, retList.size());
        AssertUtil.assertEquals(polygonName, retList.get(0));

        // giscontains binary
        Map<byte[], byte[]> bretMap = tairGisCluster.giscontains(key.getBytes(), pointWkt.getBytes());
        AssertUtil.assertEquals(1, bretMap.size());
        AssertUtil.assertTrue(bretMap.containsKey(polygonName.getBytes()));
        AssertUtil.assertTrue(Arrays.equals(polygonWktText.getBytes(), bretMap.get(polygonName.getBytes())));

        // giscontains withoutwkt binary
        List<byte[]> bretList = tairGisCluster.giscontains(key.getBytes(), pointWkt.getBytes(),
            GisParams.gisParams().withoutWkt());
        AssertUtil.assertEquals(1, bretList.size());
        AssertUtil.assertTrue(Arrays.equals(polygonName.getBytes(), bretList.get(0)));
    }

    @Test
    public void gisGetallTest() {
        String uuid = UUID.randomUUID().toString();
        String key = "hangzhou" + uuid;
        String polygonName = "alibaba-xixi-campus";
        String polygonWktText = "POLYGON((30 10,40 40,20 40,10 20,30 10))";

        long l = tairGisCluster.gisadd(key, polygonName, polygonWktText);
        AssertUtil.assertEquals(l, 1);

        // gisgetall
        Map<String, String> retMap = tairGisCluster.gisgetall(key);
        AssertUtil.assertEquals(1, retMap.size());
        AssertUtil.assertTrue(retMap.containsKey(polygonName));
        AssertUtil.assertEquals(polygonWktText, retMap.get(polygonName));

        // gisgetall withoutwkt
        List<String> retList = tairGisCluster.gisgetall(key, GisParams.gisParams().withoutWkt());
        AssertUtil.assertEquals(1, retList.size());
        AssertUtil.assertEquals(polygonName, retList.get(0));

        // gisgetall binary
        Map<byte[], byte[]> bretMap = tairGisCluster.gisgetall(key.getBytes());
        AssertUtil.assertEquals(1, bretMap.size());
        AssertUtil.assertTrue(bretMap.containsKey(polygonName.getBytes()));
        AssertUtil.assertTrue(Arrays.equals(polygonWktText.getBytes(), bretMap.get(polygonName.getBytes())));

        // gisgetall withoutwkt binary
        List<byte[]> bretList = tairGisCluster.gisgetall(key.getBytes(), GisParams.gisParams().withoutWkt());
        AssertUtil.assertEquals(1, bretList.size());
        AssertUtil.assertTrue(Arrays.equals(polygonName.getBytes(), bretList.get(0)));
    }

    @Test
    public void gisSearchByMember() {
        String uuid = UUID.randomUUID().toString();
        String key = "hangzhou" + uuid;

        tairGisCluster.gisadd(key, "Palermo", "POINT (13.361389 38.115556)");
        tairGisCluster.gisadd(key, "Catania", "POINT (15.087269 37.502669)");
        tairGisCluster.gisadd(key, "Agrigento", "POINT (13.583333 37.316667)");

        // withoutvalue
        List<GisSearchResponse> responses = tairGisCluster.gissearchByMember(key.getBytes(), "Palermo".getBytes(), 200,
            GeoUnit.KM, new GisParams().withoutValue());
        AssertUtil.assertEquals(3, responses.size());
        AssertUtil.assertEquals("Palermo", responses.get(0).getFieldByString());
        AssertUtil.assertNull(responses.get(0).getValueByString());
        equalsWithinEpsilon(0, responses.get(0).getDistance());

        // withvalue
        responses = tairGisCluster.gissearchByMember(key.getBytes(), "Palermo".getBytes(), 200,
            GeoUnit.KM, new GisParams());
        AssertUtil.assertEquals(3, responses.size());
        AssertUtil.assertEquals("Palermo", responses.get(0).getFieldByString());
        AssertUtil.assertEquals("POINT(13.361389 38.115556)", responses.get(0).getValueByString());
        equalsWithinEpsilon(0, responses.get(0).getDistance());

        // withdist
        responses = tairGisCluster.gissearchByMember(key, "Palermo", 200,
            GeoUnit.KM, new GisParams().withDist());
        AssertUtil.assertEquals("Palermo", responses.get(0).getFieldByString());
        AssertUtil.assertEquals("POINT(13.361389 38.115556)", responses.get(0).getValueByString());
        equalsWithinEpsilon(190.4424, responses.get(0).getDistance());

        // SORT ASC
        responses = tairGisCluster.gissearchByMember(key, "Palermo", 200,
            GeoUnit.KM, new GisParams().withDist().sortAscending());
        AssertUtil.assertEquals("Palermo", responses.get(0).getFieldByString());
        AssertUtil.assertEquals("POINT(13.361389 38.115556)", responses.get(0).getValueByString());
        equalsWithinEpsilon(0.0, responses.get(0).getDistance());

        // SORT DESC
        responses = tairGisCluster.gissearchByMember(key, "Palermo", 200,
            GeoUnit.KM, new GisParams().withDist().sortDescending());
        AssertUtil.assertEquals("Catania", responses.get(0).getFieldByString());
        AssertUtil.assertEquals("POINT(15.087269 37.502669)", responses.get(0).getValueByString());
        equalsWithinEpsilon(166.2743, responses.get(0).getDistance());

        // COUNT 2
        responses = tairGisCluster.gissearchByMember(key, "Palermo", 200,
            GeoUnit.KM, new GisParams().withDist().sortDescending().count(2));
        AssertUtil.assertEquals("Catania", responses.get(0).getFieldByString());
        AssertUtil.assertEquals("POINT(15.087269 37.502669)", responses.get(0).getValueByString());
        equalsWithinEpsilon(166.2743, responses.get(0).getDistance());
        AssertUtil.assertEquals("Agrigento", responses.get(1).getFieldByString());
        AssertUtil.assertEquals("POINT(13.583333 37.316667)", responses.get(1).getValueByString());
        equalsWithinEpsilon(90.9779, responses.get(1).getDistance());
    }

    @Test
    public void gisSearchWithParams() {
        String uuid = UUID.randomUUID().toString();
        String key = "hangzhou" + uuid;

        tairGisCluster.gisadd(key, "Palermo", "POINT (13.361389 38.115556)");
        tairGisCluster.gisadd(key, "Catania", "POINT (15.087269 37.502669)");
        tairGisCluster.gisadd(key, "Agrigento", "POINT (13.583333 37.316667)");

        // withoutvalue
        List<GisSearchResponse> responses = tairGisCluster.gissearch(key, 15, 37, 200,
            GeoUnit.KM, new GisParams().withoutValue());
        AssertUtil.assertEquals(3, responses.size());
        AssertUtil.assertEquals("Palermo", responses.get(0).getFieldByString());
        AssertUtil.assertNull(responses.get(0).getValueByString());
        equalsWithinEpsilon(0, responses.get(0).getDistance());

        // withvalue
        responses = tairGisCluster.gissearch(key.getBytes(), 15, 37, 200,
            GeoUnit.KM, new GisParams());
        AssertUtil.assertEquals(3, responses.size());
        AssertUtil.assertEquals("Palermo", responses.get(0).getFieldByString());
        AssertUtil.assertEquals("POINT(13.361389 38.115556)", responses.get(0).getValueByString());
        equalsWithinEpsilon(0, responses.get(0).getDistance());

        // withdist
        responses = tairGisCluster.gissearch(key.getBytes(), 15, 37, 200,
            GeoUnit.KM, new GisParams().withDist());
        AssertUtil.assertEquals("Palermo", responses.get(0).getFieldByString());
        AssertUtil.assertEquals("POINT(13.361389 38.115556)", responses.get(0).getValueByString());
        equalsWithinEpsilon(190.4424, responses.get(0).getDistance());

        // SORT ASC
        responses = tairGisCluster.gissearch(key, 15, 37, 200,
            GeoUnit.KM, new GisParams().withDist().sortAscending());
        AssertUtil.assertEquals("Catania", responses.get(0).getFieldByString());
        AssertUtil.assertEquals("POINT(15.087269 37.502669)", responses.get(0).getValueByString());
        equalsWithinEpsilon(56.4413, responses.get(0).getDistance());

        // SORT DESC
        responses = tairGisCluster.gissearch(key, 15, 37, 200,
            GeoUnit.KM, new GisParams().withDist().sortDescending());
        AssertUtil.assertEquals("Palermo", responses.get(0).getFieldByString());
        AssertUtil.assertEquals("POINT(13.361389 38.115556)", responses.get(0).getValueByString());
        equalsWithinEpsilon(190.4424, responses.get(0).getDistance());

        // COUNT 2
        responses = tairGisCluster.gissearch(key, 15, 37, 200,
            GeoUnit.KM, new GisParams().withDist().sortDescending().count(2));
        AssertUtil.assertEquals("Palermo", responses.get(0).getFieldByString());
        AssertUtil.assertEquals("POINT(13.361389 38.115556)", responses.get(0).getValueByString());
        equalsWithinEpsilon(190.4424, responses.get(0).getDistance());
        AssertUtil.assertEquals("Agrigento", responses.get(1).getFieldByString());
        AssertUtil.assertEquals("POINT(13.583333 37.316667)", responses.get(1).getValueByString());
        equalsWithinEpsilon(130.4233, responses.get(1).getDistance());
    }

    private boolean equalsWithinEpsilon(double d1, double d2) {
        double epsilon = 1E-5;
        return Math.abs(d1 - d2) < epsilon;
    }
}
