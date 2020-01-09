import org.junit.Test;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKTReader;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * @author dwan
 * @date 2019/12/26
 */
public class TairGisClusterTest extends TairGisTestBase{

    String area;
    byte[] barea;

    private static final String EXGIS_BIGKEY = "EXGIS_BIGKEY";

    public TairGisClusterTest() {
        area = "area" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        barea = ("barea" +Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
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

        String ret = tairGisCluster.gisdel(key, polygonName);
        AssertUtil.assertEquals("OK", ret);

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
}
