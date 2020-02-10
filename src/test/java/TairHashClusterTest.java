import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import com.aliyun.tair.tairhash.params.ExhgetwithverResult;
import com.aliyun.tair.tairhash.params.ExhmsetwithoptsParams;
import com.aliyun.tair.tairhash.params.ExhsetParams;
import org.junit.Assert;
import org.junit.ComparisonFailure;
import org.junit.Test;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class TairHashClusterTest extends TairHashTestBase {
    String foo;
    byte[] bfoo = {0x01, 0x02, 0x03, 0x04};
    byte[] bbar = {0x05, 0x06, 0x07, 0x08};
    final byte[] bcar = {0x09, 0x0A, 0x0B, 0x0C};

    final byte[] bbar1 = {0x05, 0x06, 0x07, 0x08, 0x0A};
    final byte[] bbar2 = {0x05, 0x06, 0x07, 0x08, 0x0B};
    final byte[] bbar3 = {0x05, 0x06, 0x07, 0x08, 0x0C};
    final byte[] bbarstar = {0x05, 0x06, 0x07, 0x08, '*'};
    private static final String EXHASH_BIGKEY = "EXHASH_BIGKEY";

    public TairHashClusterTest() {
        foo = "foo" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        bfoo = ("bfoo" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
    }

    @Test
    public void exhmsetwithopts() {
        // Binary
        List<ExhmsetwithoptsParams<byte[]>> bparams = new LinkedList<ExhmsetwithoptsParams<byte[]>>();
        ExhmsetwithoptsParams<byte[]> bparams1 = new ExhmsetwithoptsParams<byte[]>(bbar, bcar, 4, 0);
        ExhmsetwithoptsParams<byte[]> bparams2 = new ExhmsetwithoptsParams<byte[]>(bcar, bbar, 4, 0);
        bparams.add(bparams1);
        bparams.add(bparams2);

        String bstatus = tairHashCluster.exhmsetwithopts(bfoo, bparams);
        assertEquals("OK", bstatus);
        assertEquals(true, Arrays.equals(bcar, tairHashCluster.exhget(bfoo, bbar)));
        assertEquals(true, Arrays.equals(bbar, tairHashCluster.exhget(bfoo, bcar)));
    }

    @Test
    public void exhgetwithver() {
        //Binary
        tairHashCluster.exhset(bfoo, bbar, bcar);
        ExhgetwithverResult<byte[]> bresult = tairHashCluster.exhgetwithver(bfoo, bbar);
        assertEquals(true, Arrays.equals(bcar, bresult.getValue()));
        assertEquals(1, bresult.getVer());
    }

    @Test
    public void exhver() {
        // binary
        tairHashCluster.exhset(bfoo, bbar, bcar);
        assertEquals(1, (long)tairHashCluster.exhver(bfoo, bbar));
    }

    @Test
    public void exhttl() {
        // binary
        tairHashCluster.exhset(bfoo, bbar, bcar);
        tairHashCluster.exhexpire(bfoo, bbar, 20);
        assertEquals(true, tairHashCluster.exhttl(bfoo, bbar) <= 20);
        assertEquals(true, tairHashCluster.exhttl(bfoo, bbar) > 0);

    }

    @Test
    public void exhexpireAt() {
        // binary
        long unixTime = (System.currentTimeMillis() / 1000L) + 20;
        tairHashCluster.exhset(bfoo, bbar, bbar);
        Boolean status = tairHashCluster.exhexpireAt(bfoo, bbar, unixTime);
        assertEquals(true, status);
    }

    @Test
    public void exhmgetwithver() {
        //Binary
        List<ExhmsetwithoptsParams<byte[]>> bparams = new LinkedList<ExhmsetwithoptsParams<byte[]>>();
        ExhmsetwithoptsParams<byte[]> bparams1 = new ExhmsetwithoptsParams<byte[]>(bbar, bcar, 4, 0);
        ExhmsetwithoptsParams<byte[]> bparams2 = new ExhmsetwithoptsParams<byte[]>(bcar, bbar, 4, 0);
        bparams.add(bparams1);
        bparams.add(bparams2);

        String bstatus = tairHashCluster.exhmsetwithopts(bfoo, bparams);
        assertEquals("OK", bstatus);
        assertEquals(true, Arrays.equals(bcar, tairHashCluster.exhget(bfoo, bbar)));
        assertEquals(true, Arrays.equals(bbar, tairHashCluster.exhget(bfoo, bcar)));
        List<ExhgetwithverResult<byte[]>> bresults = tairHashCluster.exhmgetwithver(bfoo, bbar, bcar);
        assertEquals(2, bresults.size());
        assertEquals(1, bresults.get(0).getVer());
        assertEquals(true, Arrays.equals(bcar, bresults.get(0).getValue()));
        assertEquals(1, bresults.get(1).getVer());
        assertEquals(true, Arrays.equals(bbar, bresults.get(1).getValue()));

    }

    @Test
    public void exhset() {
        // Binary
        long bstatus = tairHashCluster.exhset(bfoo, bbar, bcar);
        assertEquals(1, bstatus);
        bstatus = tairHashCluster.exhset(bfoo, bbar, bfoo);
        assertEquals(0, bstatus);
    }

    @Test
    public void exhsetparams() {
        // Binary
        long bstatus = tairHashCluster.exhset(bfoo, bbar, bcar, ExhsetParams.ExhsetParams().ver(1));
        assertEquals(1, bstatus);
        assertEquals(1, (long)tairHashCluster.exhver(bfoo, bbar));
        bstatus = tairHashCluster.exhset(bfoo, bbar, bfoo);
        assertEquals(2, (long)tairHashCluster.exhver(bfoo, bbar));
        assertEquals(0, bstatus);
    }

    @Test
    public void exhget() {
        // Binary
        tairHashCluster.exhset(bfoo, bbar, bcar);
        assertNull(tairHashCluster.exhget(bbar, bfoo));
        assertNull(tairHashCluster.exhget(bfoo, bcar));
        assertArrayEquals(bcar, tairHashCluster.exhget(bfoo, bbar));
    }

    @Test
    public void exhsetnx() {
        // Binary
        long bstatus = tairHashCluster.exhsetnx(bfoo, bbar, bcar);
        assertEquals(1, bstatus);
        assertArrayEquals(bcar, tairHashCluster.exhget(bfoo, bbar));

        bstatus = tairHashCluster.exhsetnx(bfoo, bbar, bfoo);
        assertEquals(0, bstatus);
        assertArrayEquals(bcar, tairHashCluster.exhget(bfoo, bbar));

        bstatus = tairHashCluster.exhsetnx(bfoo, bcar, bbar);
        assertEquals(1, bstatus);
        assertArrayEquals(bbar, tairHashCluster.exhget(bfoo, bcar));

    }

    @Test
    public void exhmset() {
        // Binary
        Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>();
        bhash.put(bbar, bcar);
        bhash.put(bcar, bbar);
        String bstatus = tairHashCluster.exhmset(bfoo, bhash);
        assertEquals("OK", bstatus);
        assertArrayEquals(bcar, tairHashCluster.exhget(bfoo, bbar));
        assertArrayEquals(bbar, tairHashCluster.exhget(bfoo, bcar));
    }

    @Test
    public void exhmget() {
        // Binary
        Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>();
        bhash.put(bbar, bcar);
        bhash.put(bcar, bbar);
        tairHashCluster.exhmset(bfoo, bhash);

        List<byte[]> bvalues = tairHashCluster.exhmget(bfoo, bbar, bcar, bfoo);
        List<byte[]> bexpected = new ArrayList<byte[]>();
        bexpected.add(bcar);
        bexpected.add(bbar);
        bexpected.add(null);

        assertByteListEquals(bexpected, bvalues);
    }

    @Test
    public void exhincrBy() {
        // Binary
        long bvalue = tairHashCluster.exhincrBy(bfoo, bbar, 1);
        assertEquals(1, bvalue);
        bvalue = tairHashCluster.exhincrBy(bfoo, bbar, -1);
        assertEquals(0, bvalue);
        bvalue = tairHashCluster.exhincrBy(bfoo, bbar, -10);
        assertEquals(-10, bvalue);

    }

    @Test
    public void exhincrByFloat() {
        // Binary
        double bvalue = tairHashCluster.exhincrByFloat(bfoo, bbar, 1.5d);
        assertEquals(Double.compare(1.5d, bvalue), 0);
        bvalue = tairHashCluster.exhincrByFloat(bfoo, bbar, -1.5d);
        assertEquals(Double.compare(0d, bvalue), 0);
        bvalue = tairHashCluster.exhincrByFloat(bfoo, bbar, -10.7d);
        assertEquals(Double.compare(-10.7d, bvalue), 0);

    }

    @Test
    public void exhexists() {
        // Binary
        Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>();
        bhash.put(bbar, bcar);
        bhash.put(bcar, bbar);
        tairHashCluster.exhmset(bfoo, bhash);

        assertFalse(tairHashCluster.exhexists(bbar, bfoo));
        assertFalse(tairHashCluster.exhexists(bfoo, bfoo));
        assertTrue(tairHashCluster.exhexists(bfoo, bbar));

    }

    @Test
    public void exhdel() {
        // Binary
        Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>();
        bhash.put(bbar, bcar);
        bhash.put(bcar, bbar);
        tairHashCluster.exhmset(bfoo, bhash);

        assertEquals(0, tairHashCluster.exhdel(bbar, bfoo).intValue());
        assertEquals(0, tairHashCluster.exhdel(bfoo, bfoo).intValue());
        assertEquals(1, tairHashCluster.exhdel(bfoo, bbar).intValue());
        assertNull(tairHashCluster.exhget(bfoo, bbar));

    }

    @Test
    public void exhlen() {
        // Binary
        Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>();
        bhash.put(bbar, bcar);
        bhash.put(bcar, bbar);
        tairHashCluster.exhmset(bfoo, bhash);

        assertEquals(0, tairHashCluster.exhlen(bbar).intValue());
        assertEquals(2, tairHashCluster.exhlen(bfoo).intValue());

    }

    @Test
    public void exhkeys() {
        // Binary
        Map<byte[], byte[]> bhash = new LinkedHashMap<byte[], byte[]>();
        bhash.put(bbar, bcar);
        bhash.put(bcar, bbar);
        tairHashCluster.exhmset(bfoo, bhash);

        Set<byte[]> bkeys = tairHashCluster.exhkeys(bfoo);
        Set<byte[]> bexpected = new LinkedHashSet<byte[]>();
        bexpected.add(bbar);
        bexpected.add(bcar);
        assertByteSetEquals(bexpected, bkeys);
    }

    @Test
    public void exhvals() {
        // Binary
        Map<byte[], byte[]> bhash = new LinkedHashMap<byte[], byte[]>();
        bhash.put(bbar, bcar);
        bhash.put(bcar, bbar);
        tairHashCluster.exhmset(bfoo, bhash);

        List<byte[]> bvals = tairHashCluster.exhvals(bfoo);

        assertEquals(2, bvals.size());
        assertTrue(arrayContains(bvals, bbar));
        assertTrue(arrayContains(bvals, bcar));
    }

    @Test
    public void exhgetAll() {
        // Binary
        Map<byte[], byte[]> bh = new HashMap<byte[], byte[]>();
        bh.put(bbar, bcar);
        bh.put(bcar, bbar);
        tairHashCluster.exhmset(bfoo, bh);
        Map<byte[], byte[]> bhash = tairHashCluster.exhgetAll(bfoo);

        assertEquals(2, bhash.size());
        assertArrayEquals(bcar, bhash.get(bbar));
        assertArrayEquals(bbar, bhash.get(bcar));
    }

    @Test
    public void extestBinaryHstrLen() {
        Map<byte[], byte[]> values = new HashMap<byte[], byte[]>();
        values.put(bbar, bcar);
        tairHashCluster.exhmset(bfoo, values);
        Long response = tairHashCluster.exhstrlen(bfoo, bbar);
        assertEquals(4l, response.longValue());
    }

    @Test
    public void exhashBigKey() {
        long time = System.currentTimeMillis() % 604800;
        tairHashCluster.exhset(EXHASH_BIGKEY, UUID.randomUUID().toString(), UUID.randomUUID().toString(),
            ExhsetParams.ExhsetParams().ex((int)time));
    }

    @Test
    public void exhscanTest() {
        HashMap<String, String> map = new HashMap<String, String>();
        for (int i = 1; i < 10; i++) {
            map.put("field" + i, "val" + i);
        }

        tairHashCluster.exhmset(foo, map);

        ScanParams scanParams = new ScanParams().count(3);
        ScanResult<Entry<String, String>> scanResult = tairHashCluster.exhscan(foo, "^", "", scanParams);
        int j = 1;
        for (Entry<String, String> entry : scanResult.getResult()) {
            Assert.assertEquals("field" + j, entry.getKey());
            Assert.assertEquals("val" + j, entry.getValue());
            j++;
        }

        scanResult = tairHashCluster.exhscan(foo, ">=", scanResult.getCursor(), scanParams);
        for (Entry<String, String> entry : scanResult.getResult()) {
            Assert.assertEquals("field" + j, entry.getKey());
            Assert.assertEquals("val" + j, entry.getValue());
            j++;
        }

        scanResult = tairHashCluster.exhscan(foo, ">=", scanResult.getCursor(), scanParams);
        for (Entry<String, String> entry : scanResult.getResult()) {
            Assert.assertEquals("field" + j, entry.getKey());
            Assert.assertEquals("val" + j, entry.getValue());
            j++;
        }
    }

    // ======== common equal ========
    public boolean arrayContains(Collection<byte[]> array, byte[] expected) {
        for (byte[] a : array) {
            try {
                assertArrayEquals(a, expected);
                return true;
            } catch (AssertionError e) {

            }
        }
        return false;
    }

    public void assertByteSetEquals(Set<byte[]> expected, Set<byte[]> actual) {
        assertEquals(expected.size(), actual.size());
        Iterator<byte[]> e = expected.iterator();
        while (e.hasNext()) {
            byte[] next = e.next();
            boolean contained = false;
            for (byte[] element : expected) {
                if (Arrays.equals(next, element)) {
                    contained = true;
                }
            }
            if (!contained) {
                throw new ComparisonFailure("element is missing",
                    Arrays.toString(next), actual.toString());
            }
        }
    }

    public void assertByteListEquals(List<byte[]> expected, List<byte[]> actual) {
        assertEquals(expected.size(), actual.size());
        for (int n = 0; n < expected.size(); n++) {
            assertArrayEquals(expected.get(n), actual.get(n));
        }
    }
}
