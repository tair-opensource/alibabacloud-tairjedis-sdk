package com.aliyun.tair.tests.tairhash;

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

import com.aliyun.tair.tairhash.params.*;
import org.junit.Assert;
import org.junit.ComparisonFailure;
import org.junit.Test;
import redis.clients.jedis.Response;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class TairHashTest extends TairHashTestBase {
    String foo;
    byte[] bfoo = {0x01, 0x02, 0x03, 0x04};
    byte[] bbar = {0x05, 0x06, 0x07, 0x08};
    final byte[] bcar = {0x09, 0x0A, 0x0B, 0x0C};

    final byte[] bbar1 = {0x05, 0x06, 0x07, 0x08, 0x0A};
    final byte[] bbar2 = {0x05, 0x06, 0x07, 0x08, 0x0B};
    final byte[] bbar3 = {0x05, 0x06, 0x07, 0x08, 0x0C};
    final byte[] bbarstar = {0x05, 0x06, 0x07, 0x08, '*'};
    private static final String EXHASH_BIGKEY = "EXHASH_BIGKEY";

    private String randomkey_;
    private byte[] randomKeyBinary_;

    public TairHashTest() {
        randomkey_ = "randomkey_" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        randomKeyBinary_ = ("randomkey_" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        foo = "foo" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        bfoo = ("bfoo" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
    }

    @Test
    public void exhsetwitnoactive() throws InterruptedException {
        // Binary
        ExhsetParams exhsetParams = new ExhsetParams();
        exhsetParams.ex(1);
        assertEquals(1, (long)tairHash.exhset(bfoo, bbar, bcar, exhsetParams));
        Thread.sleep(2000);
        assertEquals(0, (long)tairHash.exhlen(bfoo));

        exhsetParams.noactive();
        assertEquals(1, (long)tairHash.exhset(bfoo, bbar, bcar, exhsetParams));
        Thread.sleep(2000);
        assertEquals(1, (long)tairHash.exhlen(bfoo));
        assertEquals(0, (long)tairHash.exhlen(bfoo, true));
        assertEquals(false, tairHash.exhexists(bfoo, bbar));
        assertEquals(0, (long)tairHash.exhlen(bfoo));
    }

    @Test
    public void exhmsetwithopts() {
        List<ExhmsetwithoptsParams<String>> params = new LinkedList<>();
        params.add(new ExhmsetwithoptsParams<>("foo", "bar", 0, 0));
        params.add(new ExhmsetwithoptsParams<>("bar", "foo", 0, 0));
        String status = tairHash.exhmsetwithopts(foo, params);
        assertEquals("OK", status);
        assertEquals("bar", tairHash.exhget(foo, "foo"));
        assertEquals("foo", tairHash.exhget(foo, "bar"));

        // Binary
        List<ExhmsetwithoptsParams<byte[]>> bparams = new LinkedList<ExhmsetwithoptsParams<byte[]>>();
        ExhmsetwithoptsParams<byte[]> bparams1 = new ExhmsetwithoptsParams<byte[]>(bbar, bcar, 4, 0);
        ExhmsetwithoptsParams<byte[]> bparams2 = new ExhmsetwithoptsParams<byte[]>(bcar, bbar, 4, 0);
        bparams.add(bparams1);
        bparams.add(bparams2);

        String bstatus = tairHash.exhmsetwithopts(bfoo, bparams);
        assertEquals("OK", bstatus);
        assertEquals(true, Arrays.equals(bcar, tairHash.exhget(bfoo, bbar)));
        assertEquals(true, Arrays.equals(bbar, tairHash.exhget(bfoo, bcar)));
    }

    @Test
    public void exhgetwithver() {
        tairHash.exhset(foo, "bar", "car");
        ExhgetwithverResult<String> result = tairHash.exhgetwithver(foo, "bar");
        Assert.assertEquals("car", result.getValue());

        //Binary
        tairHash.exhset(bfoo, bbar, bcar);
        ExhgetwithverResult<byte[]> bresult = tairHash.exhgetwithver(bfoo, bbar);
        assertEquals(true, Arrays.equals(bcar, bresult.getValue()));
        assertEquals(1, bresult.getVer());
    }

    @Test
    public void exhver() {
        // binary
        tairHash.exhset(bfoo, bbar, bcar);
        assertEquals(1, (long)tairHash.exhver(bfoo, bbar));

    }

    @Test
    public void exhttl() {
        // binary
        tairHash.exhset(bfoo, bbar, bcar);
        tairHash.exhexpire(bfoo, bbar, 20);
        assertEquals(true, tairHash.exhttl(bfoo, bbar) <= 20);
        assertEquals(true, tairHash.exhttl(bfoo, bbar) > 0);

    }

    @Test
    public void exhexpireAt() {
        // binary
        long unixTime = (System.currentTimeMillis() / 1000L) + 20;
        tairHash.exhset(bfoo, bbar, bbar);
        Boolean status = tairHash.exhexpireAt(bfoo, bbar, unixTime);
        assertEquals(true, status);
    }

    @Test
    public void exhexpireWithNoActive() throws InterruptedException {
        tairHash.exhset(foo, "bbar", "bbar");
        assertEquals(true, tairHash.exhexpire(foo, "bbar", 1));
        Thread.sleep(2000);
        assertEquals(0, (long)tairHash.exhlen(bfoo));

        tairHash.exhset(bfoo, bbar, bbar);
        assertEquals(true, tairHash.exhexpire(bfoo, bbar, 1));
        Thread.sleep(2000);
        assertEquals(0, (long)tairHash.exhlen(bfoo));

        tairHash.exhset(bfoo, bbar, bbar);
        assertEquals(true, tairHash.exhexpire(bfoo, bbar, 1, true));
        Thread.sleep(2000);
        assertEquals(1, (long)tairHash.exhlen(bfoo));
        assertEquals(0, (long)tairHash.exhlen(bfoo, true));
        assertEquals(false, tairHash.exhexists(bfoo, bbar));
        assertEquals(0, (long)tairHash.exhlen(bfoo));

        tairHash.exhset(bfoo, bbar, bbar);
        assertEquals(true, tairHash.exhexpire(new String(bfoo), new String(bbar), 1, true));
        Thread.sleep(2000);
        assertEquals(1, (long)tairHash.exhlen(bfoo));
        assertEquals(0, (long)tairHash.exhlen(bfoo, true));
        assertEquals(false, tairHash.exhexists(bfoo, bbar));
        assertEquals(0, (long)tairHash.exhlen(bfoo));
    }

    @Test
    public void exhexpireAtWithNoActive() throws InterruptedException {
        long unixTime = (System.currentTimeMillis() / 1000L) + 1;
        tairHash.exhset(bfoo, bbar, bbar);
        assertEquals(true, tairHash.exhexpireAt(bfoo, bbar, unixTime));
        Thread.sleep(2000);
        assertEquals(0, (long)tairHash.exhlen(bfoo));

        unixTime = (System.currentTimeMillis() / 1000L) + 1;
        tairHash.exhset(bfoo, bbar, bbar);
        assertEquals(true, tairHash.exhexpireAt(bfoo, bbar, unixTime, true));
        Thread.sleep(2000);
        assertEquals(1, (long)tairHash.exhlen(bfoo));
        assertEquals(0, (long)tairHash.exhlen(bfoo, true));
        assertEquals(false, tairHash.exhexists(bfoo, bbar));
        assertEquals(0, (long)tairHash.exhlen(bfoo));
    }

    @Test
    public void exhpexpireWithNoActive() throws InterruptedException {
        tairHash.exhset(foo, "bar", "car");
        assertTrue(tairHash.exhpexpire(foo, "bar", 100));
        Thread.sleep(1000);
        assertEquals(0, (long)tairHash.exhlen(foo));

        // Binary
        tairHash.exhset(bfoo, bbar, bbar);
        assertEquals(true, tairHash.exhpexpire(bfoo, bbar, 100));
        Thread.sleep(1000);
        assertEquals(0, (long)tairHash.exhlen(bfoo));

        tairHash.exhset(bfoo, bbar, bbar);
        assertEquals(true, tairHash.exhpexpire(bfoo, bbar, 100, true));
        Thread.sleep(1000);
        assertEquals(1, (long)tairHash.exhlen(bfoo));
        assertEquals(0, (long)tairHash.exhlen(bfoo, true));
        assertEquals(false, tairHash.exhexists(bfoo, bbar));
        assertEquals(0, (long)tairHash.exhlen(bfoo));

        tairHash.exhset(bfoo, bbar, bbar);
        assertEquals(true, tairHash.exhpexpire(new String(bfoo), new String(bbar), 100, true));
        Thread.sleep(1000);
        assertEquals(1, (long)tairHash.exhlen(bfoo));
        assertEquals(0, (long)tairHash.exhlen(bfoo, true));
        assertEquals(false, tairHash.exhexists(bfoo, bbar));
        assertEquals(0, (long)tairHash.exhlen(bfoo));
    }

    @Test
    public void exhpexpireAtWithNoActive() throws InterruptedException {
        long unixTime = (System.currentTimeMillis() / 1000L) + 100;
        tairHash.exhset(foo, "bbar", "bbar");
        assertEquals(true, tairHash.exhpexpireAt(foo, "bbar", unixTime));
        Thread.sleep(1000);
        assertEquals(0, (long)tairHash.exhlen(foo));

        // Binary
        unixTime = (System.currentTimeMillis() / 1000L) + 100;
        tairHash.exhset(bfoo, bbar, bbar);
        assertEquals(true, tairHash.exhpexpireAt(bfoo, bbar, unixTime));
        Thread.sleep(1000);
        assertEquals(0, (long)tairHash.exhlen(bfoo));

        unixTime = (System.currentTimeMillis() / 1000L) + 100;
        tairHash.exhset(bfoo, bbar, bbar);
        assertEquals(true, tairHash.exhpexpireAt(bfoo, bbar, unixTime, true));
        Thread.sleep(1000);
        assertEquals(1, (long)tairHash.exhlen(bfoo));
        assertEquals(0, (long)tairHash.exhlen(bfoo, true));
        assertEquals(false, tairHash.exhexists(bfoo, bbar));
        assertEquals(0, (long)tairHash.exhlen(bfoo));

        unixTime = (System.currentTimeMillis() / 1000L) + 100;
        tairHash.exhset(bfoo, bbar, bbar);
        assertEquals(true, tairHash.exhpexpireAt(new String(bfoo), new String(bbar), unixTime, true));
        Thread.sleep(1000);
        assertEquals(1, (long)tairHash.exhlen(bfoo));
        assertEquals(0, (long)tairHash.exhlen(bfoo, true));
        assertEquals(false, tairHash.exhexists(bfoo, bbar));
        assertEquals(0, (long)tairHash.exhlen(bfoo));
    }

    @Test
    public void exhgetwithverPipeline() {
        //Binary
        tairHashPipeline.exhset(bfoo, bbar, bcar);
        Response<ExhgetwithverResult<byte[]>> bresponse = tairHashPipeline.exhgetwithver(bfoo, bbar);
        tairHashPipeline.sync();
        ExhgetwithverResult<byte[]> bresult = bresponse.get();
        assertEquals(true, Arrays.equals(bcar, bresult.getValue()));
        assertEquals(1, bresult.getVer());
    }

    @Test
    public void exhmgetwithver() {
        //Binary
        List<ExhmsetwithoptsParams<byte[]>> bparams = new LinkedList<ExhmsetwithoptsParams<byte[]>>();
        ExhmsetwithoptsParams<byte[]> bparams1 = new ExhmsetwithoptsParams<byte[]>(bbar, bcar, 4, 0);
        ExhmsetwithoptsParams<byte[]> bparams2 = new ExhmsetwithoptsParams<byte[]>(bcar, bbar, 4, 0);
        bparams.add(bparams1);
        bparams.add(bparams2);

        String bstatus = tairHash.exhmsetwithopts(bfoo, bparams);
        assertEquals("OK", bstatus);
        assertEquals(true, Arrays.equals(bcar, tairHash.exhget(bfoo, bbar)));
        assertEquals(true, Arrays.equals(bbar, tairHash.exhget(bfoo, bcar)));
        List<ExhgetwithverResult<byte[]>> bresults = tairHash.exhmgetwithver(bfoo, bbar, bcar);
        assertEquals(2, bresults.size());
        assertEquals(1, bresults.get(0).getVer());
        assertEquals(true, Arrays.equals(bcar, bresults.get(0).getValue()));
        assertEquals(1, bresults.get(1).getVer());
        assertEquals(true, Arrays.equals(bbar, bresults.get(1).getValue()));

    }

    @Test
    public void exhmgetwithverPipeline() {
        //Binary
        List<ExhmsetwithoptsParams<byte[]>> bparams = new LinkedList<ExhmsetwithoptsParams<byte[]>>();
        ExhmsetwithoptsParams<byte[]> bparams1 = new ExhmsetwithoptsParams<byte[]>(bbar, bcar, 4, 0);
        ExhmsetwithoptsParams<byte[]> bparams2 = new ExhmsetwithoptsParams<byte[]>(bcar, bbar, 4, 0);
        bparams.add(bparams1);
        bparams.add(bparams2);

        Response<String> bstatus = tairHashPipeline.exhmsetwithopts(bfoo, bparams);
        Response<List<ExhgetwithverResult<byte[]>>> bresults = tairHashPipeline.exhmgetwithver(bfoo, bbar, bcar);
        tairHashPipeline.sync();

        assertEquals("OK", bstatus.get());
        assertEquals(true, Arrays.equals(bcar, tairHash.exhget(bfoo, bbar)));
        assertEquals(true, Arrays.equals(bbar, tairHash.exhget(bfoo, bcar)));

        assertEquals(2, bresults.get().size());
        assertEquals(1, bresults.get().get(0).getVer());
        assertEquals(true, Arrays.equals(bcar, bresults.get().get(0).getValue()));
        assertEquals(1, bresults.get().get(1).getVer());
        assertEquals(true, Arrays.equals(bbar, bresults.get().get(1).getValue()));

    }

    @Test
    public void exhset() {
        // Binary
        long bstatus = tairHash.exhset(bfoo, bbar, bcar);
        assertEquals(1, bstatus);
        bstatus = tairHash.exhset(bfoo, bbar, bfoo);
        assertEquals(0, bstatus);
    }

    @Test
    public void exhsetparams() {
        // Binary
        long bstatus = tairHash.exhset(bfoo, bbar, bcar, ExhsetParams.ExhsetParams().ver(1));
        assertEquals(1, bstatus);
        assertEquals(1, (long)tairHash.exhver(bfoo, bbar));
        bstatus = tairHash.exhset(bfoo, bbar, bfoo);
        assertEquals(2, (long)tairHash.exhver(bfoo, bbar));
        assertEquals(0, bstatus);
    }

    @Test
    public void exhget() {
        tairHash.exhset(bfoo, bbar, bcar);
        assertNull(tairHash.exhget(bbar, bfoo));
        assertNull(tairHash.exhget(bfoo, bcar));
        assertArrayEquals(bcar, tairHash.exhget(bfoo, bbar));
    }

    @Test
    public void exhsetnx() {
        long bstatus = tairHash.exhsetnx(new String(bfoo), new String(bbar), new String(bcar));
        assertEquals(1, bstatus);
        assertArrayEquals(bcar, tairHash.exhget(bfoo, bbar));

        bstatus = tairHash.exhsetnx(bfoo, bbar, bfoo);
        assertEquals(0, bstatus);
        assertArrayEquals(bcar, tairHash.exhget(bfoo, bbar));

        bstatus = tairHash.exhsetnx(bfoo, bcar, bbar);
        assertEquals(1, bstatus);
        assertArrayEquals(bbar, tairHash.exhget(bfoo, bcar));

    }

    @Test
    public void exhmset() {
        // Binary
        Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>();
        bhash.put(bbar, bcar);
        bhash.put(bcar, bbar);
        String bstatus = tairHash.exhmset(bfoo, bhash);
        assertEquals("OK", bstatus);
        assertArrayEquals(bcar, tairHash.exhget(bfoo, bbar));
        assertArrayEquals(bbar, tairHash.exhget(bfoo, bcar));
    }

    @Test
    public void exhmget() {
        // Binary
        Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>();
        bhash.put(bbar, bcar);
        bhash.put(bcar, bbar);
        tairHash.exhmset(bfoo, bhash);

        List<byte[]> bvalues = tairHash.exhmget(bfoo, bbar, bcar, bfoo);
        List<byte[]> bexpected = new ArrayList<byte[]>();
        bexpected.add(bcar);
        bexpected.add(bbar);
        bexpected.add(null);

        assertByteListEquals(bexpected, bvalues);
    }

    @Test
    public void exhincrBy() {
        // Binary
        long bvalue = tairHash.exhincrBy(bfoo, bbar, 1);
        assertEquals(1, bvalue);
        bvalue = tairHash.exhincrBy(bfoo, bbar, -1);
        assertEquals(0, bvalue);
        bvalue = tairHash.exhincrBy(bfoo, bbar, -10);
        assertEquals(-10, bvalue);
    }

    @Test
    public void exhincrByWithBoundary() {
        // Binary
        ExhincrByParams exhincrByParams = new ExhincrByParams();
        exhincrByParams.min(0);
        exhincrByParams.max(10);

        try {
            tairHash.exhincrBy(bfoo, bbar, 11, exhincrByParams);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("increment or decrement would overflow"));
        }

        try {
            tairHash.exhincrBy(bfoo, bbar, -1, exhincrByParams);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("increment or decrement would overflow"));
        }

        assertEquals(5, (long)tairHash.exhincrBy(bfoo, bbar, 5, exhincrByParams));

        exhincrByParams.min(10);
        exhincrByParams.max(0);

        try {
            tairHash.exhincrBy(bfoo, bbar, 5, exhincrByParams);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("min value is bigger than max value"));
        }
    }

    @Test
    public void exhincrByWithExpire() throws InterruptedException {
        ExhincrByParams exhincrByParams = new ExhincrByParams();
        exhincrByParams.ex(1);
        assertEquals(5, (long)tairHash.exhincrBy(bfoo, bbar, 5, exhincrByParams));
        // active expire
        Thread.sleep(2000);
        assertEquals(0, (long)tairHash.exhlen(bfoo));
        // no active expire
        exhincrByParams.noactive();
        assertEquals(5, (long)tairHash.exhincrBy(bfoo, bbar, 5, exhincrByParams));
        // active expire
        Thread.sleep(2000);
        assertEquals(1, (long)tairHash.exhlen(bfoo));
        assertEquals(0, (long)tairHash.exhlen(bfoo, true));
        assertEquals(false, tairHash.exhexists(bfoo, bbar));
        assertEquals(0, (long)tairHash.exhlen(bfoo));
    }

    @Test
    public void exhincrByWithVersion() {
        ExhincrByParams exhincrByParams = new ExhincrByParams();
        exhincrByParams.ver(1);
        assertEquals(5, (long)tairHash.exhincrBy(bfoo, bbar, 5, exhincrByParams));
        assertEquals(10, (long)tairHash.exhincrBy(bfoo, bbar, 5, exhincrByParams));
        assertEquals(15, (long)tairHash.exhincrBy(bfoo, bbar, 5));
        try {
            tairHash.exhincrBy(bfoo, bbar, 5, exhincrByParams);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("update version is stale"));
        }
        assertEquals(20, (long)tairHash.exhincrBy(bfoo, bbar, 5, new ExhincrByParams().abs(5)));
        assertEquals(5, (long)tairHash.exhver(bfoo, bbar));
        assertEquals(25, (long)tairHash.exhincrBy(bfoo, bbar, 5, new ExhincrByParams().ver(0)));
        assertEquals(6, (long)tairHash.exhver(bfoo, bbar));
    }

    @Test
    public void exhincrByFloat() {
        // Binary
        double bvalue = tairHash.exhincrByFloat(bfoo, bbar, 1.5d);
        assertEquals(Double.compare(1.5d, bvalue), 0);
        bvalue = tairHash.exhincrByFloat(bfoo, bbar, -1.5d);
        assertEquals(Double.compare(0d, bvalue), 0);
        bvalue = tairHash.exhincrByFloat(bfoo, bbar, -10.7d);
        assertEquals(Double.compare(-10.7d, bvalue), 0);
    }

    @Test
    public void exhincrByFloatWithBoundary() {
        // Binary
        ExhincrByFloatParams exhincrByFloatParams = new ExhincrByFloatParams();
        exhincrByFloatParams.min(0.1);
        exhincrByFloatParams.max(10.1);

        try {
            tairHash.exhincrByFloat(bfoo, bbar, 11.1, exhincrByFloatParams);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("increment or decrement would overflow"));
        }

        try {
            tairHash.exhincrByFloat(bfoo, bbar, -1.1, exhincrByFloatParams);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("increment or decrement would overflow"));
        }

        assertEquals(Double.compare(5.1, tairHash.exhincrByFloat(bfoo, bbar, 5.1, exhincrByFloatParams)), 0);

        exhincrByFloatParams.min(10.1);
        exhincrByFloatParams.max(0.1);

        try {
            tairHash.exhincrByFloat(bfoo, bbar, 5.1, exhincrByFloatParams);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("min value is bigger than max value"));
        }
    }

    @Test
    public void exhincrByFloatWithExpire() throws InterruptedException {
        ExhincrByFloatParams exhincrByFloatParams = new ExhincrByFloatParams();
        exhincrByFloatParams.ex(1);
        assertEquals(Double.compare(5.1, tairHash.exhincrByFloat(bfoo, bbar, 5.1, exhincrByFloatParams)), 0);
        // active expire
        Thread.sleep(2000);
        assertEquals(0, (long)tairHash.exhlen(bfoo));
        // no active expire
        exhincrByFloatParams.noactive();
        assertEquals(Double.compare(5.1, tairHash.exhincrByFloat(bfoo, bbar, 5.1, exhincrByFloatParams)), 0);
        // active expire
        Thread.sleep(2000);
        assertEquals(1, (long)tairHash.exhlen(bfoo));
        assertEquals(0, (long)tairHash.exhlen(bfoo, true));
        assertEquals(false, tairHash.exhexists(bfoo, bbar));
        assertEquals(0, (long)tairHash.exhlen(bfoo));
    }

    @Test
    public void exhincrByFloatWithVersion() {
        ExhincrByFloatParams exhincrByFloatParams = new ExhincrByFloatParams();
        exhincrByFloatParams.ver(1);
        assertEquals(Double.compare(5.1, tairHash.exhincrByFloat(bfoo, bbar, 5.1, exhincrByFloatParams)), 0);
        assertEquals(Double.compare(10.2, tairHash.exhincrByFloat(bfoo, bbar, 5.1, exhincrByFloatParams)), 0);
        assertEquals(Double.compare(15.3, tairHash.exhincrByFloat(bfoo, bbar, 5.1)), 0);
        try {
            tairHash.exhincrByFloat(bfoo, bbar, 5.1, exhincrByFloatParams);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("update version is stale"));
        }
        assertEquals(Double.compare(20.4, tairHash.exhincrByFloat(bfoo, bbar, 5.1, new ExhincrByFloatParams().abs(5))),
            0);
        assertEquals(5, (long)tairHash.exhver(bfoo, bbar));
        assertEquals(Double.compare(25.5, tairHash.exhincrByFloat(bfoo, bbar, 5.1, new ExhincrByFloatParams().ver(0))),
            0);
        assertEquals(6, (long)tairHash.exhver(bfoo, bbar));
    }

    @Test
    public void exhexists() {
        // Binary
        Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>();
        bhash.put(bbar, bcar);
        bhash.put(bcar, bbar);
        tairHash.exhmset(bfoo, bhash);

        assertFalse(tairHash.exhexists(bbar, bfoo));
        assertFalse(tairHash.exhexists(bfoo, bfoo));
        assertTrue(tairHash.exhexists(bfoo, bbar));

    }

    @Test
    public void exhdel() {
        // Binary
        Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>();
        bhash.put(bbar, bcar);
        bhash.put(bcar, bbar);
        tairHash.exhmset(bfoo, bhash);

        assertEquals(0, tairHash.exhdel(bbar, bfoo).intValue());
        assertEquals(0, tairHash.exhdel(bfoo, bfoo).intValue());
        assertEquals(1, tairHash.exhdel(bfoo, bbar).intValue());
        assertNull(tairHash.exhget(bfoo, bbar));

    }

    @Test
    public void exhlen() {
        // Binary
        Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>();
        bhash.put(bbar, bcar);
        bhash.put(bcar, bbar);
        tairHash.exhmset(bfoo, bhash);

        assertEquals(0, tairHash.exhlen(bbar).intValue());
        assertEquals(2, tairHash.exhlen(bfoo).intValue());

    }

    @Test
    public void exhkeys() {
        // Binary
        Map<byte[], byte[]> bhash = new LinkedHashMap<byte[], byte[]>();
        bhash.put(bbar, bcar);
        bhash.put(bcar, bbar);
        tairHash.exhmset(bfoo, bhash);

        Set<byte[]> bkeys = tairHash.exhkeys(bfoo);
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
        tairHash.exhmset(bfoo, bhash);

        List<byte[]> bvals = tairHash.exhvals(bfoo);

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
        tairHash.exhmset(bfoo, bh);
        Map<byte[], byte[]> bhash = tairHash.exhgetAll(bfoo);

        assertEquals(2, bhash.size());
        assertArrayEquals(bcar, bhash.get(bbar));
        assertArrayEquals(bbar, bhash.get(bcar));
    }

    @Test
    public void exhgetAllPipeline() {
        Map<byte[], byte[]> bh = new HashMap<byte[], byte[]>();
        bh.put(bbar, bcar);
        bh.put(bcar, bbar);
        tairHash.exhmset(bfoo, bh);
        Response<Map<byte[], byte[]>> bhashResponse = tairHashPipeline.exhgetAll(bfoo);
        tairHashPipeline.sync();
        Map<byte[], byte[]> bhash = bhashResponse.get();

        assertEquals(2, bhash.size());
        assertArrayEquals(bcar, bhash.get(bbar));
        assertArrayEquals(bbar, bhash.get(bcar));
    }

    @Test
    public void extestBinaryHstrLen() {
        Map<byte[], byte[]> values = new HashMap<byte[], byte[]>();
        values.put(bbar, bcar);
        tairHash.exhmset(bfoo, values);
        Long response = tairHash.exhstrlen(bfoo, bbar);
        assertEquals(4l, response.longValue());
    }

    @Test
    public void exhashBigKey() {
        long time = System.currentTimeMillis() % 604800;
        tairHash.exhset(EXHASH_BIGKEY, UUID.randomUUID().toString(), UUID.randomUUID().toString(),
            ExhsetParams.ExhsetParams().ex((int)time));
    }

    @Test
    public void exhscanTest() {
        HashMap<String, String> map = new HashMap<String, String>();
        for (int i = 1; i < 10; i++) {
            map.put("field" + i, "val" + i);
        }

        tairHash.exhmset(foo, map);

        ScanParams scanParams = new ScanParams().count(3);
        ScanResult<Entry<String, String>> scanResult = tairHash.exhscan(foo, "^", "", scanParams);
        int j = 1;
        for (Entry<String, String> entry : scanResult.getResult()) {
            Assert.assertEquals("field" + j, entry.getKey());
            Assert.assertEquals("val" + j, entry.getValue());
            j++;
        }

        scanResult = tairHash.exhscan(foo, ">=", scanResult.getCursor(), scanParams);
        for (Entry<String, String> entry : scanResult.getResult()) {
            Assert.assertEquals("field" + j, entry.getKey());
            Assert.assertEquals("val" + j, entry.getValue());
            j++;
        }

        scanResult = tairHash.exhscan(foo, ">=", scanResult.getCursor(), scanParams);
        for (Entry<String, String> entry : scanResult.getResult()) {
            Assert.assertEquals("field" + j, entry.getKey());
            Assert.assertEquals("val" + j, entry.getValue());
            j++;
        }

        // TEST NOVAL
        ExhscanParams exhscanParams = new ExhscanParams().count(3).noval();
        scanResult = tairHash.exhscan(foo, "^", "", exhscanParams);
        Assert.assertTrue(scanResult.getResult().isEmpty());
        Assert.assertTrue(!scanResult.getCursor().isEmpty());
        scanResult = tairHash.exhscan(foo, ">=", scanResult.getCursor(), exhscanParams);
        Assert.assertTrue(scanResult.getResult().isEmpty());
        Assert.assertTrue(!scanResult.getCursor().isEmpty());
        scanResult = tairHash.exhscan(foo, ">=", scanResult.getCursor(), exhscanParams);
        Assert.assertTrue(scanResult.getResult().isEmpty());
        Assert.assertTrue(scanResult.getCursor().isEmpty());
    }

    @Test
    public void exhgetAllException() {
        tairHash.exhgetAll(randomkey_);
        tairHash.exhgetAll(randomKeyBinary_);

        try {
            jedis.set(randomkey_, "bar");
            tairHash.exhgetAll(randomkey_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }

        try {
            jedis.set(randomKeyBinary_, "bar".getBytes());
            tairHash.exhgetAll(randomKeyBinary_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhsetException() {

        try {
            jedis.set(randomkey_, "bar");
            tairHash.exhset(randomkey_, "", "");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhsetnxException() {
        try {
            jedis.set(randomkey_, "bar");
            tairHash.exhsetnx(randomkey_, "", "");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhmsetException() {
        try {
            jedis.set(randomkey_, "bar");
            tairHash.exhmset(randomkey_, new HashMap<String, String>());
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhmsetwithoptsException() {
        try {
            jedis.set(randomkey_, "bar");
            tairHash.exhmsetwithopts(randomkey_, new LinkedList<ExhmsetwithoptsParams<String>>());
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhpexpireatException() {
        try {
            jedis.set(randomkey_, "bar");
            tairHash.exhpexpireAt(randomkey_, "", 10);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhpexpireException() {
        tairHash.exhpexpire(randomkey_, "", 10);

        try {
            jedis.set(randomkey_, "bar");
            tairHash.exhpexpire(randomkey_, "", 10);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhexpireatException() {
        tairHash.exhpexpireAt(randomkey_, "", 10);

        try {
            jedis.set(randomkey_, "bar");
            tairHash.exhpexpireAt(randomkey_, "", 10);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhexpireException() {
        tairHash.exhexpire(randomkey_, "", 10);

        try {
            jedis.set(randomkey_, "bar");
            tairHash.exhexpire(randomkey_, "", 10);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhpttlException() {
        tairHash.exhpttl(randomkey_, "");

        try {
            jedis.set(randomkey_, "bar");
            tairHash.exhpttl(randomkey_, "");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhttlException() {
        tairHash.exhttl(randomkey_, "");

        try {
            jedis.set(randomkey_, "bar");
            tairHash.exhttl(randomkey_, "");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhverException() {
        tairHash.exhver(randomkey_, "");

        try {
            jedis.set(randomkey_, "bar");
            tairHash.exhver(randomkey_, "");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhsetverException() {
        tairHash.exhsetver(randomkey_, "", 10);

        try {
            jedis.set(randomkey_, "bar");
            tairHash.exhsetver(randomkey_, "", 10);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhincrbyException() {
        tairHash.exhincrBy(randomkey_,"", 10);

        try {
            jedis.set(randomkey_, "bar");
            tairHash.exhincrBy(randomkey_, "", 10);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhincrbyfloatException() {
        tairHash.exhincrByFloat(randomkey_, "", 10.0);

        try {
            jedis.set(randomkey_, "bar");
            tairHash.exhincrByFloat(randomkey_, "", 10.0);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhgetException() {
        tairHash.exhget(randomkey_, "");

        try {
            jedis.set(randomkey_, "bar");
            tairHash.exhget(randomkey_, "");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhgetwithverException() {
        tairHash.exhgetwithver(randomkey_, "");

        try {
            jedis.set(randomkey_, "bar");
            tairHash.exhgetwithver(randomkey_, "");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhmgetException() {
        tairHash.exhmget(randomkey_, "");

        try {
            jedis.set(randomkey_, "bar");
            tairHash.exhmget(randomkey_, "");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhdelException() {
        tairHash.exhdel(randomkey_, "");

        try {
            jedis.set(randomkey_, "bar");
            tairHash.exhdel(randomkey_, "");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhlenException() {
        tairHash.exhlen(randomkey_);

        try {
            jedis.set(randomkey_, "bar");
            tairHash.exhlen(randomkey_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhexistsException() {
        tairHash.exhexists(randomkey_, "");

        try {
            jedis.set(randomkey_, "bar");
            tairHash.exhexists(randomkey_, "");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhstrlenException() {
        tairHash.exhstrlen(randomkey_, "");

        try {
            jedis.set(randomkey_, "bar");
            tairHash.exhstrlen(randomkey_, "");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhkeysException() {
        tairHash.exhkeys(randomkey_);

        try {
            jedis.set(randomkey_, "bar");
            tairHash.exhkeys(randomkey_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhvalsException() {
        tairHash.exhvals(randomkey_);

        try {
            jedis.set(randomkey_, "bar");
            tairHash.exhvals(randomkey_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhgetallException() {
        tairHash.exhgetAll(randomkey_);

        try {
            jedis.set(randomkey_, "bar");
            tairHash.exhgetAll(randomkey_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhmgetwithverException() {
        try {
            jedis.set(randomkey_, "bar");
            tairHash.exhmgetwithver(randomkey_, "", "");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhscanException() {
        try {
            jedis.set(randomkey_, "bar");
            tairHash.exhscan(randomkey_, "", "");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
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
