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
    private String randomkey_;
    private byte[] randomKeyBinary_;

    public TairHashClusterTest() {
        randomkey_ = "randomkey_" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        randomKeyBinary_ = ("randomkey_" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        foo = "foo" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        bfoo = ("bfoo" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
    }

    //@Test
    //public void exhsetWithParams() {
    //    ExhsetParams params = new ExhsetParams();
    //    params.ex(10);
    //    assertEquals(1,
    //        (long)tairHashCluster.exhset(foo, "", "", params));
    //}
    //
    //@Test
    //public void exhsetwitnoactive() throws InterruptedException {
    //    // Binary
    //    ExhsetParams exhsetParams = new ExhsetParams();
    //    exhsetParams.ex(1);
    //    assertEquals(1, (long)tairHashCluster.exhset(bfoo, bbar, bcar, exhsetParams));
    //    Thread.sleep(2000);
    //    assertEquals(0, (long)tairHashCluster.exhlen(bfoo));
    //
    //    exhsetParams.noactive();
    //    assertEquals(1, (long)tairHashCluster.exhset(bfoo, bbar, bcar, exhsetParams));
    //    Thread.sleep(2000);
    //    assertEquals(1, (long)tairHashCluster.exhlen(bfoo));
    //    assertEquals(0, (long)tairHashCluster.exhlen(bfoo, true));
    //    assertEquals(false, tairHashCluster.exhexists(bfoo, bbar));
    //    assertEquals(0, (long)tairHashCluster.exhlen(bfoo));
    //}
    //
    //@Test
    //public void exhmsetwithopts() {
    //    // Binary
    //    List<ExhmsetwithoptsParams<byte[]>> bparams = new LinkedList<ExhmsetwithoptsParams<byte[]>>();
    //    ExhmsetwithoptsParams<byte[]> bparams1 = new ExhmsetwithoptsParams<byte[]>(bbar, bcar, 4, 0);
    //    ExhmsetwithoptsParams<byte[]> bparams2 = new ExhmsetwithoptsParams<byte[]>(bcar, bbar, 4, 0);
    //    bparams.add(bparams1);
    //    bparams.add(bparams2);
    //
    //    String bstatus = tairHashCluster.exhmsetwithopts(bfoo, bparams);
    //    assertEquals("OK", bstatus);
    //    assertEquals(true, Arrays.equals(bcar, tairHashCluster.exhget(bfoo, bbar)));
    //    assertEquals(true, Arrays.equals(bbar, tairHashCluster.exhget(bfoo, bcar)));
    //}
    //
    //@Test
    //public void exhgetwithver() {
    //    //Binary
    //    tairHashCluster.exhset(bfoo, bbar, bcar);
    //    ExhgetwithverResult<byte[]> bresult = tairHashCluster.exhgetwithver(bfoo, bbar);
    //    assertEquals(true, Arrays.equals(bcar, bresult.getValue()));
    //    assertEquals(1, bresult.getVer());
    //}
    //
    //@Test
    //public void exhver() {
    //    // binary
    //    tairHashCluster.exhset(bfoo, bbar, bcar);
    //    assertEquals(1, (long)tairHashCluster.exhver(bfoo, bbar));
    //}
    //
    //@Test
    //public void exhttl() {
    //    // binary
    //    tairHashCluster.exhset(bfoo, bbar, bcar);
    //    tairHashCluster.exhexpire(bfoo, bbar, 20);
    //    assertEquals(true, tairHashCluster.exhttl(bfoo, bbar) <= 20);
    //    assertEquals(true, tairHashCluster.exhttl(bfoo, bbar) > 0);
    //
    //}
    //
    //@Test
    //public void exhexpireAt() {
    //    // binary
    //    long unixTime = (System.currentTimeMillis() / 1000L) + 20;
    //    tairHashCluster.exhset(bfoo, bbar, bbar);
    //    Boolean status = tairHashCluster.exhexpireAt(bfoo, bbar, unixTime);
    //    assertEquals(true, status);
    //}
    //
    //@Test
    //public void exhexpireWithNoActive() throws InterruptedException {
    //    tairHashCluster.exhset(bfoo, bbar, bbar);
    //    assertEquals(true, tairHashCluster.exhexpire(bfoo, bbar, 1));
    //    Thread.sleep(2000);
    //    assertEquals(0, (long)tairHashCluster.exhlen(bfoo));
    //
    //    tairHashCluster.exhset(bfoo, bbar, bbar);
    //    assertEquals(true, tairHashCluster.exhexpire(bfoo, bbar, 1, true));
    //    Thread.sleep(2000);
    //    assertEquals(1, (long)tairHashCluster.exhlen(bfoo));
    //    assertEquals(0, (long)tairHashCluster.exhlen(bfoo, true));
    //    assertEquals(false, tairHashCluster.exhexists(bfoo, bbar));
    //    assertEquals(0, (long)tairHashCluster.exhlen(bfoo));
    //}
    //
    //@Test
    //public void exhexpireAtWithNoActive() throws InterruptedException {
    //    long unixTime = (System.currentTimeMillis() / 1000L) + 1;
    //    tairHashCluster.exhset(bfoo, bbar, bbar);
    //    assertEquals(true, tairHashCluster.exhexpireAt(bfoo, bbar, unixTime));
    //    Thread.sleep(2000);
    //    assertEquals(0, (long)tairHashCluster.exhlen(bfoo));
    //
    //    unixTime = (System.currentTimeMillis() / 1000L) + 1;
    //    tairHashCluster.exhset(bfoo, bbar, bbar);
    //    assertEquals(true, tairHashCluster.exhexpireAt(bfoo, bbar, unixTime, true));
    //    Thread.sleep(2000);
    //    assertEquals(1, (long)tairHashCluster.exhlen(bfoo));
    //    assertEquals(0, (long)tairHashCluster.exhlen(bfoo, true));
    //    assertEquals(false, tairHashCluster.exhexists(bfoo, bbar));
    //    assertEquals(0, (long)tairHashCluster.exhlen(bfoo));
    //}
    //
    //@Test
    //public void exhpexpireWithNoActive() throws InterruptedException {
    //    tairHashCluster.exhset(bfoo, bbar, bbar);
    //    assertEquals(true, tairHashCluster.exhpexpire(bfoo, bbar, 1000));
    //    Thread.sleep(2000);
    //    assertEquals(0, (long)tairHashCluster.exhlen(bfoo));
    //
    //    tairHashCluster.exhset(bfoo, bbar, bbar);
    //    assertEquals(true, tairHashCluster.exhpexpire(bfoo, bbar, 1, true));
    //    Thread.sleep(2000);
    //    assertEquals(1, (long)tairHashCluster.exhlen(bfoo));
    //    assertEquals(0, (long)tairHashCluster.exhlen(bfoo, true));
    //    assertEquals(false, tairHashCluster.exhexists(bfoo, bbar));
    //    assertEquals(0, (long)tairHashCluster.exhlen(bfoo));
    //}
    //
    //@Test
    //public void exhpexpireAtWithNoActive() throws InterruptedException {
    //    long unixTime = (System.currentTimeMillis() / 1000L) + 1000;
    //    tairHashCluster.exhset(bfoo, bbar, bbar);
    //    assertEquals(true, tairHashCluster.exhpexpireAt(bfoo, bbar, unixTime));
    //    Thread.sleep(2000);
    //    assertEquals(0, (long)tairHashCluster.exhlen(bfoo));
    //
    //    unixTime = (System.currentTimeMillis() / 1000L) + 1000;
    //    tairHashCluster.exhset(bfoo, bbar, bbar);
    //    assertEquals(true, tairHashCluster.exhpexpireAt(bfoo, bbar, unixTime, true));
    //    Thread.sleep(2000);
    //    assertEquals(1, (long)tairHashCluster.exhlen(bfoo));
    //    assertEquals(0, (long)tairHashCluster.exhlen(bfoo, true));
    //    assertEquals(false, tairHashCluster.exhexists(bfoo, bbar));
    //    assertEquals(0, (long)tairHashCluster.exhlen(bfoo));
    //}
    //
    //@Test
    //public void exhmgetwithver() {
    //    //Binary
    //    List<ExhmsetwithoptsParams<byte[]>> bparams = new LinkedList<ExhmsetwithoptsParams<byte[]>>();
    //    ExhmsetwithoptsParams<byte[]> bparams1 = new ExhmsetwithoptsParams<byte[]>(bbar, bcar, 4, 0);
    //    ExhmsetwithoptsParams<byte[]> bparams2 = new ExhmsetwithoptsParams<byte[]>(bcar, bbar, 4, 0);
    //    bparams.add(bparams1);
    //    bparams.add(bparams2);
    //
    //    String bstatus = tairHashCluster.exhmsetwithopts(bfoo, bparams);
    //    assertEquals("OK", bstatus);
    //    assertEquals(true, Arrays.equals(bcar, tairHashCluster.exhget(bfoo, bbar)));
    //    assertEquals(true, Arrays.equals(bbar, tairHashCluster.exhget(bfoo, bcar)));
    //    List<ExhgetwithverResult<byte[]>> bresults = tairHashCluster.exhmgetwithver(bfoo, bbar, bcar);
    //    assertEquals(2, bresults.size());
    //    assertEquals(1, bresults.get(0).getVer());
    //    assertEquals(true, Arrays.equals(bcar, bresults.get(0).getValue()));
    //    assertEquals(1, bresults.get(1).getVer());
    //    assertEquals(true, Arrays.equals(bbar, bresults.get(1).getValue()));
    //
    //}
    //
    //@Test
    //public void exhset() {
    //    // Binary
    //    long bstatus = tairHashCluster.exhset(bfoo, bbar, bcar);
    //    assertEquals(1, bstatus);
    //    bstatus = tairHashCluster.exhset(bfoo, bbar, bfoo);
    //    assertEquals(0, bstatus);
    //}
    //
    //@Test
    //public void exhsetparams() {
    //    // Binary
    //    long bstatus = tairHashCluster.exhset(bfoo, bbar, bcar, ExhsetParams.ExhsetParams().ver(1));
    //    assertEquals(1, bstatus);
    //    assertEquals(1, (long)tairHashCluster.exhver(bfoo, bbar));
    //    bstatus = tairHashCluster.exhset(bfoo, bbar, bfoo);
    //    assertEquals(2, (long)tairHashCluster.exhver(bfoo, bbar));
    //    assertEquals(0, bstatus);
    //}
    //
    //@Test
    //public void exhget() {
    //    // Binary
    //    tairHashCluster.exhset(bfoo, bbar, bcar);
    //    assertNull(tairHashCluster.exhget(bbar, bfoo));
    //    assertNull(tairHashCluster.exhget(bfoo, bcar));
    //    assertArrayEquals(bcar, tairHashCluster.exhget(bfoo, bbar));
    //}
    //
    //@Test
    //public void exhsetnx() {
    //    // Binary
    //    long bstatus = tairHashCluster.exhsetnx(bfoo, bbar, bcar);
    //    assertEquals(1, bstatus);
    //    assertArrayEquals(bcar, tairHashCluster.exhget(bfoo, bbar));
    //
    //    bstatus = tairHashCluster.exhsetnx(bfoo, bbar, bfoo);
    //    assertEquals(0, bstatus);
    //    assertArrayEquals(bcar, tairHashCluster.exhget(bfoo, bbar));
    //
    //    bstatus = tairHashCluster.exhsetnx(bfoo, bcar, bbar);
    //    assertEquals(1, bstatus);
    //    assertArrayEquals(bbar, tairHashCluster.exhget(bfoo, bcar));
    //
    //}
    //
    //@Test
    //public void exhmset() {
    //    // Binary
    //    Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>();
    //    bhash.put(bbar, bcar);
    //    bhash.put(bcar, bbar);
    //    String bstatus = tairHashCluster.exhmset(bfoo, bhash);
    //    assertEquals("OK", bstatus);
    //    assertArrayEquals(bcar, tairHashCluster.exhget(bfoo, bbar));
    //    assertArrayEquals(bbar, tairHashCluster.exhget(bfoo, bcar));
    //}
    //
    //@Test
    //public void exhmget() {
    //    // Binary
    //    Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>();
    //    bhash.put(bbar, bcar);
    //    bhash.put(bcar, bbar);
    //    tairHashCluster.exhmset(bfoo, bhash);
    //
    //    List<byte[]> bvalues = tairHashCluster.exhmget(bfoo, bbar, bcar, bfoo);
    //    List<byte[]> bexpected = new ArrayList<byte[]>();
    //    bexpected.add(bcar);
    //    bexpected.add(bbar);
    //    bexpected.add(null);
    //
    //    assertByteListEquals(bexpected, bvalues);
    //}
    //
    //@Test
    //public void exhincrBy() {
    //    // Binary
    //    long bvalue = tairHashCluster.exhincrBy(bfoo, bbar, 1);
    //    assertEquals(1, bvalue);
    //    bvalue = tairHashCluster.exhincrBy(bfoo, bbar, -1);
    //    assertEquals(0, bvalue);
    //    bvalue = tairHashCluster.exhincrBy(bfoo, bbar, -10);
    //    assertEquals(-10, bvalue);
    //
    //}
    //
    //@Test
    //public void exhincrByWithBoundary() {
    //    // Binary
    //    ExhincrByParams exhincrByParams = new ExhincrByParams();
    //    exhincrByParams.min(0);
    //    exhincrByParams.max(10);
    //
    //    try {
    //        tairHashCluster.exhincrBy(bfoo, bbar, 11, exhincrByParams);
    //    } catch (Exception e) {
    //        assertTrue(e.getMessage().contains("increment or decrement would overflow"));
    //    }
    //
    //    try {
    //        tairHashCluster.exhincrBy(bfoo, bbar, -1, exhincrByParams);
    //    } catch (Exception e) {
    //        assertTrue(e.getMessage().contains("increment or decrement would overflow"));
    //    }
    //
    //    assertEquals(5, (long)tairHashCluster.exhincrBy(bfoo, bbar, 5, exhincrByParams));
    //
    //    exhincrByParams.min(10);
    //    exhincrByParams.max(0);
    //
    //    try {
    //        tairHashCluster.exhincrBy(bfoo, bbar, 5, exhincrByParams);
    //    } catch (Exception e) {
    //        assertTrue(e.getMessage().contains("min value is bigger than max value"));
    //    }
    //}
    //
    //@Test
    //public void exhincrByWithExpire() throws InterruptedException {
    //    ExhincrByParams exhincrByParams = new ExhincrByParams();
    //    exhincrByParams.ex(1);
    //    assertEquals(5, (long)tairHashCluster.exhincrBy(bfoo, bbar, 5, exhincrByParams));
    //    // active expire
    //    Thread.sleep(2000);
    //    assertEquals(0, (long)tairHashCluster.exhlen(bfoo));
    //    // no active expire
    //    exhincrByParams.noactive();
    //    assertEquals(5, (long)tairHashCluster.exhincrBy(bfoo, bbar, 5, exhincrByParams));
    //    // active expire
    //    Thread.sleep(2000);
    //    assertEquals(1, (long)tairHashCluster.exhlen(bfoo));
    //    assertEquals(0, (long)tairHashCluster.exhlen(bfoo, true));
    //    assertEquals(false, tairHashCluster.exhexists(bfoo, bbar));
    //    assertEquals(0, (long)tairHashCluster.exhlen(bfoo));
    //}
    //
    //@Test
    //public void exhincrByWithVersion() {
    //    ExhincrByParams exhincrByParams = new ExhincrByParams();
    //    exhincrByParams.ver(1);
    //    assertEquals(5, (long)tairHashCluster.exhincrBy(bfoo, bbar, 5, exhincrByParams));
    //    assertEquals(10, (long)tairHashCluster.exhincrBy(bfoo, bbar, 5, exhincrByParams));
    //    assertEquals(15, (long)tairHashCluster.exhincrBy(bfoo, bbar, 5));
    //    try {
    //        tairHashCluster.exhincrBy(bfoo, bbar, 5, exhincrByParams);
    //    } catch (Exception e) {
    //        assertTrue(e.getMessage().contains("update version is stale"));
    //    }
    //    assertEquals(20, (long)tairHashCluster.exhincrBy(bfoo, bbar, 5, new ExhincrByParams().abs(5)));
    //    assertEquals(5, (long)tairHashCluster.exhver(bfoo, bbar));
    //    assertEquals(25, (long)tairHashCluster.exhincrBy(bfoo, bbar, 5, new ExhincrByParams().ver(0)));
    //    assertEquals(6, (long)tairHashCluster.exhver(bfoo, bbar));
    //}
    //
    //@Test
    //public void exhincrByFloat() {
    //    // Binary
    //    double bvalue = tairHashCluster.exhincrByFloat(bfoo, bbar, 1.5d);
    //    assertEquals(Double.compare(1.5d, bvalue), 0);
    //    bvalue = tairHashCluster.exhincrByFloat(bfoo, bbar, -1.5d);
    //    assertEquals(Double.compare(0d, bvalue), 0);
    //    bvalue = tairHashCluster.exhincrByFloat(bfoo, bbar, -10.7d);
    //    assertEquals(Double.compare(-10.7d, bvalue), 0);
    //
    //}
    //
    //@Test
    //public void exhincrByFloatWithBoundary() {
    //    // Binary
    //    ExhincrByFloatParams exhincrByFloatParams = new ExhincrByFloatParams();
    //    exhincrByFloatParams.min(0.1);
    //    exhincrByFloatParams.max(10.1);
    //
    //    try {
    //        tairHashCluster.exhincrByFloat(bfoo, bbar, 11.1, exhincrByFloatParams);
    //    } catch (Exception e) {
    //        assertTrue(e.getMessage().contains("increment or decrement would overflow"));
    //    }
    //
    //    try {
    //        tairHashCluster.exhincrByFloat(bfoo, bbar, -1.1, exhincrByFloatParams);
    //    } catch (Exception e) {
    //        assertTrue(e.getMessage().contains("increment or decrement would overflow"));
    //    }
    //
    //    assertEquals(Double.compare(5.1, tairHashCluster.exhincrByFloat(bfoo, bbar, 5.1, exhincrByFloatParams)), 0);
    //
    //    exhincrByFloatParams.min(10.1);
    //    exhincrByFloatParams.max(0.1);
    //
    //    try {
    //        tairHashCluster.exhincrByFloat(bfoo, bbar, 5.1, exhincrByFloatParams);
    //    } catch (Exception e) {
    //        assertTrue(e.getMessage().contains("min value is bigger than max value"));
    //    }
    //}
    //
    //@Test
    //public void exhincrByFloatWithExpire() throws InterruptedException {
    //    ExhincrByFloatParams exhincrByFloatParams = new ExhincrByFloatParams();
    //    exhincrByFloatParams.ex(1);
    //    assertEquals(Double.compare(5.1, tairHashCluster.exhincrByFloat(bfoo, bbar, 5.1, exhincrByFloatParams)), 0);
    //    // active expire
    //    Thread.sleep(2000);
    //    assertEquals(0, (long)tairHashCluster.exhlen(bfoo));
    //    // no active expire
    //    exhincrByFloatParams.noactive();
    //    assertEquals(Double.compare(5.1, tairHashCluster.exhincrByFloat(bfoo, bbar, 5.1, exhincrByFloatParams)), 0);
    //    // active expire
    //    Thread.sleep(2000);
    //    assertEquals(1, (long)tairHashCluster.exhlen(bfoo));
    //    assertEquals(0, (long)tairHashCluster.exhlen(bfoo, true));
    //    assertEquals(false, tairHashCluster.exhexists(bfoo, bbar));
    //    assertEquals(0, (long)tairHashCluster.exhlen(bfoo));
    //}
    //
    //@Test
    //public void exhincrByFloatWithVersion() {
    //    ExhincrByFloatParams exhincrByFloatParams = new ExhincrByFloatParams();
    //    exhincrByFloatParams.ver(1);
    //    assertEquals(Double.compare(5.1, tairHashCluster.exhincrByFloat(bfoo, bbar, 5.1, exhincrByFloatParams)), 0);
    //    assertEquals(Double.compare(10.2, tairHashCluster.exhincrByFloat(bfoo, bbar, 5.1, exhincrByFloatParams)), 0);
    //    assertEquals(Double.compare(15.3, tairHashCluster.exhincrByFloat(bfoo, bbar, 5.1)), 0);
    //    try {
    //        tairHashCluster.exhincrByFloat(bfoo, bbar, 5.1, exhincrByFloatParams);
    //    } catch (Exception e) {
    //        assertTrue(e.getMessage().contains("update version is stale"));
    //    }
    //    assertEquals(
    //        Double.compare(20.4, tairHashCluster.exhincrByFloat(bfoo, bbar, 5.1, new ExhincrByFloatParams().abs(5))),
    //        0);
    //    assertEquals(5, (long)tairHashCluster.exhver(bfoo, bbar));
    //    assertEquals(
    //        Double.compare(25.5, tairHashCluster.exhincrByFloat(bfoo, bbar, 5.1, new ExhincrByFloatParams().ver(0))),
    //        0);
    //    assertEquals(6, (long)tairHashCluster.exhver(bfoo, bbar));
    //}
    //
    //@Test
    //public void exhexists() {
    //    // Binary
    //    Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>();
    //    bhash.put(bbar, bcar);
    //    bhash.put(bcar, bbar);
    //    tairHashCluster.exhmset(bfoo, bhash);
    //
    //    assertFalse(tairHashCluster.exhexists(bbar, bfoo));
    //    assertFalse(tairHashCluster.exhexists(bfoo, bfoo));
    //    assertTrue(tairHashCluster.exhexists(bfoo, bbar));
    //
    //}
    //
    //@Test
    //public void exhdel() {
    //    // Binary
    //    Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>();
    //    bhash.put(bbar, bcar);
    //    bhash.put(bcar, bbar);
    //    tairHashCluster.exhmset(bfoo, bhash);
    //
    //    assertEquals(0, tairHashCluster.exhdel(bbar, bfoo).intValue());
    //    assertEquals(0, tairHashCluster.exhdel(bfoo, bfoo).intValue());
    //    assertEquals(1, tairHashCluster.exhdel(bfoo, bbar).intValue());
    //    assertNull(tairHashCluster.exhget(bfoo, bbar));
    //
    //}
    //
    //@Test
    //public void exhlen() {
    //    // Binary
    //    Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>();
    //    bhash.put(bbar, bcar);
    //    bhash.put(bcar, bbar);
    //    tairHashCluster.exhmset(bfoo, bhash);
    //
    //    assertEquals(0, tairHashCluster.exhlen(bbar).intValue());
    //    assertEquals(2, tairHashCluster.exhlen(bfoo).intValue());
    //
    //}
    //
    //@Test
    //public void exhkeys() {
    //    // Binary
    //    Map<byte[], byte[]> bhash = new LinkedHashMap<byte[], byte[]>();
    //    bhash.put(bbar, bcar);
    //    bhash.put(bcar, bbar);
    //    tairHashCluster.exhmset(bfoo, bhash);
    //
    //    Set<byte[]> bkeys = tairHashCluster.exhkeys(bfoo);
    //    Set<byte[]> bexpected = new LinkedHashSet<byte[]>();
    //    bexpected.add(bbar);
    //    bexpected.add(bcar);
    //    assertByteSetEquals(bexpected, bkeys);
    //}
    //
    //@Test
    //public void exhvals() {
    //    // Binary
    //    Map<byte[], byte[]> bhash = new LinkedHashMap<byte[], byte[]>();
    //    bhash.put(bbar, bcar);
    //    bhash.put(bcar, bbar);
    //    tairHashCluster.exhmset(bfoo, bhash);
    //
    //    List<byte[]> bvals = tairHashCluster.exhvals(bfoo);
    //
    //    assertEquals(2, bvals.size());
    //    assertTrue(arrayContains(bvals, bbar));
    //    assertTrue(arrayContains(bvals, bcar));
    //}
    //
    //@Test
    //public void exhgetAll() {
    //    // Binary
    //    Map<byte[], byte[]> bh = new HashMap<byte[], byte[]>();
    //    bh.put(bbar, bcar);
    //    bh.put(bcar, bbar);
    //    tairHashCluster.exhmset(bfoo, bh);
    //    Map<byte[], byte[]> bhash = tairHashCluster.exhgetAll(bfoo);
    //
    //    assertEquals(2, bhash.size());
    //    assertArrayEquals(bcar, bhash.get(bbar));
    //    assertArrayEquals(bbar, bhash.get(bcar));
    //}
    //
    //@Test
    //public void extestBinaryHstrLen() {
    //    Map<byte[], byte[]> values = new HashMap<byte[], byte[]>();
    //    values.put(bbar, bcar);
    //    tairHashCluster.exhmset(bfoo, values);
    //    Long response = tairHashCluster.exhstrlen(bfoo, bbar);
    //    assertEquals(4l, response.longValue());
    //}
    //
    //@Test
    //public void exhashBigKey() {
    //    long time = System.currentTimeMillis() % 604800;
    //    tairHashCluster.exhset(EXHASH_BIGKEY, UUID.randomUUID().toString(), UUID.randomUUID().toString(),
    //        ExhsetParams.ExhsetParams().ex((int)time));
    //}
    //
    //@Test
    //public void exhscanTest() {
    //    HashMap<String, String> map = new HashMap<String, String>();
    //    for (int i = 1; i < 10; i++) {
    //        map.put("field" + i, "val" + i);
    //    }
    //
    //    tairHashCluster.exhmset(foo, map);
    //
    //    ScanParams scanParams = new ScanParams().count(3);
    //    ScanResult<Entry<String, String>> scanResult = tairHashCluster.exhscan(foo, "^", "", scanParams);
    //    int j = 1;
    //    for (Entry<String, String> entry : scanResult.getResult()) {
    //        Assert.assertEquals("field" + j, entry.getKey());
    //        Assert.assertEquals("val" + j, entry.getValue());
    //        j++;
    //    }
    //
    //    scanResult = tairHashCluster.exhscan(foo, ">=", scanResult.getCursor(), scanParams);
    //    for (Entry<String, String> entry : scanResult.getResult()) {
    //        Assert.assertEquals("field" + j, entry.getKey());
    //        Assert.assertEquals("val" + j, entry.getValue());
    //        j++;
    //    }
    //
    //    scanResult = tairHashCluster.exhscan(foo, ">=", scanResult.getCursor(), scanParams);
    //    for (Entry<String, String> entry : scanResult.getResult()) {
    //        Assert.assertEquals("field" + j, entry.getKey());
    //        Assert.assertEquals("val" + j, entry.getValue());
    //        j++;
    //    }
    //}

    @Test
    public void exhsetwitnoactive() throws InterruptedException {
        // Binary
        ExhsetParams exhsetParams = new ExhsetParams();
        exhsetParams.ex(1);
        assertEquals(1, (long)tairHashCluster.exhset(bfoo, bbar, bcar, exhsetParams));
        Thread.sleep(2000);
        assertEquals(0, (long)tairHashCluster.exhlen(bfoo));

        exhsetParams.noactive();
        assertEquals(1, (long)tairHashCluster.exhset(bfoo, bbar, bcar, exhsetParams));
        Thread.sleep(2000);
        assertEquals(1, (long)tairHashCluster.exhlen(bfoo));
        assertEquals(0, (long)tairHashCluster.exhlen(bfoo, true));
        assertEquals(false, tairHashCluster.exhexists(bfoo, bbar));
        assertEquals(0, (long)tairHashCluster.exhlen(bfoo));
    }

    @Test
    public void exhmsetwithopts() {
        List<ExhmsetwithoptsParams<String>> params = new LinkedList<>();
        params.add(new ExhmsetwithoptsParams<>("foo", "bar", 0, 0));
        params.add(new ExhmsetwithoptsParams<>("bar", "foo", 0, 0));
        String status = tairHashCluster.exhmsetwithopts(foo, params);
        assertEquals("OK", status);
        assertEquals("bar", tairHashCluster.exhget(foo, "foo"));
        assertEquals("foo", tairHashCluster.exhget(foo, "bar"));

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
        tairHashCluster.exhset(foo, "bar", "car");
        ExhgetwithverResult<String> result = tairHashCluster.exhgetwithver(foo, "bar");
        Assert.assertEquals("car", result.getValue());

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
    public void exhexpireWithNoActive() throws InterruptedException {
        tairHashCluster.exhset(foo, "bbar", "bbar");
        assertEquals(true, tairHashCluster.exhexpire(foo, "bbar", 1));
        Thread.sleep(2000);
        assertEquals(0, (long)tairHashCluster.exhlen(bfoo));

        tairHashCluster.exhset(bfoo, bbar, bbar);
        assertEquals(true, tairHashCluster.exhexpire(bfoo, bbar, 1));
        Thread.sleep(2000);
        assertEquals(0, (long)tairHashCluster.exhlen(bfoo));

        tairHashCluster.exhset(bfoo, bbar, bbar);
        assertEquals(true, tairHashCluster.exhexpire(bfoo, bbar, 1, true));
        Thread.sleep(2000);
        assertEquals(1, (long)tairHashCluster.exhlen(bfoo));
        assertEquals(0, (long)tairHashCluster.exhlen(bfoo, true));
        assertEquals(false, tairHashCluster.exhexists(bfoo, bbar));
        assertEquals(0, (long)tairHashCluster.exhlen(bfoo));

        tairHashCluster.exhset(bfoo, bbar, bbar);
        assertEquals(true, tairHashCluster.exhexpire(new String(bfoo), new String(bbar), 1, true));
        Thread.sleep(2000);
        assertEquals(1, (long)tairHashCluster.exhlen(bfoo));
        assertEquals(0, (long)tairHashCluster.exhlen(bfoo, true));
        assertEquals(false, tairHashCluster.exhexists(bfoo, bbar));
        assertEquals(0, (long)tairHashCluster.exhlen(bfoo));
    }

    @Test
    public void exhexpireAtWithNoActive() throws InterruptedException {
        long unixTime = (System.currentTimeMillis() / 1000L) + 1;
        tairHashCluster.exhset(bfoo, bbar, bbar);
        assertEquals(true, tairHashCluster.exhexpireAt(bfoo, bbar, unixTime));
        Thread.sleep(2000);
        assertEquals(0, (long)tairHashCluster.exhlen(bfoo));

        unixTime = (System.currentTimeMillis() / 1000L) + 1;
        tairHashCluster.exhset(bfoo, bbar, bbar);
        assertEquals(true, tairHashCluster.exhexpireAt(bfoo, bbar, unixTime, true));
        Thread.sleep(2000);
        assertEquals(1, (long)tairHashCluster.exhlen(bfoo));
        assertEquals(0, (long)tairHashCluster.exhlen(bfoo, true));
        assertEquals(false, tairHashCluster.exhexists(bfoo, bbar));
        assertEquals(0, (long)tairHashCluster.exhlen(bfoo));
    }

    @Test
    public void exhpexpireWithNoActive() throws InterruptedException {
        tairHashCluster.exhset(foo, "bar", "car");
        assertTrue(tairHashCluster.exhpexpire(foo, "bar", 100));
        Thread.sleep(2000);
        assertEquals(0, (long)tairHashCluster.exhlen(foo));

        // Binary
        tairHashCluster.exhset(bfoo, bbar, bbar);
        assertEquals(true, tairHashCluster.exhpexpire(bfoo, bbar, 100));
        Thread.sleep(1000);
        assertEquals(0, (long)tairHashCluster.exhlen(bfoo));

        tairHashCluster.exhset(bfoo, bbar, bbar);
        assertEquals(true, tairHashCluster.exhpexpire(bfoo, bbar, 100, true));
        Thread.sleep(1000);
        assertEquals(1, (long)tairHashCluster.exhlen(bfoo));
        assertEquals(0, (long)tairHashCluster.exhlen(bfoo, true));
        assertEquals(false, tairHashCluster.exhexists(bfoo, bbar));
        assertEquals(0, (long)tairHashCluster.exhlen(bfoo));

        tairHashCluster.exhset(bfoo, bbar, bbar);
        assertEquals(true, tairHashCluster.exhpexpire(new String(bfoo), new String(bbar), 100, true));
        Thread.sleep(1000);
        assertEquals(1, (long)tairHashCluster.exhlen(bfoo));
        assertEquals(0, (long)tairHashCluster.exhlen(bfoo, true));
        assertEquals(false, tairHashCluster.exhexists(bfoo, bbar));
        assertEquals(0, (long)tairHashCluster.exhlen(bfoo));
    }

    @Test
    public void exhpexpireAtWithNoActive() throws InterruptedException {
        long unixTime = (System.currentTimeMillis() / 1000L) + 100;
        tairHashCluster.exhset(foo, "bbar", "bbar");
        assertEquals(true, tairHashCluster.exhpexpireAt(foo, "bbar", unixTime));
        Thread.sleep(1000);
        assertEquals(0, (long)tairHashCluster.exhlen(foo));

        // Binary
        unixTime = (System.currentTimeMillis() / 1000L) + 100;
        tairHashCluster.exhset(bfoo, bbar, bbar);
        assertEquals(true, tairHashCluster.exhpexpireAt(bfoo, bbar, unixTime));
        Thread.sleep(1000);
        assertEquals(0, (long)tairHashCluster.exhlen(bfoo));

        unixTime = (System.currentTimeMillis() / 1000L) + 100;
        tairHashCluster.exhset(bfoo, bbar, bbar);
        assertEquals(true, tairHashCluster.exhpexpireAt(bfoo, bbar, unixTime, true));
        Thread.sleep(1000);
        assertEquals(1, (long)tairHashCluster.exhlen(bfoo));
        assertEquals(0, (long)tairHashCluster.exhlen(bfoo, true));
        assertEquals(false, tairHashCluster.exhexists(bfoo, bbar));
        assertEquals(0, (long)tairHashCluster.exhlen(bfoo));

        unixTime = (System.currentTimeMillis() / 1000L) + 100;
        tairHashCluster.exhset(bfoo, bbar, bbar);
        assertEquals(true, tairHashCluster.exhpexpireAt(new String(bfoo), new String(bbar), unixTime, true));
        Thread.sleep(1000);
        assertEquals(1, (long)tairHashCluster.exhlen(bfoo));
        assertEquals(0, (long)tairHashCluster.exhlen(bfoo, true));
        assertEquals(false, tairHashCluster.exhexists(bfoo, bbar));
        assertEquals(0, (long)tairHashCluster.exhlen(bfoo));
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
        tairHashCluster.exhset(bfoo, bbar, bcar);
        assertNull(tairHashCluster.exhget(bbar, bfoo));
        assertNull(tairHashCluster.exhget(bfoo, bcar));
        assertArrayEquals(bcar, tairHashCluster.exhget(bfoo, bbar));
    }

    @Test
    public void exhsetnx() {
        long bstatus = tairHashCluster.exhsetnx(new String(bfoo), new String(bbar), new String(bcar));
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
    public void exhincrByWithBoundary() {
        // Binary
        ExhincrByParams exhincrByParams = new ExhincrByParams();
        exhincrByParams.min(0);
        exhincrByParams.max(10);

        try {
            tairHashCluster.exhincrBy(bfoo, bbar, 11, exhincrByParams);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("increment or decrement would overflow"));
        }

        try {
            tairHashCluster.exhincrBy(bfoo, bbar, -1, exhincrByParams);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("increment or decrement would overflow"));
        }

        assertEquals(5, (long)tairHashCluster.exhincrBy(bfoo, bbar, 5, exhincrByParams));

        exhincrByParams.min(10);
        exhincrByParams.max(0);

        try {
            tairHashCluster.exhincrBy(bfoo, bbar, 5, exhincrByParams);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("min value is bigger than max value"));
        }
    }

    @Test
    public void exhincrByWithExpire() throws InterruptedException {
        ExhincrByParams exhincrByParams = new ExhincrByParams();
        exhincrByParams.ex(1);
        assertEquals(5, (long)tairHashCluster.exhincrBy(bfoo, bbar, 5, exhincrByParams));
        // active expire
        Thread.sleep(2000);
        assertEquals(0, (long)tairHashCluster.exhlen(bfoo));
        // no active expire
        exhincrByParams.noactive();
        assertEquals(5, (long)tairHashCluster.exhincrBy(bfoo, bbar, 5, exhincrByParams));
        // active expire
        Thread.sleep(2000);
        assertEquals(1, (long)tairHashCluster.exhlen(bfoo));
        assertEquals(0, (long)tairHashCluster.exhlen(bfoo, true));
        assertEquals(false, tairHashCluster.exhexists(bfoo, bbar));
        assertEquals(0, (long)tairHashCluster.exhlen(bfoo));
    }

    @Test
    public void exhincrByWithVersion() {
        ExhincrByParams exhincrByParams = new ExhincrByParams();
        exhincrByParams.ver(1);
        assertEquals(5, (long)tairHashCluster.exhincrBy(bfoo, bbar, 5, exhincrByParams));
        assertEquals(10, (long)tairHashCluster.exhincrBy(bfoo, bbar, 5, exhincrByParams));
        assertEquals(15, (long)tairHashCluster.exhincrBy(bfoo, bbar, 5));
        try {
            tairHashCluster.exhincrBy(bfoo, bbar, 5, exhincrByParams);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("update version is stale"));
        }
        assertEquals(20, (long)tairHashCluster.exhincrBy(bfoo, bbar, 5, new ExhincrByParams().abs(5)));
        assertEquals(5, (long)tairHashCluster.exhver(bfoo, bbar));
        assertEquals(25, (long)tairHashCluster.exhincrBy(bfoo, bbar, 5, new ExhincrByParams().ver(0)));
        assertEquals(6, (long)tairHashCluster.exhver(bfoo, bbar));
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
    public void exhincrByFloatWithBoundary() {
        // Binary
        ExhincrByFloatParams exhincrByFloatParams = new ExhincrByFloatParams();
        exhincrByFloatParams.min(0.1);
        exhincrByFloatParams.max(10.1);

        try {
            tairHashCluster.exhincrByFloat(bfoo, bbar, 11.1, exhincrByFloatParams);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("increment or decrement would overflow"));
        }

        try {
            tairHashCluster.exhincrByFloat(bfoo, bbar, -1.1, exhincrByFloatParams);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("increment or decrement would overflow"));
        }

        assertEquals(Double.compare(5.1, tairHashCluster.exhincrByFloat(bfoo, bbar, 5.1, exhincrByFloatParams)), 0);

        exhincrByFloatParams.min(10.1);
        exhincrByFloatParams.max(0.1);

        try {
            tairHashCluster.exhincrByFloat(bfoo, bbar, 5.1, exhincrByFloatParams);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("min value is bigger than max value"));
        }
    }

    @Test
    public void exhincrByFloatWithExpire() throws InterruptedException {
        ExhincrByFloatParams exhincrByFloatParams = new ExhincrByFloatParams();
        exhincrByFloatParams.ex(1);
        assertEquals(Double.compare(5.1, tairHashCluster.exhincrByFloat(bfoo, bbar, 5.1, exhincrByFloatParams)), 0);
        // active expire
        Thread.sleep(2000);
        assertEquals(0, (long)tairHashCluster.exhlen(bfoo));
        // no active expire
        exhincrByFloatParams.noactive();
        assertEquals(Double.compare(5.1, tairHashCluster.exhincrByFloat(bfoo, bbar, 5.1, exhincrByFloatParams)), 0);
        // active expire
        Thread.sleep(2000);
        assertEquals(1, (long)tairHashCluster.exhlen(bfoo));
        assertEquals(0, (long)tairHashCluster.exhlen(bfoo, true));
        assertEquals(false, tairHashCluster.exhexists(bfoo, bbar));
        assertEquals(0, (long)tairHashCluster.exhlen(bfoo));
    }

    @Test
    public void exhincrByFloatWithVersion() {
        ExhincrByFloatParams exhincrByFloatParams = new ExhincrByFloatParams();
        exhincrByFloatParams.ver(1);
        assertEquals(Double.compare(5.1, tairHashCluster.exhincrByFloat(bfoo, bbar, 5.1, exhincrByFloatParams)), 0);
        assertEquals(Double.compare(10.2, tairHashCluster.exhincrByFloat(bfoo, bbar, 5.1, exhincrByFloatParams)), 0);
        assertEquals(Double.compare(15.3, tairHashCluster.exhincrByFloat(bfoo, bbar, 5.1)), 0);
        try {
            tairHashCluster.exhincrByFloat(bfoo, bbar, 5.1, exhincrByFloatParams);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("update version is stale"));
        }
        assertEquals(Double.compare(20.4, tairHashCluster.exhincrByFloat(bfoo, bbar, 5.1, new ExhincrByFloatParams().abs(5))),
            0);
        assertEquals(5, (long)tairHashCluster.exhver(bfoo, bbar));
        assertEquals(Double.compare(25.5, tairHashCluster.exhincrByFloat(bfoo, bbar, 5.1, new ExhincrByFloatParams().ver(0))),
            0);
        assertEquals(6, (long)tairHashCluster.exhver(bfoo, bbar));
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

    @Test
    public void exhgetAllException() {
        tairHashCluster.exhgetAll(randomkey_);
        tairHashCluster.exhgetAll(randomKeyBinary_);

        try {
            jedisCluster.set(randomkey_, "bar");
            tairHashCluster.exhgetAll(randomkey_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }

        try {
            jedisCluster.set(randomKeyBinary_, "bar".getBytes());
            tairHashCluster.exhgetAll(randomKeyBinary_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhsetException() {

        try {
            jedisCluster.set(randomkey_, "bar");
            tairHashCluster.exhset(randomkey_, "", "");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhsetnxException() {
        try {
            jedisCluster.set(randomkey_, "bar");
            tairHashCluster.exhsetnx(randomkey_, "", "");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhmsetException() {
        try {
            jedisCluster.set(randomkey_, "bar");
            tairHashCluster.exhmset(randomkey_, new HashMap<String, String>());
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhmsetwithoptsException() {
        try {
            jedisCluster.set(randomkey_, "bar");
            tairHashCluster.exhmsetwithopts(randomkey_, new LinkedList<ExhmsetwithoptsParams<String>>());
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhpexpireatException() {
        try {
            jedisCluster.set(randomkey_, "bar");
            tairHashCluster.exhpexpireAt(randomkey_, "", 10);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhpexpireException() {
        tairHashCluster.exhpexpire(randomkey_, "", 10);

        try {
            jedisCluster.set(randomkey_, "bar");
            tairHashCluster.exhpexpire(randomkey_, "", 10);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhexpireatException() {
        tairHashCluster.exhpexpireAt(randomkey_, "", 10);

        try {
            jedisCluster.set(randomkey_, "bar");
            tairHashCluster.exhpexpireAt(randomkey_, "", 10);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhexpireException() {
        tairHashCluster.exhexpire(randomkey_, "", 10);

        try {
            jedisCluster.set(randomkey_, "bar");
            tairHashCluster.exhexpire(randomkey_, "", 10);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhpttlException() {
        tairHashCluster.exhpttl(randomkey_, "");

        try {
            jedisCluster.set(randomkey_, "bar");
            tairHashCluster.exhpttl(randomkey_, "");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhttlException() {
        tairHashCluster.exhttl(randomkey_, "");

        try {
            jedisCluster.set(randomkey_, "bar");
            tairHashCluster.exhttl(randomkey_, "");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhverException() {
        tairHashCluster.exhver(randomkey_, "");

        try {
            jedisCluster.set(randomkey_, "bar");
            tairHashCluster.exhver(randomkey_, "");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhsetverException() {
        tairHashCluster.exhsetver(randomkey_, "", 10);

        try {
            jedisCluster.set(randomkey_, "bar");
            tairHashCluster.exhsetver(randomkey_, "", 10);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhincrbyException() {
        tairHashCluster.exhincrBy(randomkey_,"", 10);

        try {
            jedisCluster.set(randomkey_, "bar");
            tairHashCluster.exhincrBy(randomkey_, "", 10);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhincrbyfloatException() {
        tairHashCluster.exhincrByFloat(randomkey_, "", 10.0);

        try {
            jedisCluster.set(randomkey_, "bar");
            tairHashCluster.exhincrByFloat(randomkey_, "", 10.0);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhgetException() {
        tairHashCluster.exhget(randomkey_, "");

        try {
            jedisCluster.set(randomkey_, "bar");
            tairHashCluster.exhget(randomkey_, "");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhgetwithverException() {
        tairHashCluster.exhgetwithver(randomkey_, "");

        try {
            jedisCluster.set(randomkey_, "bar");
            tairHashCluster.exhgetwithver(randomkey_, "");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhmgetException() {
        tairHashCluster.exhmget(randomkey_, "");

        try {
            jedisCluster.set(randomkey_, "bar");
            tairHashCluster.exhmget(randomkey_, "");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhdelException() {
        tairHashCluster.exhdel(randomkey_, "");

        try {
            jedisCluster.set(randomkey_, "bar");
            tairHashCluster.exhdel(randomkey_, "");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhlenException() {
        tairHashCluster.exhlen(randomkey_);

        try {
            jedisCluster.set(randomkey_, "bar");
            tairHashCluster.exhlen(randomkey_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhexistsException() {
        tairHashCluster.exhexists(randomkey_, "");

        try {
            jedisCluster.set(randomkey_, "bar");
            tairHashCluster.exhexists(randomkey_, "");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhstrlenException() {
        tairHashCluster.exhstrlen(randomkey_, "");

        try {
            jedisCluster.set(randomkey_, "bar");
            tairHashCluster.exhstrlen(randomkey_, "");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhkeysException() {
        tairHashCluster.exhkeys(randomkey_);

        try {
            jedisCluster.set(randomkey_, "bar");
            tairHashCluster.exhkeys(randomkey_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhvalsException() {
        tairHashCluster.exhvals(randomkey_);

        try {
            jedisCluster.set(randomkey_, "bar");
            tairHashCluster.exhvals(randomkey_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhgetallException() {
        tairHashCluster.exhgetAll(randomkey_);

        try {
            jedisCluster.set(randomkey_, "bar");
            tairHashCluster.exhgetAll(randomkey_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhmgetwithverException() {
        try {
            jedisCluster.set(randomkey_, "bar");
            tairHashCluster.exhmgetwithver(randomkey_, "", "");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void exhscanException() {
        try {
            jedisCluster.set(randomkey_, "bar");
            tairHashCluster.exhscan(randomkey_, "", "");
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
