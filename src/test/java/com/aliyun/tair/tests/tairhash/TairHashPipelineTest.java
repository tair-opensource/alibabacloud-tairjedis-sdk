package com.aliyun.tair.tests.tairhash;

import com.aliyun.tair.tairhash.params.ExhincrByFloatParams;
import com.aliyun.tair.tairhash.params.ExhincrByParams;
import com.aliyun.tair.tairhash.params.ExhsetParams;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.Response;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class TairHashPipelineTest extends TairHashTestBase {
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

    public TairHashPipelineTest() {
        randomkey_ = "randomkey_" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        randomKeyBinary_ = ("randomkey_" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        foo = "foo" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        bfoo = ("bfoo" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
    }

    @Ignore
    @Test
    public void exhsetwitnoactivePipeline() throws InterruptedException {
        // Binary
        ExhsetParams exhsetParams = new ExhsetParams();
        exhsetParams.ex(1);
        tairHashPipeline.exhset(bfoo, bbar, bcar, exhsetParams);
        tairHashPipeline.sync();
        Thread.sleep(2000);
        tairHashPipeline.exhlen(bfoo);

        exhsetParams.noactive();
        tairHashPipeline.exhset(bfoo, bbar, bcar, exhsetParams);
        tairHashPipeline.sync();
        Thread.sleep(2000);

        tairHashPipeline.exhlen(bfoo);
        tairHashPipeline.exhlen(bfoo, true);
        tairHashPipeline.exhexists(bfoo, bbar);
        tairHashPipeline.exhlen(bfoo);

        List<Object> objs = tairHashPipeline.syncAndReturnAll();
        assertEquals((long)1, objs.get(0));
        assertEquals((long)0, objs.get(1));
        assertEquals(false, objs.get(2));
        assertEquals((long)0, objs.get(3));
    }

    @Ignore
    @Test
    public void exhexpirewitnoactivePipeline() throws InterruptedException {
        tairHashPipeline.exhset(bfoo, bbar, bbar);
        tairHashPipeline.exhexpire(bfoo, bbar, 1);
        tairHashPipeline.sync();
        Thread.sleep(2000);
        assertEquals(1, (long)tairHash.exhlen(bfoo));

        tairHashPipeline.exhset(bfoo, bbar, bbar);
        tairHashPipeline.exhexpire(bfoo, bbar, 1, true);
        tairHashPipeline.sync();
        Thread.sleep(2000);

        tairHashPipeline.exhlen(bfoo);
        tairHashPipeline.exhlen(bfoo, true);
        tairHashPipeline.exhexists(bfoo, bbar);
        tairHashPipeline.exhlen(bfoo);

        List<Object> objs = tairHashPipeline.syncAndReturnAll();
        assertEquals((long)0, objs.get(0));
        assertEquals((long)0, objs.get(1));
        assertEquals(false, objs.get(2));
        assertEquals((long)0, objs.get(3));
    }

    @Ignore
    @Test
    public void exhexpireAtWithNoActivePipeline() throws InterruptedException {
        long unixTime = (System.currentTimeMillis() / 1000L) + 1;
        tairHashPipeline.exhset(bfoo, bbar, bbar);
        tairHashPipeline.exhexpireAt(bfoo, bbar, unixTime);
        tairHashPipeline.sync();
        Thread.sleep(2000);
        assertEquals(1, (long)tairHash.exhlen(bfoo));

        unixTime = (System.currentTimeMillis() / 1000L) + 1;
        tairHashPipeline.exhset(bfoo, bbar, bbar);
        tairHashPipeline.exhexpireAt(bfoo, bbar, unixTime, true);
        tairHashPipeline.sync();
        Thread.sleep(2000);

        tairHashPipeline.exhlen(bfoo);
        tairHashPipeline.exhlen(bfoo, true);
        tairHashPipeline.exhexists(bfoo, bbar);
        tairHashPipeline.exhlen(bfoo);

        List<Object> objs = tairHashPipeline.syncAndReturnAll();
        assertEquals((long)0, objs.get(0));
        assertEquals((long)0, objs.get(1));
        assertEquals(false, objs.get(2));
        assertEquals((long)0, objs.get(3));
    }

    @Ignore
    @Test
    public void exhpexpireWithNoActivePipeline() throws InterruptedException {
        tairHashPipeline.exhset(bfoo, bbar, bbar);
        tairHashPipeline.exhpexpire(bfoo, bbar, 1000);
        tairHashPipeline.sync();
        Thread.sleep(2000);
        assertEquals(0, (long)tairHash.exhlen(bfoo));

        tairHashPipeline.exhset(bfoo, bbar, bbar);
        tairHashPipeline.exhpexpire(bfoo, bbar, 1, true);
        tairHashPipeline.sync();
        Thread.sleep(2000);

        tairHashPipeline.exhlen(bfoo);
        tairHashPipeline.exhlen(bfoo, true);
        tairHashPipeline.exhexists(bfoo, bbar);
        tairHashPipeline.exhlen(bfoo);

        List<Object> objs = tairHashPipeline.syncAndReturnAll();
        assertEquals((long)1, objs.get(0));
        assertEquals((long)0, objs.get(1));
        assertEquals(false, objs.get(2));
        assertEquals((long)0, objs.get(3));
    }

    @Ignore
    @Test
    public void exhpexpireAtWithNoActivePipeline() throws InterruptedException {
        long unixTime = (System.currentTimeMillis() / 1000L) + 1000;
        tairHashPipeline.exhset(bfoo, bbar, bbar);
        tairHashPipeline.exhpexpireAt(bfoo, bbar, unixTime);
        tairHashPipeline.sync();
        Thread.sleep(2000);
        assertEquals(1, (long)tairHash.exhlen(bfoo));

        unixTime = (System.currentTimeMillis() / 1000L) + 1000;
        tairHashPipeline.exhset(bfoo, bbar, bbar);
        tairHashPipeline.exhpexpireAt(bfoo, bbar, unixTime, true);
        tairHashPipeline.sync();
        Thread.sleep(2000);

        tairHashPipeline.exhlen(bfoo);
        tairHashPipeline.exhlen(bfoo, true);
        tairHashPipeline.exhexists(bfoo, bbar);
        tairHashPipeline.exhlen(bfoo);

        List<Object> objs = tairHashPipeline.syncAndReturnAll();
        assertEquals((long)0, objs.get(0));
        assertEquals((long)0, objs.get(1));
        assertEquals(false, objs.get(2));
        assertEquals((long)0, objs.get(3));
    }

    @Test
    public void exhincrByWithBoundaryPipeline() {
        // Binary
        ExhincrByParams exhincrByParams = new ExhincrByParams();
        exhincrByParams.min(0);
        exhincrByParams.max(10);

        try {
            tairHashPipeline.exhincrBy(bfoo, bbar, 11, exhincrByParams);
            tairHashPipeline.sync();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("increment or decrement would overflow"));
        }

        try {
            tairHashPipeline.exhincrBy(bfoo, bbar, -1, exhincrByParams);
            tairHashPipeline.sync();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("increment or decrement would overflow"));
        }

        Response<Long> res = tairHashPipeline.exhincrBy(bfoo, bbar, 5, exhincrByParams);
        tairHashPipeline.sync();
        assertEquals(5, (long)res.get());

        exhincrByParams.min(10);
        exhincrByParams.max(0);

        try {
            tairHashPipeline.exhincrBy(bfoo, bbar, 5, exhincrByParams);
            tairHashPipeline.sync();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("min value is bigger than max value"));
        }
    }

    @Ignore
    @Test
    public void exhincrByWithExpirePipeline() throws InterruptedException {
        ExhincrByParams exhincrByParams = new ExhincrByParams();
        exhincrByParams.ex(1);
        Response<Long> res = tairHashPipeline.exhincrBy(bfoo, bbar, 5, exhincrByParams);
        tairHashPipeline.sync();
        assertEquals(5, (long)res.get());
        // active expire
        Thread.sleep(2000);
        assertEquals(1, (long)tairHash.exhlen(bfoo));
        // no active expire
        exhincrByParams.noactive();
        res = tairHashPipeline.exhincrBy(bfoo, bbar, 5, exhincrByParams);
        tairHashPipeline.sync();
        assertEquals(5, (long)res.get());
        // active expire
        Thread.sleep(2000);
        tairHashPipeline.exhlen(bfoo);
        tairHashPipeline.exhlen(bfoo, true);
        tairHashPipeline.exhexists(bfoo, bbar);
        tairHashPipeline.exhlen(bfoo);

        List<Object> objs = tairHashPipeline.syncAndReturnAll();
        assertEquals((long)1, objs.get(0));
        assertEquals((long)0, objs.get(1));
        assertEquals(false, objs.get(2));
        assertEquals((long)0, objs.get(3));
    }

    @Test
    public void exhincrByWithVersionPipeline() {
        ExhincrByParams exhincrByParams = new ExhincrByParams();
        exhincrByParams.ver(1);
        tairHashPipeline.exhincrBy(bfoo, bbar, 5, exhincrByParams);
        tairHashPipeline.exhincrBy(bfoo, bbar, 5, exhincrByParams);
        tairHashPipeline.exhincrBy(bfoo, bbar, 5);
        List<Object> objs = tairHashPipeline.syncAndReturnAll();
        assertEquals((long)5, objs.get(0));
        assertEquals((long)10, objs.get(1));
        assertEquals((long)15, objs.get(2));
        try {
            tairHashPipeline.exhincrBy(bfoo, bbar, 5, exhincrByParams);
            tairHashPipeline.sync();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("update version is stale"));
        }

        tairHashPipeline.exhincrBy(bfoo, bbar, 5, new ExhincrByParams().abs(5));
        tairHashPipeline.exhver(bfoo, bbar);
        tairHashPipeline.exhincrBy(bfoo, bbar, 5, new ExhincrByParams().ver(0));
        tairHashPipeline.exhver(bfoo, bbar);

        objs = tairHashPipeline.syncAndReturnAll();

        assertEquals((long)20, objs.get(0));
        assertEquals((long)5, objs.get(1));
        assertEquals((long)25, objs.get(2));
        assertEquals((long)6, objs.get(3));
    }

    @Test
    public void exhincrByFloatWithBoundaryPipeline() {
        // Binary
        ExhincrByFloatParams exhincrByFloatParams = new ExhincrByFloatParams();
        exhincrByFloatParams.min(0.1);
        exhincrByFloatParams.max(10.1);

        try {
            tairHashPipeline.exhincrByFloat(bfoo, bbar, 11.1, exhincrByFloatParams);
            tairHashPipeline.sync();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("increment or decrement would overflow"));
        }

        try {
            tairHashPipeline.exhincrByFloat(bfoo, bbar, -1.1, exhincrByFloatParams);
            tairHashPipeline.sync();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("increment or decrement would overflow"));
        }

        Response<Double> res = tairHashPipeline.exhincrByFloat(bfoo, bbar, 5.1, exhincrByFloatParams);
        tairHashPipeline.sync();
        assertEquals(0, Double.compare(5.1, res.get()));

        exhincrByFloatParams.min(10.1);
        exhincrByFloatParams.max(0.1);

        try {
            tairHashPipeline.exhincrByFloat(bfoo, bbar, 5.1, exhincrByFloatParams);
            tairHashPipeline.sync();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("min value is bigger than max value"));
        }
    }

    @Ignore
    @Test
    public void exhincrByFloatWithExpirePipeline() throws InterruptedException {
        ExhincrByFloatParams exhincrByFloatParams = new ExhincrByFloatParams();
        exhincrByFloatParams.ex(1);
        Response<Double> res = tairHashPipeline.exhincrByFloat(bfoo, bbar, 5.1, exhincrByFloatParams);
        tairHashPipeline.sync();
        assertEquals(0, Double.compare(5.1, res.get()));
        // active expire
        Thread.sleep(2000);
        assertEquals(1, (long)tairHash.exhlen(bfoo));
        // no active expire
        exhincrByFloatParams.noactive();
        res = tairHashPipeline.exhincrByFloat(bfoo, bbar, 5.1, exhincrByFloatParams);
        tairHashPipeline.sync();
        assertEquals(0, Double.compare(5.1, res.get()));
        // active expire
        Thread.sleep(2000);
        tairHashPipeline.exhlen(bfoo);
        tairHashPipeline.exhlen(bfoo, true);
        tairHashPipeline.exhexists(bfoo, bbar);
        tairHashPipeline.exhlen(bfoo);

        List<Object> objs = tairHashPipeline.syncAndReturnAll();
        assertEquals((long)0, objs.get(0));
        assertEquals((long)0, objs.get(1));
        assertEquals(false, objs.get(2));
        assertEquals((long)0, objs.get(3));
    }

    @Test
    public void exhincrByFloatWithVersionPipeline() {
        ExhincrByFloatParams exhincrByFloatParams = new ExhincrByFloatParams();
        exhincrByFloatParams.ver(1);
        tairHashPipeline.exhincrByFloat(bfoo, bbar, 5.1, exhincrByFloatParams);
        tairHashPipeline.exhincrByFloat(bfoo, bbar, 5.1, exhincrByFloatParams);
        tairHashPipeline.exhincrByFloat(bfoo, bbar, 5.1);
        List<Object> objs = tairHashPipeline.syncAndReturnAll();
        assertEquals(5.1, (double)objs.get(0), 0.001);
        assertEquals(10.2, (double)objs.get(1), 0.001);
        assertEquals(15.3, (double)objs.get(2), 0.001);
        try {
            tairHashPipeline.exhincrByFloat(bfoo, bbar, 5.1, exhincrByFloatParams);
            tairHashPipeline.sync();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("update version is stale"));
        }

        tairHashPipeline.exhincrByFloat(bfoo, bbar, 5.1, new ExhincrByFloatParams().abs(5));
        tairHashPipeline.exhver(bfoo, bbar);
        tairHashPipeline.exhincrByFloat(bfoo, bbar, 5.1, new ExhincrByFloatParams().ver(0));
        tairHashPipeline.exhver(bfoo, bbar);

        objs = tairHashPipeline.syncAndReturnAll();

        assertEquals(0, Double.compare(20.4, (Double)objs.get(0)));
        assertEquals((long)5, objs.get(1));
        assertEquals(0, Double.compare(25.5, (Double)objs.get(2)));
        assertEquals((long)6, objs.get(3));
    }

    @Test
    public void tairHashPipelineTest() {
        Response<Long> r1 = tairHashPipeline.exhsetnx(foo, "bar", "car");
        Response<Long> r2 = tairHashPipeline.exhset(foo, "bar", "car");
        Response<String> r3 = tairHashPipeline.exhmset(foo, new HashMap<String, String>() {{
            put("car", "bar");
        }});
        Response<String> r4 = tairHashPipeline.exhmset(foo.getBytes(), new HashMap<byte[], byte[]>() {{
            put("car".getBytes(), "bar".getBytes());
        }});

        tairHashPipeline.sync();
        Assert.assertEquals(1, (long)r1.get());
        Assert.assertEquals(0, (long)r2.get());
        Assert.assertEquals("OK", r3.get());
        Assert.assertEquals("OK", r4.get());
    }

}
