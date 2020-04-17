package com.aliyun.tair.tests.tairbloom;

import com.aliyun.tair.tairbloom.params.BfinsertParams;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.*;

public class TairBloomTest extends TairBloomTestBase {
    private String bbf;
    private byte[] bcf = "bcf".getBytes();
    private String randomkey_;
    private byte[] randomKeyBinary_;

    public TairBloomTest() {
        randomkey_ = "randomkey_" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        randomKeyBinary_ = ("randomkey_" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        bbf = "bbf" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        bcf = ("bcf" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
    }

    @Test
    public void bfaddTest() throws Exception {
        String ret = "";
        Boolean ret_bool = false;
        // String
        ret = tairBloom.bfreserve(bbf, 100, 0.001);
        assertEquals("OK", ret);
        ret_bool = tairBloom.bfadd(bbf, "val1");
        assertEquals(true, ret_bool);
        ret_bool = tairBloom.bfexists(bbf, "val1");
        assertEquals(true, ret_bool);
        ret_bool = tairBloom.bfexists(bbf, "val2");
        assertEquals(false, ret_bool);

        // binary
        ret = tairBloom.bfreserve(bcf, 100, 0.001);
        assertEquals("OK", ret);
        ret_bool = tairBloom.bfadd(bcf, "val1".getBytes());
        assertEquals(true, ret_bool);
        ret_bool = tairBloom.bfexists(bcf, "val1".getBytes());
        assertEquals(true, ret_bool);
        ret_bool = tairBloom.bfexists(bcf, "val2".getBytes());
        assertEquals(false, ret_bool);
    }

    @Test
    public void bfmaddTest() throws Exception {
        String ret = "";
        Boolean[] ret_bool_list;
        Boolean ret_bool;
        // String
        ret = tairBloom.bfreserve(bbf, 100, 0.001);
        assertEquals("OK", ret);
        ret_bool_list = tairBloom.bfmadd(bbf, "val1", "val2");
        assertEquals(true, ret_bool_list[0]);
        assertEquals(true, ret_bool_list[1]);
        ret_bool_list = tairBloom.bfmexists(bbf, "val1", "val2");
        assertEquals(true, ret_bool_list[0]);
        assertEquals(true, ret_bool_list[1]);

        // binary
        ret = tairBloom.bfreserve(bcf, 100, 0.001);
        assertEquals("OK", ret);
        ret_bool_list = tairBloom.bfmadd(bcf, "val1".getBytes(), "val2".getBytes());
        assertEquals(true, ret_bool_list[0]);
        assertEquals(true, ret_bool_list[1]);
        ret_bool_list = tairBloom.bfmexists(bcf, "val1".getBytes(), "val2".getBytes());
        assertEquals(true, ret_bool_list[0]);
        assertEquals(true, ret_bool_list[1]);
    }

    @Test
    public void bfinsertTest() throws Exception {
        String ret = "";
        Boolean[] ret_bool_list;
        Boolean ret_bool;
        // String
        ret_bool_list = tairBloom.bfinsert(bbf, "CAPACITY", 100, "ERROR", 0.001, "ITEMS", "val1", "val2");
        assertEquals(true, ret_bool_list[0]);
        assertEquals(true, ret_bool_list[1]);
        ret_bool_list = tairBloom.bfmadd(bbf, "val1", "val2");
        assertEquals(false, ret_bool_list[0]);
        assertEquals(false, ret_bool_list[1]);
        ret_bool_list = tairBloom.bfmexists(bbf, "val1", "val2");
        assertEquals(true, ret_bool_list[0]);
        assertEquals(true, ret_bool_list[1]);

        // binary
        ret_bool_list = tairBloom.bfinsert(bcf, "CAPACITY".getBytes(), 100, "ERROR".getBytes(), 0.001, "ITEMS".getBytes(), "val1".getBytes(), "val2".getBytes());
        assertEquals(true, ret_bool_list[0]);
        assertEquals(true, ret_bool_list[1]);
        ret_bool_list = tairBloom.bfmadd(bcf, "val1".getBytes(), "val2".getBytes());
        assertEquals(false, ret_bool_list[0]);
        assertEquals(false, ret_bool_list[1]);
        ret_bool_list = tairBloom.bfmexists(bcf, "val1".getBytes(), "val2".getBytes());
        assertEquals(true, ret_bool_list[0]);
        assertEquals(true, ret_bool_list[1]);
    }

    @Test
    public void bfcommandTest() {
        Assert.assertTrue(tairBloom.bfadd(randomkey_, "item1"));
        Assert.assertTrue(tairBloom.bfexists(randomkey_, "item1"));
        Assert.assertFalse(tairBloom.bfexists(randomkey_, "item2"));

        Boolean[] bfRet = tairBloom.bfmadd(randomkey_, "item2", "item3", "item4");
        for (Boolean b : bfRet) {
            Assert.assertTrue(b);
        }

        bfRet = tairBloom.bfmexists(randomkey_, "item2", "item3", "item4", "item5");
        Assert.assertFalse(bfRet[3]);
    }

    @Test
    public void bfinsert() {
        Boolean[] bfRet = tairBloom.bfinsert(randomkey_, new BfinsertParams().capacity(10000).error(0.001), "item1",
            "item2", "item3", "item4", "item5");
        for (Boolean b : bfRet) {
            Assert.assertTrue(b);
        }

        bfRet = tairBloom.bfmexists(randomkey_, "item1", "item2", "item3", "item4", "item5");
        for (Boolean b : bfRet) {
            Assert.assertTrue(b);
        }
    }

    @Test
    public void bfinsertBinary() {
        Boolean[] bfRet = tairBloom.bfinsert(randomKeyBinary_, new BfinsertParams().capacity(10000).error(0.001), "item1".getBytes(),
            "item2".getBytes(), "item3".getBytes(), "item4".getBytes(), "item5".getBytes());
        for (Boolean b : bfRet) {
            Assert.assertTrue(b);
        }

        bfRet = tairBloom.bfmexists(randomKeyBinary_, "item1".getBytes(), "item2".getBytes(), "item3".getBytes(),
            "item4".getBytes(), "item5".getBytes());
        for (Boolean b : bfRet) {
            Assert.assertTrue(b);
        }
    }

    @Test
    public void bfaddException() {
        tairBloom.bfadd(randomkey_, randomkey_);
        tairBloom.bfadd(randomKeyBinary_, randomKeyBinary_);

        try {
            jedis.set(randomkey_, "bar");
            tairBloom.bfadd(randomkey_, randomkey_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
        try {
            jedis.set(randomKeyBinary_, "bar".getBytes());
            tairBloom.bfadd(randomKeyBinary_, randomKeyBinary_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void bfmaddException() {
        tairBloom.bfmadd(randomkey_, "item");
        tairBloom.bfmadd(randomKeyBinary_, "item".getBytes());

        try {
            jedis.set(randomkey_, "bar");
            tairBloom.bfmadd(randomkey_, "item");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
        try {
            jedis.set(randomKeyBinary_, "bar".getBytes());
            tairBloom.bfmadd(randomKeyBinary_, "item".getBytes());
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void bfexistsException() {
        tairBloom.bfexists(randomkey_, randomkey_);
        tairBloom.bfexists(randomKeyBinary_, randomKeyBinary_);

        try {
            jedis.set(randomkey_, "bar");
            tairBloom.bfexists(randomkey_, randomkey_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
        try {
            jedis.set(randomKeyBinary_, "bar".getBytes());
            tairBloom.bfexists(randomKeyBinary_, randomKeyBinary_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void bfmexistsException() {
        tairBloom.bfmexists(randomkey_, randomkey_);
        tairBloom.bfmexists(randomKeyBinary_, randomKeyBinary_);

        try {
            jedis.set(randomkey_, "bar");
            tairBloom.bfmexists(randomkey_, randomkey_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
        try {
            jedis.set(randomKeyBinary_, "bar".getBytes());
            tairBloom.bfmexists(randomKeyBinary_, randomKeyBinary_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void bfinsertException() {
        tairBloom.bfinsert(randomkey_, new BfinsertParams(), randomkey_);
        tairBloom.bfinsert(randomKeyBinary_, new BfinsertParams(), randomKeyBinary_);

        try {
            jedis.set(randomkey_, "bar");
            tairBloom.bfinsert(randomkey_, new BfinsertParams(), randomkey_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
        try {
            jedis.set(randomKeyBinary_, "bar".getBytes());
            tairBloom.bfinsert(randomKeyBinary_, new BfinsertParams(), randomKeyBinary_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void bfreserveException() {
        tairBloom.bfreserve(randomkey_, 1, 0.1);
        tairBloom.bfreserve(randomKeyBinary_, 1, 0.1);

        try {
            jedis.set(randomkey_, "bar");
            tairBloom.bfreserve(randomkey_, 1, 0.1);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
        try {
            jedis.set(randomKeyBinary_, "bar".getBytes());
            tairBloom.bfreserve(randomKeyBinary_, 1, 0.1);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    public void bfdebugException() {
        tairBloom.bfdebug(randomkey_);
        tairBloom.bfdebug(randomKeyBinary_);

        try {
            jedis.set(randomkey_, "bar");
            tairBloom.bfdebug(randomkey_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
        try {
            jedis.set(randomKeyBinary_, "bar".getBytes());
            tairBloom.bfdebug(randomKeyBinary_);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }
}
