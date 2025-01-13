package com.aliyun.tair.tests.tairbloom;

import com.aliyun.tair.tairbloom.params.BfinsertParams;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class TairBloomPipelineTest extends TairBloomTestBase {
    private String bbf;
    private byte[] bcf = "bcf".getBytes();
    private String randomkey_;
    private byte[] randomKeyBinary_;

    private static final String EXBLOOM_BIGKEY = "EXBLOOM_BIGKEY";

    public TairBloomPipelineTest() {
        randomkey_ = "randomkey_" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        randomKeyBinary_ = ("randomkey_" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        bbf = "bbf" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        bcf = ("bcf" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
    }

    @Test
    public void bfaddPipelineTest() throws Exception {
        int i = 0;
        String ret = "";
        Boolean ret_bool = false;
        // String
        tairBloomPipeline.bfreserve(bbf, 100, 0.001);
        tairBloomPipeline.bfadd(bbf, "val1");
        tairBloomPipeline.bfexists(bbf, "val1");
        tairBloomPipeline.bfexists(bbf, "val2");

        List<Object> objs = tairBloomPipeline.syncAndReturnAll();
        i = 0;
        assertEquals("OK", objs.get(i++));
        assertEquals(true, objs.get(i++));
        assertEquals(true, objs.get(i++));
        assertEquals(false, objs.get(i++));

        // binary
        tairBloomPipeline.bfreserve(bcf, 100, 0.001);
        tairBloomPipeline.bfadd(bcf, "val1".getBytes());
        tairBloomPipeline.bfexists(bcf, "val1".getBytes());
        tairBloomPipeline.bfexists(bcf, "val2".getBytes());

        objs = tairBloomPipeline.syncAndReturnAll();
        i = 0;
        assertEquals("OK", objs.get(i++));
        assertEquals(true, objs.get(i++));
        assertEquals(true, objs.get(i++));
        assertEquals(false, objs.get(i++));
    }

    @Test
    public void bfmaddPipelineTest() throws Exception {
        int i = 0;
        String ret = "";
        Boolean[] ret_bool_list;
        Boolean ret_bool;
        // String
        tairBloomPipeline.bfreserve(bbf, 100, 0.001);
        tairBloomPipeline.bfmadd(bbf, "val1", "val2");
        tairBloomPipeline.bfmexists(bbf, "val1", "val2");

        List<Object> objs = tairBloomPipeline.syncAndReturnAll();
        i = 0;
        assertEquals("OK", objs.get(i++));
        assertEquals(true, Boolean[].class.cast(objs.get(i))[0]);
        assertEquals(true, Boolean[].class.cast(objs.get(i++))[1]);
        assertEquals(true, Boolean[].class.cast(objs.get(i))[0]);
        assertEquals(true, Boolean[].class.cast(objs.get(i++))[1]);

        // binary
        tairBloomPipeline.bfreserve(bcf, 100, 0.001);
        tairBloomPipeline.bfmadd(bcf, "val1".getBytes(), "val2".getBytes());
        tairBloomPipeline.bfmexists(bcf, "val1".getBytes(), "val2".getBytes());

        objs = tairBloomPipeline.syncAndReturnAll();
        i = 0;
        assertEquals("OK", objs.get(i++));
        assertEquals(true, Boolean[].class.cast(objs.get(i))[0]);
        assertEquals(true, Boolean[].class.cast(objs.get(i++))[1]);
        assertEquals(true, Boolean[].class.cast(objs.get(i))[0]);
        assertEquals(true, Boolean[].class.cast(objs.get(i++))[1]);
    }

    @Test
    public void bfinsertPipelineTest() throws Exception {
        int i = 0;
        String ret = "";
        Boolean[] ret_bool_list;
        Boolean ret_bool;
        // String
        BfinsertParams params = new BfinsertParams();
        params.capacity(100).error(0.001);
        tairBloomPipeline.bfinsert(bbf, params, "ITEMS", "val1", "val2");
        tairBloomPipeline.bfmadd(bbf, "val1", "val2");
        tairBloomPipeline.bfmexists(bbf, "val1", "val2");

        List<Object> objs = tairBloomPipeline.syncAndReturnAll();
        i = 0;

        assertEquals(true, Boolean[].class.cast(objs.get(i))[0]);
        assertEquals(true, Boolean[].class.cast(objs.get(i++))[1]);
        assertEquals(false, Boolean[].class.cast(objs.get(i))[0]);
        assertEquals(false, Boolean[].class.cast(objs.get(i++))[1]);
        assertEquals(true, Boolean[].class.cast(objs.get(i))[0]);
        assertEquals(true, Boolean[].class.cast(objs.get(i++))[1]);

        // binary
        tairBloomPipeline.bfinsert(bcf, params, "ITEMS".getBytes(),
            "val1".getBytes(), "val2".getBytes());
        tairBloomPipeline.bfmadd(bcf, "val1".getBytes(), "val2".getBytes());
        tairBloomPipeline.bfmexists(bcf, "val1".getBytes(), "val2".getBytes());

        objs = tairBloomPipeline.syncAndReturnAll();
        i = 0;

        assertEquals(true, Boolean[].class.cast(objs.get(i))[0]);
        assertEquals(true, Boolean[].class.cast(objs.get(i++))[1]);
        assertEquals(false, Boolean[].class.cast(objs.get(i))[0]);
        assertEquals(false, Boolean[].class.cast(objs.get(i++))[1]);
        assertEquals(true, Boolean[].class.cast(objs.get(i))[0]);
        assertEquals(true, Boolean[].class.cast(objs.get(i++))[1]);
    }
}
