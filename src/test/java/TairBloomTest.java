import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.*;

/**
 * @author dwan
 * @date 2019/12/26
 */
public class TairBloomTest extends TairBloomTestBase {
    private String bbf;
    private byte[] bcf = "bcf".getBytes();

    public TairBloomTest() {
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
}
