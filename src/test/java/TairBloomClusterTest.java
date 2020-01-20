import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class TairBloomClusterTest extends TairBloomTestBase {
    private String bbf;
    private byte[] bcf = "bcf".getBytes();

    public TairBloomClusterTest() {
        bbf = "bbf" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        bcf = ("bcf" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
    }

    @Test
    public void bfaddTest() throws Exception {
        String ret = "";
        Boolean ret_bool = false;
        // String
        ret = tairBloomCluster.bfreserve(bbf, 100, 0.001);
        assertEquals("OK", ret);
        ret_bool = tairBloomCluster.bfadd(bbf, "val1");
        assertEquals(true, ret_bool);
        ret_bool = tairBloomCluster.bfexists(bbf, "val1");
        assertEquals(true, ret_bool);
        ret_bool = tairBloomCluster.bfexists(bbf, "val2");
        assertEquals(false, ret_bool);

        // binary
        ret = tairBloomCluster.bfreserve(bcf, 100, 0.001);
        assertEquals("OK", ret);
        ret_bool = tairBloomCluster.bfadd(bcf, "val1".getBytes());
        assertEquals(true, ret_bool);
        ret_bool = tairBloomCluster.bfexists(bcf, "val1".getBytes());
        assertEquals(true, ret_bool);
        ret_bool = tairBloomCluster.bfexists(bcf, "val2".getBytes());
        assertEquals(false, ret_bool);
    }

    @Test
    public void bfmaddTest() throws Exception {
        String ret = "";
        Boolean[] ret_bool_list;
        Boolean ret_bool;
        // String
        ret = tairBloomCluster.bfreserve(bbf, 100, 0.001);
        assertEquals("OK", ret);
        ret_bool_list = tairBloomCluster.bfmadd(bbf, "val1", "val2");
        assertEquals(true, ret_bool_list[0]);
        assertEquals(true, ret_bool_list[1]);
        ret_bool_list = tairBloomCluster.bfmexists(bbf, "val1", "val2");
        assertEquals(true, ret_bool_list[0]);
        assertEquals(true, ret_bool_list[1]);

        // binary
        ret = tairBloomCluster.bfreserve(bcf, 100, 0.001);
        assertEquals("OK", ret);
        ret_bool_list = tairBloomCluster.bfmadd(bcf, "val1".getBytes(), "val2".getBytes());
        assertEquals(true, ret_bool_list[0]);
        assertEquals(true, ret_bool_list[1]);
        ret_bool_list = tairBloomCluster.bfmexists(bcf, "val1".getBytes(), "val2".getBytes());
        assertEquals(true, ret_bool_list[0]);
        assertEquals(true, ret_bool_list[1]);
    }

    @Test
    public void bfinsertTest() throws Exception {
        String ret = "";
        Boolean[] ret_bool_list;
        Boolean ret_bool;
        // String
        ret_bool_list = tairBloomCluster.bfinsert(bbf, "CAPACITY", 100, "ERROR", 0.001, "ITEMS", "val1", "val2");
        assertEquals(true, ret_bool_list[0]);
        assertEquals(true, ret_bool_list[1]);
        ret_bool_list = tairBloomCluster.bfmadd(bbf, "val1", "val2");
        assertEquals(false, ret_bool_list[0]);
        assertEquals(false, ret_bool_list[1]);
        ret_bool_list = tairBloomCluster.bfmexists(bbf, "val1", "val2");
        assertEquals(true, ret_bool_list[0]);
        assertEquals(true, ret_bool_list[1]);

        // binary
        ret_bool_list = tairBloomCluster.bfinsert(bcf, "CAPACITY".getBytes(), 100, "ERROR".getBytes(), 0.001,
            "ITEMS".getBytes(), "val1".getBytes(), "val2".getBytes());
        assertEquals(true, ret_bool_list[0]);
        assertEquals(true, ret_bool_list[1]);
        ret_bool_list = tairBloomCluster.bfmadd(bcf, "val1".getBytes(), "val2".getBytes());
        assertEquals(false, ret_bool_list[0]);
        assertEquals(false, ret_bool_list[1]);
        ret_bool_list = tairBloomCluster.bfmexists(bcf, "val1".getBytes(), "val2".getBytes());
        assertEquals(true, ret_bool_list[0]);
        assertEquals(true, ret_bool_list[1]);
    }
}
