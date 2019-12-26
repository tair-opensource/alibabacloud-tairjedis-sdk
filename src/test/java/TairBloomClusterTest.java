import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * @author dwan
 * @date 2019/12/26
 */
public class TairBloomClusterTest extends TairBloomTestBase {
    private String bbf;
    private byte[] bcf = "bcf".getBytes();
    private String jsonKey;
    private byte[] bjsonKey;

    public TairBloomClusterTest() {
        bbf = "bbf" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        bcf = ("bcf" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        jsonKey = "jsonkey" + "-" + Thread.currentThread().getName() + "-" + UUID.randomUUID().toString();
        bjsonKey = ("bjsonkey" + "-" + Thread.currentThread().getName() + "-" + UUID.randomUUID().toString()).getBytes();
    }

    @Test
    public void bfaddTest() throws Exception {
        String ret = "";
        Boolean ret_bool = false;
        // String
        ret = tairBloomCluster.bfreserve(jsonKey, bbf, 100, 0.001);
        assertEquals("OK", ret);
        ret_bool = tairBloomCluster.bfadd(jsonKey, bbf, "val1");
        assertEquals(true, ret_bool);
        ret_bool = tairBloomCluster.bfexists(jsonKey, bbf, "val1");
        assertEquals(true, ret_bool);
        ret_bool = tairBloomCluster.bfexists(jsonKey, bbf, "val2");
        assertEquals(false, ret_bool);

        // binary
        ret = tairBloomCluster.bfreserve(bjsonKey, bcf, 100, 0.001);
        assertEquals("OK", ret);
        ret_bool = tairBloomCluster.bfadd(bjsonKey, bcf, "val1".getBytes());
        assertEquals(true, ret_bool);
        ret_bool = tairBloomCluster.bfexists(bjsonKey, bcf, "val1".getBytes());
        assertEquals(true, ret_bool);
        ret_bool = tairBloomCluster.bfexists(bjsonKey, bcf, "val2".getBytes());
        assertEquals(false, ret_bool);
    }

    @Test
    public void bfmaddTest() throws Exception {
        String ret = "";
        Boolean[] ret_bool_list;
        Boolean ret_bool;
        // String
        ret = tairBloomCluster.bfreserve(jsonKey, bbf, 100, 0.001);
        assertEquals("OK", ret);
        ret_bool_list = tairBloomCluster.bfmadd(jsonKey, bbf, "val1", "val2");
        assertEquals(true, ret_bool_list[0]);
        assertEquals(true, ret_bool_list[1]);
        ret_bool_list = tairBloomCluster.bfmexists(jsonKey, bbf, "val1", "val2");
        assertEquals(true, ret_bool_list[0]);
        assertEquals(true, ret_bool_list[1]);

        // binary
        ret = tairBloomCluster.bfreserve(bjsonKey, bcf, 100, 0.001);
        assertEquals("OK", ret);
        ret_bool_list = tairBloomCluster.bfmadd(bjsonKey, bcf, "val1".getBytes(), "val2".getBytes());
        assertEquals(true, ret_bool_list[0]);
        assertEquals(true, ret_bool_list[1]);
        ret_bool_list = tairBloomCluster.bfmexists(bjsonKey, bcf, "val1".getBytes(), "val2".getBytes());
        assertEquals(true, ret_bool_list[0]);
        assertEquals(true, ret_bool_list[1]);
    }

    @Test
    public void bfinsertTest() throws Exception {
        String ret = "";
        Boolean[] ret_bool_list;
        Boolean ret_bool;
        // String
        ret_bool_list = tairBloomCluster.bfinsert(jsonKey, bbf, "CAPACITY", 100, "ERROR", 0.001, "ITEMS", "val1", "val2");
        assertEquals(true, ret_bool_list[0]);
        assertEquals(true, ret_bool_list[1]);
        ret_bool_list = tairBloomCluster.bfmadd(jsonKey, bbf, "val1", "val2");
        assertEquals(false, ret_bool_list[0]);
        assertEquals(false, ret_bool_list[1]);
        ret_bool_list = tairBloomCluster.bfmexists(jsonKey, bbf, "val1", "val2");
        assertEquals(true, ret_bool_list[0]);
        assertEquals(true, ret_bool_list[1]);

        // binary
        ret_bool_list = tairBloomCluster.bfinsert(bjsonKey, bcf, "CAPACITY".getBytes(), 100, "ERROR".getBytes(), 0.001, "ITEMS".getBytes(), "val1".getBytes(), "val2".getBytes());
        assertEquals(true, ret_bool_list[0]);
        assertEquals(true, ret_bool_list[1]);
        ret_bool_list = tairBloomCluster.bfmadd(bjsonKey, bcf, "val1".getBytes(), "val2".getBytes());
        assertEquals(false, ret_bool_list[0]);
        assertEquals(false, ret_bool_list[1]);
        ret_bool_list = tairBloomCluster.bfmexists(bjsonKey, bcf, "val1".getBytes(), "val2".getBytes());
        assertEquals(true, ret_bool_list[0]);
        assertEquals(true, ret_bool_list[1]);
    }
}
