import com.kvstore.jedis.params.ExincrbyFloatParams;
import com.kvstore.jedis.params.ExincrbyParams;
import com.kvstore.jedis.params.ExsetParams;
import com.kvstore.jedis.results.ExcasResult;
import com.kvstore.jedis.results.ExgetResult;
import org.junit.Test;
import redis.clients.jedis.util.SafeEncoder;

import java.util.Arrays;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author dwan
 * @date 2019/12/25
 */
public class TairStringClusterTest extends TairStringTestBase {
    private String key;
    private String value;
    private byte[] bkey;
    private byte[] bvalue;
    private String jsonKey;
    private byte[] bjsonKey;

    public TairStringClusterTest() {
        key = "key" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        value = "value" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        bkey = ("bkey" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        bvalue = ("bvalue" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        jsonKey = "jsonkey" + "-" + Thread.currentThread().getName() + "-" + UUID.randomUUID().toString();
        bjsonKey = ("bjsonkey" + "-" + Thread.currentThread().getName() + "-" + UUID.randomUUID().toString()).getBytes();
    }

    @Test
    public void exsetClusterTest() {
        String ret = "";

        // String
        ret = tairStringCluster.exset(jsonKey, key, value);
        assertEquals("OK", ret);
        ExgetResult<String> result = tairStringCluster.exget(jsonKey, key);
        assertNotNull(result);
        assertEquals(true, this.value.equals(result.getValue()));
        assertEquals((long) 1, result.getVersion());

        //binary
        ret = tairStringCluster.exset(bjsonKey, bkey, bvalue);
        assertEquals("OK", ret);
        ExgetResult<byte[]> bresult = tairStringCluster.exget(bjsonKey, bkey);
        assertNotNull(bresult);
        assertEquals(true, Arrays.equals(bvalue, bresult.getValue()));
        assertEquals((long) 1, bresult.getVersion());
    }

    @Test
    public void exsetParamsTest() {
        ExsetParams params_nx = new ExsetParams();
        params_nx.nx();
        ExsetParams params_xx = new ExsetParams();
        params_xx.xx();
        String ret_xx = "";
        String ret_nx = "";

        // String
        ret_xx = tairStringCluster.exset(jsonKey, key, value, params_xx);
        assertEquals(null, ret_xx);
        ret_nx = tairStringCluster.exset(jsonKey, key, value, params_nx);
        assertEquals("OK", ret_nx);
        ret_xx = tairStringCluster.exset(jsonKey, key, value, params_xx);
        assertEquals("OK", ret_xx);

        //binary
        ret_xx = tairStringCluster.exset(bjsonKey, bkey, bvalue, params_xx);
        assertEquals(null, ret_xx);
        ret_nx = tairStringCluster.exset(bjsonKey, bkey, bvalue, params_nx);
        assertEquals("OK", ret_nx);
        ret_xx = tairStringCluster.exset(bjsonKey, bkey, bvalue, params_xx);
        assertEquals("OK", ret_xx);
    }

    @Test
    public void exsetverTest() {
        String ret = "";
        long ret_var = 0;

        // String
        ret = tairStringCluster.exset(jsonKey, key, value);
        assertEquals("OK", ret);
        ExgetResult<String> result = tairStringCluster.exget(jsonKey, key);
        assertNotNull(result);
        assertEquals(true, this.value.equals(result.getValue()));
        assertEquals((long) 1, result.getVersion());

        ret_var = tairStringCluster.exsetver(jsonKey, key, 10);
        assertEquals(1, ret_var);
        result = tairStringCluster.exget(jsonKey, key);
        assertNotNull(result);
        assertEquals(true, this.value.equals(result.getValue()));
        assertEquals((long) 10, result.getVersion());

        //binary
        ret = tairStringCluster.exset(bjsonKey, bkey, bvalue);
        assertEquals("OK", ret);
        ExgetResult<byte[]> bresult = tairStringCluster.exget(bjsonKey, bkey);
        assertNotNull(bresult);
        assertEquals(true, Arrays.equals(bvalue, bresult.getValue()));
        assertEquals((long) 1, bresult.getVersion());

        ret_var = tairStringCluster.exsetver(bjsonKey, bkey, 10);
        assertEquals(1, ret_var);
        bresult = tairStringCluster.exget(bjsonKey, bkey);
        assertNotNull(bresult);
        assertEquals(true, Arrays.equals(bvalue, bresult.getValue()));
        assertEquals((long) 10, bresult.getVersion());
    }

    @Test
    public void exincrbyTest() {
        String ret = "";
        String num_string_value = "100";
        byte[] num_byte_value = SafeEncoder.encode("100");
        long incr_value = 100;
        String new_string_value = "200";
        byte[] new_byte_value = SafeEncoder.encode("200");
        long new_long_value = 200;
        long ret_var = 0;

        // String
        ret = tairStringCluster.exset(jsonKey, key, num_string_value);
        assertEquals("OK", ret);
        ExgetResult<String> result = tairStringCluster.exget(jsonKey, key);
        assertNotNull(result);
        assertEquals(true, num_string_value.equals(result.getValue()));
        assertEquals((long) 1, result.getVersion());

        ret_var = tairStringCluster.exincrBy(jsonKey, key, incr_value);
        assertEquals(new_long_value, ret_var);
        result = tairStringCluster.exget(jsonKey, key);
        assertNotNull(result);
        assertEquals(true, new_string_value.equals(result.getValue()));
        assertEquals((long) 2, result.getVersion());

        //binary
        ret = tairStringCluster.exset(bjsonKey, bkey, num_byte_value);
        assertEquals("OK", ret);
        ExgetResult<byte[]> bresult = tairStringCluster.exget(bjsonKey, bkey);
        assertNotNull(bresult);
        assertEquals(true, Arrays.equals(num_byte_value, bresult.getValue()));
        assertEquals((long) 1, bresult.getVersion());

        ret_var = tairStringCluster.exincrBy(bjsonKey, bkey, incr_value);
        assertEquals(new_long_value, ret_var);
        bresult = tairStringCluster.exget(bjsonKey, bkey);
        assertNotNull(bresult);
        assertEquals(true, Arrays.equals(new_byte_value, bresult.getValue()));
        assertEquals((long) 2, bresult.getVersion());
    }

    @Test
    public void exincrbyParamsTest() throws InterruptedException {
        String ret = "";
        String num_string_value = "100";
        byte[] num_byte_value = SafeEncoder.encode("100");
        long incr_value = 100;
        String new_string_value = "200";
        byte[] new_byte_value = SafeEncoder.encode("200");
        long new_long_value = 200;
        long ret_var = 0;
        ExgetResult<String> result = null;
        ExgetResult<byte[]> bresult = null;

        ExincrbyParams params_nx_px = new ExincrbyParams();
        params_nx_px.nx();
        params_nx_px.px(1000);
        ExincrbyParams params_xx_ex = new ExincrbyParams();
        params_xx_ex.xx();
        params_xx_ex.ex(1);
        ExincrbyParams params_xx_pxat = new ExincrbyParams();
        params_xx_pxat.xx();
        params_xx_pxat.pxat(System.currentTimeMillis() + 1000);

        ret_var = tairStringCluster.exincrBy(jsonKey, key, incr_value, params_nx_px);
        assertEquals(incr_value, ret_var);
        result = tairStringCluster.exget(jsonKey, key);
        assertNotNull(result);
        assertEquals(true, num_string_value.equals(result.getValue()));
        assertEquals((long) 1, result.getVersion());
        Thread.sleep(1000);
        result = tairStringCluster.exget(jsonKey, key);
        assertEquals(null, result);

        ret = tairStringCluster.exset(jsonKey, key, num_string_value);
        assertEquals("OK", ret);
        ret_var = tairStringCluster.exincrBy(jsonKey, key, incr_value, params_xx_ex);
        assertEquals(new_long_value, ret_var);
        result = tairStringCluster.exget(jsonKey, key);
        assertNotNull(result);
        assertEquals(true, new_string_value.equals(result.getValue()));
        assertEquals((long) 2, result.getVersion());
        Thread.sleep(1000);
        result = tairStringCluster.exget(jsonKey, key);
        assertEquals(null, result);

        ret = tairStringCluster.exset(jsonKey, key, num_string_value);
        assertEquals("OK", ret);
        params_xx_pxat.pxat(System.currentTimeMillis() + 1000);
        ret_var = tairStringCluster.exincrBy(jsonKey, key, incr_value, params_xx_pxat);
        assertEquals(new_long_value, ret_var);
        result = tairStringCluster.exget(jsonKey, key);
        assertNotNull(result);
        assertEquals(true, new_string_value.equals(result.getValue()));
        assertEquals((long) 2, result.getVersion());
        Thread.sleep(1000);
        result = tairStringCluster.exget(jsonKey, key);
        assertEquals(null, result);

        //binary
        ret_var = tairStringCluster.exincrBy(bjsonKey, bkey, incr_value, params_nx_px);
        assertEquals(incr_value, ret_var);
        bresult = tairStringCluster.exget(bjsonKey, bkey);
        assertNotNull(bresult);
        assertEquals(true, Arrays.equals(num_byte_value, bresult.getValue()));
        assertEquals((long) 1, bresult.getVersion());
        Thread.sleep(1000);
        bresult = tairStringCluster.exget(bjsonKey, bkey);
        assertEquals(null, bresult);

        ret = tairStringCluster.exset(bjsonKey, bkey, num_byte_value);
        assertEquals("OK", ret);
        ret_var = tairStringCluster.exincrBy(bjsonKey, bkey, incr_value, params_xx_ex);
        assertEquals(new_long_value, ret_var);
        bresult = tairStringCluster.exget(bjsonKey, bkey);
        assertNotNull(bresult);
        assertEquals(true, Arrays.equals(new_byte_value, bresult.getValue()));
        assertEquals((long) 2, bresult.getVersion());
        Thread.sleep(1000);
        bresult = tairStringCluster.exget(bjsonKey, bkey);
        assertEquals(null, bresult);

        ret = tairStringCluster.exset(bjsonKey, bkey, num_byte_value);
        assertEquals("OK", ret);
        params_xx_pxat.pxat(System.currentTimeMillis() + 1000);
        ret_var = tairStringCluster.exincrBy(bjsonKey, bkey, incr_value, params_xx_pxat);
        assertEquals(new_long_value, ret_var);
        bresult = tairStringCluster.exget(bjsonKey, bkey);
        assertNotNull(bresult);
        assertEquals(true, Arrays.equals(new_byte_value, bresult.getValue()));
        assertEquals((long) 2, bresult.getVersion());
        Thread.sleep(2000);
        bresult = tairStringCluster.exget(bjsonKey, bkey);
        assertEquals(null, bresult);
    }

    @Test
    public void exincrbyfloatTest() {
        String ret = "";
        String num_string_value = "100";
        byte[] num_byte_value = SafeEncoder.encode("100");
        Double incr_value = Double.valueOf(100);
        String new_string_value = "200";
        byte[] new_byte_value = SafeEncoder.encode("200");
        Double new_float_value = Double.valueOf(200);
        Double ret_var = Double.valueOf(0);

        // String
        ret = tairStringCluster.exset(jsonKey, key, num_string_value);
        assertEquals("OK", ret);
        ExgetResult<String> result = tairStringCluster.exget(jsonKey, key);
        assertNotNull(result);
        assertEquals(true, num_string_value.equals(result.getValue()));
        assertEquals((long) 1, result.getVersion());

        ret_var = tairStringCluster.exincrByFloat(jsonKey, key, incr_value);
        assertEquals(new_float_value, ret_var);
        result = tairStringCluster.exget(jsonKey, key);
        assertNotNull(result);
        assertEquals(true, new_string_value.equals(result.getValue()));
        assertEquals((long) 2, result.getVersion());

        //binary
        ret = tairStringCluster.exset(bjsonKey, bkey, num_byte_value);
        assertEquals("OK", ret);
        ExgetResult<byte[]> bresult = tairStringCluster.exget(bjsonKey, bkey);
        assertNotNull(bresult);
        assertEquals(true, Arrays.equals(num_byte_value, bresult.getValue()));
        assertEquals((long) 1, bresult.getVersion());

        ret_var = tairStringCluster.exincrByFloat(bjsonKey, bkey, incr_value);
        assertEquals(new_float_value, ret_var);
        bresult = tairStringCluster.exget(bjsonKey, bkey);
        assertNotNull(bresult);
        assertEquals(true, Arrays.equals(new_byte_value, bresult.getValue()));
        assertEquals((long) 2, bresult.getVersion());
    }

    @Test
    public void exincrbyfloatParamsTest() throws InterruptedException {
        String ret = "";
        String num_string_value = "100";
        byte[] num_byte_value = SafeEncoder.encode("100");
        Double incr_value = Double.valueOf(100);
        String new_string_value = "200";
        byte[] new_byte_value = SafeEncoder.encode("200");
        Double new_float_value = Double.valueOf(200);
        Double ret_var = Double.valueOf(0);
        ExgetResult<String> result = null;
        ExgetResult<byte[]> bresult = null;

        ExincrbyFloatParams params_nx_px = new ExincrbyFloatParams();
        params_nx_px.nx();
        params_nx_px.px(1000);
        ExincrbyFloatParams params_xx_ex = new ExincrbyFloatParams();
        params_xx_ex.xx();
        params_xx_ex.ex(1);
        ExincrbyFloatParams params_xx_pxat = new ExincrbyFloatParams();
        params_xx_pxat.xx();
        params_xx_pxat.pxat(System.currentTimeMillis() + 1000);

        ret_var = tairStringCluster.exincrByFloat(jsonKey, key, incr_value, params_nx_px);
        assertEquals(incr_value, ret_var);
        result = tairStringCluster.exget(jsonKey, key);
        assertNotNull(result);
        assertEquals(true, num_string_value.equals(result.getValue()));
        assertEquals((long) 1, result.getVersion());
        Thread.sleep(1000);
        result = tairStringCluster.exget(jsonKey, key);
        assertEquals(null, result);

        ret = tairStringCluster.exset(jsonKey, key, num_string_value);
        assertEquals("OK", ret);
        ret_var = tairStringCluster.exincrByFloat(jsonKey, key, incr_value, params_xx_ex);
        assertEquals(new_float_value, ret_var);
        result = tairStringCluster.exget(jsonKey, key);
        assertNotNull(result);
        assertEquals(true, new_string_value.equals(result.getValue()));
        assertEquals((long) 2, result.getVersion());
        Thread.sleep(1000);
        result = tairStringCluster.exget(jsonKey, key);
        assertEquals(null, result);

        ret = tairStringCluster.exset(jsonKey, key, num_string_value);
        assertEquals("OK", ret);
        params_xx_pxat.pxat(System.currentTimeMillis() + 1000);
        ret_var = tairStringCluster.exincrByFloat(jsonKey, key, incr_value, params_xx_pxat);
        assertEquals(new_float_value, ret_var);
        result = tairStringCluster.exget(jsonKey, key);
        assertNotNull(result);
        assertEquals(true, new_string_value.equals(result.getValue()));
        assertEquals((long) 2, result.getVersion());
        Thread.sleep(1000);
        result = tairStringCluster.exget(jsonKey, key);
        assertEquals(null, result);

        //binary
        ret_var = tairStringCluster.exincrByFloat(bjsonKey, bkey, incr_value, params_nx_px);
        assertEquals(incr_value, ret_var);
        bresult = tairStringCluster.exget(bjsonKey, bkey);
        assertNotNull(bresult);
        assertEquals(true, Arrays.equals(num_byte_value, bresult.getValue()));
        assertEquals((long) 1, bresult.getVersion());
        Thread.sleep(1000);
        bresult = tairStringCluster.exget(bjsonKey, bkey);
        assertEquals(null, bresult);

        ret = tairStringCluster.exset(bjsonKey, bkey, num_byte_value);
        assertEquals("OK", ret);
        ret_var = tairStringCluster.exincrByFloat(bjsonKey, bkey, incr_value, params_xx_ex);
        assertEquals(new_float_value, ret_var);
        bresult = tairStringCluster.exget(bjsonKey, bkey);
        assertNotNull(bresult);
        assertEquals(true, Arrays.equals(new_byte_value, bresult.getValue()));
        assertEquals((long) 2, bresult.getVersion());
        Thread.sleep(1000);
        bresult = tairStringCluster.exget(bjsonKey, bkey);
        assertEquals(null, bresult);

        ret = tairStringCluster.exset(bjsonKey, bkey, num_byte_value);
        assertEquals("OK", ret);
        params_xx_pxat.pxat(System.currentTimeMillis() + 1000);
        ret_var = tairStringCluster.exincrByFloat(bjsonKey, bkey, incr_value, params_xx_pxat);
        assertEquals(new_float_value, ret_var);
        bresult = tairStringCluster.exget(bjsonKey, bkey);
        assertNotNull(bresult);
        assertEquals(true, Arrays.equals(new_byte_value, bresult.getValue()));
        assertEquals((long) 2, bresult.getVersion());
        Thread.sleep(2000);
        bresult = tairStringCluster.exget(bjsonKey, bkey);
        assertEquals(null, bresult);
    }

    @Test
    public void excasTest() {
        String ret = "";
        ExcasResult<String> ret2 = null;
        ExcasResult<byte[]> ret3 = null;

        // String
        ret = tairStringCluster.exset(jsonKey, key, value);
        assertEquals("OK", ret);
        ret2 = tairStringCluster.excas(jsonKey, key, "new" + value, 2);
        assertEquals(value, ret2.getValue());
        assertEquals((long) 1, ret2.getVersion());
        ret2 = tairStringCluster.excas(jsonKey, key, "new" + value, 1);
        assertEquals("OK", ret2.getMsg());
        assertEquals("", ret2.getValue());
        assertEquals((long) 2, ret2.getVersion());

        //binary
        ret = tairStringCluster.exset(bjsonKey, bkey, bvalue);
        assertEquals("OK", ret);
        ret3 = tairStringCluster.excas(bjsonKey, bkey, SafeEncoder.encode("new" + bvalue), 2);
        assertEquals(true, Arrays.equals(bvalue, ret3.getValue()));
        assertEquals((long) 1, ret3.getVersion());
        ret3 = tairStringCluster.excas(bjsonKey, bkey, SafeEncoder.encode("new" + bvalue), 1);
        assertEquals(true, Arrays.equals(SafeEncoder.encode("OK"), ret3.getMsg()));
        assertEquals(true, Arrays.equals(SafeEncoder.encode(""), ret3.getValue()));
        assertEquals((long) 2, ret3.getVersion());
    }

    @Test
    public void excadTest() throws InterruptedException {
        String ret = "";
        long ret2 = 0;

        // String
        ret = tairStringCluster.exset(jsonKey, key, value);
        assertEquals("OK", ret);
        ret2 = tairStringCluster.excad(jsonKey, key, 2);
        assertEquals((long) 0, ret2);
        ret2 = tairStringCluster.excad(jsonKey, key, 1);
        assertEquals((long) 1, ret2);

        //binary
        ret = tairStringCluster.exset(bjsonKey, bkey, bvalue);
        assertEquals("OK", ret);
        ret2 = tairStringCluster.excad(bjsonKey, bkey, 2);
        assertEquals((long) 0, ret2);
        ret2 = tairStringCluster.excad(bjsonKey, bkey, 1);
        assertEquals((long) 1, ret2);
    }
}
