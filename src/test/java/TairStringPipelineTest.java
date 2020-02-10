import com.aliyun.tair.tairstring.params.ExincrbyFloatParams;
import com.aliyun.tair.tairstring.params.ExincrbyParams;
import com.aliyun.tair.tairstring.params.ExsetParams;
import com.aliyun.tair.tairstring.results.ExcasResult;
import com.aliyun.tair.tairstring.results.ExgetResult;
import org.junit.Test;
import redis.clients.jedis.util.SafeEncoder;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TairStringPipelineTest extends TairStringTestBase {
    private String key1;
    private String value1;
    private String key2;
    private String value2;
    private byte[] bkey1;
    private byte[] bvalue1;
    private byte[] bkey2;
    private byte[] bvalue2;

    public TairStringPipelineTest() {
        key1 = "key1" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        value1 = "value1" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        key2 = "key2" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        value2 = "value2" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        bkey1 = ("bkey1" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        bvalue1 = ("bvalue1" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        bkey2 = ("bkey2" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        bvalue2 = ("bvalue2" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
    }

    @Test
    public void exsetPipelineTest() {
        int i = 0;

        // String
        tairStringPipeline.exset(key1, value1);
        tairStringPipeline.exset(key2, value2);
        tairStringPipeline.exget(key1);
        tairStringPipeline.exget(key2);

        List<Object> objs = tairStringPipeline.syncAndReturnAll();

        i = 0;
        assertEquals("OK", objs.get(i++));
        assertEquals("OK", objs.get(i++));
        assertEquals(value1, ExgetResult.class.cast(objs.get(i++)).getValue());
        assertEquals(value2, ExgetResult.class.cast(objs.get(i++)).getValue());

        //binary
        tairStringPipeline.exset(bkey1, bvalue1);
        tairStringPipeline.exset(bkey2, bvalue2);
        tairStringPipeline.exget(bkey1);
        tairStringPipeline.exget(bkey2);

        objs = tairStringPipeline.syncAndReturnAll();

        i = 0;
        assertEquals("OK", objs.get(i++));
        assertEquals("OK", objs.get(i++));
        assertEquals(true, Arrays.equals(bvalue1, (byte[]) ExgetResult.class.cast(objs.get(i++)).getValue()));
        assertEquals(true, Arrays.equals(bvalue2, (byte[]) ExgetResult.class.cast(objs.get(i++)).getValue()));
    }


    @Test
    public void exsetParamsPipelineTest() {
        int i = 0;
        ExsetParams params_nx = new ExsetParams();
        params_nx.nx();
        ExsetParams params_xx = new ExsetParams();
        params_xx.xx();
        String ret_xx = "";
        String ret_nx = "";

        // String
        tairStringPipeline.exset(key1, value1, params_xx);
        tairStringPipeline.exset(key2, value2, params_nx);
        tairStringPipeline.exget(key2);
        tairStringPipeline.exset(key2, "new" + value2, params_xx);
        tairStringPipeline.exget(key2);

        List<Object> objs = tairStringPipeline.syncAndReturnAll();

        i = 0;
        assertEquals(null, objs.get(i++));
        assertEquals("OK", objs.get(i++));
        assertEquals(value2, ExgetResult.class.cast(objs.get(i++)).getValue());
        assertEquals("OK", objs.get(i++));
        assertEquals("new" + value2, ExgetResult.class.cast(objs.get(i++)).getValue());

        // binary
        tairStringPipeline.exset(bkey1, bvalue1, params_xx);
        tairStringPipeline.exset(bkey2, bvalue2, params_nx);
        tairStringPipeline.exget(bkey2);
        tairStringPipeline.exset(bkey2, SafeEncoder.encode("new" + bvalue2), params_xx);
        tairStringPipeline.exget(bkey2);

        objs = tairStringPipeline.syncAndReturnAll();

        i = 0;
        assertEquals(null, objs.get(i++));
        assertEquals("OK", objs.get(i++));
        assertEquals(true, Arrays.equals(bvalue2, (byte[]) ExgetResult.class.cast(objs.get(i++)).getValue()));
        assertEquals("OK", objs.get(i++));
        assertEquals(true, Arrays.equals(SafeEncoder.encode("new" + bvalue2), (byte[]) ExgetResult.class.cast(objs.get(i++)).getValue()));
    }

    @Test
    public void exsetverPipelineTest() {
        int i = 0;

        // String
        tairStringPipeline.exset(key1, value1);
        tairStringPipeline.exget(key1);
        tairStringPipeline.exsetver(key1, 10);
        tairStringPipeline.exget(key1);

        List<Object> objs = tairStringPipeline.syncAndReturnAll();

        i = 0;
        assertEquals("OK", objs.get(i++));
        assertEquals(value1, ExgetResult.class.cast(objs.get(i)).getValue());
        assertEquals(1, ExgetResult.class.cast(objs.get(i++)).getVersion());
        assertEquals((long) 1, objs.get(i++));
        assertEquals(value1, ExgetResult.class.cast(objs.get(i)).getValue());
        assertEquals(10, ExgetResult.class.cast(objs.get(i++)).getVersion());

        //binary
        tairStringPipeline.exset(bkey1, bvalue1);
        tairStringPipeline.exget(bkey1);
        tairStringPipeline.exsetver(bkey1, 10);
        tairStringPipeline.exget(bkey1);

        objs = tairStringPipeline.syncAndReturnAll();

        i = 0;
        assertEquals("OK", objs.get(i++));
        assertEquals(true, Arrays.equals(bvalue1, (byte[]) ExgetResult.class.cast(objs.get(i)).getValue()));
        assertEquals(1, ExgetResult.class.cast(objs.get(i++)).getVersion());
        assertEquals((long) 1, objs.get(i++));
        assertEquals(true, Arrays.equals(bvalue1, (byte[]) ExgetResult.class.cast(objs.get(i)).getValue()));
        assertEquals(10, ExgetResult.class.cast(objs.get(i++)).getVersion());
    }

    @Test
    public void exincrbyPipelineTest() {
        int i = 0;
        String num_string_value = "100";
        byte[] num_byte_value = SafeEncoder.encode("100");
        long incr_value = 100;
        String new_string_value = "200";
        byte[] new_byte_value = SafeEncoder.encode("200");
        long new_long_value = 200;

        // String
        tairStringPipeline.exset(key1, num_string_value);
        tairStringPipeline.exincrBy(key1, incr_value);
        tairStringPipeline.exget(key1);

        List<Object> objs = tairStringPipeline.syncAndReturnAll();

        i = 0;
        assertEquals("OK", objs.get(i++));
        assertEquals(new_long_value, objs.get(i++));
        assertEquals(new_string_value, ExgetResult.class.cast(objs.get(i)).getValue());
        assertEquals(2, ExgetResult.class.cast(objs.get(i++)).getVersion());

        //binary
        tairStringPipeline.exset(bkey1, num_byte_value);
        tairStringPipeline.exincrBy(bkey1, incr_value);
        tairStringPipeline.exget(bkey1);

        objs = tairStringPipeline.syncAndReturnAll();

        i = 0;
        assertEquals("OK", objs.get(i++));
        assertEquals(new_long_value, objs.get(i++));
        assertEquals(true, Arrays.equals(new_byte_value, (byte[]) ExgetResult.class.cast(objs.get(i)).getValue()));
        assertEquals(2, ExgetResult.class.cast(objs.get(i++)).getVersion());
    }

    @Test
    public void exincrbyParamsPipelineTest() throws InterruptedException {
        int i = 0;
        String num_string_value = "100";
        byte[] num_byte_value = SafeEncoder.encode("100");
        long incr_value = 100;
        String new_string_value = "200";
        byte[] new_byte_value = SafeEncoder.encode("200");
        long new_long_value = 200;

        ExincrbyParams params_nx_px = new ExincrbyParams();
        params_nx_px.nx();
        params_nx_px.px(1000);
        ExincrbyParams params_xx_ex = new ExincrbyParams();
        params_xx_ex.xx();
        params_xx_ex.ex(1);
        ExincrbyParams params_xx_pxat = new ExincrbyParams();
        params_xx_pxat.xx();
        params_xx_pxat.pxat(System.currentTimeMillis() + 1000);

        // String
        // nx_px start
        tairStringPipeline.exincrBy(key1, incr_value, params_nx_px);
        tairStringPipeline.exget(key1);
        List<Object> objs = tairStringPipeline.syncAndReturnAll();
        i = 0;
        assertEquals(incr_value, objs.get(i++));
        assertEquals(num_string_value, ExgetResult.class.cast(objs.get(i)).getValue());
        assertEquals(1, ExgetResult.class.cast(objs.get(i++)).getVersion());

        // nx_px sleep
        Thread.sleep(1000);
        tairStringPipeline.exget(key1);
        objs = tairStringPipeline.syncAndReturnAll();
        i = 0;
        assertEquals(null, objs.get(i++));

        // xx_ex start
        tairStringPipeline.exset(key1, num_string_value);
        tairStringPipeline.exincrBy(key1, incr_value, params_xx_ex);
        tairStringPipeline.exget(key1);
        objs = tairStringPipeline.syncAndReturnAll();
        i = 0;
        assertEquals("OK", objs.get(i++));
        assertEquals(new_long_value, objs.get(i++));
        assertEquals(new_string_value, ExgetResult.class.cast(objs.get(i)).getValue());
        assertEquals(2, ExgetResult.class.cast(objs.get(i++)).getVersion());

        // xx_ex sleep
        Thread.sleep(1000);
        tairStringPipeline.exget(key1);
        objs = tairStringPipeline.syncAndReturnAll();
        i = 0;
        assertEquals(null, objs.get(i++));

        // xx_pxat start
        tairStringPipeline.exset(key1, num_string_value);
        params_xx_pxat.pxat(System.currentTimeMillis() + 1000);
        tairStringPipeline.exincrBy(key1, incr_value, params_xx_pxat);
        tairStringPipeline.exget(key1);
        objs = tairStringPipeline.syncAndReturnAll();
        i = 0;
        assertEquals("OK", objs.get(i++));
        assertEquals(new_long_value, objs.get(i++));
        assertEquals(new_string_value, ExgetResult.class.cast(objs.get(i)).getValue());
        assertEquals(2, ExgetResult.class.cast(objs.get(i++)).getVersion());

        // xx_pxat sleep
        Thread.sleep(1000);
        tairStringPipeline.exget(key1);
        objs = tairStringPipeline.syncAndReturnAll();
        i = 0;
        assertEquals(null, objs.get(i++));


        // binary
        // nx_px start
        tairStringPipeline.exincrBy(bkey1, incr_value, params_nx_px);
        tairStringPipeline.exget(bkey1);
        objs = tairStringPipeline.syncAndReturnAll();
        i = 0;
        assertEquals(incr_value, objs.get(i++));
        assertEquals(true, Arrays.equals(num_byte_value, (byte[]) ExgetResult.class.cast(objs.get(i)).getValue()));
        assertEquals(1, ExgetResult.class.cast(objs.get(i++)).getVersion());

        // nx_px sleep
        Thread.sleep(1000);
        tairStringPipeline.exget(bkey1);
        objs = tairStringPipeline.syncAndReturnAll();
        i = 0;
        assertEquals(null, objs.get(i++));

        // xx_ex start
        tairStringPipeline.exset(bkey1, num_byte_value);
        tairStringPipeline.exincrBy(bkey1, incr_value, params_xx_ex);
        tairStringPipeline.exget(bkey1);
        objs = tairStringPipeline.syncAndReturnAll();
        i = 0;
        assertEquals("OK", objs.get(i++));
        assertEquals(new_long_value, objs.get(i++));
        assertEquals(true, Arrays.equals(new_byte_value, (byte[]) ExgetResult.class.cast(objs.get(i)).getValue()));
        assertEquals(2, ExgetResult.class.cast(objs.get(i++)).getVersion());

        // xx_ex sleep
        Thread.sleep(1000);
        tairStringPipeline.exget(bkey1);
        objs = tairStringPipeline.syncAndReturnAll();
        i = 0;
        assertEquals(null, objs.get(i++));

        // xx_pxat start
        tairStringPipeline.exset(bkey1, num_byte_value);
        params_xx_pxat.pxat(System.currentTimeMillis() + 1000);
        tairStringPipeline.exincrBy(bkey1, incr_value, params_xx_pxat);
        tairStringPipeline.exget(bkey1);
        objs = tairStringPipeline.syncAndReturnAll();
        i = 0;
        assertEquals("OK", objs.get(i++));
        assertEquals(new_long_value, objs.get(i++));
        assertEquals(true, Arrays.equals(new_byte_value, (byte[]) ExgetResult.class.cast(objs.get(i)).getValue()));
        assertEquals(2, ExgetResult.class.cast(objs.get(i++)).getVersion());

        // xx_pxat sleep
        Thread.sleep(1000);
        tairStringPipeline.exget(bkey1);
        objs = tairStringPipeline.syncAndReturnAll();
        i = 0;
        assertEquals(null, objs.get(i++));
    }

    @Test
    public void exincrbyfloatPipelineTest() {
        int i = 0;
        String num_string_value = "100";
        byte[] num_byte_value = SafeEncoder.encode("100");
        Double incr_value = Double.valueOf(100);
        String new_string_value = "200";
        byte[] new_byte_value = SafeEncoder.encode("200");
        Double new_float_value = Double.valueOf(200);

        // String
        tairStringPipeline.exset(key1, num_string_value);
        tairStringPipeline.exincrByFloat(key1, incr_value);
        tairStringPipeline.exget(key1);

        List<Object> objs = tairStringPipeline.syncAndReturnAll();

        i = 0;
        assertEquals("OK", objs.get(i++));
        assertEquals(new_float_value, objs.get(i++));
        assertEquals(new_string_value, ExgetResult.class.cast(objs.get(i)).getValue());
        assertEquals(2, ExgetResult.class.cast(objs.get(i++)).getVersion());

        //binary
        tairStringPipeline.exset(bkey1, num_byte_value);
        tairStringPipeline.exincrByFloat(bkey1, incr_value);
        tairStringPipeline.exget(bkey1);

        objs = tairStringPipeline.syncAndReturnAll();

        i = 0;
        assertEquals("OK", objs.get(i++));
        assertEquals(new_float_value, objs.get(i++));
        assertEquals(true, Arrays.equals(new_byte_value, (byte[]) ExgetResult.class.cast(objs.get(i)).getValue()));
        assertEquals(2, ExgetResult.class.cast(objs.get(i++)).getVersion());
    }

    @Test
    public void exincrbyfloatParamsPipelineTest() throws InterruptedException {
        int i = 0;
        String num_string_value = "100";
        byte[] num_byte_value = SafeEncoder.encode("100");
        Double incr_value = Double.valueOf(100);
        String new_string_value = "200";
        byte[] new_byte_value = SafeEncoder.encode("200");
        Double new_float_value = Double.valueOf(200);

        ExincrbyFloatParams params_nx_px = new ExincrbyFloatParams();
        params_nx_px.nx();
        params_nx_px.px(1000);
        ExincrbyFloatParams params_xx_ex = new ExincrbyFloatParams();
        params_xx_ex.xx();
        params_xx_ex.ex(1);
        ExincrbyFloatParams params_xx_pxat = new ExincrbyFloatParams();
        params_xx_pxat.xx();
        params_xx_pxat.pxat(System.currentTimeMillis() + 1000);

        // String
        // nx_px start
        tairStringPipeline.exincrByFloat(key1, incr_value, params_nx_px);
        tairStringPipeline.exget(key1);
        List<Object> objs = tairStringPipeline.syncAndReturnAll();
        i = 0;
        assertEquals(incr_value, objs.get(i++));
        assertEquals(num_string_value, ExgetResult.class.cast(objs.get(i)).getValue());
        assertEquals(1, ExgetResult.class.cast(objs.get(i++)).getVersion());

        // nx_px sleep
        Thread.sleep(1000);
        tairStringPipeline.exget(key1);
        objs = tairStringPipeline.syncAndReturnAll();
        i = 0;
        assertEquals(null, objs.get(i++));

        // xx_ex start
        tairStringPipeline.exset(key1, num_string_value);
        tairStringPipeline.exincrByFloat(key1, incr_value, params_xx_ex);
        tairStringPipeline.exget(key1);
        objs = tairStringPipeline.syncAndReturnAll();
        i = 0;
        assertEquals("OK", objs.get(i++));
        assertEquals(new_float_value, objs.get(i++));
        assertEquals(new_string_value, ExgetResult.class.cast(objs.get(i)).getValue());
        assertEquals(2, ExgetResult.class.cast(objs.get(i++)).getVersion());

        // xx_ex sleep
        Thread.sleep(1000);
        tairStringPipeline.exget(key1);
        objs = tairStringPipeline.syncAndReturnAll();
        i = 0;
        assertEquals(null, objs.get(i++));

        // xx_pxat start
        tairStringPipeline.exset(key1, num_string_value);
        params_xx_pxat.pxat(System.currentTimeMillis() + 1000);
        tairStringPipeline.exincrByFloat(key1, incr_value, params_xx_pxat);
        tairStringPipeline.exget(key1);
        objs = tairStringPipeline.syncAndReturnAll();
        i = 0;
        assertEquals("OK", objs.get(i++));
        assertEquals(new_float_value, objs.get(i++));
        assertEquals(new_string_value, ExgetResult.class.cast(objs.get(i)).getValue());
        assertEquals(2, ExgetResult.class.cast(objs.get(i++)).getVersion());

        // xx_pxat sleep
        Thread.sleep(1000);
        tairStringPipeline.exget(key1);
        objs = tairStringPipeline.syncAndReturnAll();
        i = 0;
        assertEquals(null, objs.get(i++));


        // binary
        // nx_px start
        tairStringPipeline.exincrByFloat(bkey1, incr_value, params_nx_px);
        tairStringPipeline.exget(bkey1);
        objs = tairStringPipeline.syncAndReturnAll();
        i = 0;
        assertEquals(incr_value, objs.get(i++));
        assertEquals(true, Arrays.equals(num_byte_value, (byte[]) ExgetResult.class.cast(objs.get(i)).getValue()));
        assertEquals(1, ExgetResult.class.cast(objs.get(i++)).getVersion());

        // nx_px sleep
        Thread.sleep(1000);
        tairStringPipeline.exget(bkey1);
        objs = tairStringPipeline.syncAndReturnAll();
        i = 0;
        assertEquals(null, objs.get(i++));

        // xx_ex start
        tairStringPipeline.exset(bkey1, num_byte_value);
        tairStringPipeline.exincrByFloat(bkey1, incr_value, params_xx_ex);
        tairStringPipeline.exget(bkey1);
        objs = tairStringPipeline.syncAndReturnAll();
        i = 0;
        assertEquals("OK", objs.get(i++));
        assertEquals(new_float_value, objs.get(i++));
        assertEquals(true, Arrays.equals(new_byte_value, (byte[]) ExgetResult.class.cast(objs.get(i)).getValue()));
        assertEquals(2, ExgetResult.class.cast(objs.get(i++)).getVersion());

        // xx_ex sleep
        Thread.sleep(1000);
        tairStringPipeline.exget(bkey1);
        objs = tairStringPipeline.syncAndReturnAll();
        i = 0;
        assertEquals(null, objs.get(i++));

        // xx_pxat start
        tairStringPipeline.exset(bkey1, num_byte_value);
        params_xx_pxat.pxat(System.currentTimeMillis() + 1000);
        tairStringPipeline.exincrByFloat(bkey1, incr_value, params_xx_pxat);
        tairStringPipeline.exget(bkey1);
        objs = tairStringPipeline.syncAndReturnAll();
        i = 0;
        assertEquals("OK", objs.get(i++));
        assertEquals(new_float_value, objs.get(i++));
        assertEquals(true, Arrays.equals(new_byte_value, (byte[]) ExgetResult.class.cast(objs.get(i)).getValue()));
        assertEquals(2, ExgetResult.class.cast(objs.get(i++)).getVersion());

        // xx_pxat sleep
        Thread.sleep(1000);
        tairStringPipeline.exget(bkey1);
        objs = tairStringPipeline.syncAndReturnAll();
        i = 0;
        assertEquals(null, objs.get(i++));
    }

    @Test
    public void excasPipelineTest() {
        int i = 0;

        // String
        tairStringPipeline.exset(key1, value1);
        tairStringPipeline.excas(key1, "new" + value1, 2);
        tairStringPipeline.excas(key1, "new" + value1, 1);

        List<Object> objs = tairStringPipeline.syncAndReturnAll();

        i = 0;
        assertEquals("OK", objs.get(i++));
        assertEquals(value1, ExcasResult.class.cast(objs.get(i)).getValue());
        assertEquals(1, ExcasResult.class.cast(objs.get(i++)).getVersion());
        assertEquals("OK", ExcasResult.class.cast(objs.get(i)).getMsg());
        assertEquals("", ExcasResult.class.cast(objs.get(i)).getValue());
        assertEquals(2, ExcasResult.class.cast(objs.get(i++)).getVersion());

        //binary
        tairStringPipeline.exset(bkey1, bvalue1);
        tairStringPipeline.excas(bkey1, SafeEncoder.encode("new" + bvalue1), 2);
        tairStringPipeline.excas(bkey1, SafeEncoder.encode("new" + bvalue1), 1);

        objs = tairStringPipeline.syncAndReturnAll();

        i = 0;
        assertEquals("OK", objs.get(i++));
        assertEquals(true, Arrays.equals(bvalue1, (byte[]) ExcasResult.class.cast(objs.get(i)).getValue()));
        assertEquals(1, ExcasResult.class.cast(objs.get(i++)).getVersion());
        assertEquals(true, Arrays.equals(SafeEncoder.encode("OK"), (byte[]) ExcasResult.class.cast(objs.get(i)).getMsg()));
        assertEquals(true, Arrays.equals(SafeEncoder.encode(""), (byte[]) ExcasResult.class.cast(objs.get(i)).getValue()));
        assertEquals(2, ExcasResult.class.cast(objs.get(i++)).getVersion());
    }

    @Test
    public void excadPipelineTest() {
        int i = 0;

        // String
        tairStringPipeline.exset(key1, value1);
        tairStringPipeline.excad(key1, 2);
        tairStringPipeline.excad(key1, 1);

        List<Object> objs = tairStringPipeline.syncAndReturnAll();

        i = 0;
        assertEquals("OK", objs.get(i++));
        assertEquals((long) 0, objs.get(i++));
        assertEquals((long) 1, objs.get(i++));

        //binary
        tairStringPipeline.exset(bkey1, bvalue1);
        tairStringPipeline.excad(bkey1, 2);
        tairStringPipeline.excad(bkey1, 1);

        objs = tairStringPipeline.syncAndReturnAll();

        i = 0;
        assertEquals("OK", objs.get(i++));
        assertEquals((long) 0, objs.get(i++));
        assertEquals((long) 1, objs.get(i++));
    }
}
