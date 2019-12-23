import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author dwan
 * @date 2019/12/18
 */
public class TairStringPipelineTest extends TairStringTestBase {
    private String key1;
    private String value1;
    private String key2;
    private String value2;
    byte[] bkey1;
    byte[] bvalue1;
    byte[] bkey2;
    byte[] bvalue2;

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
    public void exsetStringPipeline() {

        tairStringPipeline.exset(key1,value1);
        tairStringPipeline.exset(key2,value2);
        tairStringPipeline.exget(key1);
        tairStringPipeline.exget(key2);
        List<Object> objs = tairStringPipeline.syncAndReturnAll();

        assertEquals("OK", objs.get(0));
        assertEquals("OK", objs.get(1));
        assertEquals(value1, objs.get(2));
        assertEquals(value2, objs.get(3));

    }

    @Test
    public void exsetBytePipeline() {

        tairStringPipeline.exset(bkey1,bvalue1);
        tairStringPipeline.exset(bkey2,bvalue2);
        tairStringPipeline.exget(bkey1);
        tairStringPipeline.exget(bkey2);
        List<Object> objs = tairStringPipeline.syncAndReturnAll();

        assertEquals("OK", objs.get(0));
        assertEquals("OK", objs.get(1));
        assertEquals(bvalue1, objs.get(2));
        assertEquals(bvalue2, objs.get(3));

    }
}
