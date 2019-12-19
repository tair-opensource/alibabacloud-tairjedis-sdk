import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.*;


/**
 * @author dwan
 * @date 2019/12/18
 */
public class TairStringTest extends TairStringTestBase {
    private String key;
    private String value;
    byte[] bkey;
    byte[] bvalue;

    public TairStringTest() {
        key = "key" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        value = "value" + Thread.currentThread().getName() + UUID.randomUUID().toString();
        bkey = ("bkey" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
        bvalue = ("bvalue" + Thread.currentThread().getName() + UUID.randomUUID().toString()).getBytes();
    }

    @Test
    public void exsetTest() {
        String ret = tairString.exset(key,value);
        assertEquals("OK", ret);
        List<String> getStringResult = tairString.exget(key);
        assertNotNull(getStringResult);
        assertEquals("1",getStringResult.get(0));
        assertEquals(true, value.equals(getStringResult.get(1)));
        assertEquals(value,getStringResult.get(1));

        String bret = tairString.exset(bkey,bvalue);
        assertEquals("OK", bret);
        List<byte[]> getByteResult = tairString.exget(bkey);
        assertNotNull(getByteResult);
        assertEquals("1",getByteResult.get(0));
        assertEquals(true, Arrays.equals(bvalue,getByteResult.get(1)));
        assertEquals(bvalue,getByteResult.get(1));
    }

}
