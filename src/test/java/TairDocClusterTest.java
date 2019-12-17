import java.util.UUID;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author bodong.ybd
 * @date 2019/12/17
 */
public class TairDocClusterTest extends TestBase {
    private String jsonKey;
    private static final String JSON_STRING_EXAMPLE = "{\"foo\":\"bar\",\"baz\":42}";
    private static final String JSON_ARRAY_EXAMPLE = "{\"id\":[1,2,3]}";

    public TairDocClusterTest() {
        jsonKey = "jsonkey" + "-" + Thread.currentThread().getName() + "-" + UUID.randomUUID().toString();
    }

    @Test
    public void jsonSetTest() {
        String ret = tairDocCluster.jsonset(jsonKey, jsonKey, ".", JSON_STRING_EXAMPLE);
        assertEquals("OK", ret);

        ret = tairDocCluster.jsonget(jsonKey, jsonKey, ".");
        assertEquals(JSON_STRING_EXAMPLE, ret);

        ret = tairDocCluster.jsonget(jsonKey, jsonKey, ".foo");
        assertEquals("\"bar\"", ret);

        ret = tairDocCluster.jsonget(jsonKey, jsonKey, ".baz");
        assertEquals("42", ret);
    }
}
