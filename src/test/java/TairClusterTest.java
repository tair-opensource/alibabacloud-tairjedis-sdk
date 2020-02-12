import java.util.List;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

public class TairClusterTest extends TestBase {
    private static final int KEYSIZE = 1000;

    @Test
    public void existsCommand() {
        String[] keys = new String[KEYSIZE];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = UUID.randomUUID().toString();
        }

        Assert.assertEquals(0, (long)tairCluster.exists(keys));

        for (int i = 0; i < keys.length; i++) {
            tairCluster.set(keys[i], "value");
        }
        Assert.assertEquals(keys.length, (long)tairCluster.exists(keys));
    }

    @Test
    public void delCommand() {
        String[] keys = new String[KEYSIZE];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = UUID.randomUUID().toString();
        }

        Assert.assertEquals(0, (long)tairCluster.exists(keys));

        for (int i = 0; i < keys.length; i++) {
            tairCluster.set(keys[i], "value");
        }
        Assert.assertEquals(keys.length, (long)tairCluster.exists(keys));

        Assert.assertEquals(keys.length, (long)tairCluster.del(keys));
        Assert.assertEquals(0, (long)tairCluster.exists(keys));
    }

    @Test
    public void mgetCommand() {
        String[] keys = new String[KEYSIZE];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = UUID.randomUUID().toString();
        }

        Assert.assertEquals(0, (long)tairCluster.exists(keys));

        for (int i = 0; i < keys.length; i++) {
            tairCluster.set(keys[i], String.valueOf(i));
        }

        List<String> values = tairCluster.mget(keys);
        for (int i = 0; i < values.size(); i++) {
            Assert.assertEquals(String.valueOf(i), values.get(i));
        }
    }

    @Test
    public void msetCommand() {
        String[] keys = new String[KEYSIZE];
        String[] values = new String[KEYSIZE];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = UUID.randomUUID().toString();
            values[i] = UUID.randomUUID().toString();
        }

        Assert.assertEquals(0, (long)tairCluster.exists(keys));

        String[] keysvalues = new String[KEYSIZE * 2];
        for (int i = 0, j = 0; i < keysvalues.length; i += 2, j++) {
            keysvalues[i] = keys[j];
            keysvalues[i + 1] = values[j];
        }
        Assert.assertEquals("OK", tairCluster.mset(keysvalues));

        List<String> rets = tairCluster.mget(keys);
        for (int i = 0; i < rets.size(); i++) {
            Assert.assertEquals(values[i], rets.get(i));
        }
    }

    @Test
    public void existsByteCommand() {
        byte[][] keys = new byte[KEYSIZE][];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = UUID.randomUUID().toString().getBytes();
        }

        Assert.assertEquals(0, (long)tairCluster.exists(keys));

        for (int i = 0; i < keys.length; i++) {
            tairCluster.set(keys[i], keys[i]);
        }
        Assert.assertEquals(keys.length, (long)tairCluster.exists(keys));
    }

    @Test
    public void delByteCommand() {
        byte[][] keys = new byte[KEYSIZE][];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = UUID.randomUUID().toString().getBytes();
        }

        Assert.assertEquals(0, (long)tairCluster.exists(keys));

        for (int i = 0; i < keys.length; i++) {
            tairCluster.set(keys[i], keys[i]);
        }
        Assert.assertEquals(keys.length, (long)tairCluster.exists(keys));

        Assert.assertEquals(keys.length, (long)tairCluster.del(keys));
        Assert.assertEquals(0, (long)tairCluster.exists(keys));
    }

    @Test
    public void mgetByteCommand() {
        byte[][] keys = new byte[KEYSIZE][];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = UUID.randomUUID().toString().getBytes();
        }

        Assert.assertEquals(0, (long)tairCluster.exists(keys));

        for (int i = 0; i < keys.length; i++) {
            tairCluster.set(keys[i], String.valueOf(i).getBytes());
        }

        List<byte[]> values = tairCluster.mget(keys);
        for (int i = 0; i < values.size(); i++) {
            Assert.assertEquals(String.valueOf(i), new String(values.get(i)));
        }
    }

    @Test
    public void msetByteCommand() {
        byte[][] keys = new byte[KEYSIZE][];
        byte[][] values = new byte[KEYSIZE][];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = UUID.randomUUID().toString().getBytes();
            values[i] = UUID.randomUUID().toString().getBytes();
        }

        Assert.assertEquals(0, (long)tairCluster.exists(keys));

        byte[][] keysvalues = new byte[KEYSIZE * 2][];
        for (int i = 0, j = 0; i < keysvalues.length; i += 2, j++) {
            keysvalues[i] = keys[j];
            keysvalues[i + 1] = values[j];
        }
        Assert.assertEquals("OK", tairCluster.mset(keysvalues));

        List<byte[]> rets = tairCluster.mget(keys);
        for (int i = 0; i < rets.size(); i++) {
            Assert.assertEquals(new String(values[i]), new String(rets.get(i)));
        }
    }
}
