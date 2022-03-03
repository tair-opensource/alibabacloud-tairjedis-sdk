package com.aliyun.tair.tests.tairsearch;

import com.aliyun.tair.tairsearch.params.TFTScanParams;
import org.junit.Test;
import redis.clients.jedis.ScanResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static redis.clients.jedis.ScanParams.SCAN_POINTER_START;

public class TairSearchClusterTest extends TairSearchTestBase {

    @Test
    public void tftcreateindex() {
        jedisCluster.del("tftkey");
        String ret = tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        String mapping = tairSearchCluster.tftgetindexmappings("tftkey");
        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"},\"f1\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"}}}}}", mapping);
    }

    @Test
    public void tftupdateindex() {
        jedisCluster.del("tftkey");
        String ret = tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        String mapping = tairSearchCluster.tftgetindexmappings("tftkey");
        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"}}}}}", mapping);

        ret = tairSearchCluster.tftupdateindex("tftkey", "{\"mappings\":{\"properties\":{\"f1\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        mapping = tairSearchCluster.tftgetindexmappings("tftkey");
        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"},\"f1\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"}}}}}", mapping);
    }

    @Test
    public void tftadddoc() {
        jedisCluster.del("tftkey");
        tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v0\",\"f1\":\"3\"}", "1");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v1\",\"f1\":\"3\"}", "2");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"3\"}", "3");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"4\"}", "4");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"5\"}", "5");

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}],\"max_score\":1.223144,\"total\":{\"relation\":\"eq\",\"value\":3}}}",
                tairSearchCluster.tftsearch("tftkey", "{\"query\":{\"match\":{\"f1\":\"3\"}}}"));

        assertEquals("{\"_id\":\"3\",\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}", tairSearchCluster.tftgetdoc("tftkey", "3"));
        assertEquals("1", tairSearchCluster.tftdeldoc("tftkey", "3"));
        assertEquals(null, tairSearchCluster.tftgetdoc("tftkey", "3"));

        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"},\"f1\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"}}}}}", tairSearchCluster.tftgetindexmappings("tftkey"));
    }

    @Test
    public void tfupdatedoc() {
        jedisCluster.del("tftkey");
        String ret = tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"redis is a nosql database\"}", "1");
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.153426,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.153426,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                tairSearchCluster.tftsearch("tftkey", "{\"query\":{\"term\":{\"f0\":\"redis\"}}}"));

        ret = tairSearchCluster.tftupdateindex("tftkey", "{\"mappings\":{\"properties\":{\"f1\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        tairSearchCluster.tftupdatedoc("tftkey", "1", "{\"f1\":\"mysql is a dbms\"}");
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.191783,\"_source\":{\"f0\":\"redis is a nosql database\",\"f1\":\"mysql is a dbms\"}}],\"max_score\":0.191783,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                tairSearchCluster.tftsearch("tftkey", "{\"query\":{\"term\":{\"f1\":\"mysql\"}}}"));
    }

    @Test
    public void tfdeldoc() {
        jedisCluster.del("tftkey");
        tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v0\",\"f1\":\"3\"}", "1");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v1\",\"f1\":\"3\"}", "2");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"3\"}", "3");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"4\"}", "4");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"5\"}", "5");

        assertEquals(1, tairSearchCluster.tftexists("tftkey", "3").intValue());
        assertEquals(5, tairSearchCluster.tftdocnum("tftkey").intValue());
        assertEquals("3", tairSearchCluster.tftdeldoc("tftkey", "3", "4", "5"));
        assertEquals(0, tairSearchCluster.tftexists("tftkey", "3").intValue());
        assertEquals(2, tairSearchCluster.tftdocnum("tftkey").intValue());
    }

    @Test
    public void tfdelall() {
        jedisCluster.del("tftkey");
        tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v0\",\"f1\":\"3\"}", "1");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v1\",\"f1\":\"3\"}", "2");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"3\"}", "3");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"4\"}", "4");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"5\"}", "5");

        assertEquals("OK", tairSearchCluster.tftdelall("tftkey"));
        assertEquals(0, tairSearchCluster.tftdocnum("tftkey").intValue());
    }

    @Test
    public void tfscandocid() {
        jedisCluster.del("tftkey");
        tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v0\",\"f1\":\"3\"}", "1");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v1\",\"f1\":\"3\"}", "2");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"3\"}", "3");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"4\"}", "4");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"5\"}", "5");

        ScanResult<String> res = tairSearchCluster.tftscandocid("tftkey", "0");
        assertEquals(SCAN_POINTER_START, res.getCursor());
        assertTrue(res.getResult().size() == 5);

        assertEquals("1", res.getResult().get(0));
        assertEquals("2", res.getResult().get(1));
        assertEquals("3", res.getResult().get(2));
        assertEquals("4", res.getResult().get(3));
        assertEquals("5", res.getResult().get(4));
    }

    @Test
    public void tfscandocidwithcount() {
        jedisCluster.del("tftkey");
        tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v0\",\"f1\":\"3\"}", "1");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v1\",\"f1\":\"3\"}", "2");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"3\"}", "3");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"4\"}", "4");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"5\"}", "5");

        TFTScanParams params = new TFTScanParams();
        params.count(3);
        ScanResult<String> res = tairSearchCluster.tftscandocid("tftkey", "0", params);
        assertEquals("3", res.getCursor());
        assertTrue(res.getResult().size() == 3);

        assertEquals("1", res.getResult().get(0));
        assertEquals("2", res.getResult().get(1));
        assertEquals("3", res.getResult().get(2));

        ScanResult<String> res2 = tairSearchCluster.tftscandocid("tftkey", "3", params);
        assertEquals("0", res2.getCursor());
        assertTrue(res2.getResult().size() == 2);

        assertEquals("4", res2.getResult().get(0));
        assertEquals("5", res2.getResult().get(1));
    }

    @Test
    public void tfscandocidwithmatch() {
        jedisCluster.del("tftkey");
        tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v0\",\"f1\":\"3\"}", "1_redis_doc");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v1\",\"f1\":\"3\"}", "2_redis_doc");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"3\"}", "3_mysql_doc");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"4\"}", "4_mysql_doc");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"5\"}", "5_tidb_doc");

        TFTScanParams params = new TFTScanParams();
        params.match("*redis*");
        ScanResult<String> res = tairSearchCluster.tftscandocid("tftkey", "0", params);
        assertEquals("0", res.getCursor());
        assertTrue(res.getResult().size() == 2);

        assertEquals("1_redis_doc", res.getResult().get(0));
        assertEquals("2_redis_doc", res.getResult().get(1));

        TFTScanParams params2 = new TFTScanParams();
        params2.match("*tidb*");
        ScanResult<String> res2 = tairSearchCluster.tftscandocid("tftkey", "0", params2);
        assertEquals("0", res2.getCursor());
        assertTrue(res2.getResult().size() == 1);

        assertEquals("5_tidb_doc", res2.getResult().get(0));
    }

    @Test
    public void unicodetest() {
        jedisCluster.del("tftkey");
        tairSearchCluster.tftmappingindex("tftkey", "{\"mappings\":{\"properties\":{\"f0\":{\"type\":\"text\",\"analyzer\":\"chinese\"}}}}");
        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"analyzer\":\"chinese\",\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"}}}}}", tairSearchCluster.tftgetindexmappings("tftkey"));

        jedisCluster.del("tftkey");
        tairSearchCluster.tftmappingindex("tftkey", "{\"mappings\":{\"properties\":{\"f0\":{\"type\":\"text\",\"search_analyzer\":\"chinese\"}}}}");
        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\",\"search_analyzer\":\"chinese\"}}}}}", tairSearchCluster.tftgetindexmappings("tftkey"));
        jedisCluster.del("tftkey");

        tairSearchCluster.tftmappingindex("tftkey", "{\"mappings\":{\"properties\":{\"f0\":{\"type\":\"text\",\"analyzer\":\"chinese\", \"search_analyzer\":\"chinese\"}}}}");
        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"analyzer\":\"chinese\",\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\",\"search_analyzer\":\"chinese\"}}}}}", tairSearchCluster.tftgetindexmappings("tftkey"));
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"夏天是一个很热的季节\"}", "1");
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.077948,\"_source\":{\"f0\":\"夏天是一个很热的季节\"}}],\"max_score\":0.077948,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                tairSearchCluster.tftsearch("tftkey", "{\"query\":{\"match\":{\"f0\":\"夏天冬天\"}}}"));
    }

    @Test
    public  void searchcachetest() throws Exception {
        jedisCluster.del("tftkey");
        tairSearchCluster.tftmappingindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v0\",\"f1\":\"3\"}", "1");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v1\",\"f1\":\"3\"}", "2");

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.594535,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":0.594535,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}}],\"max_score\":0.594535,\"total\":{\"relation\":\"eq\",\"value\":2}}}",
                tairSearchCluster.tftsearch("tftkey", "{\"query\":{\"match\":{\"f1\":\"3\"}}}", true));

        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"3\"}", "3");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"4\"}", "4");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"5\"}", "5");

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.594535,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":0.594535,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}}],\"max_score\":0.594535,\"total\":{\"relation\":\"eq\",\"value\":2}}}",
                tairSearchCluster.tftsearch("tftkey", "{\"query\":{\"match\":{\"f1\":\"3\"}}}", true));

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}],\"max_score\":1.223144,\"total\":{\"relation\":\"eq\",\"value\":3}}}",
                tairSearchCluster.tftsearch("tftkey", "{\"query\":{\"match\":{\"f1\":\"3\"}}}"));

        // wait for LRU cache expired
        Thread.sleep(10000);

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}],\"max_score\":1.223144,\"total\":{\"relation\":\"eq\",\"value\":3}}}",
                tairSearchCluster.tftsearch("tftkey", "{\"query\":{\"match\":{\"f1\":\"3\"}}}", true));
    }

    @Test
    public void tftmaddteststring() {
        jedisCluster.del("tftkey");
        tairSearchCluster.tftmappingindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}");
        Map<String, String> docs = new HashMap();
        docs.put("{\"f0\":\"v0\",\"f1\":\"3\"}", "1");
        docs.put("{\"f0\":\"v1\",\"f1\":\"3\"}", "2");
        docs.put("{\"f0\":\"v3\",\"f1\":\"3\"}", "3");
        docs.put("{\"f0\":\"v3\",\"f1\":\"4\"}", "4");
        docs.put("{\"f0\":\"v3\",\"f1\":\"5\"}", "5");

        tairSearchCluster.tftmadddoc("tftkey", docs);

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}],\"max_score\":1.223144,\"total\":{\"relation\":\"eq\",\"value\":3}}}",
                tairSearchCluster.tftsearch("tftkey", "{\"query\":{\"match\":{\"f1\":\"3\"}}}"));

        assertEquals("{\"_id\":\"3\",\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}", tairSearchCluster.tftgetdoc("tftkey", "3"));
        assertEquals("1", tairSearchCluster.tftdeldoc("tftkey", "3"));
        assertEquals(null, tairSearchCluster.tftgetdoc("tftkey", "3"));

        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"},\"f1\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"}}}}}", tairSearchCluster.tftgetindexmappings("tftkey"));
    }

    @Test
    public void tftmaddtestbyte() {
        jedisCluster.del("tftkey");
        tairSearchCluster.tftmappingindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}");
        Map<byte[], byte[]> docs = new HashMap();
        docs.put("{\"f0\":\"v0\",\"f1\":\"3\"}".getBytes(), "1".getBytes());
        docs.put("{\"f0\":\"v1\",\"f1\":\"3\"}".getBytes(), "2".getBytes());
        docs.put("{\"f0\":\"v3\",\"f1\":\"3\"}".getBytes(), "3".getBytes());
        docs.put("{\"f0\":\"v3\",\"f1\":\"4\"}".getBytes(), "4".getBytes());
        docs.put("{\"f0\":\"v3\",\"f1\":\"5\"}".getBytes(), "5".getBytes());

        tairSearchCluster.tftmadddoc("tftkey".getBytes(), docs);

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}],\"max_score\":1.223144,\"total\":{\"relation\":\"eq\",\"value\":3}}}",
                tairSearchCluster.tftsearch("tftkey", "{\"query\":{\"match\":{\"f1\":\"3\"}}}"));

        assertEquals("{\"_id\":\"3\",\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}", tairSearchCluster.tftgetdoc("tftkey", "3"));
        assertEquals("1", tairSearchCluster.tftdeldoc("tftkey", "3"));
        assertEquals(null, tairSearchCluster.tftgetdoc("tftkey", "3"));

        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"},\"f1\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"}}}}}", tairSearchCluster.tftgetindexmappings("tftkey"));
    }
}
