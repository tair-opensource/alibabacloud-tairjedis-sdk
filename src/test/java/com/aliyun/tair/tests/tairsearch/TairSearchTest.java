package com.aliyun.tair.tests.tairsearch;

import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TairSearchTest extends TairSearchTestBase {

    @Test
    public void tfttest() {
        jedis.del("tftkey");
        tairSearch.tftmappingindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v0\",\"f1\":\"3\"}", "1");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v1\",\"f1\":\"3\"}", "2");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"3\"}", "3");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"4\"}", "4");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"5\"}", "5");

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}],\"max_score\":1.223144,\"total\":{\"relation\":\"eq\",\"value\":3}}}",
                tairSearch.tftsearch("tftkey", "{\"query\":{\"match\":{\"f1\":\"3\"}}}"));

        assertEquals("{\"_id\":\"3\",\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}", tairSearch.tftgetdoc("tftkey", "3"));
        assertEquals("1", tairSearch.tftdeldoc("tftkey", "3"));
        assertEquals(null, tairSearch.tftgetdoc("tftkey", "3"));

        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"copy_to\":\"\",\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"BM25\",\"store\":false,\"type\":\"text\"},\"f1\":{\"boost\":1.0,\"copy_to\":\"\",\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"BM25\",\"store\":false,\"type\":\"text\"}}}}}", tairSearch.tftgetindexmappings("tftkey"));
    }

    @Test
    public void unicodetest() {
        jedis.del("tftkey");
        tairSearch.tftmappingindex("tftkey", "{\"mappings\":{\"properties\":{\"f0\":{\"type\":\"text\",\"analyzer\":\"chinese\"}}}}");
        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"true\",\"properties\":{\"analyzer\":\"chinese\",\"f0\":{\"boost\":1.0,\"copy_to\":\"\",\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"BM25\",\"store\":false,\"type\":\"text\"}}}}}", tairSearch.tftgetindexmappings("tftkey"));

        jedis.del("tftkey");
        tairSearch.tftmappingindex("tftkey", "{\"mappings\":{\"properties\":{\"f0\":{\"type\":\"text\",\"search_analyzer\":\"chinese\"}}}}");
        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"true\",\"properties\":{\"f0\":{\"boost\":1.0,\"copy_to\":\"\",\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"BM25\",\"store\":false,\"type\":\"text\"},\"search_analyzer\":\"chinese\"}}}}", tairSearch.tftgetindexmappings("tftkey"));
        jedis.del("tftkey");

        tairSearch.tftmappingindex("tftkey", "{\"mappings\":{\"properties\":{\"f0\":{\"type\":\"text\",\"analyzer\":\"chinese\", \"search_analyzer\":\"chinese\"}}}}");
        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"true\",\"properties\":{\"analyzer\":\"chinese\",\"f0\":{\"boost\":1.0,\"copy_to\":\"\",\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"BM25\",\"store\":false,\"type\":\"text\"},\"search_analyzer\":\"chinese\"}}}}", tairSearch.tftgetindexmappings("tftkey"));
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"夏天是一个很热的季节\"}", "1");
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.058461,\"_source\":{\"f0\":\"夏天是一个很热的季节\"}}],\"max_score\":0.058461,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                tairSearch.tftsearch("tftkey", "{\"query\":{\"match\":{\"f0\":\"夏天冬天\"}}}"));
    }

    @Test
    public  void searchcachetest() throws Exception {
        jedis.del("tftkey");
        tairSearch.tftmappingindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v0\",\"f1\":\"3\"}", "1");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v1\",\"f1\":\"3\"}", "2");

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.594535,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":0.594535,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}}],\"max_score\":0.594535,\"total\":{\"relation\":\"eq\",\"value\":2}}}",
                tairSearch.tftsearch("tftkey", "{\"query\":{\"match\":{\"f1\":\"3\"}}}", true));

        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"3\"}", "3");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"4\"}", "4");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"5\"}", "5");

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.594535,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":0.594535,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}}],\"max_score\":0.594535,\"total\":{\"relation\":\"eq\",\"value\":2}}}",
                tairSearch.tftsearch("tftkey", "{\"query\":{\"match\":{\"f1\":\"3\"}}}", true));

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}],\"max_score\":1.223144,\"total\":{\"relation\":\"eq\",\"value\":3}}}",
                tairSearch.tftsearch("tftkey", "{\"query\":{\"match\":{\"f1\":\"3\"}}}"));

        // wait for LRU cache expired
        Thread.sleep(10000);

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}],\"max_score\":1.223144,\"total\":{\"relation\":\"eq\",\"value\":3}}}",
                tairSearch.tftsearch("tftkey", "{\"query\":{\"match\":{\"f1\":\"3\"}}}", true));
    }

    @Test
    public void tftmaddteststring() {
        jedis.del("tftkey");
        tairSearch.tftmappingindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}");
        Map<String, String> docs = new HashMap();
        docs.put("{\"f0\":\"v0\",\"f1\":\"3\"}", "1");
        docs.put("{\"f0\":\"v1\",\"f1\":\"3\"}", "2");
        docs.put("{\"f0\":\"v3\",\"f1\":\"3\"}", "3");
        docs.put("{\"f0\":\"v3\",\"f1\":\"4\"}", "4");
        docs.put("{\"f0\":\"v3\",\"f1\":\"5\"}", "5");

        tairSearch.tftmadddoc("tftkey", docs);

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}],\"max_score\":1.223144,\"total\":{\"relation\":\"eq\",\"value\":3}}}",
                tairSearch.tftsearch("tftkey", "{\"query\":{\"match\":{\"f1\":\"3\"}}}"));

        assertEquals("{\"_id\":\"3\",\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}", tairSearch.tftgetdoc("tftkey", "3"));
        assertEquals("1", tairSearch.tftdeldoc("tftkey", "3"));
        assertEquals(null, tairSearch.tftgetdoc("tftkey", "3"));

        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"copy_to\":\"\",\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"BM25\",\"store\":false,\"type\":\"text\"},\"f1\":{\"boost\":1.0,\"copy_to\":\"\",\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"BM25\",\"store\":false,\"type\":\"text\"}}}}}", tairSearch.tftgetindexmappings("tftkey"));
    }

    @Test
    public void tftmaddtestbyte() {
        jedis.del("tftkey");
        tairSearch.tftmappingindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}");
        Map<byte[], byte[]> docs = new HashMap();
        docs.put("{\"f0\":\"v0\",\"f1\":\"3\"}".getBytes(), "1".getBytes());
        docs.put("{\"f0\":\"v1\",\"f1\":\"3\"}".getBytes(), "2".getBytes());
        docs.put("{\"f0\":\"v3\",\"f1\":\"3\"}".getBytes(), "3".getBytes());
        docs.put("{\"f0\":\"v3\",\"f1\":\"4\"}".getBytes(), "4".getBytes());
        docs.put("{\"f0\":\"v3\",\"f1\":\"5\"}".getBytes(), "5".getBytes());

        tairSearch.tftmadddoc("tftkey".getBytes(), docs);

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}],\"max_score\":1.223144,\"total\":{\"relation\":\"eq\",\"value\":3}}}",
                tairSearch.tftsearch("tftkey", "{\"query\":{\"match\":{\"f1\":\"3\"}}}"));

        assertEquals("{\"_id\":\"3\",\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}", tairSearch.tftgetdoc("tftkey", "3"));
        assertEquals("1", tairSearch.tftdeldoc("tftkey", "3"));
        assertEquals(null, tairSearch.tftgetdoc("tftkey", "3"));

        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"copy_to\":\"\",\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"BM25\",\"store\":false,\"type\":\"text\"},\"f1\":{\"boost\":1.0,\"copy_to\":\"\",\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"BM25\",\"store\":false,\"type\":\"text\"}}}}}", tairSearch.tftgetindexmappings("tftkey"));
    }
}
