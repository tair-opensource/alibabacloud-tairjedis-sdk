package com.aliyun.tair.tests.tairsearch;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TairSearchClusterTest extends TairSearchTestBase {
    @Test
    public void tfttest() {
        jedisCluster.del("tftkey");
        tairSearchCluster.tftmappingindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v0\",\"f1\":\"3\"}", "1");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v1\",\"f1\":\"3\"}", "2");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"3\"}", "3");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"4\"}", "4");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"5\"}", "5");

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}],\"max_score\":1.223144,\"total\":{\"relation\":\"eq\",\"value\":3}}}",
                tairSearchCluster.tftsearch("tftkey", "{\"query\":{\"match\":{\"f1\":\"3\"}}}"));

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}],\"max_score\":1.223144,\"total\":{\"relation\":\"eq\",\"value\":3}}}",
                tairSearchCluster.tftsearch("tftkey", "{\"query\":{\"match\":{\"f1\":\"3\"}}}", true));


        assertEquals("{\"_id\":\"3\",\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}", tairSearchCluster.tftgetdoc("tftkey", "3"));
        assertEquals("1", tairSearchCluster.tftdeldoc("tftkey", "3"));
        assertEquals(null, tairSearchCluster.tftgetdoc("tftkey", "3"));

        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"copy_to\":\"\",\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"BM25\",\"store\":false,\"type\":\"text\"},\"f1\":{\"boost\":1.0,\"copy_to\":\"\",\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"BM25\",\"store\":false,\"type\":\"text\"}}}}}", tairSearchCluster.tftgetindexmappings("tftkey"));
    }
}
