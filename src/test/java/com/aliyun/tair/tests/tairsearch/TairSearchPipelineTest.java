package com.aliyun.tair.tests.tairsearch;

import com.aliyun.tair.tests.tairhash.TairHashTestBase;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TairSearchPipelineTest extends TairSearchTestBase {

    @Test
    public void tfttest() {
        jedis.del("tftkey");
        tairSearchPipeline.tftmappingindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}");
        tairSearchPipeline.tftadddoc("tftkey", "{\"f0\":\"v0\",\"f1\":\"3\"}", "1");
        tairSearchPipeline.tftadddoc("tftkey", "{\"f0\":\"v1\",\"f1\":\"3\"}", "2");
        tairSearchPipeline.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"3\"}", "3");
        tairSearchPipeline.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"4\"}", "4");
        tairSearchPipeline.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"5\"}", "5");

        tairSearchPipeline.tftsearch("tftkey", "{\"query\":{\"match\":{\"f1\":\"3\"}}}");
        tairSearchPipeline.tftgetdoc("tftkey", "3");
        tairSearchPipeline.tftdeldoc("tftkey", "3");
        tairSearchPipeline.tftgetdoc("tftkey", "3");
        tairSearchPipeline.tftgetindexmappings("tftkey");

        List<Object> objs = tairSearchPipeline.syncAndReturnAll();

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}],\"max_score\":1.223144,\"total\":{\"relation\":\"eq\",\"value\":3}}}",
                objs.get(6));

        assertEquals("{\"_id\":\"3\",\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"},\"_version\":1}", objs.get(7));
        assertEquals(1, ((Long)objs.get(8)).intValue());
        assertEquals(null, objs.get(9));

        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"copy_to\":\"\",\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"BM25\",\"store\":false,\"type\":\"text\"},\"f1\":{\"boost\":1.0,\"copy_to\":\"\",\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"BM25\",\"store\":false,\"type\":\"text\"}}}}}", objs.get(10));

    }
}
