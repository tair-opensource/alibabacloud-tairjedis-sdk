package com.aliyun.tair.tests.tairsearch;

import com.aliyun.tair.tests.tairhash.TairHashTestBase;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        tairSearchPipeline.tftsearch("tftkey", "{\"query\":{\"match\":{\"f1\":\"3\"}}}", true);
        tairSearchPipeline.tftgetdoc("tftkey", "3");
        tairSearchPipeline.tftdeldoc("tftkey", "3");
        tairSearchPipeline.tftgetdoc("tftkey", "3");
        tairSearchPipeline.tftgetindexmappings("tftkey");

        List<Object> objs = tairSearchPipeline.syncAndReturnAll();

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}],\"max_score\":1.223144,\"total\":{\"relation\":\"eq\",\"value\":3}}}",
                objs.get(6));

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}],\"max_score\":1.223144,\"total\":{\"relation\":\"eq\",\"value\":3}}}",
                objs.get(7));

        assertEquals("{\"_id\":\"3\",\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}", objs.get(8));
        assertEquals("1", objs.get(9));
        assertEquals(null, objs.get(10));

        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"},\"f1\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"}}}}}", objs.get(11));
    }

    @Test
    public void tftmaddtest() {
        jedis.del("tftkey");
        tairSearchPipeline.tftmappingindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}");

        Map<String, String> docs = new HashMap();
        docs.put("{\"f0\":\"v0\",\"f1\":\"3\"}", "1");
        docs.put("{\"f0\":\"v1\",\"f1\":\"3\"}", "2");
        docs.put("{\"f0\":\"v3\",\"f1\":\"3\"}", "3");
        docs.put("{\"f0\":\"v3\",\"f1\":\"4\"}", "4");
        docs.put("{\"f0\":\"v3\",\"f1\":\"5\"}", "5");

        tairSearchPipeline.tftmadddoc("tftkey", docs);

        tairSearchPipeline.tftsearch("tftkey", "{\"query\":{\"match\":{\"f1\":\"3\"}}}");
        tairSearchPipeline.tftsearch("tftkey", "{\"query\":{\"match\":{\"f1\":\"3\"}}}", true);
        tairSearchPipeline.tftgetdoc("tftkey", "3");
        tairSearchPipeline.tftdeldoc("tftkey", "3");
        tairSearchPipeline.tftgetdoc("tftkey", "3");
        tairSearchPipeline.tftgetindexmappings("tftkey");

        List<Object> objs = tairSearchPipeline.syncAndReturnAll();

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}],\"max_score\":1.223144,\"total\":{\"relation\":\"eq\",\"value\":3}}}",
                objs.get(2));

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}],\"max_score\":1.223144,\"total\":{\"relation\":\"eq\",\"value\":3}}}",
                objs.get(3));

        assertEquals("{\"_id\":\"3\",\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}", objs.get(4));
        assertEquals("1", objs.get(5));
        assertEquals(null, objs.get(6));

        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"},\"f1\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"}}}}}", objs.get(7));
    }
}
