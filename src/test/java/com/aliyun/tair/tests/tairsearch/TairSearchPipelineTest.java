package com.aliyun.tair.tests.tairsearch;

import com.aliyun.tair.tairsearch.action.search.SearchResponse;
import com.aliyun.tair.tairsearch.index.query.QueryBuilders;
import com.aliyun.tair.tairsearch.index.query.TermsQueryBuilder;
import com.aliyun.tair.tairsearch.params.DocInfo;
import com.aliyun.tair.tairsearch.params.TFTGetIndexParams;
import com.aliyun.tair.tairsearch.params.TFTGetSugParams;
import com.aliyun.tair.tairsearch.search.TotalHits;
import com.aliyun.tair.tairsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;

import java.util.*;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class TairSearchPipelineTest extends TairSearchTestBase {

    @Test
    public void tfttest() {
        jedis.del("tftkey");
        tairSearchPipeline.tftmappingindex("tftkey", "{\"settings\":{\"analysis\":{\"analyzer\":{\"my_custom_analyzer\":{\"type\":\"custom\",\"tokenizer\":\"whitespace\"}}}},\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}");
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
        tairSearchPipeline.tftgetindex("tftkey");
        tairSearchPipeline.tftgetindex("tftkey", new TFTGetIndexParams().mappings());
        tairSearchPipeline.tftgetindex("tftkey", new TFTGetIndexParams().settings());

        List<Object> objs = tairSearchPipeline.syncAndReturnAll();

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}],\"max_score\":1.223144,\"total\":{\"relation\":\"eq\",\"value\":3}}}",
                objs.get(6));

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}],\"max_score\":1.223144,\"total\":{\"relation\":\"eq\",\"value\":3}}}",
                objs.get(7));

        assertEquals("{\"_id\":\"3\",\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}", objs.get(8));
        assertEquals("1", objs.get(9));
        assertEquals(null, objs.get(10));

        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"},\"f1\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"}}}}}", objs.get(11));
        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"},\"f1\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"}}},\"settings\":{\"analysis\":{\"analyzer\":{\"my_custom_analyzer\":{\"type\":\"custom\",\"tokenizer\":\"whitespace\"}}}}}}", objs.get(12));
        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"},\"f1\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"}}}}}", objs.get(13));
        assertEquals("{\"tftkey\":{\"settings\":{\"analysis\":{\"analyzer\":{\"my_custom_analyzer\":{\"type\":\"custom\",\"tokenizer\":\"whitespace\"}}}}}}", objs.get(14));
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
        tairSearchPipeline.tftmsearch("{\"query\":{\"match\":{\"f1\":\"3\"}}}", "tftkey"); 

        List<Object> objs = tairSearchPipeline.syncAndReturnAll();

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}],\"max_score\":1.223144,\"total\":{\"relation\":\"eq\",\"value\":3}}}",
                objs.get(2));

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":1.223144,\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}],\"max_score\":1.223144,\"total\":{\"relation\":\"eq\",\"value\":3}}}",
                objs.get(3));

        assertEquals("{\"_id\":\"3\",\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}", objs.get(4));
        assertEquals("1", objs.get(5));
        assertEquals(null, objs.get(6));

        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"},\"f1\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"}}}}}", objs.get(7));
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":1.287682,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":1.287682,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}}],\"max_score\":1.287682,\"total\":{\"relation\":\"eq\",\"value\":2}},\"aux_info\":{\"index_crc64\":52600736426816810}}", objs.get(8));
    }

    @Test
    public void tftmaddtestdocinfo() {
        jedis.del("tftkey");
        tairSearchPipeline.tftmappingindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}");

        List<DocInfo> docs = new ArrayList<>();
        docs.add(new DocInfo("{\"f0\":\"v0\",\"f1\":\"3\"}", "3"));
        docs.add(new DocInfo("{\"f0\":\"v1\",\"f1\":\"3\"}", "2"));
        docs.add(new DocInfo("{\"f0\":\"v1\",\"f1\":\"3\"}", "3"));
        docs.add(new DocInfo("{\"f0\":\"v3\",\"f1\":\"4\"}", "4"));
        docs.add(new DocInfo("{\"f0\":\"v3\",\"f1\":\"5\"}", "5"));

        tairSearchPipeline.tftmadddoc("tftkey", docs);

        tairSearchPipeline.tftsearch("tftkey", "{\"query\":{\"match\":{\"f1\":\"3\"}}}");
        tairSearchPipeline.tftsearch("tftkey", "{\"query\":{\"match\":{\"f1\":\"3\"}}}", true);
        tairSearchPipeline.tftgetdoc("tftkey", "3");
        tairSearchPipeline.tftdeldoc("tftkey", "3");
        tairSearchPipeline.tftgetdoc("tftkey", "3");
        tairSearchPipeline.tftgetindexmappings("tftkey");
        tairSearchPipeline.tftmsearch("{\"query\":{\"match\":{\"f1\":\"3\"}}}", "tftkey");

        List<Object> objs = tairSearchPipeline.syncAndReturnAll();

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":1.287682,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":1.287682,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}}],\"max_score\":1.287682,\"total\":{\"relation\":\"eq\",\"value\":2}}}",
                objs.get(2));

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":1.287682,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":1.287682,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}}],\"max_score\":1.287682,\"total\":{\"relation\":\"eq\",\"value\":2}}}",
                objs.get(3));

        assertEquals("{\"_id\":\"3\",\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}}", objs.get(4));
        assertEquals("1", objs.get(5));
        assertEquals(null, objs.get(6));

        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"},\"f1\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"}}}}}", objs.get(7));
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":1.405465,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}}],\"max_score\":1.405465,\"total\":{\"relation\":\"eq\",\"value\":1}},\"aux_info\":{\"index_crc64\":52600736426816810}}", objs.get(8));
    }

    @Test
    public void tftgetsugtest() {
        jedis.del("tftkey");
        Set<String> visited = new HashSet<>();
        Map<String, Integer> docs = new HashMap();
        docs.put("redis is a memory database", 1);
        docs.put("redis cluster", 2);
        docs.put("redis", 3);

        tairSearchPipeline.tftaddsug("tftkey", docs);
        tairSearchPipeline.tftgetsug("tftkey", "redis");
        tairSearchPipeline.tftsugnum("tftkey");
        TFTGetSugParams params = new TFTGetSugParams();
        params.maxCount(2);
        params.fuzzy();
        tairSearchPipeline.tftgetsug("tftkey", "redis", params);
        tairSearchPipeline.tftgetallsugs("tftkey");
        tairSearchPipeline.tftdelsug("tftkey", "redis cluster", "redis");
        tairSearchPipeline.tftsugnum("tftkey");

        List<Object> objs = tairSearchPipeline.syncAndReturnAll();

        assertEquals(docs.size(), ((Long) objs.get(0)).intValue());
        List<String> result = (List<String>) objs.get(1);
        assertEquals(3, result.size());
        for (int i = 0; i < result.size(); i++) {
            if (!docs.containsKey(result.get(i)) || visited.contains(result.get(i))) {
                assertTrue(false);
            }
            visited.add(result.get(i));
        }
        visited.clear();
        assertEquals(docs.size(), ((Long) objs.get(2)).intValue());
        List<String> maxCountResult = (List<String>) objs.get(3);
        assertEquals(2, maxCountResult.size());
        for (int i = 0; i < maxCountResult.size(); i++) {
            if (!docs.containsKey(maxCountResult.get(i)) || visited.contains(maxCountResult.get(i))) {
                assertTrue(false);
            }
            visited.add(result.get(i));
        }
        visited.clear();

        List<String> allResult = (List<String>) objs.get(4);
        assertEquals(docs.size(), allResult.size());
        for (int i = 0; i < docs.size(); i++) {
            if (!docs.containsKey(allResult.get(i)) || visited.contains(allResult.get(i))) {
                assertTrue(false);
            }
        }

        assertEquals(2, ((Long) objs.get(5)).intValue());
        assertEquals(docs.size() - 2, ((Long) objs.get(6)).intValue());
    }

    @Test
    public void tftgetsugtestbyte() {
        jedis.del("tftkey");

        Set<String> visited = new HashSet<>();
        Map<String, Integer> cmpDocs = new HashMap();
        cmpDocs.put("redis is a memory database", 1);
        cmpDocs.put("redis cluster", 2);
        cmpDocs.put("redis", 3);

        Map<byte[], Integer> docs = new HashMap();
        docs.put("redis is a memory database".getBytes(), 1);
        docs.put("redis cluster".getBytes(), 2);
        docs.put("redis".getBytes(), 3);

        tairSearchPipeline.tftaddsug("tftkey".getBytes(), docs);
        tairSearchPipeline.tftgetsug("tftkey".getBytes(), "redis".getBytes());
        tairSearchPipeline.tftsugnum("tftkey".getBytes());
        TFTGetSugParams params = new TFTGetSugParams();
        params.maxCount(2);
        params.fuzzy();
        tairSearchPipeline.tftgetsug("tftkey".getBytes(), "redis".getBytes(), params);
        tairSearchPipeline.tftgetallsugs("tftkey".getBytes());
        tairSearchPipeline.tftdelsug("tftkey".getBytes(), "redis cluster".getBytes(), "redis".getBytes());
        tairSearchPipeline.tftsugnum("tftkey".getBytes());

        List<Object> objs = tairSearchPipeline.syncAndReturnAll();

        assertEquals(docs.size(), ((Long) objs.get(0)).intValue());
        List<byte[]> result = (List<byte[]>) objs.get(1);
        assertEquals(3, result.size());
        for (int i = 0; i < result.size(); i++) {
            String tmpString = new String(result.get(i));
            if (!cmpDocs.containsKey(tmpString) || visited.contains(tmpString)) {
                assertTrue(false);
            }
            visited.add(tmpString);
        }
        visited.clear();

        assertEquals(docs.size(), ((Long) objs.get(2)).intValue());
        List<byte[]> maxCountResult = (List<byte[]>) objs.get(3);
        assertEquals(2, maxCountResult.size());
        for (int i = 0; i < maxCountResult.size(); i++) {
            String tmpString = new String(maxCountResult.get(i));
            if (!cmpDocs.containsKey(tmpString) || visited.contains(tmpString)) {
                assertTrue(false);
            }
            visited.add(tmpString);
        }
        visited.clear();
        List<byte[]> allResult = (List<byte[]>) objs.get(4);
        assertEquals(docs.size(), allResult.size());
        for (int i = 0; i < docs.size(); i++) {
            String tmpString = new String(allResult.get(i));
            if (!cmpDocs.containsKey(tmpString) || visited.contains(tmpString)) {
                System.out.println(tmpString);
                assertTrue(false);
            }
        }

        assertEquals(2, ((Long) objs.get(5)).intValue());
        assertEquals(docs.size() - 2, ((Long) objs.get(6)).intValue());
        jedis.del("tftkey".getBytes());
    }



    @Test
    public void tftquerybuildertest(){
        jedis.del("tftkey");
        tairSearchPipeline.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}");

        tairSearchPipeline.tftadddoc("tftkey", "{\"f0\":\"redis is a nosql database\"}", "1");
        tairSearchPipeline.tftsearch("tftkey", "{\"query\":{\"term\":{\"f0\":\"redis\"}}}");

        List<String> values = new ArrayList<>();
        values.add("redis");
        values.add("database");

        TermsQueryBuilder qb = QueryBuilders.termsQuery("f0","redis", "database").boost(2.0F);
        assertEquals("f0", qb.fieldName());
        assertEquals(values, qb.values());
        SearchSourceBuilder ssb = new SearchSourceBuilder().query(qb);
        tairSearchPipeline.tftsearch("tftkey", ssb);

        assertEquals("{\"query\":{\"terms\":{\"f0\":[\"redis\",\"database\"],\"boost\":2.0}}}",
                ssb.toString());

        qb = QueryBuilders.termsQuery("f0",values).boost(2.0F);
        assertEquals("f0", qb.fieldName());
        assertEquals(values, qb.values());
        ssb = new SearchSourceBuilder().query(qb);
        assertEquals("{\"query\":{\"terms\":{\"f0\":[\"redis\",\"database\"],\"boost\":2.0}}}",
                ssb.toString());
        tairSearchPipeline.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"terms\":{\"f0\":[\"redis\",\"database\"],\"boost\":2.0}}}",
                ssb.toString());
        List<Object> objs = tairSearchPipeline.syncAndReturnAll();

        assertEquals((String) objs.get(0), "OK");
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.153426,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.153426,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                (String) objs.get(2));
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.216978,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.216978,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                ((SearchResponse) objs.get(3)).toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.216978,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.216978,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                ((SearchResponse) objs.get(4)).toString());
        SearchResponse result = (SearchResponse) objs.get(4);
        assertEquals(1,result.getHits().getTotalHits().value);
        assertEquals(TotalHits.Relation.EQUAL_TO,result.getHits().getTotalHits().relation);
        assertEquals(0.216978,result.getHits().getMaxScore(),0.01);
        assertEquals(0.216978,result.getHits().getAt(0).getScore(),0.01);
        assertEquals("1",result.getHits().getAt(0).getId());
        assertEquals("tftkey",result.getHits().getAt(0).getIndex());
        assertEquals("{\"f0\":\"redis is a nosql database\"}",result.getHits().getAt(0).getSourceAsString());
        Map<String,Object> tmp = result.getHits().getAt(0).getSourceAsMap();
        assertEquals("redis is a nosql database",tmp.get("f0"));
    }
}
