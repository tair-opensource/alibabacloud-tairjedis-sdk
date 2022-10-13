package com.aliyun.tair.tests.tairsearch;

import com.aliyun.tair.tairsearch.index.query.*;
import com.aliyun.tair.tairsearch.params.TFTGetSugParams;
import com.aliyun.tair.tairsearch.params.TFTScanParams;
import com.aliyun.tair.tairsearch.search.aggregations.AggregationBuilders;
import com.aliyun.tair.tairsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import com.aliyun.tair.tairsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import com.aliyun.tair.tairsearch.search.aggregations.metrics.*;
import com.aliyun.tair.tairsearch.search.builder.SearchSourceBuilder;
import com.aliyun.tair.tairsearch.search.sort.SortOrder;
import com.aliyun.tair.tairsearch.action.search.SearchResponse;
import com.aliyun.tair.tairsearch.search.TotalHits;
import com.aliyun.tair.tairsearch.search.aggregations.BucketOrder;
import com.aliyun.tair.tairsearch.search.aggregations.InternalAggregation;
import com.aliyun.tair.tairsearch.search.aggregations.bucket.filter.Filter;
import com.aliyun.tair.tairsearch.search.aggregations.bucket.terms.IncludeExclude;
import com.aliyun.tair.tairsearch.search.aggregations.bucket.terms.Terms;
import junit.framework.TestCase;
import org.junit.Test;
import redis.clients.jedis.ScanResult;

import java.util.*;

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
    public void tfupdatedocfield() {
        jedisCluster.del("tftkey");
        String ret = tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"redis is a nosql database\"}", "1");
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.153426,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.153426,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                tairSearchCluster.tftsearch("tftkey", "{\"query\":{\"term\":{\"f0\":\"redis\"}}}"));

        ret = tairSearchCluster.tftupdateindex("tftkey", "{\"mappings\":{\"properties\":{\"f1\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        tairSearchCluster.tftupdatedocfield("tftkey", "1", "{\"f1\":\"mysql is a dbms\"}");
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.191783,\"_source\":{\"f0\":\"redis is a nosql database\",\"f1\":\"mysql is a dbms\"}}],\"max_score\":0.191783,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                tairSearchCluster.tftsearch("tftkey", "{\"query\":{\"term\":{\"f1\":\"mysql\"}}}"));
    }

    @Test
    public void tfincrlongdocfield() {
        jedisCluster.del("tftkey");

        try {
            tairSearchCluster.tftincrlongdocfield("tftkey", "1", "f0", 1);
        } catch (Exception e) {
            TestCase.assertTrue(e.getMessage().contains("not exists"));
        }

        String ret = tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        try {
            tairSearchCluster.tftincrlongdocfield("tftkey", "1", "f0", 1);
        } catch (Exception e) {
            TestCase.assertTrue(e.getMessage().contains("failed to parse field"));
        }

        jedisCluster.del("tftkey");

        ret = tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"long\"}}}}");
        assertEquals(ret, "OK");

        assertEquals(1, tairSearchCluster.tftincrlongdocfield("tftkey", "1", "f0", 1).intValue());
        assertEquals(0, tairSearchCluster.tftincrlongdocfield("tftkey", "1", "f0", -1).intValue());

        assertEquals(1, tairSearchCluster.tftexists("tftkey", "1").intValue());
    }

    @Test
    public void tfincrfloatdocfield() {
        jedisCluster.del("tftkey");

        try {
            tairSearchCluster.tftincrfloatdocfield("tftkey", "1", "f0", 1.1);
        } catch (Exception e) {
            TestCase.assertTrue(e.getMessage().contains("not exists"));
        }

        String ret = tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        try {
            tairSearchCluster.tftincrfloatdocfield("tftkey", "1", "f0", 1.1);
        } catch (Exception e) {
            TestCase.assertTrue(e.getMessage().contains("failed to parse field"));
        }

        jedisCluster.del("tftkey");

        ret = tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"double\"}}}}");
        assertEquals(ret, "OK");

        double value = tairSearchCluster.tftincrfloatdocfield("tftkey", "1", "f0", 1.1);
        assertEquals(Double.compare(1.1, value), 0);
        value = tairSearchCluster.tftincrfloatdocfield("tftkey", "1", "f0", -1.1);
        assertEquals(Double.compare(0, value), 0);

        assertEquals(1, tairSearchCluster.tftexists("tftkey", "1").intValue());
    }

    @Test
    public void tftdeldocfield() {
        jedisCluster.del("tftkey");

        assertEquals(0, tairSearchCluster.tftdeldocfield("tftkey", "1", "f0").intValue());

        String ret = tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"long\"}}}}");
        assertEquals(ret, "OK");

        tairSearchCluster.tftincrlongdocfield("tftkey", "1", "f0", 1);
        tairSearchCluster.tftincrfloatdocfield("tftkey", "1", "f1", 1.1);

        assertEquals(2, tairSearchCluster.tftdeldocfield("tftkey", "1", "f0", "f1", "f2").intValue());
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
    public void searchcachetest() throws Exception {
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
    public void tftmsearchtest() throws Exception {
        jedisCluster.del("{tftkey}1");
        jedisCluster.del("{tftkey}2");
        tairSearchCluster.tftmappingindex("{tftkey}1", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"long\"}}}}");
        tairSearchCluster.tftadddoc("{tftkey}1", "{\"f0\":1234}", "1");
        tairSearchCluster.tftmappingindex("{tftkey}2", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"long\"}}}}");
        tairSearchCluster.tftadddoc("{tftkey}2", "{\"f0\":1234}", "1");

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"{tftkey}1\",\"_score\":1.0,\"_source\":{\"f0\":1234}},{\"_id\":\"1\",\"_index\":\"{tftkey}2\",\"_score\":1.0,\"_source\":{\"f0\":1234}}],\"max_score\":1.0,\"total\":{\"relation\":\"eq\",\"value\":2}},\"aux_info\":{\"index_crc64\":10084399559244916810}}",
               tairSearchCluster.tftmsearch("{\"query\":{\"term\":{\"f0\":1234}}}", "{tftkey}1", "{tftkey}2"));
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

    @Test
    public void tftgetsugtest() {
        jedisCluster.del("tftkey");
        Set<String> visited = new HashSet<>();
        Map<String, String> docs = new HashMap();
        docs.put("redis is a memory database", "1");
        docs.put("redis cluster", "2");
        docs.put("redis", "3");

        assertEquals(docs.size(), tairSearchCluster.tftaddsug("tftkey", docs).intValue());

        assertEquals(docs.size(), tairSearchCluster.tftsugnum("tftkey").intValue());

        List<String> result = tairSearchCluster.tftgetsug("tftkey", "redis");

        assertEquals(3, result.size());
        for (int i = 0; i < result.size(); i++) {
            if (!docs.containsKey(result.get(i)) || visited.contains(result.get(i))) {
                TestCase.assertTrue(false);
            }
            visited.add(result.get(i));
        }
        visited.clear();
        TFTGetSugParams params = new TFTGetSugParams();
        params.maxCount(2);
        params.fuzzy();
        List<String> maxCountResult = tairSearchCluster.tftgetsug("tftkey", "redis", params);
        assertEquals(2, maxCountResult.size());
        Collections.sort(maxCountResult);
        for (int i = 0; i < maxCountResult.size(); i++) {
            if (!docs.containsKey(maxCountResult.get(i)) || visited.contains(maxCountResult.get(i))) {
                TestCase.assertTrue(false);
            }
            visited.add(result.get(i));
        }
        visited.clear();
        List<String> allResult = tairSearchCluster.tftgetallsugs("tftkey");
        assertEquals(docs.size(), allResult.size());
        for (int i = 0; i < docs.size(); i++) {
            if (!docs.containsKey(allResult.get(i)) || visited.contains(allResult.get(i))) {
                TestCase.assertTrue(false);
            }
        }

        assertEquals(2, tairSearchCluster.tftdelsug("tftkey", "redis cluster", "redis").intValue());
        assertEquals(docs.size() - 2, tairSearchCluster.tftsugnum("tftkey").intValue());
    }

    @Test
    public void tftgetsugtestbyte() {
        jedisCluster.del("tftkey");

        Set<String> visited = new HashSet<>();
        Map<String, String> cmpDocs = new HashMap();
        cmpDocs.put("redis is a memory database", "1");
        cmpDocs.put("redis cluster", "2");
        cmpDocs.put("redis", "3");

        Map<byte[], byte[]> docs = new HashMap();
        docs.put("redis is a memory database".getBytes(), "1".getBytes());
        docs.put("redis cluster".getBytes(), "2".getBytes());
        docs.put("redis".getBytes(), "3".getBytes());

        assertEquals(docs.size(), tairSearchCluster.tftaddsug("tftkey".getBytes(), docs).intValue());

        assertEquals(docs.size(), tairSearchCluster.tftsugnum("tftkey".getBytes()).intValue());

        List<byte[]> result = tairSearchCluster.tftgetsug("tftkey".getBytes(), "redis".getBytes());

        assertEquals(3, result.size());
        for (int i = 0; i < result.size(); i++) {
            String tmpString = new String(result.get(i));
            if (!cmpDocs.containsKey(tmpString) || visited.contains(tmpString)) {
                TestCase.assertTrue(false);
            }
            visited.add(tmpString);
        }
        visited.clear();

        TFTGetSugParams params = new TFTGetSugParams();
        params.maxCount(2);
        params.fuzzy();
        List<byte[]> maxCountResult = tairSearchCluster.tftgetsug("tftkey".getBytes(), "redis".getBytes(), params);
        assertEquals(2, maxCountResult.size());
        for (int i = 0; i < maxCountResult.size(); i++) {
            String tmpString = new String(maxCountResult.get(i));
            if (!cmpDocs.containsKey(tmpString) || visited.contains(tmpString)) {
                TestCase.assertTrue(false);
            }
            visited.add(tmpString);
        }
        visited.clear();
        List<byte[]> allResult = tairSearchCluster.tftgetallsugs("tftkey".getBytes());
        assertEquals(docs.size(), allResult.size());
        for (int i = 0; i < docs.size(); i++) {
            String tmpString = new String(allResult.get(i));
            if (!cmpDocs.containsKey(tmpString) || visited.contains(tmpString)) {
                System.out.println(tmpString);
                TestCase.assertTrue(false);
            }
        }

        assertEquals(2, tairSearchCluster.tftdelsug("tftkey".getBytes(), "redis cluster".getBytes(), "redis".getBytes()).intValue());
        assertEquals(docs.size() - 2, tairSearchCluster.tftsugnum("tftkey".getBytes()).intValue());
        jedis.del("tftkey".getBytes());
    }

    @Test
    public void tfttermquerybuildertest(){
        jedisCluster.del("tftkey");
        String ret = tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"redis is a nosql database\"}", "1");
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.153426,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.153426,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                tairSearchCluster.tftsearch("tftkey", "{\"query\":{\"term\":{\"f0\":\"redis\"}}}"));

        TermQueryBuilder qb = QueryBuilders.termQuery("f0","redis").boost(2.0F);
        assertEquals("f0", qb.fieldName());
        assertEquals("redis", qb.value());
        assertEquals(2.0, qb.boost(),0.01);
        SearchSourceBuilder ssb = new SearchSourceBuilder().query(qb);
        SearchResponse result = tairSearchCluster.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"term\":{\"f0\":{\"boost\":2.0,\"value\":\"redis\"}}}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.153426,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.153426,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                result.toString());
        assertEquals(1,result.getHits().getTotalHits().value);
        assertEquals(TotalHits.Relation.EQUAL_TO,result.getHits().getTotalHits().relation);
        assertEquals(0.153426,result.getHits().getMaxScore(),0.01);
        assertEquals(0.153426,result.getHits().getAt(0).getScore(),0.01);
        assertEquals("1",result.getHits().getAt(0).getId());
        assertEquals("tftkey",result.getHits().getAt(0).getIndex());
        assertEquals("{\"f0\":\"redis is a nosql database\"}",result.getHits().getAt(0).getSourceAsString());
        Map<String,Object> tmp = result.getHits().getAt(0).getSourceAsMap();
        assertEquals("redis is a nosql database",tmp.get("f0"));
    }

    @Test
    public void tftsourceasmaptest(){
        jedis.del("tftkey");
        String ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"properties\":{\"f1\":{\"type\":\"text\"}}},\"f2\":{\"type\":\"long\"},\"f3\":{\"type\":\"double\"},\"f4\":{\"type\":\"integer\"}}}}");
        assertEquals(ret, "OK");

        tairSearch.tftadddoc("tftkey", "{\"f0\":{\"f1\":\"redis is a nosql database\"},\"f2\":1,\"f3\":1.0,\"f4\":10}", "1");
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.153426,\"_source\":{\"f0\":{\"f1\":\"redis is a nosql database\"},\"f2\":1,\"f3\":1.0,\"f4\":10}}],\"max_score\":0.153426,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                tairSearch.tftsearch("tftkey", "{\"query\":{\"term\":{\"f0.f1\":\"redis\"}}}"));

        TermQueryBuilder qb = QueryBuilders.termQuery("f0.f1","redis").boost(2.0F);
        assertEquals("f0.f1", qb.fieldName());
        assertEquals("redis", qb.value());
        assertEquals(2.0, qb.boost(),0.01);
        SearchSourceBuilder ssb = new SearchSourceBuilder().query(qb);
        SearchResponse result = tairSearch.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"term\":{\"f0.f1\":{\"boost\":2.0,\"value\":\"redis\"}}}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.153426,\"_source\":{\"f0\":{\"f1\":\"redis is a nosql database\"},\"f2\":1,\"f3\":1.0,\"f4\":10}}],\"max_score\":0.153426,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                result.toString());
        assertEquals(1,result.getHits().getTotalHits().value);
        assertEquals(TotalHits.Relation.EQUAL_TO,result.getHits().getTotalHits().relation);
        assertEquals(0.153426,result.getHits().getMaxScore(),0.01);
        assertEquals(0.153426,result.getHits().getAt(0).getScore(),0.01);
        assertEquals("1",result.getHits().getAt(0).getId());
        assertEquals("tftkey",result.getHits().getAt(0).getIndex());
        assertEquals("{\"f0\":{\"f1\":\"redis is a nosql database\"},\"f2\":1,\"f3\":1.0,\"f4\":10}",result.getHits().getAt(0).getSourceAsString());
        Map<String,Object> tmp = result.getHits().getAt(0).getSourceAsMap();
        assertEquals("redis is a nosql database",((Map<String, Object>)tmp.get("f0")).get("f1"));
        long f2 = ((Number)tmp.get("f2")).longValue();
        assertEquals(1, f2);
        double f3 = ((Number)tmp.get("f3")).doubleValue();
        assertEquals(1.0, f3,1e-6);
        int f4 = ((Number)tmp.get("f4")).intValue();
        assertEquals(10, f4);

        jedis.del("tftkey");
        String ret1 = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"properties\":{\"f1\":{\"type\":\"long\"}}}}}}");
        assertEquals(ret1, "OK");

        tairSearch.tftadddoc("tftkey", "{\"f0\":{\"f1\":10}}", "1");
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":1.0,\"_source\":{\"f0\":{\"f1\":10}}}],\"max_score\":1.0,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                tairSearch.tftsearch("tftkey", "{\"query\":{\"term\":{\"f0.f1\":10}}}"));

        TermQueryBuilder qb1 = QueryBuilders.termQuery("f0.f1",10).boost(2.0F);
        assertEquals("f0.f1", qb1.fieldName());
        assertEquals(10, qb1.value());
        assertEquals(2.0, qb1.boost(),0.01);
        SearchSourceBuilder ssb1 = new SearchSourceBuilder().query(qb1);
        SearchResponse result1 = tairSearch.tftsearch("tftkey", ssb1);
        Map<String,Object> tmp1 = result1.getHits().getAt(0).getSourceAsMap();
        Map<String, Object> f0Map = (Map<String, Object>)(tmp1.get("f0"));
        long f0F1 = ((Number)f0Map.get("f1")).longValue();
        assertEquals(10, f0F1);
    }

    @Test
    public void tfttermsquerybuildertest(){
        jedisCluster.del("tftkey");
        String ret = tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"redis is a nosql database\"}", "1");
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.153426,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.153426,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                tairSearchCluster.tftsearch("tftkey", "{\"query\":{\"term\":{\"f0\":\"redis\"}}}"));

        List<String> values = new ArrayList<>();
        values.add("redis");
        values.add("database");

        TermsQueryBuilder qb = QueryBuilders.termsQuery("f0","redis", "database").boost(2.0F);
        assertEquals("f0", qb.fieldName());
        assertEquals(values, qb.values());
        SearchSourceBuilder ssb = new SearchSourceBuilder().query(qb);
        SearchResponse result = tairSearchCluster.tftsearch("tftkey", ssb);

        assertEquals("{\"query\":{\"terms\":{\"f0\":[\"redis\",\"database\"],\"boost\":2.0}}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.216978,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.216978,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                result.toString());

        qb = QueryBuilders.termsQuery("f0",values).boost(2.0F);
        assertEquals("f0", qb.fieldName());
        assertEquals(values, qb.values());
        ssb = new SearchSourceBuilder().query(qb);
        assertEquals("{\"query\":{\"terms\":{\"f0\":[\"redis\",\"database\"],\"boost\":2.0}}}",
                ssb.constructJSON().toString());
        result = tairSearchCluster.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"terms\":{\"f0\":[\"redis\",\"database\"],\"boost\":2.0}}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.216978,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.216978,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                result.toString());
    }

    @Test
    public void tftwildcardquerybuildertest(){
        jedisCluster.del("tftkey");
        String ret = tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"redis is a nosql database\"}", "1");
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.153426,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.153426,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                tairSearchCluster.tftsearch("tftkey", "{\"query\":{\"term\":{\"f0\":\"redis\"}}}"));

        WildcardQueryBuilder qb = QueryBuilders.wildcardQuery("f0","redis*").boost(2.0F);
        assertEquals("f0", qb.fieldName());
        assertEquals("redis*", qb.value());
        SearchSourceBuilder ssb = new SearchSourceBuilder().query(qb);
        SearchResponse result = tairSearchCluster.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"wildcard\":{\"f0\":{\"boost\":2.0,\"value\":\"redis*\"}}}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":2.0,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":2.0,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                result.toString());

    }

    @Test
    public void tftprefixquerybuildertest(){
        jedisCluster.del("tftkey");
        String ret = tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"redis is a nosql database\"}", "1");
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.153426,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.153426,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                tairSearchCluster.tftsearch("tftkey", "{\"query\":{\"term\":{\"f0\":\"redis\"}}}"));

        PrefixQueryBuilder qb = QueryBuilders.prefixQuery("f0","redi");
        assertEquals("f0", qb.fieldName());
        assertEquals("redi", qb.value());
        SearchSourceBuilder ssb = new SearchSourceBuilder().query(qb);
        SearchResponse result = tairSearchCluster.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"prefix\":{\"f0\":{\"value\":\"redi\"}}}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":1.0,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":1.0,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                result.toString());


    }

    @Test
    public void tftmatchallquerybuildertest(){
        jedisCluster.del("tftkey");
        String ret = tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"redis is a nosql database\"}", "1");
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.153426,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.153426,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                tairSearchCluster.tftsearch("tftkey", "{\"query\":{\"term\":{\"f0\":\"redis\"}}}"));

        MatchAllQueryBuilder qb = QueryBuilders.matchAllQuery().boost(2.0F);
        SearchSourceBuilder ssb = new SearchSourceBuilder().query(qb);
        SearchResponse result = tairSearchCluster.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"match_all\":{\"boost\":2.0}}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":2.0,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":2.0,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                result.toString());
    }

    @Test
    public void tftrangequerybuildertest(){
        jedisCluster.del("tftkey");
        String ret = tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"redis is a nosql database\"}", "1");
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.153426,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.153426,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                tairSearchCluster.tftsearch("tftkey", "{\"query\":{\"term\":{\"f0\":\"redis\"}}}"));

        RangeQueryBuilder qb = QueryBuilders.rangeQuery("f0").gt("ra").lt("rf").boost(2.0F);
        assertEquals("ra", qb.from());
        assertEquals(false, qb.includeLower());
        assertEquals(false, qb.includeUpper());
        assertEquals("rf", qb.to());
        SearchSourceBuilder ssb = new SearchSourceBuilder().query(qb);
        SearchResponse result = tairSearchCluster.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"range\":{\"f0\":{\"gt\":\"ra\",\"lt\":\"rf\",\"boost\":2.0}}}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":2.0,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":2.0,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                result.toString());

        qb = QueryBuilders.rangeQuery("f0").from("ra",false).to("rf",false).boost(2.0F);
        ssb = new SearchSourceBuilder().query(qb);
        result = tairSearchCluster.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"range\":{\"f0\":{\"gt\":\"ra\",\"lt\":\"rf\",\"boost\":2.0}}}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":2.0,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":2.0,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                result.toString());

        qb = QueryBuilders.rangeQuery("f0").gte("ra").lte("rf").boost(2.0F);
        assertEquals("ra", qb.from());
        TestCase.assertTrue(qb.includeLower());
        TestCase.assertTrue( qb.includeUpper());
        assertEquals("rf", qb.to());
        ssb = new SearchSourceBuilder().query(qb);
        result = tairSearchCluster.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"range\":{\"f0\":{\"gte\":\"ra\",\"lte\":\"rf\",\"boost\":2.0}}}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":2.0,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":2.0,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                result.toString());

        qb = QueryBuilders.rangeQuery("f0").gte("ra").boost(2.0F);
        ssb = new SearchSourceBuilder().query(qb);
        result = tairSearchCluster.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"range\":{\"f0\":{\"gte\":\"ra\",\"boost\":2.0}}}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":2.0,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":2.0,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                result.toString());

        qb = QueryBuilders.rangeQuery("f0").from("ra").to("rf").boost(2.0F);
        ssb = new SearchSourceBuilder().query(qb);
        result = tairSearchCluster.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"range\":{\"f0\":{\"gte\":\"ra\",\"lte\":\"rf\",\"boost\":2.0}}}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":2.0,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":2.0,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                result.toString());

    }

    @Test
    public void tftmatchquerybuildertest(){
        jedisCluster.del("tftkey");
        String ret = tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"redis is a nosql database\"}", "1");

        MatchQueryBuilder qb = QueryBuilders.matchQuery("f0","nosql").boost(2.0F).prefixLength(1).fuzziness(1);
        assertEquals(1, qb.prefixLength());
        assertEquals(1, qb.fuzziness());
        SearchSourceBuilder ssb = new SearchSourceBuilder().query(qb);
        SearchResponse result = tairSearchCluster.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"match\":{\"f0\":{\"query\":\"nosql\",\"fuzziness\":1,\"prefix_length\":1}}}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.153426,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.153426,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                result.toString());

        qb = QueryBuilders.matchQuery("f0","redis nosql").boost(2.0F).analyzer("standard").minimumShouldMatch(1);
        ssb = new SearchSourceBuilder().query(qb);
        assertEquals("standard", qb.analyzer());
        assertEquals(1, qb.minimumShouldMatch());
        result = tairSearchCluster.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"match\":{\"f0\":{\"query\":\"redis nosql\",\"analyzer\":\"standard\",\"minimum_should_match\":1}}}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.216978,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.216978,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                result.toString());
        Operator and = Operator.fromString("and");
        qb = QueryBuilders.matchQuery("f0","redis nosql").boost(2.0F).analyzer("standard").operator(and);
        assertEquals("standard", qb.analyzer());
        assertEquals(and, qb.operator());
        ssb = new SearchSourceBuilder().query(qb);
        result = tairSearchCluster.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"match\":{\"f0\":{\"query\":\"redis nosql\",\"analyzer\":\"standard\",\"operator\":\"and\"}}}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.216978,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.216978,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                result.toString());

    }

    @Test
    public void tftconstantscorequerybuildertest(){
        jedisCluster.del("tftkey");
        String ret = tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"redis is a nosql database\"}", "1");
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.153426,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.153426,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                tairSearchCluster.tftsearch("tftkey", "{\"query\":{\"term\":{\"f0\":\"redis\"}}}"));

        TermQueryBuilder termqb = QueryBuilders.termQuery("f0","redis").boost(2.0F);
        ConstantScoreQueryBuilder qb = QueryBuilders.constantScoreQuery(termqb).boost(0.153426F);
        assertEquals(termqb, qb.innerQuery());
        SearchSourceBuilder ssb = new SearchSourceBuilder().query(qb);
        SearchResponse result = tairSearchCluster.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"constant_score\":{\"boost\":0.153426,\"filter\":{\"term\":{\"f0\":{\"boost\":2.0,\"value\":\"redis\"}}}}}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.153426,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.153426,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                result.toString());
    }

    @Test
    public void tftdismaxquerybuildertest(){
        jedisCluster.del("tftkey");
        String ret = tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"redis is a nosql database\"}", "1");
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.153426,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.153426,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                tairSearchCluster.tftsearch("tftkey", "{\"query\":{\"term\":{\"f0\":\"redis\"}}}"));

        TermQueryBuilder termqb = QueryBuilders.termQuery("f0","redis");
        DisMaxQueryBuilder qb = QueryBuilders.disMaxQuery();
        qb.add(termqb);
        qb.tieBreaker(0.5F);
        assertEquals(0.5, qb.tieBreaker(), 0.01);
        TestCase.assertTrue( qb.innerQueries().contains(termqb));
        SearchSourceBuilder ssb = new SearchSourceBuilder().query(qb);
        SearchResponse result = tairSearchCluster.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"dis_max\":{\"tie_breaker\":0.5,\"queries\":[{\"term\":{\"f0\":{\"boost\":1.0,"
                        + "\"value\":\"redis\"}}}]}}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.153426,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.153426,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                result.toString());
    }

    @Test
    public void tftboolquerybuildertest(){
        jedisCluster.del("tftkey");
        String ret = tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"redis is a nosql database\"}", "1");
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.153426,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.153426,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                tairSearchCluster.tftsearch("tftkey", "{\"query\":{\"term\":{\"f0\":\"redis\"}}}"));
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"redis is a nosql kvstore\"}", "2");

        TermQueryBuilder termqb1 = QueryBuilders.termQuery("f0","redis");
        TermQueryBuilder termqb2 = QueryBuilders.termQuery("f0","kvstore");
        TermQueryBuilder termqb3 = QueryBuilders.termQuery("f0","nosql");
        BoolQueryBuilder qb = QueryBuilders.boolQuery();
        qb.must(termqb1);
        qb.mustNot(termqb2);
        qb.should(termqb3);
        qb.minimumShouldMatch(1);
        assertEquals(1, qb.minimumShouldMatch());
        TestCase.assertTrue(qb.must().contains(termqb1));
        TestCase.assertTrue(qb.mustNot().contains(termqb2));
        TestCase.assertTrue(qb.should().contains(termqb3));
        SearchSourceBuilder ssb = new SearchSourceBuilder().query(qb);
        SearchResponse result = tairSearchCluster.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"bool\":{\"must\":[{\"term\":{\"f0\":{\"boost\":1.0,\"value\":\"redis\"}}}],\"must_not\":[{\"term\":{\"f0\":{\"boost\":1.0,\"value\":\"kvstore\"}}}],\"should\":[{\"term\":{\"f0\":{\"boost\":1.0,\"value\":\"nosql\"}}}],\"minimum_should_match\":1}}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.140133,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.140133,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                result.toString());
    }

    @Test
    public void tftsearchsourcebuildertest(){
        jedisCluster.del("tftkey");
        String ret = tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"redis is a nosql database\"}", "1");
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.153426,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.153426,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                tairSearchCluster.tftsearch("tftkey", "{\"query\":{\"term\":{\"f0\":\"redis\"}}}"));
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"redis is an in-memory database that persists on disk\"}", "2");
        tairSearchCluster.tftadddoc("tftkey", "{\"f0\":\"redis supports many different kind of values\"}", "3");

        TermQueryBuilder qb = QueryBuilders.termQuery("f0","redis");
        SearchSourceBuilder ssb = new SearchSourceBuilder().query(qb).sort("_score");
        assertEquals(qb, ssb.query());
        assertEquals(SortOrder.DESC, ssb.sorts().get(0).order());
        SearchResponse result = tairSearchCluster.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"term\":{\"f0\":{\"boost\":1.0,\"value\":\"redis\"}}},\"sort\":[{\"_score\":{\"order\":\"desc\"}}]}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.356159,\"_source\":{\"f0\":\"redis is a nosql database\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":0.311639,\"_source\":{\"f0\":\"redis is an in-memory database that persists on disk\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":0.267119,\"_source\":{\"f0\":\"redis supports many different kind of values\"}}],\"max_score\":0.356159,\"total\":{\"relation\":\"eq\",\"value\":3}}}",
                result.toString());


        ssb = new SearchSourceBuilder().query(qb).from(1).size(1).sort("_doc");
        assertEquals(1, ssb.from());
        assertEquals(1, ssb.size());
        assertEquals(SortOrder.ASC, ssb.sorts().get(0).order());
        result = tairSearchCluster.tftsearch("tftkey", ssb);
        assertEquals("{\"size\":1,\"from\":1,\"query\":{\"term\":{\"f0\":{\"boost\":1.0,\"value\":\"redis\"}}},\"sort\":[{\"_doc\":{\"order\":\"asc\"}}]}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":0.311639,\"_source\":{\"f0\":\"redis is an in-memory database that persists on disk\"}}],\"max_score\":0.311639,\"total\":{\"relation\":\"eq\",\"value\":3}}}",
                result.toString());

        ssb = new SearchSourceBuilder().query(qb).trackTotalHits(true).fetchSource("f0",null);
        TestCase.assertTrue(ssb.trackTotalHits());
        assertEquals("f0", ssb.fetchSource().includes()[0]);
        result = tairSearchCluster.tftsearch("tftkey", ssb);
        assertEquals("{\"track_total_hits\":true,\"query\":{\"term\":{\"f0\":{\"boost\":1.0,\"value\":\"redis\"}}},\"_source\":{\"includes\":[\"f0\"],\"excludes\":[]}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.356159,\"_source\":{\"f0\":\"redis is a nosql database\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":0.311639,\"_source\":{\"f0\":\"redis is an in-memory database that persists on disk\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":0.267119,\"_source\":{\"f0\":\"redis supports many different kind of values\"}}],\"max_score\":0.356159,\"total\":{\"relation\":\"eq\",\"value\":3}}}",
                result.toString());

        ssb = new SearchSourceBuilder().query(qb).trackTotalHits(true).fetchSource(null, "f0");
        assertEquals("f0", ssb.fetchSource().excludes()[0]);
        result = tairSearchCluster.tftsearch("tftkey", ssb);
        assertEquals("{\"track_total_hits\":true,\"query\":{\"term\":{\"f0\":{\"boost\":1.0,\"value\":\"redis\"}}},\"_source\":{\"includes\":[],\"excludes\":[\"f0\"]}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.356159,\"_source\":{}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":0.311639,\"_source\":{}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":0.267119,\"_source\":{}}],\"max_score\":0.356159,\"total\":{\"relation\":\"eq\",\"value\":3}}}",
                result.toString());
    }

    @Test
    public void tftsumaggsbuildertest(){
        jedisCluster.del("tftkey");
        String ret = tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"properties\":{\"shares_name\":{\"type"
                + "\":\"keyword\"}, \"logictime\":{\"type\":\"long\"}, \"purchase_type\":{\"type\":\"integer\"}, "
                + "\"purchase_price\":{\"type\":\"double\"}, \"purchase_count\":{\"type\":\"long\"}, "
                + "\"investor\":{\"type\":\"keyword\"}}}}");
        assertEquals(ret, "OK");

        tairSearchCluster.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300210, \"purchase_type\":1, "
                + "\"purchase_price\":101.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearchCluster.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300310, \"purchase_type\":1, "
                + "\"purchase_price\":111.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearchCluster.tftadddoc("tftkey", "{\"shares_name\":\"YBY\", \"logictime\":14300410, \"purchase_type\":1, "
                + "\"purchase_price\":11.1, \"purchase_count\":100, \"investor\":\"Mila\"}");

        SumAggregationBuilder ab = AggregationBuilders.sum("price_sum").field("purchase_price");
        assertEquals("purchase_price", ab.field());
        assertEquals("sum", ab.getType());
        assertEquals("price_sum", ab.getName());
        TermQueryBuilder qb = QueryBuilders.termQuery("investor","Jay");
        SearchSourceBuilder ssb = new SearchSourceBuilder().size(0).query(qb).aggregation(ab);

        SearchResponse sr = tairSearchCluster.tftsearch("tftkey", ssb);
        Sum s = sr.getAggregations().get("price_sum");
        assertEquals(212.2, s.value(),0.001);
        assertEquals("sum", s.getType());
        assertEquals("price_sum", ab.getName());
        assertEquals("{\"size\":0,\"query\":{\"term\":{\"investor\":{\"boost\":1.0,\"value\":\"Jay\"}}},"
                        + "\"aggs\":{\"price_sum\":{\"sum\":{\"field\":\"purchase_price\"}}}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[],\"max_score\":null,\"total\":{\"relation\":\"eq\",\"value\":2}},\"aggregations\":{\"price_sum\":{\"value\":212.2,\"type\":\"sum\"}}}",
                sr.toString());
    }

    @Test
    public void tftmaxaggsbuildertest(){
        jedisCluster.del("tftkey");
        String ret = tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"properties\":{\"shares_name\":{\"type"
                + "\":\"keyword\"}, \"logictime\":{\"type\":\"long\"}, \"purchase_type\":{\"type\":\"integer\"}, "
                + "\"purchase_price\":{\"type\":\"double\"}, \"purchase_count\":{\"type\":\"long\"}, "
                + "\"investor\":{\"type\":\"keyword\"}}}}");
        assertEquals(ret, "OK");

        tairSearchCluster.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300210, \"purchase_type\":1, "
                + "\"purchase_price\":101.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearchCluster.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300310, \"purchase_type\":1, "
                + "\"purchase_price\":111.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearchCluster.tftadddoc("tftkey", "{\"shares_name\":\"YBY\", \"logictime\":14300410, \"purchase_type\":1, "
                + "\"purchase_price\":11.1, \"purchase_count\":100, \"investor\":\"Mila\"}");

        MaxAggregationBuilder ab = AggregationBuilders.max("price_max").field("purchase_price");
        assertEquals("purchase_price", ab.field());
        assertEquals("max", ab.getType());
        assertEquals("price_max", ab.getName());
        TermQueryBuilder qb = QueryBuilders.termQuery("investor","Jay");
        SearchSourceBuilder ssb = new SearchSourceBuilder().size(0).query(qb).aggregation(ab);

        SearchResponse sr = tairSearchCluster.tftsearch("tftkey", ssb);
        Max s = sr.getAggregations().get("price_max");
        assertEquals(111.1, s.value(),0.001);
        assertEquals("max", s.getType());
        assertEquals("price_max", s.getName());
        assertEquals("{\"size\":0,\"query\":{\"term\":{\"investor\":{\"boost\":1.0,\"value\":\"Jay\"}}},"
                        + "\"aggs\":{\"price_max\":{\"max\":{\"field\":\"purchase_price\"}}}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[],\"max_score\":null,\"total\":{\"relation\":\"eq\",\"value\":2}},\"aggregations\":{\"price_max\":{\"value\":111.1,\"type\":\"max\"}}}",
                sr.toString());
    }

    @Test
    public void tftavgaggsbuildertest(){
        jedisCluster.del("tftkey");
        String ret = tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"properties\":{\"shares_name\":{\"type"
                + "\":\"keyword\"}, \"logictime\":{\"type\":\"long\"}, \"purchase_type\":{\"type\":\"integer\"}, "
                + "\"purchase_price\":{\"type\":\"double\"}, \"purchase_count\":{\"type\":\"long\"}, "
                + "\"investor\":{\"type\":\"keyword\"}}}}");
        assertEquals(ret, "OK");

        tairSearchCluster.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300210, \"purchase_type\":1, "
                + "\"purchase_price\":101.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearchCluster.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300310, \"purchase_type\":1, "
                + "\"purchase_price\":111.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearchCluster.tftadddoc("tftkey", "{\"shares_name\":\"YBY\", \"logictime\":14300410, \"purchase_type\":1, "
                + "\"purchase_price\":11.1, \"purchase_count\":100, \"investor\":\"Mila\"}");

        AvgAggregationBuilder ab = AggregationBuilders.avg("price_avg").field("purchase_price");
        assertEquals("purchase_price", ab.field());
        assertEquals("avg", ab.getType());
        assertEquals("price_avg", ab.getName());
        TermQueryBuilder qb = QueryBuilders.termQuery("investor","Jay");
        SearchSourceBuilder ssb = new SearchSourceBuilder().size(0).query(qb).aggregation(ab);

        SearchResponse sr = tairSearchCluster.tftsearch("tftkey", ssb);
        Avg s = sr.getAggregations().get("price_avg");
        assertEquals(106.1, s.value(),0.001);
        assertEquals("avg", s.getType());
        assertEquals("price_avg", s.getName());
        assertEquals("{\"size\":0,\"query\":{\"term\":{\"investor\":{\"boost\":1.0,\"value\":\"Jay\"}}},"
                        + "\"aggs\":{\"price_avg\":{\"avg\":{\"field\":\"purchase_price\"}}}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[],\"max_score\":null,\"total\":{\"relation\":\"eq\",\"value\":2}},\"aggregations\":{\"price_avg\":{\"value\":106.1,\"type\":\"avg\"}}}",
                sr.toString());
    }

    @Test
    public void tftminaggsbuildertest(){
        jedisCluster.del("tftkey");
        String ret = tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"properties\":{\"shares_name\":{\"type"
                + "\":\"keyword\"}, \"logictime\":{\"type\":\"long\"}, \"purchase_type\":{\"type\":\"integer\"}, "
                + "\"purchase_price\":{\"type\":\"double\"}, \"purchase_count\":{\"type\":\"long\"}, "
                + "\"investor\":{\"type\":\"keyword\"}}}}");
        assertEquals(ret, "OK");

        tairSearchCluster.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300210, \"purchase_type\":1, "
                + "\"purchase_price\":101.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearchCluster.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300310, \"purchase_type\":1, "
                + "\"purchase_price\":111.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearchCluster.tftadddoc("tftkey", "{\"shares_name\":\"YBY\", \"logictime\":14300410, \"purchase_type\":1, "
                + "\"purchase_price\":11.1, \"purchase_count\":100, \"investor\":\"Mila\"}");

        MinAggregationBuilder ab = AggregationBuilders.min("price_min").field("purchase_price");
        assertEquals("purchase_price", ab.field());
        assertEquals("min", ab.getType());
        assertEquals("price_min", ab.getName());
        TermQueryBuilder qb = QueryBuilders.termQuery("investor","Jay");
        SearchSourceBuilder ssb = new SearchSourceBuilder().size(0).query(qb).aggregation(ab);

        SearchResponse sr = tairSearchCluster.tftsearch("tftkey", ssb);
        InternalMin s = sr.getAggregations().get("price_min");
        assertEquals("min", s.getType());
        assertEquals("price_min", s.getName());
        assertEquals(101.1, s.value(),0.001);
        assertEquals("{\"size\":0,\"query\":{\"term\":{\"investor\":{\"boost\":1.0,\"value\":\"Jay\"}}},"
                        + "\"aggs\":{\"price_min\":{\"min\":{\"field\":\"purchase_price\"}}}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[],\"max_score\":null,\"total\":{\"relation\":\"eq\",\"value\":2}},"
                        + "\"aggregations\":{\"price_min\":{\"value\":101.1,\"type\":\"min\"}}}",
                sr.toString());
    }

    @Test
    public void tftsumofsquaresaggsbuildertest(){
        jedisCluster.del("tftkey");
        String ret = tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"properties\":{\"shares_name\":{\"type"
                + "\":\"keyword\"}, \"logictime\":{\"type\":\"long\"}, \"purchase_type\":{\"type\":\"integer\"}, "
                + "\"purchase_price\":{\"type\":\"double\"}, \"purchase_count\":{\"type\":\"long\"}, "
                + "\"investor\":{\"type\":\"keyword\"}}}}");
        assertEquals(ret, "OK");

        tairSearchCluster.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300210, \"purchase_type\":1, "
                + "\"purchase_price\":101.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearchCluster.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300310, \"purchase_type\":1, "
                + "\"purchase_price\":111.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearchCluster.tftadddoc("tftkey", "{\"shares_name\":\"YBY\", \"logictime\":14300410, \"purchase_type\":1, "
                + "\"purchase_price\":11.1, \"purchase_count\":100, \"investor\":\"Mila\"}");

        SumOfSquaresAggregationBuilder ab = AggregationBuilders.sumOfSquares("price_sum_of_squares").field("purchase_price");
        assertEquals("purchase_price", ab.field());
        assertEquals("sum_of_squares", ab.getType());
        assertEquals("price_sum_of_squares", ab.getName());
        TermQueryBuilder qb = QueryBuilders.termQuery("investor","Jay");
        SearchSourceBuilder ssb = new SearchSourceBuilder().size(0).query(qb).aggregation(ab);

        SearchResponse sr = tairSearchCluster.tftsearch("tftkey", ssb);
        SumOfSquares s = sr.getAggregations().get("price_sum_of_squares");
        assertEquals("sum_of_squares", s.getType());
        assertEquals("price_sum_of_squares", s.getName());
        assertEquals(22564.42, s.value(),0.001);
        assertEquals("{\"size\":0,\"query\":{\"term\":{\"investor\":{\"boost\":1.0,\"value\":\"Jay\"}}},"
                        + "\"aggs\":{\"price_sum_of_squares\":{\"sum_of_squares\":{\"field\":\"purchase_price\"}}}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[],\"max_score\":null,\"total\":{\"relation\":\"eq\",\"value\":2}},\"aggregations\":{\"price_sum_of_squares\":{\"value\":22564.42,\"type\":\"sum_of_squares\"}}}",
                sr.toString());
    }

    @Test
    public void tftvarianceaggsbuildertest(){
        jedisCluster.del("tftkey");
        String ret = tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"properties\":{\"shares_name\":{\"type"
                + "\":\"keyword\"}, \"logictime\":{\"type\":\"long\"}, \"purchase_type\":{\"type\":\"integer\"}, "
                + "\"purchase_price\":{\"type\":\"double\"}, \"purchase_count\":{\"type\":\"long\"}, "
                + "\"investor\":{\"type\":\"keyword\"}}}}");
        assertEquals(ret, "OK");

        tairSearchCluster.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300210, \"purchase_type\":1, "
                + "\"purchase_price\":101.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearchCluster.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300310, \"purchase_type\":1, "
                + "\"purchase_price\":111.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearchCluster.tftadddoc("tftkey", "{\"shares_name\":\"YBY\", \"logictime\":14300410, \"purchase_type\":1, "
                + "\"purchase_price\":11.1, \"purchase_count\":100, \"investor\":\"Mila\"}");

        VarianceAggregationBuilder ab = AggregationBuilders.variance("price_variance").field("purchase_price");
        assertEquals("purchase_price", ab.field());
        assertEquals("variance", ab.getType());
        assertEquals("price_variance", ab.getName());
        TermQueryBuilder qb = QueryBuilders.termQuery("investor","Jay");
        SearchSourceBuilder ssb = new SearchSourceBuilder().size(0).query(qb).aggregation(ab);

        SearchResponse sr = tairSearchCluster.tftsearch("tftkey", ssb);
        Variance s = sr.getAggregations().get("price_variance");
        assertEquals(25.0, s.value(),0.001);
        assertEquals("variance", s.getType());
        assertEquals("price_variance", s.getName());
        assertEquals("{\"size\":0,\"query\":{\"term\":{\"investor\":{\"boost\":1.0,\"value\":\"Jay\"}}},"
                        + "\"aggs\":{\"price_variance\":{\"variance\":{\"field\":\"purchase_price\"}}}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[],\"max_score\":null,\"total\":{\"relation\":\"eq\",\"value\":2}},\"aggregations\":{\"price_variance\":{\"value\":25.0,\"type\":\"variance\"}}}",
                sr.toString());
    }

    @Test
    public void tftstddeviationaggsbuildertest(){
        jedisCluster.del("tftkey");
        String ret = tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"properties\":{\"shares_name\":{\"type"
                + "\":\"keyword\"}, \"logictime\":{\"type\":\"long\"}, \"purchase_type\":{\"type\":\"integer\"}, "
                + "\"purchase_price\":{\"type\":\"double\"}, \"purchase_count\":{\"type\":\"long\"}, "
                + "\"investor\":{\"type\":\"keyword\"}}}}");
        assertEquals(ret, "OK");

        tairSearchCluster.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300210, \"purchase_type\":1, "
                + "\"purchase_price\":101.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearchCluster.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300310, \"purchase_type\":1, "
                + "\"purchase_price\":111.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearchCluster.tftadddoc("tftkey", "{\"shares_name\":\"YBY\", \"logictime\":14300410, \"purchase_type\":1, "
                + "\"purchase_price\":11.1, \"purchase_count\":100, \"investor\":\"Mila\"}");

        StdDeviationAggregationBuilder ab = AggregationBuilders.stdDeviation("price_std_deviation").field("purchase_price");
        assertEquals("purchase_price", ab.field());
        assertEquals("std_deviation", ab.getType());
        assertEquals("price_std_deviation", ab.getName());
        TermQueryBuilder qb = QueryBuilders.termQuery("investor","Jay");
        SearchSourceBuilder ssb = new SearchSourceBuilder().size(0).query(qb).aggregation(ab);

        SearchResponse sr = tairSearchCluster.tftsearch("tftkey", ssb);
        StdDeviation s = sr.getAggregations().get("price_std_deviation");
        assertEquals(5.0, s.value(),0.001);
        assertEquals("std_deviation", s.getType());
        assertEquals("price_std_deviation", s.getName());
        assertEquals("{\"size\":0,\"query\":{\"term\":{\"investor\":{\"boost\":1.0,\"value\":\"Jay\"}}},"
                        + "\"aggs\":{\"price_std_deviation\":{\"std_deviation\":{\"field\":\"purchase_price\"}}}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[],\"max_score\":null,\"total\":{\"relation\":\"eq\",\"value\":2}},\"aggregations\":{\"price_std_deviation\":{\"value\":5.0,\"type\":\"std_deviation\"}}}",
                sr.toString());
    }

    @Test
    public void tftextendedstatsaggsbuildertest(){
        jedisCluster.del("tftkey");
        String ret = tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"properties\":{\"shares_name\":{\"type"
                + "\":\"keyword\"}, \"logictime\":{\"type\":\"long\"}, \"purchase_type\":{\"type\":\"integer\"}, "
                + "\"purchase_price\":{\"type\":\"double\"}, \"purchase_count\":{\"type\":\"long\"}, "
                + "\"investor\":{\"type\":\"keyword\"}}}}");
        assertEquals(ret, "OK");

        tairSearchCluster.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300210, \"purchase_type\":1, "
                + "\"purchase_price\":101.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearchCluster.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300310, \"purchase_type\":1, "
                + "\"purchase_price\":111.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearchCluster.tftadddoc("tftkey", "{\"shares_name\":\"YBY\", \"logictime\":14300410, \"purchase_type\":1, "
                + "\"purchase_price\":11.1, \"purchase_count\":100, \"investor\":\"Mila\"}");

        ExtendedStatsAggregationBuilder ab = AggregationBuilders.extendedStats("price_extended_stats").field("purchase_price");
        assertEquals("purchase_price", ab.field());
        assertEquals("extended_stats", ab.getType());
        assertEquals("price_extended_stats", ab.getName());
        TermQueryBuilder qb = QueryBuilders.termQuery("investor","Jay");
        SearchSourceBuilder ssb = new SearchSourceBuilder().size(0).query(qb).aggregation(ab);
        SearchResponse sr = tairSearchCluster.tftsearch("tftkey", ssb);
        ExtendedStats s = sr.getAggregations().get("price_extended_stats");
        assertEquals(106.1, s.getAvg(),0.001);
        assertEquals(2, s.getCount(),0.001);
        assertEquals(111.1, s.getMax(),0.001);
        assertEquals(101.1, s.getMin(),0.001);
        assertEquals(5.0, s.getStdDeviation(),0.001);
        assertEquals(212.2, s.getSum(),0.001);
        assertEquals(22564.42, s.getSumOfSquares(),0.001);
        assertEquals(25.0, s.getVariance(),0.001);
        assertEquals("extended_stats", s.getType());
        assertEquals("price_extended_stats", s.getName());
        assertEquals("{\"size\":0,\"query\":{\"term\":{\"investor\":{\"boost\":1.0,\"value\":\"Jay\"}}},"
                        + "\"aggs\":{\"price_extended_stats\":{\"extended_stats\":{\"field\":\"purchase_price\"}}}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[],\"max_score\":null,\"total\":{\"relation\":\"eq\",\"value\":2}},\"aggregations\":{\"price_extended_stats\":{\"count\":2,\"sum\":212.2,\"max\":111.1,\"min\":101.1,\"avg\":106.1,\"sum_of_squares\":22564.42,\"variance\":25.0,\"std_deviation\":5.0,\"type\":\"extended_stats\"}}}",
                sr.toString());
    }

    @Test
    public void tftvaluecountaggsbuildertest(){
        jedisCluster.del("tftkey");
        String ret = tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"properties\":{\"shares_name\":{\"type"
                + "\":\"keyword\"}, \"logictime\":{\"type\":\"long\"}, \"purchase_type\":{\"type\":\"integer\"}, "
                + "\"purchase_price\":{\"type\":\"double\"}, \"purchase_count\":{\"type\":\"long\"}, "
                + "\"investor\":{\"type\":\"keyword\"}}}}");
        assertEquals(ret, "OK");

        tairSearchCluster.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300210, \"purchase_type\":1, "
                + "\"purchase_price\":101.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearchCluster.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300310, \"purchase_type\":1, "
                + "\"purchase_price\":111.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearchCluster.tftadddoc("tftkey", "{\"shares_name\":\"YBY\", \"logictime\":14300410, \"purchase_type\":1, "
                + "\"purchase_price\":11.1, \"purchase_count\":100, \"investor\":\"Mila\"}");

        ValueCountAggregationBuilder ab = AggregationBuilders.count("price_count").field("purchase_price");
        assertEquals("purchase_price", ab.field());
        assertEquals("value_count", ab.getType());
        assertEquals("price_count", ab.getName());
        TermQueryBuilder qb = QueryBuilders.termQuery("investor","Jay");
        SearchSourceBuilder ssb = new SearchSourceBuilder().size(0).query(qb).aggregation(ab);

        SearchResponse sr = tairSearchCluster.tftsearch("tftkey", ssb);
        ValueCount s = sr.getAggregations().get("price_count");
        assertEquals(2, s.getValue());
        assertEquals(2.0, s.value(),0.001);
        assertEquals("value_count", s.getType());
        assertEquals("price_count", s.getName());
        assertEquals("{\"size\":0,\"query\":{\"term\":{\"investor\":{\"boost\":1.0,\"value\":\"Jay\"}}},"
                        + "\"aggs\":{\"price_count\":{\"value_count\":{\"field\":\"purchase_price\"}}}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[],\"max_score\":null,\"total\":{\"relation\":\"eq\",\"value\":2}},\"aggregations\":{\"price_count\":{\"value\":2.0,\"type\":\"value_count\"}}}",
                sr.toString());
    }

    @Test
    public void tftfilteraggsbuildertest(){
        jedisCluster.del("tftkey");
        String ret = tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"properties\":{\"shares_name\":{\"type"
                + "\":\"keyword\"}, \"logictime\":{\"type\":\"long\"}, \"purchase_type\":{\"type\":\"integer\"}, "
                + "\"purchase_price\":{\"type\":\"double\"}, \"purchase_count\":{\"type\":\"long\"}, "
                + "\"investor\":{\"type\":\"keyword\"}}}}");
        assertEquals(ret, "OK");

        tairSearchCluster.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300210, \"purchase_type\":1, "
                + "\"purchase_price\":101.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearchCluster.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300310, \"purchase_type\":1, "
                + "\"purchase_price\":111.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearchCluster.tftadddoc("tftkey", "{\"shares_name\":\"YBY\", \"logictime\":14300410, \"purchase_type\":1, "
                + "\"purchase_price\":11.1, \"purchase_count\":100, \"investor\":\"Mila\"}");

        ExtendedStatsAggregationBuilder ab = AggregationBuilders.extendedStats("Jay_BuyIn_Quatation").field("purchase_price");
        TermQueryBuilder qb = QueryBuilders.termQuery("investor","Jay");
        TermQueryBuilder filterQb = QueryBuilders.termQuery("purchase_type",1);
        FilterAggregationBuilder filterAggregationBuilder = AggregationBuilders.filter("Jay_BuyIn_Filter",filterQb).subAggregation(ab);
        assertEquals(filterQb, filterAggregationBuilder.getFilter());
        TestCase.assertTrue(filterAggregationBuilder.getSubAggregations().contains(ab));
        assertEquals("filter", filterAggregationBuilder.getType());
        assertEquals("Jay_BuyIn_Filter", filterAggregationBuilder.getName());
        SearchSourceBuilder ssb = new SearchSourceBuilder().size(0).query(qb).aggregation(filterAggregationBuilder);
        SearchResponse sr = tairSearchCluster.tftsearch("tftkey", ssb);
        assertEquals((long)2,((InternalAggregation)sr.getAggregations().get("Jay_BuyIn_Filter")).getProperty("_count"));
        assertEquals(106.1,((InternalAggregation)sr.getAggregations().get("Jay_BuyIn_Filter")).getProperty("Jay_BuyIn_Quatation.avg"));
        Filter result = sr.getAggregations().get("Jay_BuyIn_Filter");
        assertEquals("filter", result.getType());
        assertEquals("Jay_BuyIn_Filter", result.getName());
        assertEquals(2, result.getDocCount());
        ExtendedStats s = result.getAggregations().get("Jay_BuyIn_Quatation");
        assertEquals(106.1, s.getAvg(),0.001);
        assertEquals(2, s.getCount(),0.001);
        assertEquals(111.1, s.getMax(),0.001);
        assertEquals(101.1, s.getMin(),0.001);
        assertEquals(5.0, s.getStdDeviation(),0.001);
        assertEquals(212.2, s.getSum(),0.001);
        assertEquals(22564.42, s.getSumOfSquares(),0.001);
        assertEquals(25.0, s.getVariance(),0.001);
        assertEquals("{\"size\":0,\"query\":{\"term\":{\"investor\":{\"boost\":1.0,\"value\":\"Jay\"}}},\"aggs\":{\"Jay_BuyIn_Filter\":{\"filter\":{\"term\":{\"purchase_type\":{\"boost\":1.0,\"value\":1}}},\"aggs\":{\"Jay_BuyIn_Quatation\":{\"extended_stats\":{\"field\":\"purchase_price\"}}}}}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[],\"max_score\":null,\"total\":{\"relation\":\"eq\",\"value\":2}},\"aggregations\":{\"Jay_BuyIn_Filter\":{\"doc_count\":2,\"type\":\"filter\",\"Jay_BuyIn_Quatation\":{\"count\":2,\"sum\":212.2,\"max\":111.1,\"min\":101.1,\"avg\":106.1,\"sum_of_squares\":22564.42,\"variance\":25.0,\"std_deviation\":5.0,\"type\":\"extended_stats\"}}}}",
                sr.toString());
    }

    @Test
    public void tfttermsaggsbuildertest(){
        jedisCluster.del("tftkey");
        String ret = tairSearchCluster.tftcreateindex("tftkey", "{\"mappings\":{\"properties\":{\"shares_name\":{\"type"
                + "\":\"keyword\"}, \"logictime\":{\"type\":\"long\"}, \"purchase_type\":{\"type\":\"integer\"}, "
                + "\"purchase_price\":{\"type\":\"double\"}, \"purchase_count\":{\"type\":\"long\"}, "
                + "\"investor\":{\"type\":\"keyword\"}}}}");
        assertEquals(ret, "OK");

        tairSearchCluster.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300210, \"purchase_type\":1, "
                + "\"purchase_price\":101.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearchCluster.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300310, \"purchase_type\":1, "
                + "\"purchase_price\":111.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearchCluster.tftadddoc("tftkey", "{\"shares_name\":\"YBY\", \"logictime\":14300410, \"purchase_type\":1, "
                + "\"purchase_price\":11.1, \"purchase_count\":100, \"investor\":\"Mila\"}");

        TermQueryBuilder qb = QueryBuilders.termQuery("purchase_type",1);
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("Per_Investor_Freq").field("investor");
        assertEquals("investor", termsAggregationBuilder.field());
        assertEquals("terms", termsAggregationBuilder.getType());
        assertEquals("Per_Investor_Freq", termsAggregationBuilder.getName());
        SearchSourceBuilder ssb = new SearchSourceBuilder().size(0).query(qb).aggregation(termsAggregationBuilder);
        SearchResponse sr = tairSearchCluster.tftsearch("tftkey", ssb);
        Terms result = sr.getAggregations().get("Per_Investor_Freq");
        assertEquals(2,result.getBucketByKey("Jay").getDocCount());
        assertEquals("sterms", result.getType());
        assertEquals("Per_Investor_Freq", result.getName());

        List<?> buckets = result.getBuckets();
        assertEquals("Jay", ((Terms.Bucket)buckets.get(0)).getKey());
        assertEquals(2, ((Terms.Bucket)buckets.get(0)).getDocCount());
        assertEquals("Mila", ((Terms.Bucket)buckets.get(1)).getKey());
        assertEquals(1, ((Terms.Bucket)buckets.get(1)).getDocCount());
        assertEquals("{\"size\":0,\"query\":{\"term\":{\"purchase_type\":{\"boost\":1.0,\"value\":1}}},\"aggs\":{\"Per_Investor_Freq\":{\"terms\":{\"field\":\"investor\",\"size\":10,\"min_doc_count\":1,\"order\":{\"_count\":\"desc\"}}}}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[],\"max_score\":null,\"total\":{\"relation\":\"eq\",\"value\":3}},\"aggregations\":{\"Per_Investor_Freq\":{\"type\":\"sterms\",\"buckets\":[{\"key\":\"Jay\",\"doc_count\":2},{\"key\":\"Mila\",\"doc_count\":1}]}}}",
                sr.toString());

        termsAggregationBuilder = AggregationBuilders.terms("Per_Investor_Freq").field("investor").order(BucketOrder.key(false));
        assertEquals(BucketOrder.key(false), termsAggregationBuilder.order());
        ssb = new SearchSourceBuilder().size(0).query(qb).aggregation(termsAggregationBuilder);
        sr = tairSearchCluster.tftsearch("tftkey", ssb);
        result = sr.getAggregations().get("Per_Investor_Freq");
        buckets = result.getBuckets();
        assertEquals("Mila", ((Terms.Bucket)buckets.get(0)).getKey());
        assertEquals(1, ((Terms.Bucket)buckets.get(0)).getDocCount());
        assertEquals("Jay", ((Terms.Bucket)buckets.get(1)).getKey());
        assertEquals(2, ((Terms.Bucket)buckets.get(1)).getDocCount());
        assertEquals("{\"size\":0,\"query\":{\"term\":{\"purchase_type\":{\"boost\":1.0,\"value\":1}}},\"aggs\":{\"Per_Investor_Freq\":{\"terms\":{\"field\":\"investor\",\"size\":10,\"min_doc_count\":1,\"order\":{\"_key\":\"desc\"}}}}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[],\"max_score\":null,\"total\":{\"relation\":\"eq\",\"value\":3}},\"aggregations\":{\"Per_Investor_Freq\":{\"type\":\"sterms\",\"buckets\":[{\"key\":\"Mila\",\"doc_count\":1},{\"key\":\"Jay\",\"doc_count\":2}]}}}",
                sr.toString());

        termsAggregationBuilder = AggregationBuilders.terms("Per_Investor_Freq").field("investor").minDocCount(2);
        assertEquals(2, termsAggregationBuilder.minDocCount());
        ssb = new SearchSourceBuilder().size(0).query(qb).aggregation(termsAggregationBuilder);
        sr = tairSearchCluster.tftsearch("tftkey", ssb);
        result = sr.getAggregations().get("Per_Investor_Freq");
        buckets = result.getBuckets();
        assertEquals("Jay", ((Terms.Bucket)buckets.get(0)).getKey());
        assertEquals(2, ((Terms.Bucket)buckets.get(0)).getDocCount());
        assertEquals("{\"size\":0,\"query\":{\"term\":{\"purchase_type\":{\"boost\":1.0,\"value\":1}}},\"aggs\":{\"Per_Investor_Freq\":{\"terms\":{\"field\":\"investor\",\"size\":10,\"min_doc_count\":2,\"order\":{\"_count\":\"desc\"}}}}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[],\"max_score\":null,\"total\":{\"relation\":\"eq\",\"value\":3}},\"aggregations\":{\"Per_Investor_Freq\":{\"type\":\"sterms\",\"buckets\":[{\"key\":\"Jay\",\"doc_count\":2}]}}}",
                sr.toString());

        termsAggregationBuilder = AggregationBuilders.terms("Per_Investor_Freq").field("investor").size(1);
        assertEquals(1, termsAggregationBuilder.size());
        ssb = new SearchSourceBuilder().size(0).query(qb).aggregation(termsAggregationBuilder);
        sr = tairSearchCluster.tftsearch("tftkey", ssb);
        result = sr.getAggregations().get("Per_Investor_Freq");
        buckets = result.getBuckets();
        assertEquals("Jay", ((Terms.Bucket)buckets.get(0)).getKey());
        assertEquals(2, ((Terms.Bucket)buckets.get(0)).getDocCount());
        assertEquals("{\"size\":0,\"query\":{\"term\":{\"purchase_type\":{\"boost\":1.0,\"value\":1}}},\"aggs\":{\"Per_Investor_Freq\":{\"terms\":{\"field\":\"investor\",\"size\":1,\"min_doc_count\":1,\"order\":{\"_count\":\"desc\"}}}}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[],\"max_score\":null,\"total\":{\"relation\":\"eq\",\"value\":3}},\"aggregations\":{\"Per_Investor_Freq\":{\"type\":\"sterms\",\"buckets\":[{\"key\":\"Jay\",\"doc_count\":2}]}}}",
                sr.toString());

        SortedSet exclude = new TreeSet();
        exclude.add("XAX");
        termsAggregationBuilder = AggregationBuilders.terms("Per_Investor_Freq").field("shares_name").includeExclude(new IncludeExclude("[A-Z]+",exclude));
        assertEquals("[A-Z]+", termsAggregationBuilder.includeExclude().include());
        TestCase.assertTrue( termsAggregationBuilder.includeExclude().excludeValues().contains("XAX"));
        ssb = new SearchSourceBuilder().size(0).query(qb).aggregation(termsAggregationBuilder);
        sr = tairSearchCluster.tftsearch("tftkey", ssb);
        result = sr.getAggregations().get("Per_Investor_Freq");
        buckets = result.getBuckets();
        assertEquals("YBY", ((Terms.Bucket)buckets.get(0)).getKey());
        assertEquals(1, ((Terms.Bucket)buckets.get(0)).getDocCount());
        assertEquals("{\"size\":0,\"query\":{\"term\":{\"purchase_type\":{\"boost\":1.0,\"value\":1}}},\"aggs\":{\"Per_Investor_Freq\":{\"terms\":{\"field\":\"shares_name\",\"size\":10,\"min_doc_count\":1,\"include\":\"[A-Z]+\",\"exclude\":[\"XAX\"],\"order\":{\"_count\":\"desc\"}}}}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[],\"max_score\":null,\"total\":{\"relation\":\"eq\",\"value\":3}},\"aggregations\":{\"Per_Investor_Freq\":{\"type\":\"sterms\",\"buckets\":[{\"key\":\"YBY\",\"doc_count\":1}]}}}",
                sr.toString());

        SumAggregationBuilder ab = AggregationBuilders.sum("Jay_BuyIn_Sum").field("purchase_price");
        termsAggregationBuilder = AggregationBuilders.terms("Per_Investor_Freq").field("investor").subAggregation(ab);
        TestCase.assertTrue( termsAggregationBuilder.getSubAggregations().contains(ab));
        ssb = new SearchSourceBuilder().size(0).query(qb).aggregation(termsAggregationBuilder);
        sr = tairSearchCluster.tftsearch("tftkey", ssb);
        result = sr.getAggregations().get("Per_Investor_Freq");
        assertEquals((long)2,((InternalAggregation)sr.getAggregations().get("Per_Investor_Freq")).getProperty("\'Jay\'._count"));
        assertEquals(212.2,((InternalAggregation)sr.getAggregations().get("Per_Investor_Freq")).getProperty("\'Jay\'>Jay_BuyIn_Sum.value"));
        buckets = result.getBuckets();
        assertEquals("Jay", ((Terms.Bucket)buckets.get(0)).getKey());
        assertEquals(2, ((Terms.Bucket)buckets.get(0)).getDocCount());
        Sum sum = ((Terms.Bucket)buckets.get(0)).getAggregations().get("Jay_BuyIn_Sum");
        assertEquals(212.2, sum.value(),0.001);
        assertEquals("Mila", ((Terms.Bucket)buckets.get(1)).getKey());
        assertEquals(1, ((Terms.Bucket)buckets.get(1)).getDocCount());
        sum = ((Terms.Bucket)buckets.get(1)).getAggregations().get("Jay_BuyIn_Sum");
        assertEquals(11.1, sum.value(),0.001);
        assertEquals("{\"size\":0,\"query\":{\"term\":{\"purchase_type\":{\"boost\":1.0,\"value\":1}}},\"aggs\":{\"Per_Investor_Freq\":{\"terms\":{\"field\":\"investor\",\"size\":10,\"min_doc_count\":1,\"order\":{\"_count\":\"desc\"}},\"aggs\":{\"Jay_BuyIn_Sum\":{\"sum\":{\"field\":\"purchase_price\"}}}}}}",
                ssb.constructJSON().toString());
        assertEquals("{\"hits\":{\"hits\":[],\"max_score\":null,\"total\":{\"relation\":\"eq\",\"value\":3}},\"aggregations\":{\"Per_Investor_Freq\":{\"type\":\"sterms\",\"buckets\":[{\"key\":\"Jay\",\"doc_count\":2,\"Jay_BuyIn_Sum\":{\"value\":212.2,\"type\":\"sum\"}},{\"key\":\"Mila\",\"doc_count\":1,\"Jay_BuyIn_Sum\":{\"value\":11.1,\"type\":\"sum\"}}]}}}",
                sr.toString());
    }

}
