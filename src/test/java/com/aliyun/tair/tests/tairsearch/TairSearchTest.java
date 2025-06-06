package com.aliyun.tair.tests.tairsearch;

import com.aliyun.tair.tairsearch.action.search.MSearchResponse;
import com.aliyun.tair.tairsearch.index.query.*;
import com.aliyun.tair.tairsearch.params.*;
import com.aliyun.tair.tairsearch.search.AuxInfo;
import com.aliyun.tair.tairsearch.search.aggregations.AggregationBuilders;
import com.aliyun.tair.tairsearch.search.aggregations.metrics.*;
import com.aliyun.tair.tairsearch.search.builder.KeyCursors;
import com.aliyun.tair.tairsearch.search.builder.MSearchSourceBuilder;
import com.aliyun.tair.tairsearch.search.builder.SearchSourceBuilder;
import com.aliyun.tair.tairsearch.search.aggregations.BucketOrder;
import com.aliyun.tair.tairsearch.action.search.SearchResponse;
import com.aliyun.tair.tairsearch.search.aggregations.InternalAggregation;
import com.aliyun.tair.tairsearch.search.aggregations.bucket.filter.Filter;
import com.aliyun.tair.tairsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import com.aliyun.tair.tairsearch.search.aggregations.bucket.terms.IncludeExclude;
import com.aliyun.tair.tairsearch.search.aggregations.bucket.terms.Terms;
import com.aliyun.tair.tairsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import com.aliyun.tair.tairsearch.search.TotalHits;
import com.aliyun.tair.tairsearch.search.sort.SortOrder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.Assert;
import org.junit.Test;
import com.aliyun.tair.jedis3.ScanResult;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static com.aliyun.tair.jedis3.ScanParams.SCAN_POINTER_START;

public class TairSearchTest extends TairSearchTestBase {

    @Test
    public void tftcreateindex() {
        getJedis().del("tftkey");
        String ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        String mapping = tairSearch.tftgetindexmappings("tftkey");
        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"},\"f1\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"}}}}}", mapping);

    }

    @Test
    public void tftupdateindex() {
        getJedis().del("tftkey");
        String ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        String mapping = tairSearch.tftgetindexmappings("tftkey");
        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"}}}}}", mapping);

        ret = tairSearch.tftupdateindex("tftkey", "{\"mappings\":{\"properties\":{\"f1\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        mapping = tairSearch.tftgetindexmappings("tftkey");
        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"},\"f1\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"}}}}}", mapping);
    }

    @Test
    public void tftgetindex() {
        getJedis().del("tftkey");
        String ret = tairSearch.tftcreateindex("tftkey", "{\"settings\":{\"analysis\":{\"analyzer\":{\"my_custom_analyzer\":{\"type\":\"custom\",\"tokenizer\":\"whitespace\"}}}},\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        TFTGetIndexParams mappingsParam = new TFTGetIndexParams().mappings();
        TFTGetIndexParams settingsParam = new TFTGetIndexParams().settings();

        String mappings = tairSearch.tftgetindex("tftkey", mappingsParam);
        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"},\"f1\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"}}}}}", mappings);

        String settings = tairSearch.tftgetindex("tftkey", settingsParam);
        assertEquals("{\"tftkey\":{\"settings\":{\"analysis\":{\"analyzer\":{\"my_custom_analyzer\":{\"type\":\"custom\",\"tokenizer\":\"whitespace\"}}}}}}", settings);

        String index = tairSearch.tftgetindex("tftkey");
        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"},\"f1\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"}}},\"settings\":{\"analysis\":{\"analyzer\":{\"my_custom_analyzer\":{\"type\":\"custom\",\"tokenizer\":\"whitespace\"}}}}}}", index);

        mappings = tairSearch.tftgetindex("tftkey".getBytes(), mappingsParam);
        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"},\"f1\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"}}}}}", mappings);

        settings = tairSearch.tftgetindex("tftkey".getBytes(), settingsParam);
        assertEquals("{\"tftkey\":{\"settings\":{\"analysis\":{\"analyzer\":{\"my_custom_analyzer\":{\"type\":\"custom\",\"tokenizer\":\"whitespace\"}}}}}}", settings);

        index = tairSearch.tftgetindex("tftkey".getBytes());
        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"},\"f1\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"}}},\"settings\":{\"analysis\":{\"analyzer\":{\"my_custom_analyzer\":{\"type\":\"custom\",\"tokenizer\":\"whitespace\"}}}}}}", index);
    }

    @Test
    public void tftadddoc() {
        getJedis().del("tftkey");
        tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v0\",\"f1\":\"3\"}", "1");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v1\",\"f1\":\"3\"}", "2");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"3\"}", "3");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"4\"}", "4");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"5\"}", "5");

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":1.49608,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":1.49608,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":1.49608,\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}],\"max_score\":1.49608,\"total\":{\"relation\":\"eq\",\"value\":3}}}",
                tairSearch.tftsearch("tftkey", "{\"query\":{\"match\":{\"f1\":\"3\"}}}"));

        assertEquals("{\"_id\":\"3\",\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}", tairSearch.tftgetdoc("tftkey", "3"));
        assertEquals("1", tairSearch.tftdeldoc("tftkey", "3"));
        assertEquals(null, tairSearch.tftgetdoc("tftkey", "3"));

        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"},\"f1\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"}}}}}", tairSearch.tftgetindexmappings("tftkey"));
    }

    @Test
    public void tfupdatedocfield() {
        getJedis().del("tftkey");
        String ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        tairSearch.tftadddoc("tftkey", "{\"f0\":\"redis is a nosql database\"}", "1");
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.054363,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.054363,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                tairSearch.tftsearch("tftkey", "{\"query\":{\"term\":{\"f0\":\"redis\"}}}"));

        ret = tairSearch.tftupdateindex("tftkey", "{\"mappings\":{\"properties\":{\"f1\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        tairSearch.tftupdatedocfield("tftkey", "1", "{\"f1\":\"mysql is a dbms\"}");
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.06658,\"_source\":{\"f1\":\"mysql is a dbms\",\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.06658,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                tairSearch.tftsearch("tftkey", "{\"query\":{\"term\":{\"f1\":\"mysql\"}}}"));
    }

    @Test
    public void tfincrlongdocfield() {
        getJedis().del("tftkey");

        try {
            tairSearch.tftincrlongdocfield("tftkey", "1", "f0", 1);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("not exists"));
        }

        String ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        try {
            tairSearch.tftincrlongdocfield("tftkey", "1", "f0", 1);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("incrlongdocfield only supports field of int or long type"));
        }

        getJedis().del("tftkey");

        ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"long\"}}}}");
        assertEquals(ret, "OK");

        assertEquals(1, tairSearch.tftincrlongdocfield("tftkey", "1", "f0", 1).intValue());
        assertEquals(0, tairSearch.tftincrlongdocfield("tftkey", "1", "f0", -1).intValue());

        assertEquals(1, tairSearch.tftexists("tftkey", "1").intValue());
    }

    @Test
    public void tfincrfloatdocfield() {
        getJedis().del("tftkey");

        try {
            tairSearch.tftincrfloatdocfield("tftkey", "1", "f0", 1.1);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("not exists"));
        }

        String ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        try {
            tairSearch.tftincrfloatdocfield("tftkey", "1", "f0", 1.1);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("incrfloatdocfield only supports field of double type"));
        }

        getJedis().del("tftkey");

        ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"double\"}}}}");
        assertEquals(ret, "OK");

        double value = tairSearch.tftincrfloatdocfield("tftkey", "1", "f0", 1.1);
        assertEquals(Double.compare(1.1, value), 0);
        value = tairSearch.tftincrfloatdocfield("tftkey", "1", "f0", -1.1);
        assertEquals(Double.compare(0, value), 0);

        assertEquals(1, tairSearch.tftexists("tftkey", "1").intValue());
    }

    @Test
    public void tftdeldocfield() {
        getJedis().del("tftkey");

        assertEquals(0, tairSearch.tftdeldocfield("tftkey", "1", "f0").intValue());

        String ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"long\"}}}}");
        assertEquals(ret, "OK");

        tairSearch.tftincrlongdocfield("tftkey", "1", "f0", 1);
        tairSearch.tftincrfloatdocfield("tftkey", "1", "f1", 1.1);

        assertEquals(2, tairSearch.tftdeldocfield("tftkey", "1", "f0", "f1", "f2").intValue());
    }

    @Test
    public void tfdeldoc() {
        getJedis().del("tftkey");
        tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v0\",\"f1\":\"3\"}", "1");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v1\",\"f1\":\"3\"}", "2");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"3\"}", "3");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"4\"}", "4");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"5\"}", "5");

        assertEquals(1, tairSearch.tftexists("tftkey", "3").intValue());
        assertEquals(5, tairSearch.tftdocnum("tftkey").intValue());
        assertEquals("3", tairSearch.tftdeldoc("tftkey", "3", "4", "5"));
        assertEquals(0, tairSearch.tftexists("tftkey", "3").intValue());
        assertEquals(2, tairSearch.tftdocnum("tftkey").intValue());
    }

    @Test
    public void tfdelall() {
        getJedis().del("tftkey");
        tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v0\",\"f1\":\"3\"}", "1");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v1\",\"f1\":\"3\"}", "2");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"3\"}", "3");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"4\"}", "4");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"5\"}", "5");

        assertEquals("OK", tairSearch.tftdelall("tftkey"));
        assertEquals(0, tairSearch.tftdocnum("tftkey").intValue());
    }

    @Test
    public void tfscandocid() {
        getJedis().del("tftkey");
        tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v0\",\"f1\":\"3\"}", "1");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v1\",\"f1\":\"3\"}", "2");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"3\"}", "3");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"4\"}", "4");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"5\"}", "5");

        ScanResult<String> res = tairSearch.tftscandocid("tftkey", "0");
        assertEquals(SCAN_POINTER_START, res.getCursor());
        assertTrue(res.getResult().size() == 5);

        assertEquals("1", res.getResult().get(0));
        assertEquals("2", res.getResult().get(1));
        assertEquals("3", res.getResult().get(2));
        assertEquals("4", res.getResult().get(3));
        assertEquals("5", res.getResult().get(4));
    }

    @Test
    public void tftanalyzer() {
        getJedis().del("tftkey");

        String res = tairSearch.tftanalyzer("standard", "tair is a nosql database");
        assertEquals("{\"tokens\":[{\"token\":\"tair\",\"start_offset\":0,\"end_offset\":4,\"position\":0},{\"token\":\"nosql\",\"start_offset\":10,\"end_offset\":15,\"position\":3},{\"token\":\"database\",\"start_offset\":16,\"end_offset\":24,\"position\":4}]}", res);

        res = tairSearch.tftanalyzer("standard".getBytes(StandardCharsets.UTF_8), "tair is a nosql database".getBytes(StandardCharsets.UTF_8));
        assertEquals("{\"tokens\":[{\"token\":\"tair\",\"start_offset\":0,\"end_offset\":4,\"position\":0},{\"token\":\"nosql\",\"start_offset\":10,\"end_offset\":15,\"position\":3},{\"token\":\"database\",\"start_offset\":16,\"end_offset\":24,\"position\":4}]}", res);

        res = tairSearch.tftanalyzer("standard", "tair is a nosql database", new TFTAnalyzerParams().showTime());
        Assert.assertTrue(res.contains("consuming time (us)"));

        tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"properties\":{\"f0\":{\"type\":\"text\",\"analyzer\":\"my_jieba_analyzer\"}}},\"settings\":{\"analysis\":{\"analyzer\":{\"my_jieba_analyzer\":{\"type\":\"jieba\",\"userwords\":[\"key-value数据结构存储\"],\"use_hmm\":true}}}}}");

        res = tairSearch.tftanalyzer("jieba", "Redis是完全开源免费的，遵守BSD协议，是一个灵活的高性能key-value数据结构存储，可以用来作为数据库、缓存和消息队列。Redis比其他key-value缓存产品有以下三个特点：Redis支持数据的持久化，可以将内存中的数据保存在磁盘中，重启的时候可以再次加载到内存使用。");
        assertEquals("{\"tokens\":[{\"token\":\"redis\",\"start_offset\":0,\"end_offset\":5,\"position\":0},{\"token\":\"完全\",\"start_offset\":6,\"end_offset\":8,\"position\":2},{\"token\":\"开源\",\"start_offset\":8,\"end_offset\":10,\"position\":3},{\"token\":\"免费\",\"start_offset\":10,\"end_offset\":12,\"position\":4},{\"token\":\"遵守\",\"start_offset\":14,\"end_offset\":16,\"position\":7},{\"token\":\"bsd\",\"start_offset\":16,\"end_offset\":19,\"position\":8},{\"token\":\"协议\",\"start_offset\":19,\"end_offset\":21,\"position\":9},{\"token\":\"一个\",\"start_offset\":23,\"end_offset\":25,\"position\":12},{\"token\":\"灵活\",\"start_offset\":25,\"end_offset\":27,\"position\":13},{\"token\":\"性能\",\"start_offset\":29,\"end_offset\":31,\"position\":15},{\"token\":\"高性能\",\"start_offset\":28,\"end_offset\":31,\"position\":16},{\"token\":\"key\",\"start_offset\":31,\"end_offset\":34,\"position\":17},{\"token\":\"value\",\"start_offset\":35,\"end_offset\":40,\"position\":19},{\"token\":\"数据\",\"start_offset\":40,\"end_offset\":42,\"position\":20},{\"token\":\"结构\",\"start_offset\":42,\"end_offset\":44,\"position\":21},{\"token\":\"数据结构\",\"start_offset\":40,\"end_offset\":44,\"position\":22},{\"token\":\"存储\",\"start_offset\":44,\"end_offset\":46,\"position\":23},{\"token\":\"数据\",\"start_offset\":53,\"end_offset\":55,\"position\":28},{\"token\":\"数据库\",\"start_offset\":53,\"end_offset\":56,\"position\":29},{\"token\":\"缓存\",\"start_offset\":57,\"end_offset\":59,\"position\":31},{\"token\":\"消息\",\"start_offset\":60,\"end_offset\":62,\"position\":33},{\"token\":\"队列\",\"start_offset\":62,\"end_offset\":64,\"position\":34},{\"token\":\"redis\",\"start_offset\":65,\"end_offset\":70,\"position\":36},{\"token\":\"key\",\"start_offset\":73,\"end_offset\":76,\"position\":39},{\"token\":\"value\",\"start_offset\":77,\"end_offset\":82,\"position\":41},{\"token\":\"缓存\",\"start_offset\":82,\"end_offset\":84,\"position\":42},{\"token\":\"产品\",\"start_offset\":84,\"end_offset\":86,\"position\":43},{\"token\":\"以下\",\"start_offset\":87,\"end_offset\":89,\"position\":45},{\"token\":\"三个\",\"start_offset\":89,\"end_offset\":91,\"position\":46},{\"token\":\"特点\",\"start_offset\":91,\"end_offset\":93,\"position\":47},{\"token\":\"redis\",\"start_offset\":94,\"end_offset\":99,\"position\":49},{\"token\":\"支持\",\"start_offset\":99,\"end_offset\":101,\"position\":50},{\"token\":\"数据\",\"start_offset\":101,\"end_offset\":103,\"position\":51},{\"token\":\"持久\",\"start_offset\":104,\"end_offset\":106,\"position\":53},{\"token\":\"化\",\"start_offset\":106,\"end_offset\":107,\"position\":54},{\"token\":\"内存\",\"start_offset\":111,\"end_offset\":113,\"position\":58},{\"token\":\"中\",\"start_offset\":113,\"end_offset\":114,\"position\":59},{\"token\":\"数据\",\"start_offset\":115,\"end_offset\":117,\"position\":61},{\"token\":\"保存\",\"start_offset\":117,\"end_offset\":119,\"position\":62},{\"token\":\"磁盘\",\"start_offset\":120,\"end_offset\":122,\"position\":64},{\"token\":\"中\",\"start_offset\":122,\"end_offset\":123,\"position\":65},{\"token\":\"重启\",\"start_offset\":124,\"end_offset\":126,\"position\":67},{\"token\":\"再次\",\"start_offset\":131,\"end_offset\":133,\"position\":71},{\"token\":\"加载\",\"start_offset\":133,\"end_offset\":135,\"position\":72},{\"token\":\"内存\",\"start_offset\":136,\"end_offset\":138,\"position\":74},{\"token\":\"使用\",\"start_offset\":138,\"end_offset\":140,\"position\":75}]}", res);
        Assert.assertFalse(res.contains("key-value数据结构存储"));

        res = tairSearch.tftanalyzer("my_jieba_analyzer", "Redis是完全开源免费的，遵守BSD协议，是一个灵活的高性能key-value数据结构存储，可以用来作为数据库、缓存和消息队列。Redis比其他key-value缓存产品有以下三个特点：Redis支持数据的持久化，可以将内存中的数据保存在磁盘中，重启的时候可以再次加载到内存使用。", new TFTAnalyzerParams().index("tftkey"));
        assertEquals("{\"tokens\":[{\"token\":\"redis\",\"start_offset\":0,\"end_offset\":5,\"position\":0},{\"token\":\"完全\",\"start_offset\":6,\"end_offset\":8,\"position\":2},{\"token\":\"开源\",\"start_offset\":8,\"end_offset\":10,\"position\":3},{\"token\":\"免费\",\"start_offset\":10,\"end_offset\":12,\"position\":4},{\"token\":\"遵守\",\"start_offset\":14,\"end_offset\":16,\"position\":7},{\"token\":\"bsd\",\"start_offset\":16,\"end_offset\":19,\"position\":8},{\"token\":\"协议\",\"start_offset\":19,\"end_offset\":21,\"position\":9},{\"token\":\"一个\",\"start_offset\":23,\"end_offset\":25,\"position\":12},{\"token\":\"灵活\",\"start_offset\":25,\"end_offset\":27,\"position\":13},{\"token\":\"性能\",\"start_offset\":29,\"end_offset\":31,\"position\":15},{\"token\":\"高性能\",\"start_offset\":28,\"end_offset\":31,\"position\":16},{\"token\":\"数据\",\"start_offset\":40,\"end_offset\":42,\"position\":17},{\"token\":\"结构\",\"start_offset\":42,\"end_offset\":44,\"position\":18},{\"token\":\"存储\",\"start_offset\":44,\"end_offset\":46,\"position\":19},{\"token\":\"key-value数据结构存储\",\"start_offset\":31,\"end_offset\":46,\"position\":20},{\"token\":\"数据\",\"start_offset\":53,\"end_offset\":55,\"position\":25},{\"token\":\"数据库\",\"start_offset\":53,\"end_offset\":56,\"position\":26},{\"token\":\"缓存\",\"start_offset\":57,\"end_offset\":59,\"position\":28},{\"token\":\"消息\",\"start_offset\":60,\"end_offset\":62,\"position\":30},{\"token\":\"队列\",\"start_offset\":62,\"end_offset\":64,\"position\":31},{\"token\":\"redis\",\"start_offset\":65,\"end_offset\":70,\"position\":33},{\"token\":\"key\",\"start_offset\":73,\"end_offset\":76,\"position\":36},{\"token\":\"value\",\"start_offset\":77,\"end_offset\":82,\"position\":38},{\"token\":\"缓存\",\"start_offset\":82,\"end_offset\":84,\"position\":39},{\"token\":\"产品\",\"start_offset\":84,\"end_offset\":86,\"position\":40},{\"token\":\"以下\",\"start_offset\":87,\"end_offset\":89,\"position\":42},{\"token\":\"三个\",\"start_offset\":89,\"end_offset\":91,\"position\":43},{\"token\":\"特点\",\"start_offset\":91,\"end_offset\":93,\"position\":44},{\"token\":\"redis\",\"start_offset\":94,\"end_offset\":99,\"position\":46},{\"token\":\"支持\",\"start_offset\":99,\"end_offset\":101,\"position\":47},{\"token\":\"数据\",\"start_offset\":101,\"end_offset\":103,\"position\":48},{\"token\":\"持久\",\"start_offset\":104,\"end_offset\":106,\"position\":50},{\"token\":\"化\",\"start_offset\":106,\"end_offset\":107,\"position\":51},{\"token\":\"内存\",\"start_offset\":111,\"end_offset\":113,\"position\":55},{\"token\":\"中\",\"start_offset\":113,\"end_offset\":114,\"position\":56},{\"token\":\"数据\",\"start_offset\":115,\"end_offset\":117,\"position\":58},{\"token\":\"保存\",\"start_offset\":117,\"end_offset\":119,\"position\":59},{\"token\":\"磁盘\",\"start_offset\":120,\"end_offset\":122,\"position\":61},{\"token\":\"中\",\"start_offset\":122,\"end_offset\":123,\"position\":62},{\"token\":\"重启\",\"start_offset\":124,\"end_offset\":126,\"position\":64},{\"token\":\"再次\",\"start_offset\":131,\"end_offset\":133,\"position\":68},{\"token\":\"加载\",\"start_offset\":133,\"end_offset\":135,\"position\":69},{\"token\":\"内存\",\"start_offset\":136,\"end_offset\":138,\"position\":71},{\"token\":\"使用\",\"start_offset\":138,\"end_offset\":140,\"position\":72}]}", res);
        Assert.assertTrue(res.contains("key-value数据结构存储"));
    }

    @Test
    public void tftanalyzerwithunicodekey() {
        getJedis().del("这是一个unicode_key");
        tairSearch.tftcreateindex("这是一个unicode_key", "{\"mappings\":{\"properties\":{\"f0\":{\"type\":\"text\",\"analyzer\":\"my_jieba_analyzer\"}}},\"settings\":{\"analysis\":{\"analyzer\":{\"my_jieba_analyzer\":{\"type\":\"jieba\",\"userwords\":[\"key-value数据结构存储\"],\"use_hmm\":true}}}}}");

        String res = tairSearch.tftanalyzer("jieba", "Redis是完全开源免费的，遵守BSD协议，是一个灵活的高性能key-value数据结构存储，可以用来作为数据库、缓存和消息队列。Redis比其他key-value缓存产品有以下三个特点：Redis支持数据的持久化，可以将内存中的数据保存在磁盘中，重启的时候可以再次加载到内存使用。");
        assertEquals("{\"tokens\":[{\"token\":\"redis\",\"start_offset\":0,\"end_offset\":5,\"position\":0},{\"token\":\"完全\",\"start_offset\":6,\"end_offset\":8,\"position\":2},{\"token\":\"开源\",\"start_offset\":8,\"end_offset\":10,\"position\":3},{\"token\":\"免费\",\"start_offset\":10,\"end_offset\":12,\"position\":4},{\"token\":\"遵守\",\"start_offset\":14,\"end_offset\":16,\"position\":7},{\"token\":\"bsd\",\"start_offset\":16,\"end_offset\":19,\"position\":8},{\"token\":\"协议\",\"start_offset\":19,\"end_offset\":21,\"position\":9},{\"token\":\"一个\",\"start_offset\":23,\"end_offset\":25,\"position\":12},{\"token\":\"灵活\",\"start_offset\":25,\"end_offset\":27,\"position\":13},{\"token\":\"性能\",\"start_offset\":29,\"end_offset\":31,\"position\":15},{\"token\":\"高性能\",\"start_offset\":28,\"end_offset\":31,\"position\":16},{\"token\":\"key\",\"start_offset\":31,\"end_offset\":34,\"position\":17},{\"token\":\"value\",\"start_offset\":35,\"end_offset\":40,\"position\":19},{\"token\":\"数据\",\"start_offset\":40,\"end_offset\":42,\"position\":20},{\"token\":\"结构\",\"start_offset\":42,\"end_offset\":44,\"position\":21},{\"token\":\"数据结构\",\"start_offset\":40,\"end_offset\":44,\"position\":22},{\"token\":\"存储\",\"start_offset\":44,\"end_offset\":46,\"position\":23},{\"token\":\"数据\",\"start_offset\":53,\"end_offset\":55,\"position\":28},{\"token\":\"数据库\",\"start_offset\":53,\"end_offset\":56,\"position\":29},{\"token\":\"缓存\",\"start_offset\":57,\"end_offset\":59,\"position\":31},{\"token\":\"消息\",\"start_offset\":60,\"end_offset\":62,\"position\":33},{\"token\":\"队列\",\"start_offset\":62,\"end_offset\":64,\"position\":34},{\"token\":\"redis\",\"start_offset\":65,\"end_offset\":70,\"position\":36},{\"token\":\"key\",\"start_offset\":73,\"end_offset\":76,\"position\":39},{\"token\":\"value\",\"start_offset\":77,\"end_offset\":82,\"position\":41},{\"token\":\"缓存\",\"start_offset\":82,\"end_offset\":84,\"position\":42},{\"token\":\"产品\",\"start_offset\":84,\"end_offset\":86,\"position\":43},{\"token\":\"以下\",\"start_offset\":87,\"end_offset\":89,\"position\":45},{\"token\":\"三个\",\"start_offset\":89,\"end_offset\":91,\"position\":46},{\"token\":\"特点\",\"start_offset\":91,\"end_offset\":93,\"position\":47},{\"token\":\"redis\",\"start_offset\":94,\"end_offset\":99,\"position\":49},{\"token\":\"支持\",\"start_offset\":99,\"end_offset\":101,\"position\":50},{\"token\":\"数据\",\"start_offset\":101,\"end_offset\":103,\"position\":51},{\"token\":\"持久\",\"start_offset\":104,\"end_offset\":106,\"position\":53},{\"token\":\"化\",\"start_offset\":106,\"end_offset\":107,\"position\":54},{\"token\":\"内存\",\"start_offset\":111,\"end_offset\":113,\"position\":58},{\"token\":\"中\",\"start_offset\":113,\"end_offset\":114,\"position\":59},{\"token\":\"数据\",\"start_offset\":115,\"end_offset\":117,\"position\":61},{\"token\":\"保存\",\"start_offset\":117,\"end_offset\":119,\"position\":62},{\"token\":\"磁盘\",\"start_offset\":120,\"end_offset\":122,\"position\":64},{\"token\":\"中\",\"start_offset\":122,\"end_offset\":123,\"position\":65},{\"token\":\"重启\",\"start_offset\":124,\"end_offset\":126,\"position\":67},{\"token\":\"再次\",\"start_offset\":131,\"end_offset\":133,\"position\":71},{\"token\":\"加载\",\"start_offset\":133,\"end_offset\":135,\"position\":72},{\"token\":\"内存\",\"start_offset\":136,\"end_offset\":138,\"position\":74},{\"token\":\"使用\",\"start_offset\":138,\"end_offset\":140,\"position\":75}]}", res);
        Assert.assertFalse(res.contains("key-value数据结构存储"));

        res = tairSearch.tftanalyzer("my_jieba_analyzer", "Redis是完全开源免费的，遵守BSD协议，是一个灵活的高性能key-value数据结构存储，可以用来作为数据库、缓存和消息队列。Redis比其他key-value缓存产品有以下三个特点：Redis支持数据的持久化，可以将内存中的数据保存在磁盘中，重启的时候可以再次加载到内存使用。", new TFTAnalyzerParams().index("这是一个unicode_key"));
        assertEquals("{\"tokens\":[{\"token\":\"redis\",\"start_offset\":0,\"end_offset\":5,\"position\":0},{\"token\":\"完全\",\"start_offset\":6,\"end_offset\":8,\"position\":2},{\"token\":\"开源\",\"start_offset\":8,\"end_offset\":10,\"position\":3},{\"token\":\"免费\",\"start_offset\":10,\"end_offset\":12,\"position\":4},{\"token\":\"遵守\",\"start_offset\":14,\"end_offset\":16,\"position\":7},{\"token\":\"bsd\",\"start_offset\":16,\"end_offset\":19,\"position\":8},{\"token\":\"协议\",\"start_offset\":19,\"end_offset\":21,\"position\":9},{\"token\":\"一个\",\"start_offset\":23,\"end_offset\":25,\"position\":12},{\"token\":\"灵活\",\"start_offset\":25,\"end_offset\":27,\"position\":13},{\"token\":\"性能\",\"start_offset\":29,\"end_offset\":31,\"position\":15},{\"token\":\"高性能\",\"start_offset\":28,\"end_offset\":31,\"position\":16},{\"token\":\"数据\",\"start_offset\":40,\"end_offset\":42,\"position\":17},{\"token\":\"结构\",\"start_offset\":42,\"end_offset\":44,\"position\":18},{\"token\":\"存储\",\"start_offset\":44,\"end_offset\":46,\"position\":19},{\"token\":\"key-value数据结构存储\",\"start_offset\":31,\"end_offset\":46,\"position\":20},{\"token\":\"数据\",\"start_offset\":53,\"end_offset\":55,\"position\":25},{\"token\":\"数据库\",\"start_offset\":53,\"end_offset\":56,\"position\":26},{\"token\":\"缓存\",\"start_offset\":57,\"end_offset\":59,\"position\":28},{\"token\":\"消息\",\"start_offset\":60,\"end_offset\":62,\"position\":30},{\"token\":\"队列\",\"start_offset\":62,\"end_offset\":64,\"position\":31},{\"token\":\"redis\",\"start_offset\":65,\"end_offset\":70,\"position\":33},{\"token\":\"key\",\"start_offset\":73,\"end_offset\":76,\"position\":36},{\"token\":\"value\",\"start_offset\":77,\"end_offset\":82,\"position\":38},{\"token\":\"缓存\",\"start_offset\":82,\"end_offset\":84,\"position\":39},{\"token\":\"产品\",\"start_offset\":84,\"end_offset\":86,\"position\":40},{\"token\":\"以下\",\"start_offset\":87,\"end_offset\":89,\"position\":42},{\"token\":\"三个\",\"start_offset\":89,\"end_offset\":91,\"position\":43},{\"token\":\"特点\",\"start_offset\":91,\"end_offset\":93,\"position\":44},{\"token\":\"redis\",\"start_offset\":94,\"end_offset\":99,\"position\":46},{\"token\":\"支持\",\"start_offset\":99,\"end_offset\":101,\"position\":47},{\"token\":\"数据\",\"start_offset\":101,\"end_offset\":103,\"position\":48},{\"token\":\"持久\",\"start_offset\":104,\"end_offset\":106,\"position\":50},{\"token\":\"化\",\"start_offset\":106,\"end_offset\":107,\"position\":51},{\"token\":\"内存\",\"start_offset\":111,\"end_offset\":113,\"position\":55},{\"token\":\"中\",\"start_offset\":113,\"end_offset\":114,\"position\":56},{\"token\":\"数据\",\"start_offset\":115,\"end_offset\":117,\"position\":58},{\"token\":\"保存\",\"start_offset\":117,\"end_offset\":119,\"position\":59},{\"token\":\"磁盘\",\"start_offset\":120,\"end_offset\":122,\"position\":61},{\"token\":\"中\",\"start_offset\":122,\"end_offset\":123,\"position\":62},{\"token\":\"重启\",\"start_offset\":124,\"end_offset\":126,\"position\":64},{\"token\":\"再次\",\"start_offset\":131,\"end_offset\":133,\"position\":68},{\"token\":\"加载\",\"start_offset\":133,\"end_offset\":135,\"position\":69},{\"token\":\"内存\",\"start_offset\":136,\"end_offset\":138,\"position\":71},{\"token\":\"使用\",\"start_offset\":138,\"end_offset\":140,\"position\":72}]}", res);
        Assert.assertTrue(res.contains("key-value数据结构存储"));

        // unicode key
        getJedis().del("这是一个unicode_key");
        tairSearch.tftcreateindex("这是一个unicode_key".getBytes(StandardCharsets.UTF_8), "{\"mappings\":{\"properties\":{\"f0\":{\"type\":\"text\",\"analyzer\":\"my_jieba_analyzer\"}}},\"settings\":{\"analysis\":{\"analyzer\":{\"my_jieba_analyzer\":{\"type\":\"jieba\",\"userwords\":[\"key-value数据结构存储\"],\"use_hmm\":true}}}}}".getBytes(StandardCharsets.UTF_8));

        res = tairSearch.tftanalyzer("my_jieba_analyzer".getBytes(StandardCharsets.UTF_8), "Redis是完全开源免费的，遵守BSD协议，是一个灵活的高性能key-value数据结构存储，可以用来作为数据库、缓存和消息队列。Redis比其他key-value缓存产品有以下三个特点：Redis支持数据的持久化，可以将内存中的数据保存在磁盘中，重启的时候可以再次加载到内存使用。".getBytes(StandardCharsets.UTF_8), new TFTAnalyzerParams().index("这是一个unicode_key"));
        assertEquals("{\"tokens\":[{\"token\":\"redis\",\"start_offset\":0,\"end_offset\":5,\"position\":0},{\"token\":\"完全\",\"start_offset\":6,\"end_offset\":8,\"position\":2},{\"token\":\"开源\",\"start_offset\":8,\"end_offset\":10,\"position\":3},{\"token\":\"免费\",\"start_offset\":10,\"end_offset\":12,\"position\":4},{\"token\":\"遵守\",\"start_offset\":14,\"end_offset\":16,\"position\":7},{\"token\":\"bsd\",\"start_offset\":16,\"end_offset\":19,\"position\":8},{\"token\":\"协议\",\"start_offset\":19,\"end_offset\":21,\"position\":9},{\"token\":\"一个\",\"start_offset\":23,\"end_offset\":25,\"position\":12},{\"token\":\"灵活\",\"start_offset\":25,\"end_offset\":27,\"position\":13},{\"token\":\"性能\",\"start_offset\":29,\"end_offset\":31,\"position\":15},{\"token\":\"高性能\",\"start_offset\":28,\"end_offset\":31,\"position\":16},{\"token\":\"数据\",\"start_offset\":40,\"end_offset\":42,\"position\":17},{\"token\":\"结构\",\"start_offset\":42,\"end_offset\":44,\"position\":18},{\"token\":\"存储\",\"start_offset\":44,\"end_offset\":46,\"position\":19},{\"token\":\"key-value数据结构存储\",\"start_offset\":31,\"end_offset\":46,\"position\":20},{\"token\":\"数据\",\"start_offset\":53,\"end_offset\":55,\"position\":25},{\"token\":\"数据库\",\"start_offset\":53,\"end_offset\":56,\"position\":26},{\"token\":\"缓存\",\"start_offset\":57,\"end_offset\":59,\"position\":28},{\"token\":\"消息\",\"start_offset\":60,\"end_offset\":62,\"position\":30},{\"token\":\"队列\",\"start_offset\":62,\"end_offset\":64,\"position\":31},{\"token\":\"redis\",\"start_offset\":65,\"end_offset\":70,\"position\":33},{\"token\":\"key\",\"start_offset\":73,\"end_offset\":76,\"position\":36},{\"token\":\"value\",\"start_offset\":77,\"end_offset\":82,\"position\":38},{\"token\":\"缓存\",\"start_offset\":82,\"end_offset\":84,\"position\":39},{\"token\":\"产品\",\"start_offset\":84,\"end_offset\":86,\"position\":40},{\"token\":\"以下\",\"start_offset\":87,\"end_offset\":89,\"position\":42},{\"token\":\"三个\",\"start_offset\":89,\"end_offset\":91,\"position\":43},{\"token\":\"特点\",\"start_offset\":91,\"end_offset\":93,\"position\":44},{\"token\":\"redis\",\"start_offset\":94,\"end_offset\":99,\"position\":46},{\"token\":\"支持\",\"start_offset\":99,\"end_offset\":101,\"position\":47},{\"token\":\"数据\",\"start_offset\":101,\"end_offset\":103,\"position\":48},{\"token\":\"持久\",\"start_offset\":104,\"end_offset\":106,\"position\":50},{\"token\":\"化\",\"start_offset\":106,\"end_offset\":107,\"position\":51},{\"token\":\"内存\",\"start_offset\":111,\"end_offset\":113,\"position\":55},{\"token\":\"中\",\"start_offset\":113,\"end_offset\":114,\"position\":56},{\"token\":\"数据\",\"start_offset\":115,\"end_offset\":117,\"position\":58},{\"token\":\"保存\",\"start_offset\":117,\"end_offset\":119,\"position\":59},{\"token\":\"磁盘\",\"start_offset\":120,\"end_offset\":122,\"position\":61},{\"token\":\"中\",\"start_offset\":122,\"end_offset\":123,\"position\":62},{\"token\":\"重启\",\"start_offset\":124,\"end_offset\":126,\"position\":64},{\"token\":\"再次\",\"start_offset\":131,\"end_offset\":133,\"position\":68},{\"token\":\"加载\",\"start_offset\":133,\"end_offset\":135,\"position\":69},{\"token\":\"内存\",\"start_offset\":136,\"end_offset\":138,\"position\":71},{\"token\":\"使用\",\"start_offset\":138,\"end_offset\":140,\"position\":72}]}", res);
        Assert.assertTrue(res.contains("key-value数据结构存储"));

        // bytes key
        byte[] bytes_key = new byte[]{0x00};
        getJedis().del(bytes_key);
        tairSearch.tftcreateindex(bytes_key, "{\"mappings\":{\"properties\":{\"f0\":{\"type\":\"text\",\"analyzer\":\"my_jieba_analyzer\"}}},\"settings\":{\"analysis\":{\"analyzer\":{\"my_jieba_analyzer\":{\"type\":\"jieba\",\"userwords\":[\"key-value数据结构存储\"],\"use_hmm\":true}}}}}".getBytes(StandardCharsets.UTF_8));
        res = tairSearch.tftanalyzer("my_jieba_analyzer".getBytes(StandardCharsets.UTF_8), "Redis是完全开源免费的，遵守BSD协议，是一个灵活的高性能key-value数据结构存储，可以用来作为数据库、缓存和消息队列。Redis比其他key-value缓存产品有以下三个特点：Redis支持数据的持久化，可以将内存中的数据保存在磁盘中，重启的时候可以再次加载到内存使用。".getBytes(StandardCharsets.UTF_8), new TFTAnalyzerParams().index(bytes_key));
        assertEquals("{\"tokens\":[{\"token\":\"redis\",\"start_offset\":0,\"end_offset\":5,\"position\":0},{\"token\":\"完全\",\"start_offset\":6,\"end_offset\":8,\"position\":2},{\"token\":\"开源\",\"start_offset\":8,\"end_offset\":10,\"position\":3},{\"token\":\"免费\",\"start_offset\":10,\"end_offset\":12,\"position\":4},{\"token\":\"遵守\",\"start_offset\":14,\"end_offset\":16,\"position\":7},{\"token\":\"bsd\",\"start_offset\":16,\"end_offset\":19,\"position\":8},{\"token\":\"协议\",\"start_offset\":19,\"end_offset\":21,\"position\":9},{\"token\":\"一个\",\"start_offset\":23,\"end_offset\":25,\"position\":12},{\"token\":\"灵活\",\"start_offset\":25,\"end_offset\":27,\"position\":13},{\"token\":\"性能\",\"start_offset\":29,\"end_offset\":31,\"position\":15},{\"token\":\"高性能\",\"start_offset\":28,\"end_offset\":31,\"position\":16},{\"token\":\"数据\",\"start_offset\":40,\"end_offset\":42,\"position\":17},{\"token\":\"结构\",\"start_offset\":42,\"end_offset\":44,\"position\":18},{\"token\":\"存储\",\"start_offset\":44,\"end_offset\":46,\"position\":19},{\"token\":\"key-value数据结构存储\",\"start_offset\":31,\"end_offset\":46,\"position\":20},{\"token\":\"数据\",\"start_offset\":53,\"end_offset\":55,\"position\":25},{\"token\":\"数据库\",\"start_offset\":53,\"end_offset\":56,\"position\":26},{\"token\":\"缓存\",\"start_offset\":57,\"end_offset\":59,\"position\":28},{\"token\":\"消息\",\"start_offset\":60,\"end_offset\":62,\"position\":30},{\"token\":\"队列\",\"start_offset\":62,\"end_offset\":64,\"position\":31},{\"token\":\"redis\",\"start_offset\":65,\"end_offset\":70,\"position\":33},{\"token\":\"key\",\"start_offset\":73,\"end_offset\":76,\"position\":36},{\"token\":\"value\",\"start_offset\":77,\"end_offset\":82,\"position\":38},{\"token\":\"缓存\",\"start_offset\":82,\"end_offset\":84,\"position\":39},{\"token\":\"产品\",\"start_offset\":84,\"end_offset\":86,\"position\":40},{\"token\":\"以下\",\"start_offset\":87,\"end_offset\":89,\"position\":42},{\"token\":\"三个\",\"start_offset\":89,\"end_offset\":91,\"position\":43},{\"token\":\"特点\",\"start_offset\":91,\"end_offset\":93,\"position\":44},{\"token\":\"redis\",\"start_offset\":94,\"end_offset\":99,\"position\":46},{\"token\":\"支持\",\"start_offset\":99,\"end_offset\":101,\"position\":47},{\"token\":\"数据\",\"start_offset\":101,\"end_offset\":103,\"position\":48},{\"token\":\"持久\",\"start_offset\":104,\"end_offset\":106,\"position\":50},{\"token\":\"化\",\"start_offset\":106,\"end_offset\":107,\"position\":51},{\"token\":\"内存\",\"start_offset\":111,\"end_offset\":113,\"position\":55},{\"token\":\"中\",\"start_offset\":113,\"end_offset\":114,\"position\":56},{\"token\":\"数据\",\"start_offset\":115,\"end_offset\":117,\"position\":58},{\"token\":\"保存\",\"start_offset\":117,\"end_offset\":119,\"position\":59},{\"token\":\"磁盘\",\"start_offset\":120,\"end_offset\":122,\"position\":61},{\"token\":\"中\",\"start_offset\":122,\"end_offset\":123,\"position\":62},{\"token\":\"重启\",\"start_offset\":124,\"end_offset\":126,\"position\":64},{\"token\":\"再次\",\"start_offset\":131,\"end_offset\":133,\"position\":68},{\"token\":\"加载\",\"start_offset\":133,\"end_offset\":135,\"position\":69},{\"token\":\"内存\",\"start_offset\":136,\"end_offset\":138,\"position\":71},{\"token\":\"使用\",\"start_offset\":138,\"end_offset\":140,\"position\":72}]}", res);
        Assert.assertTrue(res.contains("key-value数据结构存储"));
    }

    @Test
    public void tfscandocidwithcount() {
        getJedis().del("tftkey");
        tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v0\",\"f1\":\"3\"}", "1");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v1\",\"f1\":\"3\"}", "2");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"3\"}", "3");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"4\"}", "4");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"5\"}", "5");

        TFTScanParams params = new TFTScanParams();
        params.count(3);
        ScanResult<String> res = tairSearch.tftscandocid("tftkey", "0", params);
        assertEquals("3", res.getCursor());
        assertTrue(res.getResult().size() == 3);

        assertEquals("1", res.getResult().get(0));
        assertEquals("2", res.getResult().get(1));
        assertEquals("3", res.getResult().get(2));

        ScanResult<String> res2 = tairSearch.tftscandocid("tftkey", "3", params);
        assertEquals("0", res2.getCursor());
        assertTrue(res2.getResult().size() == 2);

        assertEquals("4", res2.getResult().get(0));
        assertEquals("5", res2.getResult().get(1));
    }

    @Test
    public void tfscandocidwithmatch() {
        getJedis().del("tftkey");
        tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v0\",\"f1\":\"3\"}", "1_redis_doc");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v1\",\"f1\":\"3\"}", "2_redis_doc");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"3\"}", "3_mysql_doc");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"4\"}", "4_mysql_doc");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"5\"}", "5_tidb_doc");

        TFTScanParams params = new TFTScanParams();
        params.match("*redis*");
        ScanResult<String> res = tairSearch.tftscandocid("tftkey", "0", params);
        assertEquals("0", res.getCursor());
        assertTrue(res.getResult().size() == 2);

        assertEquals("1_redis_doc", res.getResult().get(0));
        assertEquals("2_redis_doc", res.getResult().get(1));

        TFTScanParams params2 = new TFTScanParams();
        params2.match("*tidb*");
        ScanResult<String> res2 = tairSearch.tftscandocid("tftkey", "0", params2);
        assertEquals("0", res2.getCursor());
        assertTrue(res2.getResult().size() == 1);

        assertEquals("5_tidb_doc", res2.getResult().get(0));
    }

    @Test
    public void unicodetest() {
        getJedis().del("tftkey");
        tairSearch.tftmappingindex("tftkey", "{\"mappings\":{\"properties\":{\"f0\":{\"type\":\"text\",\"analyzer\":\"chinese\"}}}}");
        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"analyzer\":\"chinese\",\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"}}}}}", tairSearch.tftgetindexmappings("tftkey"));

        getJedis().del("tftkey");
        tairSearch.tftmappingindex("tftkey", "{\"mappings\":{\"properties\":{\"f0\":{\"type\":\"text\",\"search_analyzer\":\"chinese\"}}}}");
        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\",\"search_analyzer\":\"chinese\"}}}}}", tairSearch.tftgetindexmappings("tftkey"));
        getJedis().del("tftkey");

        tairSearch.tftmappingindex("tftkey", "{\"mappings\":{\"properties\":{\"f0\":{\"type\":\"text\",\"analyzer\":\"chinese\", \"search_analyzer\":\"chinese\"}}}}");
        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"analyzer\":\"chinese\",\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\",\"search_analyzer\":\"chinese\"}}}}}", tairSearch.tftgetindexmappings("tftkey"));
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"夏天是一个很热的季节\"}", "1");
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.089327,\"_source\":{\"f0\":\"夏天是一个很热的季节\"}}],\"max_score\":0.089327,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                tairSearch.tftsearch("tftkey", "{\"query\":{\"match\":{\"f0\":\"夏天冬天\"}}}"));
    }

    @Test
    public void searchcachetest() throws Exception {
        getJedis().del("tftkey");
        tairSearch.tftmappingindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v0\",\"f1\":\"3\"}", "1");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v1\",\"f1\":\"3\"}", "2");

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.353472,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":0.353472,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}}],\"max_score\":0.353472,\"total\":{\"relation\":\"eq\",\"value\":2}}}",
                tairSearch.tftsearch("tftkey", "{\"query\":{\"match\":{\"f1\":\"3\"}}}", true));

        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"3\"}", "3");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"4\"}", "4");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"v3\",\"f1\":\"5\"}", "5");

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.353472,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":0.353472,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}}],\"max_score\":0.353472,\"total\":{\"relation\":\"eq\",\"value\":2}}}",
                tairSearch.tftsearch("tftkey", "{\"query\":{\"match\":{\"f1\":\"3\"}}}", true));

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":1.49608,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":1.49608,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":1.49608,\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}],\"max_score\":1.49608,\"total\":{\"relation\":\"eq\",\"value\":3}}}",
                tairSearch.tftsearch("tftkey", "{\"query\":{\"match\":{\"f1\":\"3\"}}}"));

        // wait for LRU cache expired
        Thread.sleep(10000);

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":1.49608,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":1.49608,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":1.49608,\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}],\"max_score\":1.49608,\"total\":{\"relation\":\"eq\",\"value\":3}}}",
                tairSearch.tftsearch("tftkey", "{\"query\":{\"match\":{\"f1\":\"3\"}}}", true));
    }

    @Test
    public void msearchtest() throws Exception {
        getJedis().del("{tftkey}1");
        getJedis().del("{tftkey}2");
        tairSearch.tftmappingindex("{tftkey}1", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"long\"}}}}");
        tairSearch.tftadddoc("{tftkey}1", "{\"f0\":1234}", "1");
        tairSearch.tftmappingindex("{tftkey}2", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"long\"}}}}");
        tairSearch.tftadddoc("{tftkey}2", "{\"f0\":1234}", "1");
        
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"{tftkey}1\",\"_score\":1.0,\"_source\":{\"f0\":1234}},{\"_id\":\"1\",\"_index\":\"{tftkey}2\",\"_score\":1.0,\"_source\":{\"f0\":1234}}],\"max_score\":1.0,\"total\":{\"relation\":\"eq\",\"value\":2}},\"aux_info\":{\"index_crc64\":10084399559244916810}}",
                tairSearch.tftmsearch("{\"query\":{\"term\":{\"f0\":1234}}}", "{tftkey}1", "{tftkey}2"));
    } 

    @Test
    public void tftmaddteststring() {
        getJedis().del("tftkey");
        tairSearch.tftmappingindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}");
        Map<String, String> docs = new HashMap();
        docs.put("{\"f0\":\"v0\",\"f1\":\"3\"}", "1");
        docs.put("{\"f0\":\"v1\",\"f1\":\"3\"}", "2");
        docs.put("{\"f0\":\"v3\",\"f1\":\"3\"}", "3");
        docs.put("{\"f0\":\"v3\",\"f1\":\"4\"}", "4");
        docs.put("{\"f0\":\"v3\",\"f1\":\"5\"}", "5");

        tairSearch.tftmadddoc("tftkey", docs);

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":1.49608,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":1.49608,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":1.49608,\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}],\"max_score\":1.49608,\"total\":{\"relation\":\"eq\",\"value\":3}}}",
                tairSearch.tftsearch("tftkey", "{\"query\":{\"match\":{\"f1\":\"3\"}}}"));

        assertEquals("{\"_id\":\"3\",\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}", tairSearch.tftgetdoc("tftkey", "3"));
        assertEquals("1", tairSearch.tftdeldoc("tftkey", "3"));
        assertEquals(null, tairSearch.tftgetdoc("tftkey", "3"));

        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"},\"f1\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"}}}}}", tairSearch.tftgetindexmappings("tftkey"));
    }

    @Test
    public void tftmaddtestbyte() {
        getJedis().del("tftkey");
        tairSearch.tftmappingindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}");
        Map<byte[], byte[]> docs = new HashMap();
        docs.put("{\"f0\":\"v0\",\"f1\":\"3\"}".getBytes(), "1".getBytes());
        docs.put("{\"f0\":\"v1\",\"f1\":\"3\"}".getBytes(), "2".getBytes());
        docs.put("{\"f0\":\"v3\",\"f1\":\"3\"}".getBytes(), "3".getBytes());
        docs.put("{\"f0\":\"v3\",\"f1\":\"4\"}".getBytes(), "4".getBytes());
        docs.put("{\"f0\":\"v3\",\"f1\":\"5\"}".getBytes(), "5".getBytes());

        tairSearch.tftmadddoc("tftkey".getBytes(), docs);

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":1.49608,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":1.49608,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":1.49608,\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}],\"max_score\":1.49608,\"total\":{\"relation\":\"eq\",\"value\":3}}}",
                tairSearch.tftsearch("tftkey", "{\"query\":{\"match\":{\"f1\":\"3\"}}}"));

        assertEquals("{\"_id\":\"3\",\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}", tairSearch.tftgetdoc("tftkey", "3"));
        assertEquals("1", tairSearch.tftdeldoc("tftkey", "3"));
        assertEquals(null, tairSearch.tftgetdoc("tftkey", "3"));

        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"},\"f1\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"}}}}}", tairSearch.tftgetindexmappings("tftkey"));
    }

    @Test
    public void tftmaddtestdocinfo() {
        getJedis().del("tftkey");
        tairSearch.tftmappingindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}");
        List<DocInfo> docs = new ArrayList<>();
        docs.add(new DocInfo("{\"f0\":\"v0\",\"f1\":\"3\"}", "3"));
        docs.add(new DocInfo("{\"f0\":\"v1\",\"f1\":\"3\"}", "2"));
        docs.add(new DocInfo("{\"f0\":\"v1\",\"f1\":\"3\"}", "3"));
        docs.add(new DocInfo("{\"f0\":\"v3\",\"f1\":\"4\"}", "4"));
        docs.add(new DocInfo("{\"f0\":\"v3\",\"f1\":\"5\"}", "5"));

        tairSearch.tftmadddoc("tftkey", docs);

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":1.658125,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":1.658125,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}}],\"max_score\":1.658125,\"total\":{\"relation\":\"eq\",\"value\":2}}}",
                tairSearch.tftsearch("tftkey", "{\"query\":{\"match\":{\"f1\":\"3\"}}}"));

        assertEquals("{\"_id\":\"3\",\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}}", tairSearch.tftgetdoc("tftkey", "3"));
        assertEquals("1", tairSearch.tftdeldoc("tftkey", "3"));
        assertEquals(null, tairSearch.tftgetdoc("tftkey", "3"));

        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"},\"f1\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"}}}}}", tairSearch.tftgetindexmappings("tftkey"));
    }

    @Test
    public void tftmaddtestdocinfobyte() {
        getJedis().del("tftkey");
        tairSearch.tftmappingindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}");
        List<DocInfoByte> docs = new ArrayList<>();
        docs.add(new DocInfoByte("{\"f0\":\"v0\",\"f1\":\"3\"}".getBytes(), "3".getBytes()));
        docs.add(new DocInfoByte("{\"f0\":\"v1\",\"f1\":\"3\"}".getBytes(), "2".getBytes()));
        docs.add(new DocInfoByte("{\"f0\":\"v1\",\"f1\":\"3\"}".getBytes(), "3".getBytes()));
        docs.add(new DocInfoByte("{\"f0\":\"v3\",\"f1\":\"4\"}".getBytes(), "4".getBytes()));
        docs.add(new DocInfoByte("{\"f0\":\"v3\",\"f1\":\"5\"}".getBytes(), "5".getBytes()));

        tairSearch.tftmadddoc("tftkey".getBytes(), docs);

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":1.658125,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":1.658125,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}}],\"max_score\":1.658125,\"total\":{\"relation\":\"eq\",\"value\":2}}}",
                tairSearch.tftsearch("tftkey", "{\"query\":{\"match\":{\"f1\":\"3\"}}}"));

        assertEquals("{\"_id\":\"3\",\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}}", tairSearch.tftgetdoc("tftkey", "3"));
        assertEquals("1", tairSearch.tftdeldoc("tftkey", "3"));
        assertEquals(null, tairSearch.tftgetdoc("tftkey", "3"));

        assertEquals("{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"},\"f1\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"}}}}}", tairSearch.tftgetindexmappings("tftkey"));
    }

    @Test
    public void tftgetsugtest() {
        getJedis().del("tftkey");
        Set<String> visited = new HashSet<>();
        Map<String, Integer> docs = new HashMap();
        docs.put("redis is a memory database", 1);
        docs.put("redis cluster", 2);
        docs.put("redis", 3);

        assertEquals(docs.size(), tairSearch.tftaddsug("tftkey", docs).intValue());

        assertEquals(docs.size(), tairSearch.tftsugnum("tftkey").intValue());

        List<String> result = tairSearch.tftgetsug("tftkey", "redis");

        assertEquals(3, result.size());
        for (int i = 0; i < result.size(); i++) {
            if (!docs.containsKey(result.get(i)) || visited.contains(result.get(i))) {
                assertTrue(false);
            }
            visited.add(result.get(i));
        }
        visited.clear();
        TFTGetSugParams params = new TFTGetSugParams();
        params.maxCount(2);
        params.fuzzy();
        List<String> maxCountResult = tairSearch.tftgetsug("tftkey", "redis", params);
        assertEquals(2, maxCountResult.size());
        Collections.sort(maxCountResult);
        for (int i = 0; i < maxCountResult.size(); i++) {
            if (!docs.containsKey(maxCountResult.get(i)) || visited.contains(maxCountResult.get(i))) {
                assertTrue(false);
            }
            visited.add(result.get(i));
        }
        visited.clear();
        List<String> allResult = tairSearch.tftgetallsugs("tftkey");
        assertEquals(docs.size(), allResult.size());
        for (int i = 0; i < docs.size(); i++) {
            if (!docs.containsKey(allResult.get(i)) || visited.contains(allResult.get(i))) {
                assertTrue(false);
            }
        }

        assertEquals(2, tairSearch.tftdelsug("tftkey", "redis cluster", "redis").intValue());
        assertEquals(docs.size() - 2, tairSearch.tftsugnum("tftkey").intValue());
    }

    @Test
    public void tftgetsugtestbyte() {
        getJedis().del("tftkey");

        Set<String> visited = new HashSet<>();
        Map<String, Integer> cmpDocs = new HashMap();
        cmpDocs.put("redis is a memory database", 1);
        cmpDocs.put("redis cluster", 2);
        cmpDocs.put("redis", 3);

        Map<byte[], Integer> docs = new HashMap();
        docs.put("redis is a memory database".getBytes(), 1);
        docs.put("redis cluster".getBytes(), 2);
        docs.put("redis".getBytes(), 3);

        assertEquals(docs.size(), tairSearch.tftaddsug("tftkey".getBytes(), docs).intValue());

        assertEquals(docs.size(), tairSearch.tftsugnum("tftkey".getBytes()).intValue());

        List<byte[]> result = tairSearch.tftgetsug("tftkey".getBytes(), "redis".getBytes());

        assertEquals(3, result.size());
        for (int i = 0; i < result.size(); i++) {
            String tmpString = new String(result.get(i));
            if (!cmpDocs.containsKey(tmpString) || visited.contains(tmpString)) {
                assertTrue(false);
            }
            visited.add(tmpString);
        }
        visited.clear();

        TFTGetSugParams params = new TFTGetSugParams();
        params.maxCount(2);
        params.fuzzy();
        List<byte[]> maxCountResult = tairSearch.tftgetsug("tftkey".getBytes(), "redis".getBytes(), params);
        assertEquals(2, maxCountResult.size());
        for (int i = 0; i < maxCountResult.size(); i++) {
            String tmpString = new String(maxCountResult.get(i));
            if (!cmpDocs.containsKey(tmpString) || visited.contains(tmpString)) {
                assertTrue(false);
            }
            visited.add(tmpString);
        }
        visited.clear();
        List<byte[]> allResult = tairSearch.tftgetallsugs("tftkey".getBytes());
        assertEquals(docs.size(), allResult.size());
        for (int i = 0; i < docs.size(); i++) {
            String tmpString = new String(allResult.get(i));
            if (!cmpDocs.containsKey(tmpString) || visited.contains(tmpString)) {
                assertTrue(false);
            }
        }

        assertEquals(2, tairSearch.tftdelsug("tftkey".getBytes(), "redis cluster".getBytes(), "redis".getBytes()).intValue());
        assertEquals(docs.size() - 2, tairSearch.tftsugnum("tftkey".getBytes()).intValue());
        getJedis().del("tftkey".getBytes());
    }

    @Test
    public void tfttermquerybuildertest(){
        getJedis().del("tftkey");
        String ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        tairSearch.tftadddoc("tftkey", "{\"f0\":\"redis is a nosql database\"}", "1");
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.054363,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.054363,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
            tairSearch.tftsearch("tftkey", "{\"query\":{\"term\":{\"f0\":\"redis\"}}}"));

        TermQueryBuilder qb = QueryBuilders.termQuery("f0","redis").boost(2.0F);
        assertEquals("f0", qb.fieldName());
        assertEquals("redis", qb.value());
        assertEquals(2.0, qb.boost(),0.01);
        SearchSourceBuilder ssb = new SearchSourceBuilder().query(qb);
        SearchResponse result = tairSearch.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"term\":{\"f0\":{\"boost\":2.0,\"value\":\"redis\"}}}}",
            ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.108725,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.108725,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
            result.toString());
        assertEquals("{\"relation\":\"eq\",\"value\":1}", result.getHits().getTotalHits().toString());
        assertEquals("{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.108725,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.108725,\"total\":{\"relation\":\"eq\",\"value\":1}}", result.getHits().toString());
        assertEquals("[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.108725,\"_source\":{\"f0\":\"redis is a nosql database\"}}]",Arrays.toString(result.getHits().getHits()));
        assertEquals("{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.108725,\"_source\":{\"f0\":\"redis is a nosql database\"}}", result.getHits().getAt(0).toString());
        assertEquals(1,result.getHits().getTotalHits().value);
        assertEquals(TotalHits.Relation.EQUAL_TO,result.getHits().getTotalHits().relation);
        assertEquals(0.108725,result.getHits().getMaxScore(),0.01);
        assertEquals(0.108725,result.getHits().getAt(0).getScore(),0.01);
        assertEquals("1",result.getHits().getAt(0).getId());
        assertEquals("tftkey",result.getHits().getAt(0).getIndex());
        assertEquals("{\"f0\":\"redis is a nosql database\"}",result.getHits().getAt(0).getSourceAsString());
        Map<String,Object> tmp = result.getHits().getAt(0).getSourceAsMap();
        assertEquals("redis is a nosql database",tmp.get("f0"));

        getJedis().del("tftkey");
        ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\",\"analyzer\":\"whitespace\"}}}}");
        assertEquals(ret, "OK");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"Redis is a nosql database\"}", "1");
        qb = QueryBuilders.termQuery("f0","Redis").lowercase(false);
        assertEquals(false, qb.lowercase());
        ssb = new SearchSourceBuilder().query(qb);
        result = tairSearch.tftsearch("tftkey", ssb);
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.042109,\"_source\":{\"f0\":\"Redis is a nosql database\"}}],\"max_score\":0.042109,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                result.toString());
    }

    @Test
    public void tftsourceasmaptest(){
        getJedis().del("tftkey");
        String ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"properties\":{\"f1\":{\"type\":\"text\"}}},\"f2\":{\"type\":\"long\"},\"f3\":{\"type\":\"double\"},\"f4\":{\"type\":\"integer\"}}}}");
        assertEquals(ret, "OK");

        tairSearch.tftadddoc("tftkey", "{\"f0\":{\"f1\":\"redis is a nosql database\"},\"f2\":1,\"f3\":1.0,\"f4\":10}", "1");
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.054363,\"_source\":{\"f0\":{\"f1\":\"redis is a nosql database\"},\"f2\":1,\"f3\":1.0,\"f4\":10}}],\"max_score\":0.054363,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                tairSearch.tftsearch("tftkey", "{\"query\":{\"term\":{\"f0.f1\":\"redis\"}}}"));

        TermQueryBuilder qb = QueryBuilders.termQuery("f0.f1","redis").boost(2.0F);
        assertEquals("f0.f1", qb.fieldName());
        assertEquals("redis", qb.value());
        assertEquals(2.0, qb.boost(),0.01);
        SearchSourceBuilder ssb = new SearchSourceBuilder().query(qb);
        SearchResponse result = tairSearch.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"term\":{\"f0.f1\":{\"boost\":2.0,\"value\":\"redis\"}}}}",
                ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.108725,\"_source\":{\"f0\":{\"f1\":\"redis is a nosql database\"},\"f2\":1,\"f3\":1.0,\"f4\":10}}],\"max_score\":0.108725,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                result.toString());
        assertEquals(1,result.getHits().getTotalHits().value);
        assertEquals(TotalHits.Relation.EQUAL_TO,result.getHits().getTotalHits().relation);
        assertEquals(0.108725,result.getHits().getMaxScore(),0.01);
        assertEquals(0.108725,result.getHits().getAt(0).getScore(),0.01);
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

        getJedis().del("tftkey");
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

        tairSearch.tftadddoc("tftkey", "{\"f0\":{\"f1\":100},\"f2\":[10,\"abc\",20]}", "10");
        TermQueryBuilder qb2 = QueryBuilders.termQuery("f0.f1",100).boost(2.0F);
        assertEquals("f0.f1", qb2.fieldName());
        assertEquals(100, qb2.value());
        assertEquals(2.0, qb2.boost(),0.01);
        SearchSourceBuilder ssb2 = new SearchSourceBuilder().query(qb2);
        SearchResponse result2 = tairSearch.tftsearch("tftkey", ssb2);
        Map<String,Object> tmp2 = result2.getHits().getAt(0).getSourceAsMap();
        Map<String, Object> f0Map2 = (Map<String, Object>)(tmp2.get("f0"));
        f0F1 = ((Number)f0Map2.get("f1")).longValue();
        assertEquals(100, f0F1);
        ArrayList<Object> list = (ArrayList<Object>)(tmp2.get("f2"));
        assertEquals(10, ((Number)list.get(0)).longValue());
        assertEquals("abc", list.get(1));
        assertEquals(20, ((Number)list.get(2)).longValue());
        assertEquals(3, list.size());
    }


    @Test
    public void tfttermsquerybuildertest(){
        getJedis().del("tftkey");
        String ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        tairSearch.tftadddoc("tftkey", "{\"f0\":\"redis is a nosql database\"}", "1");
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.054363,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.054363,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
            tairSearch.tftsearch("tftkey", "{\"query\":{\"term\":{\"f0\":\"redis\"}}}"));

        List<String> values = new ArrayList<>();
        values.add("redis");
        values.add("database");

        TermsQueryBuilder qb = QueryBuilders.termsQuery("f0","redis", "database").boost(2.0F);
        assertEquals("f0", qb.fieldName());
        assertEquals(values, qb.values());
        SearchSourceBuilder ssb = new SearchSourceBuilder().query(qb);
        SearchResponse result = tairSearch.tftsearch("tftkey", ssb);

        assertEquals("{\"query\":{\"terms\":{\"f0\":[\"redis\",\"database\"],\"boost\":2.0}}}",
            ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.21745,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.21745,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
            result.toString());

        qb = QueryBuilders.termsQuery("f0",values).boost(2.0F);
        assertEquals("f0", qb.fieldName());
        assertEquals(values, qb.values());
        ssb = new SearchSourceBuilder().query(qb);
        assertEquals("{\"query\":{\"terms\":{\"f0\":[\"redis\",\"database\"],\"boost\":2.0}}}",
                ssb.toString());
        result = tairSearch.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"terms\":{\"f0\":[\"redis\",\"database\"],\"boost\":2.0}}}",
            ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.21745,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.21745,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
            result.toString());

        getJedis().del("tftkey");
        ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\",\"analyzer\":\"whitespace\"}}}}");
        assertEquals(ret, "OK");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"Redis is a nosql database\"}", "1");
        qb = QueryBuilders.termsQuery("f0","Redis", "apple").lowercase(false);
        assertEquals(false, qb.lowercase());
        ssb = new SearchSourceBuilder().query(qb);
        result = tairSearch.tftsearch("tftkey", ssb);
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.021055,\"_source\":{\"f0\":\"Redis is a nosql database\"}}],\"max_score\":0.021055,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                result.toString());
    }

    @Test
    public void tftwildcardquerybuildertest(){
        getJedis().del("tftkey");
        String ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        tairSearch.tftadddoc("tftkey", "{\"f0\":\"redis is a nosql database\"}", "1");
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.054363,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.054363,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
            tairSearch.tftsearch("tftkey", "{\"query\":{\"term\":{\"f0\":\"redis\"}}}"));

        WildcardQueryBuilder qb = QueryBuilders.wildcardQuery("f0","redis*").boost(2.0F);
        assertEquals("f0", qb.fieldName());
        assertEquals("redis*", qb.value());
        SearchSourceBuilder ssb = new SearchSourceBuilder().query(qb);
        SearchResponse result = tairSearch.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"wildcard\":{\"f0\":{\"boost\":2.0,\"value\":\"redis*\"}}}}",
            ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":2.0,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":2.0,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
            result.toString());

    }

    @Test
    public void tftprefixquerybuildertest(){
        getJedis().del("tftkey");
        String ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        tairSearch.tftadddoc("tftkey", "{\"f0\":\"redis is a nosql database\"}", "1");
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.054363,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.054363,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
            tairSearch.tftsearch("tftkey", "{\"query\":{\"term\":{\"f0\":\"redis\"}}}"));

        PrefixQueryBuilder qb = QueryBuilders.prefixQuery("f0","redi");
        assertEquals("f0", qb.fieldName());
        assertEquals("redi", qb.value());
        SearchSourceBuilder ssb = new SearchSourceBuilder().query(qb);
        SearchResponse result = tairSearch.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"prefix\":{\"f0\":{\"value\":\"redi\"}}}}",
            ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":1.0,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":1.0,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
            result.toString());


    }

    @Test
    public void tftmatchallquerybuildertest(){
        getJedis().del("tftkey");
        String ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        tairSearch.tftadddoc("tftkey", "{\"f0\":\"redis is a nosql database\"}", "1");
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.054363,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.054363,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
            tairSearch.tftsearch("tftkey", "{\"query\":{\"term\":{\"f0\":\"redis\"}}}"));

        MatchAllQueryBuilder qb = QueryBuilders.matchAllQuery().boost(2.0F);
        SearchSourceBuilder ssb = new SearchSourceBuilder().query(qb);
        SearchResponse result = tairSearch.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"match_all\":{\"boost\":2.0}}}",
            ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":2.0,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":2.0,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
            result.toString());
    }

    @Test
    public void tftrangequerybuildertest(){
        getJedis().del("tftkey");
        String ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        tairSearch.tftadddoc("tftkey", "{\"f0\":\"redis is a nosql database\"}", "1");
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.054363,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.054363,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
            tairSearch.tftsearch("tftkey", "{\"query\":{\"term\":{\"f0\":\"redis\"}}}"));

        RangeQueryBuilder qb = QueryBuilders.rangeQuery("f0").gt("ra").lt("rf").boost(2.0F);
        assertEquals("ra", qb.from());
        assertEquals(false, qb.includeLower());
        assertEquals(false, qb.includeUpper());
        assertEquals("rf", qb.to());
        SearchSourceBuilder ssb = new SearchSourceBuilder().query(qb);
        SearchResponse result = tairSearch.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"range\":{\"f0\":{\"gt\":\"ra\",\"lt\":\"rf\",\"boost\":2.0}}}}",
            ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":2.0,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":2.0,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
            result.toString());

        qb = QueryBuilders.rangeQuery("f0").from("ra",false).to("rf",false).boost(2.0F);
        ssb = new SearchSourceBuilder().query(qb);
        result = tairSearch.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"range\":{\"f0\":{\"gt\":\"ra\",\"lt\":\"rf\",\"boost\":2.0}}}}",
                ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":2.0,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":2.0,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                result.toString());

        qb = QueryBuilders.rangeQuery("f0").gte("ra").lte("rf").boost(2.0F);
        assertEquals("ra", qb.from());
        assertTrue(qb.includeLower());
        assertTrue( qb.includeUpper());
        assertEquals("rf", qb.to());
        ssb = new SearchSourceBuilder().query(qb);
        result = tairSearch.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"range\":{\"f0\":{\"gte\":\"ra\",\"lte\":\"rf\",\"boost\":2.0}}}}",
            ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":2.0,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":2.0,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
            result.toString());

        qb = QueryBuilders.rangeQuery("f0").gte("ra").boost(2.0F);
        ssb = new SearchSourceBuilder().query(qb);
        result = tairSearch.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"range\":{\"f0\":{\"gte\":\"ra\",\"boost\":2.0}}}}",
            ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":2.0,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":2.0,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
            result.toString());

        qb = QueryBuilders.rangeQuery("f0").from("ra").to("rf").boost(2.0F);
        ssb = new SearchSourceBuilder().query(qb);
        result = tairSearch.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"range\":{\"f0\":{\"gte\":\"ra\",\"lte\":\"rf\",\"boost\":2.0}}}}",
                ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":2.0,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":2.0,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                result.toString());

    }

    @Test
    public void tftmatchquerybuildertest(){
        getJedis().del("tftkey");
        String ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        tairSearch.tftadddoc("tftkey", "{\"f0\":\"redis is a nosql database\"}", "1");

        MatchQueryBuilder qb = QueryBuilders.matchQuery("f0","nosql").boost(2.0F).prefixLength(1).fuzziness(1);
        assertEquals(1, qb.prefixLength());
        assertEquals(1, qb.fuzziness());
        SearchSourceBuilder ssb = new SearchSourceBuilder().query(qb);
        SearchResponse result = tairSearch.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"match\":{\"f0\":{\"query\":\"nosql\",\"fuzziness\":1,\"prefix_length\":1}}}}",
            ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.054363,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.054363,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
            result.toString());

        qb = QueryBuilders.matchQuery("f0","redis nosql").boost(2.0F).analyzer("standard").minimumShouldMatch(1);
        ssb = new SearchSourceBuilder().query(qb);
        assertEquals("standard", qb.analyzer());
        assertEquals(1, qb.minimumShouldMatch());
        result = tairSearch.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"match\":{\"f0\":{\"query\":\"redis nosql\",\"analyzer\":\"standard\",\"minimum_should_match\":1}}}}",
            ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.108725,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.108725,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
            result.toString());
        Operator and = Operator.fromString("and");
        qb = QueryBuilders.matchQuery("f0","redis nosql").boost(2.0F).analyzer("standard").operator(and);
        assertEquals("standard", qb.analyzer());
        assertEquals(and, qb.operator());
        ssb = new SearchSourceBuilder().query(qb);
        result = tairSearch.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"match\":{\"f0\":{\"query\":\"redis nosql\",\"analyzer\":\"standard\",\"operator\":\"and\"}}}}",
            ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.108725,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.108725,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
            result.toString());

    }

    @Test
    public void tftconstantscorequerybuildertest(){
        getJedis().del("tftkey");
        String ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        tairSearch.tftadddoc("tftkey", "{\"f0\":\"redis is a nosql database\"}", "1");
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.054363,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.054363,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
            tairSearch.tftsearch("tftkey", "{\"query\":{\"term\":{\"f0\":\"redis\"}}}"));

        TermQueryBuilder termqb = QueryBuilders.termQuery("f0","redis").boost(2.0F);
        ConstantScoreQueryBuilder qb = QueryBuilders.constantScoreQuery(termqb).boost(0.153426F);
        assertEquals(termqb, qb.innerQuery());
        SearchSourceBuilder ssb = new SearchSourceBuilder().query(qb);
        SearchResponse result = tairSearch.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"constant_score\":{\"boost\":0.153426,\"filter\":{\"term\":{\"f0\":{\"boost\":2.0,\"value\":\"redis\"}}}}}}",
            ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.153426,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.153426,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
            result.toString());
    }

    @Test
    public void tftdismaxquerybuildertest(){
        getJedis().del("tftkey");
        String ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        tairSearch.tftadddoc("tftkey", "{\"f0\":\"redis is a nosql database\"}", "1");
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.054363,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.054363,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
            tairSearch.tftsearch("tftkey", "{\"query\":{\"term\":{\"f0\":\"redis\"}}}"));

        TermQueryBuilder termqb = QueryBuilders.termQuery("f0","redis");
        DisMaxQueryBuilder qb = QueryBuilders.disMaxQuery();
        qb.add(termqb);
        qb.tieBreaker(0.5F);
        assertEquals(0.5, qb.tieBreaker(), 0.01);
        assertTrue( qb.innerQueries().contains(termqb));
        SearchSourceBuilder ssb = new SearchSourceBuilder().query(qb);
        SearchResponse result = tairSearch.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"dis_max\":{\"tie_breaker\":0.5,\"queries\":[{\"term\":{\"f0\":{\"boost\":1.0,"
                + "\"value\":\"redis\"}}}]}}}",
            ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.054363,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.054363,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
            result.toString());
    }

    @Test
    public void tftboolquerybuildertest(){
        getJedis().del("tftkey");
        String ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        tairSearch.tftadddoc("tftkey", "{\"f0\":\"redis is a nosql database\"}", "1");
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.054363,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.054363,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
            tairSearch.tftsearch("tftkey", "{\"query\":{\"term\":{\"f0\":\"redis\"}}}"));
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"redis is a nosql kvstore\"}", "2");

        TermQueryBuilder termqb1 = QueryBuilders.termQuery("f0","redis");
        TermQueryBuilder termqb2 = QueryBuilders.termQuery("f0","kvstore");
        TermQueryBuilder termqb3 = QueryBuilders.termQuery("f0","nosql");
        BoolQueryBuilder qb = QueryBuilders.boolQuery();
        qb.must(termqb1);
        qb.mustNot(termqb2);
        qb.should(termqb3);
        qb.minimumShouldMatch(1);
        assertEquals(1, qb.minimumShouldMatch());
        assertTrue(qb.must().contains(termqb1));
        assertTrue(qb.mustNot().contains(termqb2));
        assertTrue(qb.should().contains(termqb3));
        SearchSourceBuilder ssb = new SearchSourceBuilder().query(qb);
        SearchResponse result = tairSearch.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"bool\":{\"must\":[{\"term\":{\"f0\":{\"boost\":1.0,\"value\":\"redis\"}}}],\"must_not\":[{\"term\":{\"f0\":{\"boost\":1.0,\"value\":\"kvstore\"}}}],\"should\":[{\"term\":{\"f0\":{\"boost\":1.0,\"value\":\"nosql\"}}}],\"minimum_should_match\":1}}}",
            ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.408154,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.408154,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
            result.toString());

        qb = QueryBuilders.boolQuery();
        qb.must(termqb1);
        qb.should(termqb3);
        qb.minimumShouldMatch(1);
        ssb = new SearchSourceBuilder().query(qb);
        result = tairSearch.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"bool\":{\"must\":[{\"term\":{\"f0\":{\"boost\":1.0,\"value\":\"redis\"}}}],\"should\":[{\"term\":{\"f0\":{\"boost\":1.0,\"value\":\"nosql\"}}}],\"minimum_should_match\":1}}}",
                ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.408154,\"_source\":{\"f0\":\"redis is a nosql database\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":0.408154,\"_source\":{\"f0\":\"redis is a nosql kvstore\"}}],\"max_score\":0.408154,\"total\":{\"relation\":\"eq\",\"value\":2}}}",
                result.toString());
    }

    @Test
    public void tftsearchsourcebuildertest(){
        getJedis().del("tftkey");
        String ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        tairSearch.tftadddoc("tftkey", "{\"f0\":\"redis is a nosql database\"}", "1");
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.054363,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.054363,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                tairSearch.tftsearch("tftkey", "{\"query\":{\"term\":{\"f0\":\"redis\"}}}"));
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"redis is an in-memory database that persists on disk\"}", "2");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"redis supports many different kind of values\"}", "3");

        TermQueryBuilder qb = QueryBuilders.termQuery("f0","redis");
        SearchSourceBuilder ssb = new SearchSourceBuilder().query(qb).sort("_score");
        assertEquals(qb.toString(),"{\"term\":{\"f0\":{\"boost\":1.0,\"value\":\"redis\"}}}");
        assertEquals(qb, ssb.query());
        assertEquals(SortOrder.DESC, ssb.sorts().get(0).order());
        assertEquals("{\"_score\":{\"order\":\"desc\"}}", ssb.sorts().get(0).toString());
        assertEquals("[{\"_score\":{\"order\":\"desc\"}}]", ssb.sorts().toString());
        SearchResponse result = tairSearch.tftsearch("tftkey", ssb);
        assertEquals("{\"query\":{\"term\":{\"f0\":{\"boost\":1.0,\"value\":\"redis\"}}},\"sort\":[{\"_score\":{\"order\":\"desc\"}}]}",
                ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.292946,\"_source\":{\"f0\":\"redis is a nosql database\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":0.226915,\"_source\":{\"f0\":\"redis is an in-memory database that persists on disk\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":0.207144,\"_source\":{\"f0\":\"redis supports many different kind of values\"}}],\"max_score\":0.292946,\"total\":{\"relation\":\"eq\",\"value\":3}}}",
                result.toString());


        ssb = new SearchSourceBuilder().query(qb).from(1).size(1).sort("_doc");
        assertEquals(1, ssb.from());
        assertEquals(1, ssb.size());
        assertEquals(SortOrder.ASC, ssb.sorts().get(0).order());
        result = tairSearch.tftsearch("tftkey", ssb);
        assertEquals("{\"size\":1,\"from\":1,\"query\":{\"term\":{\"f0\":{\"boost\":1.0,\"value\":\"redis\"}}},\"sort\":[{\"_doc\":{\"order\":\"asc\"}}]}",
                ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":1.0,\"_source\":{\"f0\":\"redis is an in-memory database that persists on disk\"}}],\"max_score\":1.0,\"total\":{\"relation\":\"eq\",\"value\":3}}}",
                result.toString());

        ssb = new SearchSourceBuilder().query(qb).trackTotalHits(true).fetchSource("f0",null);
        assertTrue(ssb.trackTotalHits());
        assertEquals("f0", ssb.fetchSource().includes()[0]);
        assertEquals("{\"includes\":[\"f0\"],\"excludes\":[]}", ssb.fetchSource().toString());
        result = tairSearch.tftsearch("tftkey", ssb);
        assertEquals("{\"track_total_hits\":true,\"query\":{\"term\":{\"f0\":{\"boost\":1.0,\"value\":\"redis\"}}},\"_source\":{\"includes\":[\"f0\"],\"excludes\":[]}}",
                ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.292946,\"_source\":{\"f0\":\"redis is a nosql database\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":0.226915,\"_source\":{\"f0\":\"redis is an in-memory database that persists on disk\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":0.207144,\"_source\":{\"f0\":\"redis supports many different kind of values\"}}],\"max_score\":0.292946,\"total\":{\"relation\":\"eq\",\"value\":3}}}",
                result.toString());

        ssb = new SearchSourceBuilder().query(qb).trackTotalHits(true).fetchSource(null, "f0");
        assertEquals("f0", ssb.fetchSource().excludes()[0]);
        result = tairSearch.tftsearch("tftkey", ssb);
        assertEquals("{\"track_total_hits\":true,\"query\":{\"term\":{\"f0\":{\"boost\":1.0,\"value\":\"redis\"}}},\"_source\":{\"includes\":[],\"excludes\":[\"f0\"]}}",
                ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.292946,\"_source\":{}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":0.226915,\"_source\":{}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":0.207144,\"_source\":{}}],\"max_score\":0.292946,\"total\":{\"relation\":\"eq\",\"value\":3}}}",
                result.toString());
    }

    @Test
    public void tftmsearchsourcebuildertest(){
        getJedis().del("key0");
        getJedis().del("key1");
        getJedis().del("key2");
        String ret = tairSearch.tftcreateindex("key0", "{\"mappings\":{\"properties\":{\"describe\":{\"type\":\"text\"}, \"name\":{\"type\":\"keyword\"}, \"id\":{\"type\":\"long\"}, \"price\":{\"type\":\"integer\"}, \"sale\":{\"type\":\"double\"}}}}");
        assertEquals(ret, "OK");
        ret = tairSearch.tftcreateindex("key1", "{\"mappings\":{\"properties\":{\"describe\":{\"type\":\"text\"}, \"name\":{\"type\":\"keyword\"}, \"id\":{\"type\":\"long\"}, \"price\":{\"type\":\"integer\"}, \"sale\":{\"type\":\"double\"}}}}");
        assertEquals(ret, "OK");
        ret = tairSearch.tftcreateindex("key2", "{\"mappings\":{\"properties\":{\"describe\":{\"type\":\"text\"}, \"name\":{\"type\":\"keyword\"}, \"id\":{\"type\":\"long\"}, \"price\":{\"type\":\"integer\"}, \"sale\":{\"type\":\"double\"}}}}");
        assertEquals(ret, "OK");

        tairSearch.tftadddoc("key0", "{\"describe\":\"fruit apple red canada\",  \"name\":\"apple\", \"id\":1234567, \"price\":50, \"sale\":0.5}", "1");
        tairSearch.tftadddoc("key0", "{\"describe\":\"fruit banana yellow china\",  \"name\":\"banana\", \"id\":12345678, \"price\":60, \"sale\":0.5}", "2");
        tairSearch.tftadddoc("key0", "{\"describe\":\"fruit banana yellow america\",  \"name\":\"banana\", \"id\":1234568, \"price\":60, \"sale\":0.8}", "3");
        tairSearch.tftadddoc("key0", "{\"describe\":\"fruit apple red england\",  \"name\":\"apple\", \"id\":1234567, \"price\":100, \"sale\":0.3}", "3");

        tairSearch.tftadddoc("key1", "{\"describe\":\"fruit apple red canada\",  \"name\":\"apple\", \"id\":1234567, \"price\":50, \"sale\":0.5}", "1");
        tairSearch.tftadddoc("key1", "{\"describe\":\"fruit banana yellow china\",  \"name\":\"banana\", \"id\":12345678, \"price\":60, \"sale\":0.5}", "2");
        tairSearch.tftadddoc("key1", "{\"describe\":\"fruit banana yellow america\",  \"name\":\"banana\", \"id\":1234568, \"price\":60, \"sale\":0.8}", "3");
        tairSearch.tftadddoc("key1", "{\"describe\":\"fruit apple red england\",  \"name\":\"apple\", \"id\":1234567, \"price\":100, \"sale\":0.3}", "3");

        tairSearch.tftadddoc("key2", "{\"describe\":\"fruit apple red canada\",  \"name\":\"apple\", \"id\":1234567, \"price\":50, \"sale\":0.5}", "1");
        tairSearch.tftadddoc("key2", "{\"describe\":\"fruit banana yellow china\",  \"name\":\"banana\", \"id\":12345678, \"price\":60, \"sale\":0.5}", "2");
        tairSearch.tftadddoc("key2", "{\"describe\":\"fruit banana yellow america\",  \"name\":\"banana\", \"id\":1234568, \"price\":60, \"sale\":0.8}", "3");
        tairSearch.tftadddoc("key2", "{\"describe\":\"fruit apple red england\",  \"name\":\"apple\", \"id\":1234567, \"price\":100, \"sale\":0.3}", "3");

        TermQueryBuilder qb = QueryBuilders.termQuery("describe","apple");
        KeyCursors cursors = new KeyCursors().add("key0", 2).add("key1", 1).add("key2", 0);
        MSearchSourceBuilder ssb = new MSearchSourceBuilder().query(qb).size(2).replyWithKeysCursor(true).keysCursors(cursors);
        MSearchResponse result = tairSearch.tftmsearch(ssb, "key0", "key1", "key2");
        assertEquals("{\"size\":2,\"query\":{\"term\":{\"describe\":{\"boost\":1.0,\"value\":\"apple\"}}},\"reply_with_keys_cursor\":true,\"keys_cursor\":{\"key1\":1,\"key2\":0,\"key0\":2}}",
                ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"3\",\"_index\":\"key1\",\"_score\":0.5,\"_source\":{\"describe\":\"fruit apple red england\",\"name\":\"apple\",\"id\":1234567,\"price\":100,\"sale\":0.3}},{\"_id\":\"1\",\"_index\":\"key2\",\"_score\":0.5,\"_source\":{\"describe\":\"fruit apple red canada\",\"name\":\"apple\",\"id\":1234567,\"price\":50,\"sale\":0.5}}],\"max_score\":0.5,\"total\":{\"relation\":\"eq\",\"value\":6}},\"aux_info\":{\"index_crc64\":15096806844241479487,\"keys_cursor\":{\"key0\":2,\"key1\":2,\"key2\":1}}}",
                result.toString());
        assertEquals(6,result.getHits().getTotalHits().value);
        assertEquals(TotalHits.Relation.EQUAL_TO,result.getHits().getTotalHits().relation);
        assertEquals(0.5,result.getHits().getMaxScore(),0.01);
        assertEquals(0.5,result.getHits().getAt(0).getScore(),0.01);
        assertEquals("3",result.getHits().getAt(0).getId());
        assertEquals("key1",result.getHits().getAt(0).getIndex());
        assertEquals("{\"describe\":\"fruit apple red england\",\"name\":\"apple\",\"id\":1234567,\"price\":100,\"sale\":0.3}",result.getHits().getAt(0).getSourceAsString());
        Map<String,Object> tmp = result.getHits().getAt(0).getSourceAsMap();
        assertEquals(100, ((Number)tmp.get("price")).longValue());
        AuxInfo auxInfo = result.getAuxInfo();
        assertEquals(2,auxInfo.getKeysCursorsAsMap().get("key0").intValue());
        assertEquals(2,auxInfo.getKeysCursorsAsMap().get("key1").intValue());
        assertEquals(1,auxInfo.getKeysCursorsAsMap().get("key2").intValue());
        assertEquals("15096806844241479487",Long.toUnsignedString(auxInfo.getCrc64()));


        ssb = new MSearchSourceBuilder().query(qb).size(1).sort("_doc");
        assertEquals(1, ssb.size());
        assertEquals(SortOrder.ASC, ssb.sorts().get(0).order());
        result = tairSearch.tftmsearch(ssb, "key0", "key1", "key2");
        assertEquals("{\"size\":1,\"query\":{\"term\":{\"describe\":{\"boost\":1.0,\"value\":\"apple\"}}},\"sort\":[{\"_doc\":{\"order\":\"asc\"}}]}",
                ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"key0\",\"_score\":1.0,\"_source\":{\"describe\":\"fruit apple red canada\",\"name\":\"apple\",\"id\":1234567,\"price\":50,\"sale\":0.5}}],\"max_score\":1.0,\"total\":{\"relation\":\"eq\",\"value\":6}},\"aux_info\":{\"index_crc64\":15096806844241479487}}",
                result.toString());

        ssb = new MSearchSourceBuilder().query(qb).trackTotalHits(true).fetchSource("describe",null);
        assertTrue(ssb.trackTotalHits());
        assertEquals("describe", ssb.fetchSource().includes()[0]);
        assertEquals("{\"includes\":[\"describe\"],\"excludes\":[]}", ssb.fetchSource().toString());
        result = tairSearch.tftmsearch(ssb, "key0", "key1", "key2");
        assertEquals("{\"track_total_hits\":true,\"query\":{\"term\":{\"describe\":{\"boost\":1.0,\"value\":\"apple\"}}},\"_source\":{\"includes\":[\"describe\"],\"excludes\":[]}}",
                ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"key0\",\"_score\":0.5,\"_source\":{\"describe\":\"fruit apple red canada\"}},{\"_id\":\"3\",\"_index\":\"key0\",\"_score\":0.5,\"_source\":{\"describe\":\"fruit apple red england\"}},{\"_id\":\"1\",\"_index\":\"key1\",\"_score\":0.5,\"_source\":{\"describe\":\"fruit apple red canada\"}},{\"_id\":\"3\",\"_index\":\"key1\",\"_score\":0.5,\"_source\":{\"describe\":\"fruit apple red england\"}},{\"_id\":\"1\",\"_index\":\"key2\",\"_score\":0.5,\"_source\":{\"describe\":\"fruit apple red canada\"}},{\"_id\":\"3\",\"_index\":\"key2\",\"_score\":0.5,\"_source\":{\"describe\":\"fruit apple red england\"}}],\"max_score\":0.5,\"total\":{\"relation\":\"eq\",\"value\":6}},\"aux_info\":{\"index_crc64\":15096806844241479487}}",
                result.toString());

        ssb = new MSearchSourceBuilder().query(qb).trackTotalHits(true).fetchSource(null, "describe");
        assertEquals("describe", ssb.fetchSource().excludes()[0]);
        result = tairSearch.tftmsearch(ssb, "key0", "key1", "key2");
        assertEquals("{\"track_total_hits\":true,\"query\":{\"term\":{\"describe\":{\"boost\":1.0,\"value\":\"apple\"}}},\"_source\":{\"includes\":[],\"excludes\":[\"describe\"]}}",
                ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"key0\",\"_score\":0.5,\"_source\":{\"name\":\"apple\",\"id\":1234567,\"price\":50,\"sale\":0.5}},{\"_id\":\"3\",\"_index\":\"key0\",\"_score\":0.5,\"_source\":{\"name\":\"apple\",\"id\":1234567,\"price\":100,\"sale\":0.3}},{\"_id\":\"1\",\"_index\":\"key1\",\"_score\":0.5,\"_source\":{\"name\":\"apple\",\"id\":1234567,\"price\":50,\"sale\":0.5}},{\"_id\":\"3\",\"_index\":\"key1\",\"_score\":0.5,\"_source\":{\"name\":\"apple\",\"id\":1234567,\"price\":100,\"sale\":0.3}},{\"_id\":\"1\",\"_index\":\"key2\",\"_score\":0.5,\"_source\":{\"name\":\"apple\",\"id\":1234567,\"price\":50,\"sale\":0.5}},{\"_id\":\"3\",\"_index\":\"key2\",\"_score\":0.5,\"_source\":{\"name\":\"apple\",\"id\":1234567,\"price\":100,\"sale\":0.3}}],\"max_score\":0.5,\"total\":{\"relation\":\"eq\",\"value\":6}},\"aux_info\":{\"index_crc64\":15096806844241479487}}",
                result.toString());
    }

    @Test
    public void tftsumaggsbuildertest(){
        getJedis().del("tftkey");
        String ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"properties\":{\"shares_name\":{\"type"
            + "\":\"keyword\"}, \"logictime\":{\"type\":\"long\"}, \"purchase_type\":{\"type\":\"integer\"}, "
            + "\"purchase_price\":{\"type\":\"double\"}, \"purchase_count\":{\"type\":\"long\"}, "
            + "\"investor\":{\"type\":\"keyword\"}}}}");
        assertEquals(ret, "OK");

        tairSearch.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300210, \"purchase_type\":1, "
            + "\"purchase_price\":101.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearch.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300310, \"purchase_type\":1, "
            + "\"purchase_price\":111.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearch.tftadddoc("tftkey", "{\"shares_name\":\"YBY\", \"logictime\":14300410, \"purchase_type\":1, "
            + "\"purchase_price\":11.1, \"purchase_count\":100, \"investor\":\"Mila\"}");

        SumAggregationBuilder ab = AggregationBuilders.sum("price_sum").field("purchase_price");
        assertEquals("purchase_price", ab.field());
        assertEquals("sum", ab.getType());
        assertEquals("price_sum", ab.getName());
        TermQueryBuilder qb = QueryBuilders.termQuery("investor","Jay");
        SearchSourceBuilder ssb = new SearchSourceBuilder().size(0).query(qb).aggregation(ab);
        SearchResponse sr = tairSearch.tftsearch("tftkey", ssb);
        Sum s = sr.getAggregations().get("price_sum");
        assertEquals("{\"price_sum\":{\"value\":212.2,\"type\":\"sum\"}}", s.toString());
        assertEquals(212.2, s.value(),0.001);
        assertEquals("sum", s.getType());
        assertEquals("price_sum", ab.getName());
        assertEquals("{\"size\":0,\"query\":{\"term\":{\"investor\":{\"boost\":1.0,\"value\":\"Jay\"}}},"
                + "\"aggs\":{\"price_sum\":{\"sum\":{\"field\":\"purchase_price\"}}}}",
            ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[],\"max_score\":null,\"total\":{\"relation\":\"eq\",\"value\":2}},\"aggregations\":{\"price_sum\":{\"value\":212.2,\"type\":\"sum\"}}}",
            sr.toString());
    }

    @Test
    public void tftmaxaggsbuildertest(){
        getJedis().del("tftkey");
        String ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"properties\":{\"shares_name\":{\"type"
            + "\":\"keyword\"}, \"logictime\":{\"type\":\"long\"}, \"purchase_type\":{\"type\":\"integer\"}, "
            + "\"purchase_price\":{\"type\":\"double\"}, \"purchase_count\":{\"type\":\"long\"}, "
            + "\"investor\":{\"type\":\"keyword\"}}}}");
        assertEquals(ret, "OK");

        tairSearch.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300210, \"purchase_type\":1, "
            + "\"purchase_price\":101.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearch.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300310, \"purchase_type\":1, "
            + "\"purchase_price\":111.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearch.tftadddoc("tftkey", "{\"shares_name\":\"YBY\", \"logictime\":14300410, \"purchase_type\":1, "
            + "\"purchase_price\":11.1, \"purchase_count\":100, \"investor\":\"Mila\"}");

        MaxAggregationBuilder ab = AggregationBuilders.max("price_max").field("purchase_price");
        assertEquals("purchase_price", ab.field());
        assertEquals("max", ab.getType());
        assertEquals("price_max", ab.getName());
        TermQueryBuilder qb = QueryBuilders.termQuery("investor","Jay");
        SearchSourceBuilder ssb = new SearchSourceBuilder().size(0).query(qb).aggregation(ab);

        SearchResponse sr = tairSearch.tftsearch("tftkey", ssb);
        Max s = sr.getAggregations().get("price_max");
        assertEquals(111.1, s.value(),0.001);
        assertEquals("max", s.getType());
        assertEquals("price_max", s.getName());
        assertEquals("{\"size\":0,\"query\":{\"term\":{\"investor\":{\"boost\":1.0,\"value\":\"Jay\"}}},"
                + "\"aggs\":{\"price_max\":{\"max\":{\"field\":\"purchase_price\"}}}}",
            ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[],\"max_score\":null,\"total\":{\"relation\":\"eq\",\"value\":2}},\"aggregations\":{\"price_max\":{\"value\":111.1,\"type\":\"max\"}}}",
            sr.toString());
    }

    @Test
    public void tftavgaggsbuildertest(){
        getJedis().del("tftkey");
        String ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"properties\":{\"shares_name\":{\"type"
                + "\":\"keyword\"}, \"logictime\":{\"type\":\"long\"}, \"purchase_type\":{\"type\":\"integer\"}, "
                + "\"purchase_price\":{\"type\":\"double\"}, \"purchase_count\":{\"type\":\"long\"}, "
                + "\"investor\":{\"type\":\"keyword\"}}}}");
        assertEquals(ret, "OK");

        tairSearch.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300210, \"purchase_type\":1, "
                + "\"purchase_price\":101.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearch.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300310, \"purchase_type\":1, "
                + "\"purchase_price\":111.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearch.tftadddoc("tftkey", "{\"shares_name\":\"YBY\", \"logictime\":14300410, \"purchase_type\":1, "
                + "\"purchase_price\":11.1, \"purchase_count\":100, \"investor\":\"Mila\"}");

        AvgAggregationBuilder ab = AggregationBuilders.avg("price_avg").field("purchase_price");
        assertEquals("purchase_price", ab.field());
        assertEquals("avg", ab.getType());
        assertEquals("price_avg", ab.getName());
        TermQueryBuilder qb = QueryBuilders.termQuery("investor","Jay");
        SearchSourceBuilder ssb = new SearchSourceBuilder().size(0).query(qb).aggregation(ab);

        SearchResponse sr = tairSearch.tftsearch("tftkey", ssb);
        Avg s = sr.getAggregations().get("price_avg");
        assertEquals(106.1, s.value(),0.001);
        assertEquals("avg", s.getType());
        assertEquals("price_avg", s.getName());
        assertEquals("{\"size\":0,\"query\":{\"term\":{\"investor\":{\"boost\":1.0,\"value\":\"Jay\"}}},"
                        + "\"aggs\":{\"price_avg\":{\"avg\":{\"field\":\"purchase_price\"}}}}",
                ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[],\"max_score\":null,\"total\":{\"relation\":\"eq\",\"value\":2}},\"aggregations\":{\"price_avg\":{\"value\":106.1,\"type\":\"avg\"}}}",
                sr.toString());
    }

    @Test
    public void tftminaggsbuildertest(){
        getJedis().del("tftkey");
        String ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"properties\":{\"shares_name\":{\"type"
                + "\":\"keyword\"}, \"logictime\":{\"type\":\"long\"}, \"purchase_type\":{\"type\":\"integer\"}, "
                + "\"purchase_price\":{\"type\":\"double\"}, \"purchase_count\":{\"type\":\"long\"}, "
                + "\"investor\":{\"type\":\"keyword\"}}}}");
        assertEquals(ret, "OK");

        tairSearch.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300210, \"purchase_type\":1, "
                + "\"purchase_price\":101.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearch.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300310, \"purchase_type\":1, "
                + "\"purchase_price\":111.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearch.tftadddoc("tftkey", "{\"shares_name\":\"YBY\", \"logictime\":14300410, \"purchase_type\":1, "
                + "\"purchase_price\":11.1, \"purchase_count\":100, \"investor\":\"Mila\"}");

        MinAggregationBuilder ab = AggregationBuilders.min("price_min").field("purchase_price");
        assertEquals("purchase_price", ab.field());
        assertEquals("min", ab.getType());
        assertEquals("price_min", ab.getName());
        TermQueryBuilder qb = QueryBuilders.termQuery("investor","Jay");
        SearchSourceBuilder ssb = new SearchSourceBuilder().size(0).query(qb).aggregation(ab);

        SearchResponse sr = tairSearch.tftsearch("tftkey", ssb);
        InternalMin s = sr.getAggregations().get("price_min");
        assertEquals("min", s.getType());
        assertEquals("price_min", s.getName());
        assertEquals(101.1, s.value(),0.001);
        assertEquals("{\"size\":0,\"query\":{\"term\":{\"investor\":{\"boost\":1.0,\"value\":\"Jay\"}}},"
                        + "\"aggs\":{\"price_min\":{\"min\":{\"field\":\"purchase_price\"}}}}",
                ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[],\"max_score\":null,\"total\":{\"relation\":\"eq\",\"value\":2}},"
                        + "\"aggregations\":{\"price_min\":{\"value\":101.1,\"type\":\"min\"}}}",
                sr.toString());
    }

    @Test
    public void tftsumofsquaresaggsbuildertest(){
        getJedis().del("tftkey");
        String ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"properties\":{\"shares_name\":{\"type"
                + "\":\"keyword\"}, \"logictime\":{\"type\":\"long\"}, \"purchase_type\":{\"type\":\"integer\"}, "
                + "\"purchase_price\":{\"type\":\"double\"}, \"purchase_count\":{\"type\":\"long\"}, "
                + "\"investor\":{\"type\":\"keyword\"}}}}");
        assertEquals(ret, "OK");

        tairSearch.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300210, \"purchase_type\":1, "
                + "\"purchase_price\":101.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearch.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300310, \"purchase_type\":1, "
                + "\"purchase_price\":111.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearch.tftadddoc("tftkey", "{\"shares_name\":\"YBY\", \"logictime\":14300410, \"purchase_type\":1, "
                + "\"purchase_price\":11.1, \"purchase_count\":100, \"investor\":\"Mila\"}");

        SumOfSquaresAggregationBuilder ab = AggregationBuilders.sumOfSquares("price_sum_of_squares").field("purchase_price");
        assertEquals("purchase_price", ab.field());
        assertEquals("sum_of_squares", ab.getType());
        assertEquals("price_sum_of_squares", ab.getName());
        TermQueryBuilder qb = QueryBuilders.termQuery("investor","Jay");
        SearchSourceBuilder ssb = new SearchSourceBuilder().size(0).query(qb).aggregation(ab);

        SearchResponse sr = tairSearch.tftsearch("tftkey", ssb);
        SumOfSquares s = sr.getAggregations().get("price_sum_of_squares");
        assertEquals("sum_of_squares", s.getType());
        assertEquals("price_sum_of_squares", s.getName());
        assertEquals(22564.42, s.value(),0.001);
        assertEquals("{\"size\":0,\"query\":{\"term\":{\"investor\":{\"boost\":1.0,\"value\":\"Jay\"}}},"
                        + "\"aggs\":{\"price_sum_of_squares\":{\"sum_of_squares\":{\"field\":\"purchase_price\"}}}}",
                ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[],\"max_score\":null,\"total\":{\"relation\":\"eq\",\"value\":2}},\"aggregations\":{\"price_sum_of_squares\":{\"value\":22564.42,\"type\":\"sum_of_squares\"}}}",
                sr.toString());
    }

    @Test
    public void tftvarianceaggsbuildertest(){
        getJedis().del("tftkey");
        String ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"properties\":{\"shares_name\":{\"type"
                + "\":\"keyword\"}, \"logictime\":{\"type\":\"long\"}, \"purchase_type\":{\"type\":\"integer\"}, "
                + "\"purchase_price\":{\"type\":\"double\"}, \"purchase_count\":{\"type\":\"long\"}, "
                + "\"investor\":{\"type\":\"keyword\"}}}}");
        assertEquals(ret, "OK");

        tairSearch.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300210, \"purchase_type\":1, "
                + "\"purchase_price\":101.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearch.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300310, \"purchase_type\":1, "
                + "\"purchase_price\":111.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearch.tftadddoc("tftkey", "{\"shares_name\":\"YBY\", \"logictime\":14300410, \"purchase_type\":1, "
                + "\"purchase_price\":11.1, \"purchase_count\":100, \"investor\":\"Mila\"}");

        VarianceAggregationBuilder ab = AggregationBuilders.variance("price_variance").field("purchase_price");
        assertEquals("purchase_price", ab.field());
        assertEquals("variance", ab.getType());
        assertEquals("price_variance", ab.getName());
        TermQueryBuilder qb = QueryBuilders.termQuery("investor","Jay");
        SearchSourceBuilder ssb = new SearchSourceBuilder().size(0).query(qb).aggregation(ab);

        SearchResponse sr = tairSearch.tftsearch("tftkey", ssb);
        Variance s = sr.getAggregations().get("price_variance");
        assertEquals(25.0, s.value(),0.001);
        assertEquals("variance", s.getType());
        assertEquals("price_variance", s.getName());
        assertEquals("{\"size\":0,\"query\":{\"term\":{\"investor\":{\"boost\":1.0,\"value\":\"Jay\"}}},"
                        + "\"aggs\":{\"price_variance\":{\"variance\":{\"field\":\"purchase_price\"}}}}",
                ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[],\"max_score\":null,\"total\":{\"relation\":\"eq\",\"value\":2}},\"aggregations\":{\"price_variance\":{\"value\":25.0,\"type\":\"variance\"}}}",
                sr.toString());
    }

    @Test
    public void tftstddeviationaggsbuildertest(){
        getJedis().del("tftkey");
        String ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"properties\":{\"shares_name\":{\"type"
                + "\":\"keyword\"}, \"logictime\":{\"type\":\"long\"}, \"purchase_type\":{\"type\":\"integer\"}, "
                + "\"purchase_price\":{\"type\":\"double\"}, \"purchase_count\":{\"type\":\"long\"}, "
                + "\"investor\":{\"type\":\"keyword\"}}}}");
        assertEquals(ret, "OK");

        tairSearch.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300210, \"purchase_type\":1, "
                + "\"purchase_price\":101.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearch.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300310, \"purchase_type\":1, "
                + "\"purchase_price\":111.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearch.tftadddoc("tftkey", "{\"shares_name\":\"YBY\", \"logictime\":14300410, \"purchase_type\":1, "
                + "\"purchase_price\":11.1, \"purchase_count\":100, \"investor\":\"Mila\"}");

        StdDeviationAggregationBuilder ab = AggregationBuilders.stdDeviation("price_std_deviation").field("purchase_price");
        assertEquals("purchase_price", ab.field());
        assertEquals("std_deviation", ab.getType());
        assertEquals("price_std_deviation", ab.getName());
        TermQueryBuilder qb = QueryBuilders.termQuery("investor","Jay");
        SearchSourceBuilder ssb = new SearchSourceBuilder().size(0).query(qb).aggregation(ab);

        SearchResponse sr = tairSearch.tftsearch("tftkey", ssb);
        StdDeviation s = sr.getAggregations().get("price_std_deviation");
        assertEquals(5.0, s.value(),0.001);
        assertEquals("std_deviation", s.getType());
        assertEquals("price_std_deviation", s.getName());
        assertEquals("{\"size\":0,\"query\":{\"term\":{\"investor\":{\"boost\":1.0,\"value\":\"Jay\"}}},"
                        + "\"aggs\":{\"price_std_deviation\":{\"std_deviation\":{\"field\":\"purchase_price\"}}}}",
                ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[],\"max_score\":null,\"total\":{\"relation\":\"eq\",\"value\":2}},\"aggregations\":{\"price_std_deviation\":{\"value\":5.0,\"type\":\"std_deviation\"}}}",
                sr.toString());
    }

    @Test
    public void tftextendedstatsaggsbuildertest(){
        getJedis().del("tftkey");
        String ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"properties\":{\"shares_name\":{\"type"
                + "\":\"keyword\"}, \"logictime\":{\"type\":\"long\"}, \"purchase_type\":{\"type\":\"integer\"}, "
                + "\"purchase_price\":{\"type\":\"double\"}, \"purchase_count\":{\"type\":\"long\"}, "
                + "\"investor\":{\"type\":\"keyword\"}}}}");
        assertEquals(ret, "OK");

        tairSearch.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300210, \"purchase_type\":1, "
                + "\"purchase_price\":101.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearch.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300310, \"purchase_type\":1, "
                + "\"purchase_price\":111.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearch.tftadddoc("tftkey", "{\"shares_name\":\"YBY\", \"logictime\":14300410, \"purchase_type\":1, "
                + "\"purchase_price\":11.1, \"purchase_count\":100, \"investor\":\"Mila\"}");

        ExtendedStatsAggregationBuilder ab = AggregationBuilders.extendedStats("price_extended_stats").field("purchase_price");
        assertEquals("purchase_price", ab.field());
        assertEquals("extended_stats", ab.getType());
        assertEquals("price_extended_stats", ab.getName());
        TermQueryBuilder qb = QueryBuilders.termQuery("investor","Jay");
        SearchSourceBuilder ssb = new SearchSourceBuilder().size(0).query(qb).aggregation(ab);
        SearchResponse sr = tairSearch.tftsearch("tftkey", ssb);
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
                ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[],\"max_score\":null,\"total\":{\"relation\":\"eq\",\"value\":2}},\"aggregations\":{\"price_extended_stats\":{\"count\":2,\"sum\":212.2,\"max\":111.1,\"min\":101.1,\"avg\":106.1,\"sum_of_squares\":22564.42,\"variance\":25.0,\"std_deviation\":5.0,\"type\":\"extended_stats\"}}}",
                sr.toString());
    }

    @Test
    public void tftvaluecountaggsbuildertest(){
        getJedis().del("tftkey");
        String ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"properties\":{\"shares_name\":{\"type"
                + "\":\"keyword\"}, \"logictime\":{\"type\":\"long\"}, \"purchase_type\":{\"type\":\"integer\"}, "
                + "\"purchase_price\":{\"type\":\"double\"}, \"purchase_count\":{\"type\":\"long\"}, "
                + "\"investor\":{\"type\":\"keyword\"}}}}");
        assertEquals(ret, "OK");

        tairSearch.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300210, \"purchase_type\":1, "
                + "\"purchase_price\":101.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearch.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300310, \"purchase_type\":1, "
                + "\"purchase_price\":111.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearch.tftadddoc("tftkey", "{\"shares_name\":\"YBY\", \"logictime\":14300410, \"purchase_type\":1, "
                + "\"purchase_price\":11.1, \"purchase_count\":100, \"investor\":\"Mila\"}");

        ValueCountAggregationBuilder ab = AggregationBuilders.count("price_count").field("purchase_price");
        assertEquals("purchase_price", ab.field());
        assertEquals("value_count", ab.getType());
        assertEquals("price_count", ab.getName());
        TermQueryBuilder qb = QueryBuilders.termQuery("investor","Jay");
        SearchSourceBuilder ssb = new SearchSourceBuilder().size(0).query(qb).aggregation(ab);

        SearchResponse sr = tairSearch.tftsearch("tftkey", ssb);
        ValueCount s = sr.getAggregations().get("price_count");
        assertEquals(2, s.getValue());
        assertEquals(2.0, s.value(),0.001);
        assertEquals("value_count", s.getType());
        assertEquals("price_count", s.getName());
        assertEquals("{\"size\":0,\"query\":{\"term\":{\"investor\":{\"boost\":1.0,\"value\":\"Jay\"}}},"
                        + "\"aggs\":{\"price_count\":{\"value_count\":{\"field\":\"purchase_price\"}}}}",
                ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[],\"max_score\":null,\"total\":{\"relation\":\"eq\",\"value\":2}},\"aggregations\":{\"price_count\":{\"value\":2.0,\"type\":\"value_count\"}}}",
                sr.toString());
    }

    @Test
    public void tftfilteraggsbuildertest(){
        getJedis().del("tftkey");
        String ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"properties\":{\"shares_name\":{\"type"
                + "\":\"keyword\"}, \"logictime\":{\"type\":\"long\"}, \"purchase_type\":{\"type\":\"integer\"}, "
                + "\"purchase_price\":{\"type\":\"double\"}, \"purchase_count\":{\"type\":\"long\"}, "
                + "\"investor\":{\"type\":\"keyword\"}}}}");
        assertEquals(ret, "OK");

        tairSearch.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300210, \"purchase_type\":1, "
                + "\"purchase_price\":101.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearch.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300310, \"purchase_type\":1, "
                + "\"purchase_price\":111.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearch.tftadddoc("tftkey", "{\"shares_name\":\"YBY\", \"logictime\":14300410, \"purchase_type\":1, "
                + "\"purchase_price\":11.1, \"purchase_count\":100, \"investor\":\"Mila\"}");

        ExtendedStatsAggregationBuilder ab = AggregationBuilders.extendedStats("Jay_BuyIn_Quatation").field("purchase_price");
        TermQueryBuilder qb = QueryBuilders.termQuery("investor","Jay");
        TermQueryBuilder filterQb = QueryBuilders.termQuery("purchase_type",1);
        FilterAggregationBuilder filterAggregationBuilder = AggregationBuilders.filter("Jay_BuyIn_Filter",filterQb).subAggregation(ab);
        assertEquals(filterQb, filterAggregationBuilder.getFilter());
        assertTrue(filterAggregationBuilder.getSubAggregations().contains(ab));
        assertEquals("filter", filterAggregationBuilder.getType());
        assertEquals("Jay_BuyIn_Filter", filterAggregationBuilder.getName());
        SearchSourceBuilder ssb = new SearchSourceBuilder().size(0).query(qb).aggregation(filterAggregationBuilder);
        SearchResponse sr = tairSearch.tftsearch("tftkey", ssb);
        assertEquals("{\"Jay_BuyIn_Filter\":{\"doc_count\":2,\"type\":\"filter\",\"Jay_BuyIn_Quatation\":{\"count\":2,\"sum\":212.2,\"max\":111.1,\"min\":101.1,\"avg\":106.1,\"sum_of_squares\":22564.42,\"variance\":25.0,\"std_deviation\":5.0,\"type\":\"extended_stats\"}}}", sr.getAggregations().toString());
        assertEquals((long)2,((InternalAggregation)sr.getAggregations().get("Jay_BuyIn_Filter")).getProperty("_count"));
        assertEquals(106.1,((InternalAggregation)sr.getAggregations().get("Jay_BuyIn_Filter")).getProperty("Jay_BuyIn_Quatation.avg"));
        Filter result = sr.getAggregations().get("Jay_BuyIn_Filter");
        assertEquals("{\"Jay_BuyIn_Filter\":{\"doc_count\":2,\"type\":\"filter\",\"Jay_BuyIn_Quatation\":{\"count\":2,\"sum\":212.2,\"max\":111.1,\"min\":101.1,\"avg\":106.1,\"sum_of_squares\":22564.42,\"variance\":25.0,\"std_deviation\":5.0,\"type\":\"extended_stats\"}}}", result.toString());
        assertEquals("filter", result.getType());
        assertEquals("Jay_BuyIn_Filter", result.getName());
        assertEquals(2, result.getDocCount());
        ExtendedStats s = result.getAggregations().get("Jay_BuyIn_Quatation");
        assertEquals("{\"Jay_BuyIn_Quatation\":{\"count\":2,\"sum\":212.2,\"max\":111.1,\"min\":101.1,\"avg\":106.1,\"sum_of_squares\":22564.42,\"variance\":25.0,\"std_deviation\":5.0,\"type\":\"extended_stats\"}}", s.toString());
        assertEquals(106.1, s.getAvg(),0.001);
        assertEquals(2, s.getCount(),0.001);
        assertEquals(111.1, s.getMax(),0.001);
        assertEquals(101.1, s.getMin(),0.001);
        assertEquals(5.0, s.getStdDeviation(),0.001);
        assertEquals(212.2, s.getSum(),0.001);
        assertEquals(22564.42, s.getSumOfSquares(),0.001);
        assertEquals(25.0, s.getVariance(),0.001);
        assertEquals("{\"size\":0,\"query\":{\"term\":{\"investor\":{\"boost\":1.0,\"value\":\"Jay\"}}},\"aggs\":{\"Jay_BuyIn_Filter\":{\"filter\":{\"term\":{\"purchase_type\":{\"boost\":1.0,\"value\":1}}},\"aggs\":{\"Jay_BuyIn_Quatation\":{\"extended_stats\":{\"field\":\"purchase_price\"}}}}}}",
                ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[],\"max_score\":null,\"total\":{\"relation\":\"eq\",\"value\":2}},\"aggregations\":{\"Jay_BuyIn_Filter\":{\"doc_count\":2,\"type\":\"filter\",\"Jay_BuyIn_Quatation\":{\"count\":2,\"sum\":212.2,\"max\":111.1,\"min\":101.1,\"avg\":106.1,\"sum_of_squares\":22564.42,\"variance\":25.0,\"std_deviation\":5.0,\"type\":\"extended_stats\"}}}}",
                sr.toString());
    }

    @Test
    public void tfttermsaggsbuildertest(){
        getJedis().del("tftkey");
        String ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"properties\":{\"shares_name\":{\"type"
                + "\":\"keyword\"}, \"logictime\":{\"type\":\"long\"}, \"purchase_type\":{\"type\":\"integer\"}, "
                + "\"purchase_price\":{\"type\":\"double\"}, \"purchase_count\":{\"type\":\"long\"}, "
                + "\"investor\":{\"type\":\"keyword\"}}}}");
        assertEquals(ret, "OK");

        tairSearch.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300210, \"purchase_type\":1, "
                + "\"purchase_price\":101.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearch.tftadddoc("tftkey", "{\"shares_name\":\"XAX\", \"logictime\":14300310, \"purchase_type\":1, "
                + "\"purchase_price\":111.1, \"purchase_count\":100, \"investor\":\"Jay\"}");
        tairSearch.tftadddoc("tftkey", "{\"shares_name\":\"YBY\", \"logictime\":14300410, \"purchase_type\":1, "
                + "\"purchase_price\":11.1, \"purchase_count\":100, \"investor\":\"Mila\"}");

        TermQueryBuilder qb = QueryBuilders.termQuery("purchase_type",1);
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("Per_Investor_Freq").field("investor");
        assertEquals("investor", termsAggregationBuilder.field());
        assertEquals("terms", termsAggregationBuilder.getType());
        assertEquals("Per_Investor_Freq", termsAggregationBuilder.getName());
        SearchSourceBuilder ssb = new SearchSourceBuilder().size(0).query(qb).aggregation(termsAggregationBuilder);
        SearchResponse sr = tairSearch.tftsearch("tftkey", ssb);
        Terms result = sr.getAggregations().get("Per_Investor_Freq");
        assertEquals("{\"Per_Investor_Freq\":{\"type\":\"sterms\",\"buckets\":[{\"key\":\"Jay\",\"doc_count\":2},{\"key\":\"Mila\",\"doc_count\":1}]}}", result.toString());
        assertEquals(2,result.getBucketByKey("Jay").getDocCount());
        assertEquals("sterms", result.getType());
        assertEquals("Per_Investor_Freq", result.getName());

        List<?> buckets = result.getBuckets();
        assertEquals("[{\"key\":\"Jay\",\"doc_count\":2}, {\"key\":\"Mila\",\"doc_count\":1}]", buckets.toString());
        assertEquals("Jay", ((Terms.Bucket)buckets.get(0)).getKey());
        assertEquals(2, ((Terms.Bucket)buckets.get(0)).getDocCount());
        assertEquals("Mila", ((Terms.Bucket)buckets.get(1)).getKey());
        assertEquals(1, ((Terms.Bucket)buckets.get(1)).getDocCount());
        assertEquals("{\"size\":0,\"query\":{\"term\":{\"purchase_type\":{\"boost\":1.0,\"value\":1}}},\"aggs\":{\"Per_Investor_Freq\":{\"terms\":{\"field\":\"investor\",\"size\":10,\"min_doc_count\":1,\"order\":{\"_count\":\"desc\"}}}}}",
                ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[],\"max_score\":null,\"total\":{\"relation\":\"eq\",\"value\":3}},\"aggregations\":{\"Per_Investor_Freq\":{\"type\":\"sterms\",\"buckets\":[{\"key\":\"Jay\",\"doc_count\":2},{\"key\":\"Mila\",\"doc_count\":1}]}}}",
                sr.toString());

        termsAggregationBuilder = AggregationBuilders.terms("Per_Investor_Freq").field("investor").order(BucketOrder.key(false));
        assertEquals(BucketOrder.key(false), termsAggregationBuilder.order());
        ssb = new SearchSourceBuilder().size(0).query(qb).aggregation(termsAggregationBuilder);
        sr = tairSearch.tftsearch("tftkey", ssb);
        result = sr.getAggregations().get("Per_Investor_Freq");
        buckets = result.getBuckets();
        assertEquals("Mila", ((Terms.Bucket)buckets.get(0)).getKey());
        assertEquals(1, ((Terms.Bucket)buckets.get(0)).getDocCount());
        assertEquals("Jay", ((Terms.Bucket)buckets.get(1)).getKey());
        assertEquals(2, ((Terms.Bucket)buckets.get(1)).getDocCount());
        assertEquals("{\"size\":0,\"query\":{\"term\":{\"purchase_type\":{\"boost\":1.0,\"value\":1}}},\"aggs\":{\"Per_Investor_Freq\":{\"terms\":{\"field\":\"investor\",\"size\":10,\"min_doc_count\":1,\"order\":{\"_key\":\"desc\"}}}}}",
                ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[],\"max_score\":null,\"total\":{\"relation\":\"eq\",\"value\":3}},\"aggregations\":{\"Per_Investor_Freq\":{\"type\":\"sterms\",\"buckets\":[{\"key\":\"Mila\",\"doc_count\":1},{\"key\":\"Jay\",\"doc_count\":2}]}}}",
                sr.toString());

        termsAggregationBuilder = AggregationBuilders.terms("Per_Investor_Freq").field("investor").minDocCount(2);
        assertEquals(2, termsAggregationBuilder.minDocCount());
        ssb = new SearchSourceBuilder().size(0).query(qb).aggregation(termsAggregationBuilder);
        sr = tairSearch.tftsearch("tftkey", ssb);
        result = sr.getAggregations().get("Per_Investor_Freq");
        buckets = result.getBuckets();
        assertEquals("Jay", ((Terms.Bucket)buckets.get(0)).getKey());
        assertEquals(2, ((Terms.Bucket)buckets.get(0)).getDocCount());
        assertEquals("{\"size\":0,\"query\":{\"term\":{\"purchase_type\":{\"boost\":1.0,\"value\":1}}},\"aggs\":{\"Per_Investor_Freq\":{\"terms\":{\"field\":\"investor\",\"size\":10,\"min_doc_count\":2,\"order\":{\"_count\":\"desc\"}}}}}",
                ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[],\"max_score\":null,\"total\":{\"relation\":\"eq\",\"value\":3}},\"aggregations\":{\"Per_Investor_Freq\":{\"type\":\"sterms\",\"buckets\":[{\"key\":\"Jay\",\"doc_count\":2}]}}}",
                sr.toString());

        termsAggregationBuilder = AggregationBuilders.terms("Per_Investor_Freq").field("investor").size(1);
        assertEquals(1, termsAggregationBuilder.size());
        ssb = new SearchSourceBuilder().size(0).query(qb).aggregation(termsAggregationBuilder);
        sr = tairSearch.tftsearch("tftkey", ssb);
        result = sr.getAggregations().get("Per_Investor_Freq");
        buckets = result.getBuckets();
        assertEquals("Jay", ((Terms.Bucket)buckets.get(0)).getKey());
        assertEquals(2, ((Terms.Bucket)buckets.get(0)).getDocCount());
        assertEquals("{\"size\":0,\"query\":{\"term\":{\"purchase_type\":{\"boost\":1.0,\"value\":1}}},\"aggs\":{\"Per_Investor_Freq\":{\"terms\":{\"field\":\"investor\",\"size\":1,\"min_doc_count\":1,\"order\":{\"_count\":\"desc\"}}}}}",
                ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[],\"max_score\":null,\"total\":{\"relation\":\"eq\",\"value\":3}},\"aggregations\":{\"Per_Investor_Freq\":{\"type\":\"sterms\",\"buckets\":[{\"key\":\"Jay\",\"doc_count\":2}]}}}",
                sr.toString());

        SortedSet exclude = new TreeSet();
        exclude.add("XAX");
        termsAggregationBuilder = AggregationBuilders.terms("Per_Investor_Freq").field("shares_name").includeExclude(new IncludeExclude("[A-Z]+",exclude));
        assertEquals("[A-Z]+", termsAggregationBuilder.includeExclude().include());
        assertTrue( termsAggregationBuilder.includeExclude().excludeValues().contains("XAX"));
        ssb = new SearchSourceBuilder().size(0).query(qb).aggregation(termsAggregationBuilder);
        sr = tairSearch.tftsearch("tftkey", ssb);
        result = sr.getAggregations().get("Per_Investor_Freq");
        buckets = result.getBuckets();
        assertEquals("YBY", ((Terms.Bucket)buckets.get(0)).getKey());
        assertEquals(1, ((Terms.Bucket)buckets.get(0)).getDocCount());
        assertEquals("{\"size\":0,\"query\":{\"term\":{\"purchase_type\":{\"boost\":1.0,\"value\":1}}},\"aggs\":{\"Per_Investor_Freq\":{\"terms\":{\"field\":\"shares_name\",\"size\":10,\"min_doc_count\":1,\"include\":\"[A-Z]+\",\"exclude\":[\"XAX\"],\"order\":{\"_count\":\"desc\"}}}}}",
                ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[],\"max_score\":null,\"total\":{\"relation\":\"eq\",\"value\":3}},\"aggregations\":{\"Per_Investor_Freq\":{\"type\":\"sterms\",\"buckets\":[{\"key\":\"YBY\",\"doc_count\":1}]}}}",
                sr.toString());

        SumAggregationBuilder ab = AggregationBuilders.sum("Jay_BuyIn_Sum").field("purchase_price");
        termsAggregationBuilder = AggregationBuilders.terms("Per_Investor_Freq").field("investor").subAggregation(ab);
        assertTrue( termsAggregationBuilder.getSubAggregations().contains(ab));
        ssb = new SearchSourceBuilder().size(0).query(qb).aggregation(termsAggregationBuilder);
        sr = tairSearch.tftsearch("tftkey", ssb);
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
                ssb.toString());
        assertEquals("{\"hits\":{\"hits\":[],\"max_score\":null,\"total\":{\"relation\":\"eq\",\"value\":3}},\"aggregations\":{\"Per_Investor_Freq\":{\"type\":\"sterms\",\"buckets\":[{\"key\":\"Jay\",\"doc_count\":2,\"Jay_BuyIn_Sum\":{\"value\":212.2,\"type\":\"sum\"}},{\"key\":\"Mila\",\"doc_count\":1,\"Jay_BuyIn_Sum\":{\"value\":11.1,\"type\":\"sum\"}}]}}}",
                sr.toString());
    }

    @Test
    public void tftexplaincosttest() {
        getJedis().del("tftkey");
        String ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        tairSearch.tftadddoc("tftkey", "{\"f0\":\"redis is a nosql database\"}", "1");
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.054363,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0.054363,\"total\":{\"relation\":\"eq\",\"value\":1}}}",
                tairSearch.tftsearch("tftkey", "{\"query\":{\"term\":{\"f0\":\"redis\"}}}"));
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"redis is an in-memory database that persists on disk\"}", "2");
        tairSearch.tftadddoc("tftkey", "{\"f0\":\"redis supports many different kind of values\"}", "3");

        TermQueryBuilder qb = QueryBuilders.termQuery("f0", "redis");
        SearchSourceBuilder ssb = new SearchSourceBuilder().query(qb).sort("_score");

        String response = tairSearch.tftexplaincost("tftkey", ssb);
        JsonObject result = JsonParser.parseString(response).getAsJsonObject();
        assertTrue(result.get("QUERY_COST") != null);
    }

    @Test
    public void tftexplainscoretest() {
        getJedis().del("tftkey");
        String ret = tairSearch.tftcreateindex("tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}");
        assertEquals(ret, "OK");

        tairSearch.tftadddoc("tftkey", "{\"f0\":\"redis is a nosql database\"}", "1");

        TermQueryBuilder qb = QueryBuilders.termQuery("f0", "redis");
        SearchSourceBuilder ssb = new SearchSourceBuilder().query(qb);

        String response = tairSearch.tftexplainscore("tftkey", ssb);
        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0.054363,\"_source\":{\"f0\":\"redis is a nosql database\"},\"_explanation\":{\"score\":0.054363,\"description\":\"score, computed as query_boost * idf * idf * tf\",\"field\":\"f0\",\"term\":\"redis\",\"query_boost\":1.0,\"details\":[{\"value\":0.306853,\"description\":\"idf, computed as 1 + log(N / (n + 1))\",\"details\":[{\"value\":1,\"description\":\"n, number of documents containing term\"},{\"value\":1,\"description\":\"N, total number of documents\"}]},{\"value\":0.57735,\"description\":\"tf, computed as sqrt(freq) / sqrt(dl)\",\"details\":[{\"value\":1,\"description\":\"freq, occurrences of term within document\"},{\"value\":3,\"description\":\"dl, length of field\"}]}]}}],\"max_score\":0.054363,\"total\":{\"relation\":\"eq\",\"value\":1}}}", response);

        String result = tairSearch.tftexplainscore("tftkey", ssb, "0", "1", "2");
        assertEquals(result, response);
    }
}
