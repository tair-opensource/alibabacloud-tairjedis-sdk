package com.aliyun.tair.tests.tairvector;

import com.aliyun.tair.tairvector.factory.VectorBuilderFactory;
import com.aliyun.tair.tairvector.params.DistanceMethod;
import com.aliyun.tair.tairvector.params.HscanParams;
import com.aliyun.tair.tairvector.params.IndexAlgorithm;
import org.junit.Test;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.util.SafeEncoder;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class TairVectorPipelineTest extends TairVectorTestBase {

    final String index = "default_index_pipeline";
    final int dims = 8;
    final IndexAlgorithm algorithm = IndexAlgorithm.FLAT;
    final DistanceMethod method = DistanceMethod.IP;
    final long dbid = 2;

    private void tvs_del_index(String index) {
        tairVectorPipeline.tvsdelindex(index);
        tairVectorPipeline.syncAndReturnAll();
    }
    private void tvs_check_index(int dims, String index, IndexAlgorithm algorithm, DistanceMethod method) {
        tairVectorPipeline.tvsgetindex(index);
        List<Object> objs = tairVectorPipeline.syncAndReturnAll();
        if (((Map)objs.get(0)).isEmpty()) {
            tvs_create_index(dims, algorithm, method);
        }
    }
    private void tvs_create_index(int dims, IndexAlgorithm algorithm, DistanceMethod method) {
        tairVectorPipeline.tvsdelindex(index);
        tairVectorPipeline.tvscreateindex(index, dims, algorithm, method);
        tairVectorPipeline.syncAndReturnAll();
    }

    private void tvs_hset(final String entityid, final String vector, final String param_k, final String param_v) {
        tairVectorPipeline.tvshset(index, entityid, vector, param_k, param_v);
    }

    private void tvs_hset(byte[] entityid, byte[] vector, byte[] param_k, byte[] param_v) {
        tairVectorPipeline.tvshset(SafeEncoder.encode(index), entityid, vector, param_k, param_v);
    }

    private void tvs_del_entity(String entity) {
        tairVectorPipeline.tvsdel(index, entity);
    }
    private void tvs_del_entity(byte[] entity) {
        tairVectorPipeline.tvsdel(SafeEncoder.encode(index), entity);
    }

    @Test
    public void tvs_create_index_test() {
        tvs_del_index(index);
        tairVectorPipeline.tvscreateindex(index, dims, algorithm, method);
        tairVectorPipeline.tvscreateindex(SafeEncoder.encode(index), dims, algorithm, method);
        List<Object> objs = tairVectorPipeline.syncAndReturnAll();
        assertEquals("OK", objs.get(0));
        assertNotEquals("OK", objs.get(1));
    }


    @Test
    public  void tvs_get_index() {
        tvs_create_index(dims, algorithm, method);

        tairVectorPipeline.tvsgetindex(index);
        tairVectorPipeline.tvsgetindex(SafeEncoder.encode(index));
        List<Object> objs = tairVectorPipeline.syncAndReturnAll();

        Map<String, String> schema = (Map<String, String>)objs.get(0);
        assertEquals(index, schema.get("index_name"));
        assertEquals(algorithm.name(), schema.get("algorithm"));
        assertEquals(method.name(), schema.get("distance_method"));
        assertEquals(String.valueOf(0), schema.get("data_count"));

        Map<byte[], byte[]> schema_bytecode = (Map<byte[], byte[]>)objs.get(1);
        Iterator<Map.Entry<byte[], byte[]>> entries = schema_bytecode.entrySet().iterator();
        while(entries.hasNext()) {
            Map.Entry<byte[], byte[]> entry = entries.next();
            assertEquals(schema.get(SafeEncoder.encode(entry.getKey())), SafeEncoder.encode(entry.getValue()));
        }
    }

    @Test
    public  void tvs_del_index() {
        tvs_create_index(dims, algorithm, method);

        tairVectorPipeline.tvsdelindex(index);
        List<Object> objs = tairVectorPipeline.syncAndReturnAll();

        Long result = (Long)objs.get(0);
        assert(1L == result);
    }

    @Test
    public  void tvs_scan_index() {
        tvs_create_index(dims, algorithm, method);

        HscanParams hscanParams = new HscanParams();
        hscanParams.count(5);
        hscanParams.match(index);
        tairVectorPipeline.tvsscanindex(0L, hscanParams);
        List<Object> objs = tairVectorPipeline.syncAndReturnAll();

        ScanResult<String> result = (ScanResult<String>) objs.get(0);
        assertEquals(1, result.getResult().size());
        assertEquals(index, result.getResult().get(0));
    }

    @Test
    public  void tvs_hset() {
        tvs_create_index(dims, algorithm, method);
        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");
        tvs_hset("first_entity", "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name", "sammy");
        tvs_hset(SafeEncoder.encode("second_entity"), SafeEncoder.encode("[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]"), SafeEncoder.encode("name"), SafeEncoder.encode("tiddy"));
        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");
        List<Object> objs = tairVectorPipeline.syncAndReturnAll();

        assertEquals((long)objs.get(2), 2L);
        assertEquals((long)objs.get(3), 2L);
    }

    @Test
    public  void tvs_hgetall() {
        tvs_check_index(dims, index, algorithm, method);

        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");
        tvs_hset("first_entity", "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name", "sammy");
        tvs_hset(SafeEncoder.encode("second_entity"), SafeEncoder.encode("[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]"), SafeEncoder.encode("name"), SafeEncoder.encode("tiddy"));

        tairVectorPipeline.tvshgetall(index, "first_entity");
        tairVectorPipeline.tvshgetall(SafeEncoder.encode(index), SafeEncoder.encode("first_entity"));
        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");

        List<Object> objs = tairVectorPipeline.syncAndReturnAll();
        Map<String, String> entity_string = (Map<String, String>)objs.get(4);
        assertEquals("[0.12,0.23,0.56,0.67,0.78,0.89,0.01,0.89]", entity_string.get(VectorBuilderFactory.VECTOR_TAG));
        assertEquals("sammy", entity_string.get("name"));


        Map<byte[], byte[]> entity_byte = (Map<byte[], byte[]>)objs.get(5);
        assertEquals("[0.12,0.23,0.56,0.67,0.78,0.89,0.01,0.89]", SafeEncoder.encode(entity_byte.get(SafeEncoder.encode(VectorBuilderFactory.VECTOR_TAG))));
        assertEquals("sammy", SafeEncoder.encode(entity_byte.get(SafeEncoder.encode("name"))));
    }

    @Test
    public  void tvs_hmgetall() {
        tvs_check_index(dims, index, algorithm, method);

        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");
        tvs_hset("first_entity", "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name", "sammy");
        tvs_hset(SafeEncoder.encode("second_entity"), SafeEncoder.encode("[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]"), SafeEncoder.encode("name"), SafeEncoder.encode("tiddy"));

        tairVectorPipeline.tvshmget(index, "first_entity", VectorBuilderFactory.VECTOR_TAG, "name");
        tairVectorPipeline.tvshmget(SafeEncoder.encode(index), SafeEncoder.encode("first_entity"), SafeEncoder.encode(VectorBuilderFactory.VECTOR_TAG), SafeEncoder.encode("name"));

        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");

        List<Object> objs = tairVectorPipeline.syncAndReturnAll();
        List<String> entity_string = (List<String>) objs.get(4);
                assertEquals("[0.12,0.23,0.56,0.67,0.78,0.89,0.01,0.89]", entity_string.get(0));
        assertEquals("sammy", entity_string.get(1));

        List<byte[]> entity_byte = (List<byte[]>) objs.get(5);
        assertEquals("[0.12,0.23,0.56,0.67,0.78,0.89,0.01,0.89]", SafeEncoder.encode(entity_byte.get(0)));
        assertEquals("sammy", SafeEncoder.encode(entity_byte.get(1)));
    }

    @Test
    public  void tvs_del() {
        tvs_check_index(dims, index, algorithm, method);

        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");

        tvs_hset("first_entity", "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name", "sammy");
        tvs_hset(SafeEncoder.encode("second_entity"), SafeEncoder.encode("[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]"), SafeEncoder.encode("name"), SafeEncoder.encode("tiddy"));

        tvs_del_entity("first_entity");
        tvs_del_entity(SafeEncoder.encode("second_entity"));

        List<Object> objs = tairVectorPipeline.syncAndReturnAll();
        assertEquals(1L, (long)objs.get(4));
        assertEquals(1L, (long)objs.get(5));
    }

    @Test
    public  void tvs_hdel() {
        tvs_check_index(dims, index, algorithm, method);

        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");
        tvs_hset("first_entity", "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name", "sammy");
        tvs_hset(SafeEncoder.encode("second_entity"), SafeEncoder.encode("[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]"), SafeEncoder.encode("name"), SafeEncoder.encode("tiddy"));

        tairVectorPipeline.tvshdel(index, "first_entity", "name");
        tairVectorPipeline.tvshgetall(index, "first_entity");
        tairVectorPipeline.tvshdel(SafeEncoder.encode(index), SafeEncoder.encode("second_entity"), SafeEncoder.encode(VectorBuilderFactory.VECTOR_TAG));
        tairVectorPipeline.tvshgetall(index, "second_entity");

        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");
        List<Object> objs = tairVectorPipeline.syncAndReturnAll();

        long count_string = (long)objs.get(4);
        assertEquals(1, count_string);
        Map<String, String> entity_string = (Map<String, String>) objs.get(5);
        assertTrue(entity_string.size() == 1 && (! entity_string.containsKey("name")));
        long count_byte = (long)objs.get(6);
        assertEquals(1, count_byte);
        Map<String, String> entity_byte = (Map<String, String>) objs.get(7);
        assertTrue(entity_byte.size() == 1 && (! entity_byte.containsKey(VectorBuilderFactory.VECTOR_TAG)));
    }

    @Test
    public  void tvs_scan() {
        tvs_check_index(dims, index, algorithm, method);

        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");

        tvs_hset("first_entity", "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name", "sammy");
        tvs_hset(SafeEncoder.encode("second_entity"), SafeEncoder.encode("[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]"), SafeEncoder.encode("name"), SafeEncoder.encode("tiddy"));

        long cursor = 0;
        HscanParams hscanParams = new HscanParams();
        hscanParams.count(1);
        hscanParams.match("*entit*");
        tairVectorPipeline.tvsscan(index, cursor, hscanParams);
        tairVectorPipeline.tvsscan(SafeEncoder.encode(index), cursor, hscanParams);

        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");
        List<Object> objs = tairVectorPipeline.syncAndReturnAll();

        ScanResult<String> result_string = (ScanResult<String>)objs.get(4);
        assert(result_string.getResult().size() >= 1);
        ScanResult<byte[]> entity_byte = (ScanResult<byte[]>)objs.get(5);
        assert(entity_byte.getResult().size() >= 1);
    }

    @Test
    public  void tvs_knnsearch() {
        tvs_check_index(dims, index, algorithm, method);

        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");

        tvs_hset("first_entity", "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name", "sammy");
        tvs_hset(SafeEncoder.encode("second_entity"), SafeEncoder.encode("[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]"), SafeEncoder.encode("name"), SafeEncoder.encode("tiddy"));

        long topn = 10L;
        tairVectorPipeline.tvsknnsearch(index, topn, "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]");
        tairVectorPipeline.tvsknnsearch(SafeEncoder.encode(index), topn, SafeEncoder.encode("[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]"));

        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");

        List<Object> objs = tairVectorPipeline.syncAndReturnAll();

        VectorBuilderFactory.Knn<String> result_string = (VectorBuilderFactory.Knn<String>)objs.get(4);
        assertEquals(2, result_string.getKnnResults().size());
        VectorBuilderFactory.Knn<byte[]> entity_byte = (VectorBuilderFactory.Knn<byte[]>)objs.get(5);
        assertEquals(2, entity_byte.getKnnResults().size());
    }

    @Test
    public  void tvs_mknnsearch() {
        tvs_check_index(dims, index, algorithm, method);

        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");

        tvs_hset("first_entity", "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name", "sammy");
        tvs_hset(SafeEncoder.encode("second_entity"), SafeEncoder.encode("[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]"), SafeEncoder.encode("name"), SafeEncoder.encode("tiddy"));

        long topn = 10L;
        List<String> vectors = Arrays.asList("[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]");
        String pattern = "";
        tairVectorPipeline.tvsmknnsearch(index, topn, vectors, pattern);
        tairVectorPipeline.tvsmknnsearch(SafeEncoder.encode(index), topn, vectors.stream().map(item -> SafeEncoder.encode(item)).collect(Collectors.toList()), SafeEncoder.encode(pattern));

        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");
        List<Object> objs = tairVectorPipeline.syncAndReturnAll();

        Collection<VectorBuilderFactory.Knn<String>> result_string = (Collection<VectorBuilderFactory.Knn<String>>)objs.get(4);
        assertEquals(2, result_string.size());
        result_string.forEach(one -> System.out.printf("string: %s\n", one.toString()));

        Collection<VectorBuilderFactory.Knn<byte[]>> entity_byte= (Collection<VectorBuilderFactory.Knn<byte[]>>)objs.get(5);
        assertEquals(2, entity_byte.size());
        result_string.forEach(one -> System.out.printf("byte: %s\n", one.toString()));
    }
}
