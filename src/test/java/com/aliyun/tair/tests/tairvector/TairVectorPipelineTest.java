package com.aliyun.tair.tests.tairvector;

import com.aliyun.tair.tairvector.factory.VectorBuilderFactory;
import com.aliyun.tair.tairvector.factory.VectorBuilderFactory.KnnItem;
import com.aliyun.tair.tairvector.params.DistanceMethod;
import com.aliyun.tair.tairvector.params.HscanParams;
import com.aliyun.tair.tairvector.params.IndexAlgorithm;
import org.junit.Test;
import com.aliyun.tair.jedis3.ScanResult;
import io.valkey.util.SafeEncoder;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class TairVectorPipelineTest extends TairVectorTestBase {

    final String index = "default_index_pipeline";
    final int dims = 8;
    final IndexAlgorithm algorithm = IndexAlgorithm.HNSW;
    final DistanceMethod method = DistanceMethod.IP;
    final long dbid = 2;
    final List<String> index_params = Arrays.asList("ef_construct", "100", "M", "16");
    final List<String> ef_params = Arrays.asList("ef_search", "100");
    final List<String[]> test_data = Arrays.asList(
            new String[]{"VECTOR", "[7,3]", "name", "Aaron", "age", "12"}, // dist 58
            new String[]{"VECTOR", "[9,2]", "name", "Bob", "age", "33"}, // dist 85
            new String[]{"VECTOR", "[6,6]", "name", "Charlie", "age", "29"}, // dist 72
            new String[]{"VECTOR", "[3,5]", "name", "Daniel", "age", "23"}, // dist 34
            new String[]{"VECTOR", "[3,7]", "name", "Eason", "age", "22"}, // dist 58
            new String[]{"VECTOR", "[3,6]", "name", "Fabian", "age", "35"}, // dist 45
            new String[]{"VECTOR", "[5,2]", "name", "George", "age", "12"}, // dist 29
            new String[]{"VECTOR", "[8,9]", "name", "Henry", "age", "30"}, // dist 145
            new String[]{"VECTOR", "[5,5]", "name", "Ivan", "age", "16"}, // dist 50
            new String[]{"VECTOR", "[2,7]", "name", "James", "age", "12"}); // dist 53

    private void tvs_del_index(String index) {
        tairVectorPipeline.tvsdelindex(index);
        tairVectorPipeline.syncAndReturnAll();
    }

    private void tvs_check_index(int dims, String index, IndexAlgorithm algorithm, DistanceMethod method) {
        tairVectorPipeline.tvsgetindex(index);
        List<Object> objs = tairVectorPipeline.syncAndReturnAll();
        if (((Map) objs.get(0)).isEmpty()) {
            tvs_create_index(dims, index, algorithm, method);
        }
    }

    private void tvs_create_index(int dims, String index, IndexAlgorithm algorithm, DistanceMethod method) {
        tairVectorPipeline.tvsdelindex(index);
        tairVectorPipeline.tvscreateindex(index, dims, algorithm, method, index_params.toArray(new String[0]));
        List<Object> result = tairVectorPipeline.syncAndReturnAll();
        assertEquals(result.size(), 2);
        assertEquals(result.get(1).toString(), "OK");
    }

    private void tvs_hset(final String entityid, final String vector, final String param_k, final String param_v) {
        tairVectorPipeline.tvshset(index, entityid, vector, param_k, param_v);
    }

    private void tvs_hset(byte[] entityid, byte[] vector, byte[] param_k, byte[] param_v) {
        tairVectorPipeline.tvshset(SafeEncoder.encode(index), entityid, vector, param_k, param_v);
    }

    private void tvs_del_entity(String... entity) {
        tairVectorPipeline.tvsdel(index, entity);
    }

    private void tvs_del_entity(byte[]... entity) {
        tairVectorPipeline.tvsdel(SafeEncoder.encode(index), entity);
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
        tairVectorPipeline.tvscreateindex(index, dims, algorithm, method, index_params.toArray(new String[0]));
        tairVectorPipeline.tvscreateindex(SafeEncoder.encode(index), dims, algorithm, method, SafeEncoder.encodeMany(index_params.toArray(new String[0])));
        List<Object> objs = tairVectorPipeline.syncAndReturnAll();
        assertEquals("OK", objs.get(0));
        assertNotEquals("OK", objs.get(1));
    }


    @Test
    public void tvs_get_index() {
        tvs_create_index(dims, index, algorithm, method);

        tairVectorPipeline.tvsgetindex(index);
        tairVectorPipeline.tvsgetindex(SafeEncoder.encode(index));
        List<Object> objs = tairVectorPipeline.syncAndReturnAll();

        Map<String, String> schema = (Map<String, String>) objs.get(0);
        assertEquals(algorithm.name(), schema.get("algorithm"));
        assertEquals(method.name(), schema.get("distance_method"));
        assertEquals(String.valueOf(0), schema.get("data_count"));

        Map<byte[], byte[]> schema_bytecode = (Map<byte[], byte[]>) objs.get(1);
        Iterator<Map.Entry<byte[], byte[]>> entries = schema_bytecode.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry<byte[], byte[]> entry = entries.next();
            assertEquals(schema.get(SafeEncoder.encode(entry.getKey())), SafeEncoder.encode(entry.getValue()));
        }
    }

    @Test
    public void tvs_del_index() {
        tvs_create_index(dims, index, algorithm, method);

        tairVectorPipeline.tvsdelindex(index);
        List<Object> objs = tairVectorPipeline.syncAndReturnAll();

        Long result = (Long) objs.get(0);
        assert (1L == result);
    }

    @Test
    public void tvs_scan_index() {
        tvs_create_index(dims, index, algorithm, method);

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
    public void tvs_hset() {
        tvs_create_index(dims, index, algorithm, method);
        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");
        tvs_hset("first_entity", "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name", "sammy");
        tvs_hset(SafeEncoder.encode("second_entity"), SafeEncoder.encode("[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]"),
                SafeEncoder.encode("name"), SafeEncoder.encode("tiddy"));
        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");
        List<Object> objs = tairVectorPipeline.syncAndReturnAll();

        assertEquals((long) objs.get(2), 2L);
        assertEquals((long) objs.get(3), 2L);
    }

    @Test
    public void tvs_hgetall() {
        tvs_check_index(dims, index, algorithm, method);

        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");
        tvs_hset("first_entity", "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name", "sammy");
        tvs_hset(SafeEncoder.encode("second_entity"), SafeEncoder.encode("[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]"),
                SafeEncoder.encode("name"), SafeEncoder.encode("tiddy"));

        tairVectorPipeline.tvshgetall(index, "first_entity");
        tairVectorPipeline.tvshgetall(SafeEncoder.encode(index), SafeEncoder.encode("first_entity"));
        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");

        List<Object> objs = tairVectorPipeline.syncAndReturnAll();
        Map<String, String> entity_string = (Map<String, String>) objs.get(4);
        assertEquals("[0.12,0.23,0.56,0.67,0.78,0.89,0.01,0.89]", entity_string.get(VectorBuilderFactory.VECTOR_TAG));
        assertEquals("sammy", entity_string.get("name"));


        Map<byte[], byte[]> entity_byte = (Map<byte[], byte[]>) objs.get(5);
        assertEquals("[0.12,0.23,0.56,0.67,0.78,0.89,0.01,0.89]", SafeEncoder.encode(entity_byte.get(SafeEncoder.encode(VectorBuilderFactory.VECTOR_TAG))));
        assertEquals("sammy", SafeEncoder.encode(entity_byte.get(SafeEncoder.encode("name"))));
    }

    @Test
    public void tvs_hmgetall() {
        tvs_check_index(dims, index, algorithm, method);

        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");
        tvs_hset("first_entity", "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name", "sammy");
        tvs_hset(SafeEncoder.encode("second_entity"), SafeEncoder.encode("[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]"),
                SafeEncoder.encode("name"), SafeEncoder.encode("tiddy"));

        tairVectorPipeline.tvshmget(index, "first_entity", VectorBuilderFactory.VECTOR_TAG, "name");
        tairVectorPipeline.tvshmget(SafeEncoder.encode(index), SafeEncoder.encode("first_entity"),
                SafeEncoder.encode(VectorBuilderFactory.VECTOR_TAG), SafeEncoder.encode("name"));

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
    public void tvs_del() {
        tvs_check_index(dims, index, algorithm, method);

        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");

        tvs_hset("first_entity", "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name", "sammy");
        tvs_hset(SafeEncoder.encode("second_entity"), SafeEncoder.encode("[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]"),
                SafeEncoder.encode("name"), SafeEncoder.encode("tiddy"));

        tvs_del_entity("first_entity");
        tvs_del_entity(SafeEncoder.encode("second_entity"));

        List<Object> objs = tairVectorPipeline.syncAndReturnAll();
        assertEquals(1L, (long) objs.get(4));
        assertEquals(1L, (long) objs.get(5));

        tvs_hset("first_entity", "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name", "sammy");
        tvs_hset(SafeEncoder.encode("second_entity"), SafeEncoder.encode("[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]"),
          SafeEncoder.encode("name"), SafeEncoder.encode("tiddy"));
        tvs_del_entity("first_entity", "second_entity");
        objs = tairVectorPipeline.syncAndReturnAll();
        assertEquals(2L, (long) objs.get(2));

        tvs_hset("first_entity", "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name", "sammy");
        tvs_hset(SafeEncoder.encode("second_entity"), SafeEncoder.encode("[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]"),
          SafeEncoder.encode("name"), SafeEncoder.encode("tiddy"));
        tvs_del_entity(SafeEncoder.encodeMany("first_entity", "second_entity"));
        objs = tairVectorPipeline.syncAndReturnAll();
        assertEquals(2L, (long) objs.get(2));
    }

    @Test
    public void tvs_hdel() {
        tvs_check_index(dims, index, algorithm, method);

        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");
        tvs_hset("first_entity", "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name", "sammy");
        tvs_hset(SafeEncoder.encode("second_entity"), SafeEncoder.encode("[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]"),
                SafeEncoder.encode("name"), SafeEncoder.encode("tiddy"));

        tairVectorPipeline.tvshdel(index, "first_entity", "name");
        tairVectorPipeline.tvshgetall(index, "first_entity");
        tairVectorPipeline.tvshdel(SafeEncoder.encode(index), SafeEncoder.encode("second_entity"),
                SafeEncoder.encode(VectorBuilderFactory.VECTOR_TAG));
        tairVectorPipeline.tvshgetall(index, "second_entity");

        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");
        List<Object> objs = tairVectorPipeline.syncAndReturnAll();

        long count_string = (long) objs.get(4);
        assertEquals(1, count_string);
        Map<String, String> entity_string = (Map<String, String>) objs.get(5);
        assertTrue(entity_string.size() == 1 && (!entity_string.containsKey("name")));
        long count_byte = (long) objs.get(6);
        assertEquals(1, count_byte);
        Map<String, String> entity_byte = (Map<String, String>) objs.get(7);
        assertTrue(entity_byte.size() == 1 && (!entity_byte.containsKey(VectorBuilderFactory.VECTOR_TAG)));
    }

    @Test
    public void tvs_scan() {
        tvs_check_index(dims, index, algorithm, method);

        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");

        tvs_hset("first_entity", "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name", "sammy");
        tvs_hset(SafeEncoder.encode("second_entity"), SafeEncoder.encode("[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]"),
                SafeEncoder.encode("name"), SafeEncoder.encode("tiddy"));

        long cursor = 0;
        HscanParams hscanParams = new HscanParams();
        hscanParams.count(1);
        hscanParams.match("*entit*");
        tairVectorPipeline.tvsscan(index, cursor, hscanParams);
        tairVectorPipeline.tvsscan(SafeEncoder.encode(index), cursor, hscanParams);

        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");
        List<Object> objs = tairVectorPipeline.syncAndReturnAll();

        ScanResult<String> result_string = (ScanResult<String>) objs.get(4);
        assert (result_string.getResult().size() >= 1);
        ScanResult<byte[]> entity_byte = (ScanResult<byte[]>) objs.get(5);
        assert (entity_byte.getResult().size() >= 1);
    }

    @Test
    public void tvs_knnsearch() {
        tvs_check_index(dims, index, algorithm, method);

        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");

        tvs_hset("first_entity", "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name", "sammy");
        tvs_hset(SafeEncoder.encode("second_entity"), SafeEncoder.encode("[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]"),
                SafeEncoder.encode("name"), SafeEncoder.encode("tiddy"));

        long topn = 10L;
        tairVectorPipeline.tvsknnsearch(index, topn, "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]");
        tairVectorPipeline.tvsknnsearch(SafeEncoder.encode(index), topn, SafeEncoder.encode("[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]"));
        tairVectorPipeline.tvsknnsearchfield(index, topn, "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", Collections.singletonList("name"));
        tairVectorPipeline.tvsknnsearchfield(SafeEncoder.encode(index), topn, SafeEncoder.encode("[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]"), Collections.singletonList(SafeEncoder.encode("name")));

        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");

        List<Object> objs = tairVectorPipeline.syncAndReturnAll();

        VectorBuilderFactory.Knn<String> result_string = (VectorBuilderFactory.Knn<String>) objs.get(4);
        assertEquals(2, result_string.getKnnResults().size());
        VectorBuilderFactory.Knn<byte[]> entity_byte = (VectorBuilderFactory.Knn<byte[]>) objs.get(5);
        assertEquals(2, entity_byte.getKnnResults().size());
        VectorBuilderFactory.KnnField<String> result_string_field = (VectorBuilderFactory.KnnField<String>) objs.get(6);
        assertEquals(2, result_string_field.getKnnResults().size());
        VectorBuilderFactory.KnnField<byte[]> entity_byte_field = (VectorBuilderFactory.KnnField<byte[]>) objs.get(7);
        assertEquals(2, entity_byte_field.getKnnResults().size());
    }

    @Test
    public void tvs_knnsearch_filter() {
        tvs_check_index(dims, index, algorithm, method);

        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");

        tvs_hset("first_entity", "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name", "sammy");
        tvs_hset(SafeEncoder.encode("second_entity"), SafeEncoder.encode("[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]"),
                SafeEncoder.encode("name"), SafeEncoder.encode("tiddy"));

        long topn = 10L;
        tairVectorPipeline.tvsknnsearchfilter(index, topn, "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name == \"sammy\"");
        tairVectorPipeline.tvsknnsearchfilter(SafeEncoder.encode(index), topn, SafeEncoder.encode("[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]"),
                SafeEncoder.encode("name == \"sammy\""));
        tairVectorPipeline.tvsknnsearchfilterfield(index, topn, "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]",
                Collections.singletonList("name"), "name == \"sammy\"");
        tairVectorPipeline.tvsknnsearchfilterfield(SafeEncoder.encode(index), topn, SafeEncoder.encode("[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]"),
                Collections.singletonList(SafeEncoder.encode("name")), SafeEncoder.encode("name == \"sammy\""));

        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");

        List<Object> objs = tairVectorPipeline.syncAndReturnAll();

        VectorBuilderFactory.Knn<String> result_string = (VectorBuilderFactory.Knn<String>) objs.get(4);
        assertEquals(1, result_string.getKnnResults().size());
        VectorBuilderFactory.Knn<byte[]> entity_byte = (VectorBuilderFactory.Knn<byte[]>) objs.get(5);
        assertEquals(1, entity_byte.getKnnResults().size());
        VectorBuilderFactory.KnnField<String> result_string_field = (VectorBuilderFactory.KnnField<String>) objs.get(6);
        assertEquals(1, result_string_field.getKnnResults().size());
        VectorBuilderFactory.KnnField<byte[]> entity_byte_field = (VectorBuilderFactory.KnnField<byte[]>) objs.get(7);
        assertEquals(1, entity_byte_field.getKnnResults().size());
    }

    @Test
    public void tvs_knnsearch_with_params() {
        tvs_check_index(dims, index, algorithm, method);

        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");

        tvs_hset("first_entity", "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name", "sammy");
        tvs_hset(SafeEncoder.encode("second_entity"), SafeEncoder.encode("[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]"),
                SafeEncoder.encode("name"), SafeEncoder.encode("tiddy"));

        long topn = 10L;
        tairVectorPipeline.tvsknnsearch(index, topn, "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", ef_params.toArray(new String[0]));
        tairVectorPipeline.tvsknnsearch(SafeEncoder.encode(index), topn, SafeEncoder.encode("[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]"),
                SafeEncoder.encodeMany(ef_params.toArray(new String[0])));
        tairVectorPipeline.tvsknnsearchfield(index, topn, "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]",
                Collections.singletonList("name"), ef_params.toArray(new String[0]));
        tairVectorPipeline.tvsknnsearchfield(SafeEncoder.encode(index), topn, SafeEncoder.encode("[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]"),
                Collections.singletonList(SafeEncoder.encode("name")), SafeEncoder.encodeMany(ef_params.toArray(new String[0])));

        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");

        List<Object> objs = tairVectorPipeline.syncAndReturnAll();

        VectorBuilderFactory.Knn<String> result_string = (VectorBuilderFactory.Knn<String>) objs.get(4);
        assertEquals(2, result_string.getKnnResults().size());
        VectorBuilderFactory.Knn<byte[]> entity_byte = (VectorBuilderFactory.Knn<byte[]>) objs.get(5);
        assertEquals(2, entity_byte.getKnnResults().size());
        VectorBuilderFactory.KnnField<String> result_string_field = (VectorBuilderFactory.KnnField<String>) objs.get(6);
        assertEquals(2, result_string_field.getKnnResults().size());
        VectorBuilderFactory.KnnField<byte[]> entity_byte_field = (VectorBuilderFactory.KnnField<byte[]>) objs.get(7);
        assertEquals(2, entity_byte_field.getKnnResults().size());
    }

    @Test
    public void tvs_mknnsearch() {
        tvs_check_index(dims, index, algorithm, method);

        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");

        tvs_hset("first_entity", "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name", "sammy");
        tvs_hset(SafeEncoder.encode("second_entity"), SafeEncoder.encode("[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]"),
                SafeEncoder.encode("name"), SafeEncoder.encode("tiddy"));

        long topn = 10L;
        List<String> vectors = Arrays.asList("[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]");
        tairVectorPipeline.tvsmknnsearch(index, topn, vectors);
        tairVectorPipeline.tvsmknnsearch(SafeEncoder.encode(index), topn, vectors.stream().map(item -> SafeEncoder.encode(item)).collect(Collectors.toList()));

        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");
        List<Object> objs = tairVectorPipeline.syncAndReturnAll();

        Collection<VectorBuilderFactory.Knn<String>> result_string = (Collection<VectorBuilderFactory.Knn<String>>) objs.get(4);
        assertEquals(2, result_string.size());
        result_string.forEach(one -> System.out.printf("string: %s\n", one.toString()));

        Collection<VectorBuilderFactory.Knn<byte[]>> entity_byte = (Collection<VectorBuilderFactory.Knn<byte[]>>) objs.get(5);
        assertEquals(2, entity_byte.size());
        result_string.forEach(one -> System.out.printf("byte: %s\n", one.toString()));
    }


    @Test
    public void tvs_mknnsearch_filter() {
        tvs_check_index(dims, index, algorithm, method);

        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");
        tvs_hset("first_entity", "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name", "sammy");
        tvs_hset(SafeEncoder.encode("second_entity"), SafeEncoder.encode("[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]"),
                SafeEncoder.encode("name"), SafeEncoder.encode("tiddy"));

        long topn = 10L;
        List<String> vectors = Arrays.asList("[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]");
        String pattern = "name == \"no-sammy\"";
        tairVectorPipeline.tvsmknnsearchfilter(index, topn, vectors, pattern);
        tairVectorPipeline.tvsmknnsearchfilter(SafeEncoder.encode(index), topn, vectors.stream().map(item -> SafeEncoder.encode(item)).collect(Collectors.toList()),
                SafeEncoder.encode(pattern));

        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");
        List<Object> objs = tairVectorPipeline.syncAndReturnAll();

        Collection<VectorBuilderFactory.Knn<String>> result_string = (Collection<VectorBuilderFactory.Knn<String>>) objs.get(4);
        assertEquals(2, result_string.size());
        result_string.forEach(result -> {
            assertEquals(0, result.getKnnResults().size());
        });
        result_string.forEach(one -> System.out.printf("string: %s\n", one.toString()));

        Collection<VectorBuilderFactory.Knn<byte[]>> entity_byte = (Collection<VectorBuilderFactory.Knn<byte[]>>) objs.get(5);
        assertEquals(2, entity_byte.size());
        entity_byte.forEach(result -> {
            assertEquals(0, result.getKnnResults().size());
        });
        entity_byte.forEach(one -> System.out.printf("byte: %s\n", one.toString()));
    }

    @Test
    public void tvs_mknnsearch_with_params() {
        tvs_check_index(dims, index, algorithm, method);

        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");

        tvs_hset("first_entity", "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name", "sammy");
        tvs_hset(SafeEncoder.encode("second_entity"), SafeEncoder.encode("[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]"),
                SafeEncoder.encode("name"), SafeEncoder.encode("tiddy"));

        long topn = 10L;
        List<String> vectors = Arrays.asList("[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]");
        tairVectorPipeline.tvsmknnsearch(index, topn, vectors, ef_params.toArray(new String[0]));
        tairVectorPipeline.tvsmknnsearch(SafeEncoder.encode(index), topn,
                vectors.stream().map(item -> SafeEncoder.encode(item)).collect(Collectors.toList()),
                SafeEncoder.encodeMany(ef_params.toArray(new String[0])));

        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");
        List<Object> objs = tairVectorPipeline.syncAndReturnAll();

        Collection<VectorBuilderFactory.Knn<String>> result_string = (Collection<VectorBuilderFactory.Knn<String>>) objs.get(4);
        assertEquals(2, result_string.size());
        result_string.forEach(one -> System.out.printf("string: %s\n", one.toString()));

        Collection<VectorBuilderFactory.Knn<byte[]>> entity_byte = (Collection<VectorBuilderFactory.Knn<byte[]>>) objs.get(5);
        assertEquals(2, entity_byte.size());
        result_string.forEach(one -> System.out.printf("byte: %s\n", one.toString()));
    }

    @Test
    public void tvs_mindexknnsearch() {
        tvs_check_index(dims, "index1", algorithm, DistanceMethod.L2);
        tvs_check_index(dims, "index2", algorithm, DistanceMethod.L2);

        tairVectorPipeline.tvshset("index1", "first_entity_knn", "[1, 1, 1, 1, 1, 1, 1, 1]", "name", "sammy");
        tairVectorPipeline.tvshset("index1", "second_entity_knn", "[3, 1, 1, 1, 1, 1, 1, 1]", "name", "sammy");
        tairVectorPipeline.tvshset("index2", "third_entity_knn", "[2, 1, 1, 1, 1, 1, 1, 1]", "name", "sammy");
        tairVectorPipeline.tvshset("index2", "fourth_entity_knn", "[4, 1, 1, 1, 1, 1, 1, 1]", "name", "sammy");

        long topn = 2L;
        List<String> indexs = Arrays.asList("index1", "index2");
        String vector = "[0, 0, 0, 0, 0, 0, 0, 0]";
        tairVectorPipeline.tvsmindexknnsearch(indexs, topn, vector, ef_params.toArray(new String[0]));
        tairVectorPipeline.tvsmindexknnsearch(indexs.stream().map(item -> SafeEncoder.encode(item)).collect(Collectors.toList()), topn, SafeEncoder.encode(vector), SafeEncoder.encodeMany(ef_params.toArray(new String[0])));
        tairVectorPipeline.tvsmindexknnsearchField(indexs, topn, vector, Collections.singletonList("name"), ef_params.toArray(new String[0]));
        tairVectorPipeline.tvsmindexknnsearchField(indexs.stream().map(item -> SafeEncoder.encode(item)).collect(Collectors.toList()), topn, SafeEncoder.encode(vector),
                Collections.singletonList(SafeEncoder.encode("name")), SafeEncoder.encodeMany(ef_params.toArray(new String[0])));

        List<Object> objs = tairVectorPipeline.syncAndReturnAll();
        VectorBuilderFactory.Knn<String> result_string = (VectorBuilderFactory.Knn<String>) objs.get(4);
        assertEquals(2, result_string.getKnnResults().size());
        VectorBuilderFactory.Knn<byte[]> entity_byte = (VectorBuilderFactory.Knn<byte[]>) objs.get(5);
        assertEquals(2, entity_byte.getKnnResults().size());
        VectorBuilderFactory.KnnField<String> result_string_field = (VectorBuilderFactory.KnnField<String>) objs.get(6);
        assertEquals(2, result_string_field.getKnnResults().size());
        VectorBuilderFactory.KnnField<byte[]> entity_byte_field = (VectorBuilderFactory.KnnField<byte[]>) objs.get(7);
        assertEquals(2, entity_byte_field.getKnnResults().size());
    }

    @Test
    public void tvs_mindexmknnsearch() {
        tvs_check_index(dims, "index1", algorithm, DistanceMethod.L2);
        tvs_check_index(dims, "index2", algorithm, DistanceMethod.L2);

        tairVectorPipeline.tvshset("index1", "first_entity_knn", "[1, 1, 1, 1, 1, 1, 1, 1]", "name", "sammy");
        tairVectorPipeline.tvshset("index1", "second_entity_knn", "[3, 1, 1, 1, 1, 1, 1, 1]", "name", "sammy");
        tairVectorPipeline.tvshset("index2", "third_entity_knn", "[2, 1, 1, 1, 1, 1, 1, 1]", "name", "sammy");
        tairVectorPipeline.tvshset("index2", "fourth_entity_knn", "[4, 1, 1, 1, 1, 1, 1, 1]", "name", "sammy");

        long topn = 2L;
        List<String> indexs = Arrays.asList("index1", "index2");
        List<String> vectors = Arrays.asList("[0, 0, 0, 0, 0, 0, 0, 0]", "[1, 1, 1, 1, 1, 1, 1, 1]");
        tairVectorPipeline.tvsmindexmknnsearch(indexs, topn, vectors, ef_params.toArray(new String[0]));
        tairVectorPipeline.tvsmindexmknnsearch(indexs.stream().map(item -> SafeEncoder.encode(item)).collect(Collectors.toList()), topn,
                vectors.stream().map(item -> SafeEncoder.encode(item)).collect(Collectors.toList()), SafeEncoder.encodeMany(ef_params.toArray(new String[0])));
        tairVectorPipeline.tvsmindexmknnsearchfilter(indexs, topn, vectors, "", ef_params.toArray(new String[0]));
        tairVectorPipeline.tvsmindexmknnsearchfilter(indexs.stream().map(item -> SafeEncoder.encode(item)).collect(Collectors.toList()), topn,
                vectors.stream().map(item -> SafeEncoder.encode(item)).collect(Collectors.toList()), SafeEncoder.encode(""), SafeEncoder.encodeMany(ef_params.toArray(new String[0])));

        List<Object> objs = tairVectorPipeline.syncAndReturnAll();
        Collection<VectorBuilderFactory.Knn<String>> result_string = (Collection<VectorBuilderFactory.Knn<String>>) objs.get(4);
        assertEquals(2, result_string.size());
        Collection<VectorBuilderFactory.Knn<byte[]>> entity_byte = (Collection<VectorBuilderFactory.Knn<byte[]>>) objs.get(5);
        assertEquals(2, entity_byte.size());
    }

    @Test
    public void tvs_hincrby_tvs_hincrbyfloat() {
        tvs_check_index(dims, index, algorithm, method);
        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");
        tairVectorPipeline.tvshincrby(index, "first_entity", "field", 2);
        tairVectorPipeline.tvshincrby(SafeEncoder.encode(index), SafeEncoder.encode("first_entity"), SafeEncoder.encode("field"), 2);

        tairVectorPipeline.tvshincrbyfloat(index, "second_entity", "field", 1.5d);
        tairVectorPipeline.tvshincrbyfloat(SafeEncoder.encode(index), SafeEncoder.encode("second_entity"), SafeEncoder.encode("field"), 1.5d);

        List<Object> objs = tairVectorPipeline.syncAndReturnAll();
        assertEquals(2L, (long) objs.get(2));
        assertEquals(4L, (long) objs.get(3));
        assertEquals(Double.compare(1.5d, (double) objs.get(4)), 0);
        assertEquals(Double.compare(3.0d, (double) objs.get(5)), 0);
    }

    public void tvs_getdistance() {
        final String index_name = "getdistance_test";
        tvs_create_index(2, index_name, algorithm, DistanceMethod.L2);

        for (int i = 0; i < test_data.size(); ++i) {
            String[] args = test_data.get(i);
            tairVectorPipeline.tvshset(index_name, String.format("key-%d", i), args[1],
                    Arrays.copyOfRange(args, 2, 6));
        }
        tairVectorPipeline.syncAndReturnAll();

        List<String> keys = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            String key = "key-" + i;
            keys.add(key);
        }

        // getdistance
        {
            tairVectorPipeline.tvsgetdistance(index_name, "[0,0]", keys, null, null, null);
            List<Object> objs = tairVectorPipeline.syncAndReturnAll();
            assertEquals(objs.size(), 1);
            VectorBuilderFactory.Knn<String> result = (VectorBuilderFactory.Knn<String>) objs.get(0);
            assertEquals(10, result.getKnnResults().size());
        }
        {
            tairVectorPipeline.tvsgetdistance(SafeEncoder.encode(index_name), SafeEncoder.encode("[0,0]"),
                    keys.stream().map(key -> SafeEncoder.encode(key)).collect(Collectors.toList()), null, null, null);
            List<Object> objs = tairVectorPipeline.syncAndReturnAll();
            assertEquals(objs.size(), 1);
            VectorBuilderFactory.Knn<byte[]> result = (VectorBuilderFactory.Knn<byte[]>) objs.get(0);
            assertEquals(10, result.getKnnResults().size());
        }

        // getdistance with TOPN
        {
            tairVectorPipeline.tvsgetdistance(index_name, "[0,0]", keys, Long.valueOf(5), null, null);
            List<Object> objs = tairVectorPipeline.syncAndReturnAll();
            assertEquals(objs.size(), 1);
            VectorBuilderFactory.Knn<String> result = (VectorBuilderFactory.Knn<String>) objs.get(0);
            assertEquals(5, result.getKnnResults().size());
            KnnItem<String> items[] = result.getKnnResults().toArray(new KnnItem[0]);
            for (int i = 0; i < items.length - 1; ++i) {
                assertTrue(items[i].getScore() <= items[i + 1].getScore());
            }
        }
        {
            tairVectorPipeline.tvsgetdistance(SafeEncoder.encode(index_name), SafeEncoder.encode("[0,0]"),
                    keys.stream().map(key -> SafeEncoder.encode(key)).collect(Collectors.toList()), Long.valueOf(5), null, null);
            List<Object> objs = tairVectorPipeline.syncAndReturnAll();
            assertEquals(objs.size(), 1);
            VectorBuilderFactory.Knn<byte[]> result = (VectorBuilderFactory.Knn<byte[]>) objs.get(0);
            assertEquals(5, result.getKnnResults().size());
            KnnItem<byte[]> items[] = result.getKnnResults().toArray(new KnnItem[0]);
            for (int i = 0; i < items.length - 1; ++i) {
                assertTrue(items[i].getScore() <= items[i + 1].getScore());
            }
        }

        // getdistance with MAX_DIST
        {
            tairVectorPipeline.tvsgetdistance(index_name, "[0,0]", keys, null, Float.valueOf(50), null);
            List<Object> objs = tairVectorPipeline.syncAndReturnAll();
            assertEquals(objs.size(), 1);
            VectorBuilderFactory.Knn<String> result = (VectorBuilderFactory.Knn<String>) objs.get(0);
            assertEquals(3, result.getKnnResults().size());
            KnnItem<String> items[] = result.getKnnResults().toArray(new KnnItem[0]);
            for (int i = 0; i < items.length; ++i) {
                assertTrue(items[i].getScore() < 50);
            }
        }
        {
            tairVectorPipeline.tvsgetdistance(SafeEncoder.encode(index_name), SafeEncoder.encode("[0,0]"),
                    keys.stream().map(key -> SafeEncoder.encode(key)).collect(Collectors.toList()), null, Float.valueOf(50), null);
            List<Object> objs = tairVectorPipeline.syncAndReturnAll();
            assertEquals(objs.size(), 1);
            VectorBuilderFactory.Knn<byte[]> result = (VectorBuilderFactory.Knn<byte[]>) objs.get(0);
            assertEquals(3, result.getKnnResults().size());
            KnnItem<byte[]> items[] = result.getKnnResults().toArray(new KnnItem[0]);
            for (int i = 0; i < items.length; ++i) {
                assertTrue(items[i].getScore() < 50);
            }
        }

        // getdistance with FILTER
        {
            tairVectorPipeline.tvsgetdistance(index_name, "[0,0]", keys, null, null, "name>\"H\"");
            List<Object> objs = tairVectorPipeline.syncAndReturnAll();
            assertEquals(objs.size(), 1);
            VectorBuilderFactory.Knn<String> result = (VectorBuilderFactory.Knn<String>) objs.get(0);
            assertEquals(3, result.getKnnResults().size());
            KnnItem<String> items[] = result.getKnnResults().toArray(new KnnItem[0]);
            for (int i = 0; i < items.length; ++i) {
                assertTrue(items[i].getId().equals("key-7") || items[i].getId().equals("key-8") || items[i].getId().equals("key-9"));
            }
        }
        {
            tairVectorPipeline.tvsgetdistance(SafeEncoder.encode(index_name), SafeEncoder.encode("[0,0]"),
                    keys.stream().map(key -> SafeEncoder.encode(key)).collect(Collectors.toList()), null, null, SafeEncoder.encode("name>\"H\""));
            List<Object> objs = tairVectorPipeline.syncAndReturnAll();
            assertEquals(objs.size(), 1);
            VectorBuilderFactory.Knn<byte[]> result = (VectorBuilderFactory.Knn<byte[]>) objs.get(0);
            assertEquals(3, result.getKnnResults().size());
            KnnItem<byte[]> items[] = result.getKnnResults().toArray(new KnnItem[0]);
            for (int i = 0; i < items.length; ++i) {
                assertTrue(SafeEncoder.encode((byte[]) items[i].getId()).equals("key-7") ||
                        SafeEncoder.encode((byte[]) items[i].getId()).equals("key-8") ||
                        SafeEncoder.encode((byte[]) items[i].getId()).equals("key-9"));
            }
        }

        tvs_del_index(index_name);
    }

    @Test
    public void tvs_expire() throws InterruptedException {
        tvs_create_index(dims, index, algorithm, method);
        List<String> keys = new ArrayList<>();
        int keySize = 10;
        for (int i = 0; i < keySize; ++i) {
            String key = UUID.randomUUID().toString();
            String vector = generateVector(dims);
            tairVectorPipeline.tvshset(index, key, vector,
                    "name", "tom", "age", String.valueOf(random.nextInt(100)));
            keys.add(key);
        }
        tairVectorPipeline.sync();

        int expireSecond = 10;
        long unixTime = System.currentTimeMillis() + expireSecond * 1000;
        Map<String, Integer> keyCommands = new HashMap<>();
        for (String key : keys) {
            int rate = random.nextInt(4);
            if (rate == 0) {
                tairVectorPipeline.tvshexpire(index, key, expireSecond);
            } else if (rate == 1) {
                tairVectorPipeline.tvshpexpire(index, key, expireSecond * 1000);
            } else if (rate == 2) {
                tairVectorPipeline.tvshexpireAt(index, key, unixTime / 1000);
            } else {
                tairVectorPipeline.tvshpexpireAt(index, key, unixTime);
            }
            keyCommands.put(key, rate);
        }

        for (String key : keys) {
            int rate = keyCommands.get(key);
            if (rate == 0) {
                tairVectorPipeline.tvshttl(index, key);
            } else if (rate == 1) {
                tairVectorPipeline.tvshpttl(index, key);
            } else if (rate == 2) {
                tairVectorPipeline.tvshexpiretime(index, key);
            } else {
                tairVectorPipeline.tvshpexpiretime(index, key);
            }
        }
        List<Object> objs = tairVectorPipeline.syncAndReturnAll();
        assertEquals(keySize * 2, objs.size());
        int pos = 0;

        while(pos < keySize) {
            assertTrue(Boolean.parseBoolean(objs.get(pos++).toString()));
        }

        for (String key : keys) {
            int rate = keyCommands.get(key);
            if (rate == 0) {
                long ttl = (long) objs.get(pos++);
                assertTrue(0 < ttl && ttl <= expireSecond);
            } else if (rate == 1) {
                long ttl = (long) objs.get(pos++);
                assertTrue(0 < ttl && ttl <= expireSecond * 1000);
            } else if (rate == 2) {
                assertEquals(unixTime / 1000, objs.get(pos++));
            } else {
                assertEquals(unixTime, objs.get(pos++));
            }
        }

        // update all key expire after 100 milliseconds
        for (String key : keys) {
            tairVectorPipeline.tvshpexpire(index, key, 100);
        }
        objs = tairVectorPipeline.syncAndReturnAll();
        for (Object obj : objs) {
            assertTrue(Boolean.parseBoolean(obj.toString()));
        }
        Thread.sleep(500);

        for (String key : keys) {
            tairVectorPipeline.tvshgetall(index, key);
        }

        objs = tairVectorPipeline.syncAndReturnAll();
        for (Object obj : objs) {
            assertTrue(((Map<?, ?>)obj).isEmpty());
        }
    }

}

