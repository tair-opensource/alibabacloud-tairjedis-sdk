package com.aliyun.tair.tests.tairvector;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.aliyun.tair.tairvector.factory.VectorBuilderFactory;
import com.aliyun.tair.tairvector.params.DistanceMethod;
import com.aliyun.tair.tairvector.params.HscanParams;
import com.aliyun.tair.tairvector.params.IndexAlgorithm;
import org.junit.Test;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.util.SafeEncoder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class TairVectorClusterTest extends TairVectorTestBase {
    final String index = "default_index_cluster";
    final int dims = 8;
    final IndexAlgorithm algorithm = IndexAlgorithm.HNSW;
    final DistanceMethod method = DistanceMethod.IP;
    final String attr = "name";

    private void tvs_create_index(int dims, IndexAlgorithm algorithm, DistanceMethod method) {
        assertEquals("OK", tairVectorCluster.tvscreateindex(index, dims, algorithm, method));
    }

    private void tvs_hset(final String entityid, final String vector, final String param_k, final String param_v) {
        long result =  tairVectorCluster.tvshset(index, entityid, vector, param_k, param_v);
        assertEquals(result, 1);
    }

    private void tvs_hset(byte[] entityid, byte[] vector, byte[] param_k, byte[] param_v) {
        long result =  tairVectorCluster.tvshset(SafeEncoder.encode(index), entityid, vector, param_k, param_v);
        assertEquals(result, 1);
    }

    private long tvs_del_entity(String entity) {
        return tairVectorCluster.tvsdel(index, entity);
    }
    private long tvs_del_entity(byte[] entity) {
        return tairVectorCluster.tvsdel(SafeEncoder.encode(index), entity);
    }

    @Test
    public void tvs_create_index_test() {
        assertEquals("OK", tairVectorCluster.tvscreateindex(index, dims, algorithm, method));
        assertNotEquals("OK", tairVectorCluster.tvscreateindex(SafeEncoder.encode(index), dims, algorithm, method));
    }


    @Test
    public  void tvs_get_index() {
        tvs_create_index(dims, algorithm, method);

        Map<String, String> schema =  tairVectorCluster.tvsgetindex(index);
        assertEquals(index, schema.get("index_name"));
        assertEquals(algorithm.name(), schema.get("algorithm"));
        assertEquals(method.name(), schema.get("distance_method"));
        assertEquals(String.valueOf(0), schema.get("data_count"));


        Map<byte[], byte[]> schema_bytecode =  tairVectorCluster.tvsgetindex(SafeEncoder.encode(index));
        Iterator<Map.Entry<byte[], byte[]>> entries = schema_bytecode.entrySet().iterator();
        while(entries.hasNext()) {
            Map.Entry<byte[], byte[]> entry = entries.next();
            assertEquals(schema.get(SafeEncoder.encode(entry.getKey())), SafeEncoder.encode(entry.getValue()));
        }
    }

    @Test
    public  void tvs_scan_index() {
        tvs_create_index(dims, algorithm, method);

        HscanParams exhscanParams = new HscanParams();
        exhscanParams.count(5);
        ScanResult<String> result =  tairVectorCluster.tvsscanindex(0L, exhscanParams);
        assertEquals(String.valueOf(1), result.getCursor());
        assertEquals(1, result.getResult().size());
        assertEquals(index, result.getResult().get(0));
    }

    @Test
    public  void tvs_del_index() {
        tvs_create_index(dims, algorithm, method);

        Map<String, String> schema =  tairVectorCluster.tvsgetindex(index);
        assertEquals(index, schema.get("index_name"));
        assertEquals(algorithm.name(), schema.get("algorithm"));
        assertEquals(method.name(), schema.get("distance_method"));
        assertEquals(String.valueOf(0), schema.get("data_count"));

        assertEquals((long)tairVectorCluster.tvsdelindex(index), 1L);
    }

    @Test
    public  void tvs_hset() {
        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");
        tvs_hset("first_entity", "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name", "sammy");
        tvs_hset(SafeEncoder.encode("second_entity"), SafeEncoder.encode("[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]"), SafeEncoder.encode("name"), SafeEncoder.encode("tiddy"));
    }

    @Test
    public  void tvs_hgetall() {
        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");
        tvs_hset("first_entity", "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name", "sammy");
        tvs_hset(SafeEncoder.encode("second_entity"), SafeEncoder.encode("[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]"), SafeEncoder.encode("name"), SafeEncoder.encode("tiddy"));

        Map<String, String> entity_string =  tairVectorCluster.tvshgetall(index, "first_entity");
        assertEquals("[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", entity_string.get(VectorBuilderFactory.VECTOR_TAG));
        assertEquals("sammy", entity_string.get("name"));

        Map<byte[], byte[]> entity_byte = tairVectorCluster.tvshgetall(SafeEncoder.encode(index), SafeEncoder.encode("first_entity"));
        assertEquals(SafeEncoder.encode("[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]"), entity_byte.get(SafeEncoder.encode(VectorBuilderFactory.VECTOR_TAG)));
        assertEquals(SafeEncoder.encode("sammy"), entity_byte.get(SafeEncoder.encode("name")));
    }

    @Test
    public  void tvs_hmgetall() {
        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");
        tvs_hset("first_entity", "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name", "sammy");
        tvs_hset(SafeEncoder.encode("second_entity"), SafeEncoder.encode("[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]"), SafeEncoder.encode("name"), SafeEncoder.encode("tiddy"));

        List<String> entity_string =  tairVectorCluster.tvshmget(index, "first_entity", VectorBuilderFactory.VECTOR_TAG, "name");
        assertEquals("[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", entity_string.get(0));
        assertEquals("sammy", entity_string.get(1));

        List<byte[]> entity_byte = tairVectorCluster.tvshmget(SafeEncoder.encode(index), SafeEncoder.encode("first_entity"), SafeEncoder.encode(VectorBuilderFactory.VECTOR_TAG), SafeEncoder.encode("name"));
        assertEquals(SafeEncoder.encode("[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]"), entity_byte.get(0));
        assertEquals(SafeEncoder.encode("sammy"), entity_byte.get(1));
    }

    @Test
    public  void tvs_del() {
        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");

        tvs_hset("first_entity", "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name", "sammy");
        tvs_hset(SafeEncoder.encode("second_entity"), SafeEncoder.encode("[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]"), SafeEncoder.encode("name"), SafeEncoder.encode("tiddy"));

        long count_string =  tvs_del_entity("first_entity");
        assertEquals(1, count_string);

        long count_byte = tvs_del_entity(SafeEncoder.encode("second_entity"));
        assertEquals(1, count_byte);
    }

    @Test
    public  void tvs_hdel() {
        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");

        tvs_hset("first_entity", "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name", "sammy");
        tvs_hset(SafeEncoder.encode("second_entity"), SafeEncoder.encode("[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]"), SafeEncoder.encode("name"), SafeEncoder.encode("tiddy"));

        long count_string =  tairVectorCluster.tvshdel(index, "first_entity", "name");
        assertEquals(1, count_string);
        Map<String, String> entity_string =  tairVectorCluster.tvshgetall(index, "first_entity");
        assertTrue(entity_string.size() == 1 && (! entity_string.containsKey("name")));

        long count_byte = tairVectorCluster.tvshdel(SafeEncoder.encode(index), SafeEncoder.encode("second_entity"), SafeEncoder.encode(VectorBuilderFactory.VECTOR_TAG));
        assertEquals(1, count_byte);
        Map<String, String> entity_byte =  tairVectorCluster.tvshgetall(index, "second_entity");
        assertTrue(entity_byte.size() == 1 && (! entity_byte.containsKey(VectorBuilderFactory.VECTOR_TAG)));
    }

    @Test
    public  void tvs_scan() {
        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");

        tvs_hset("first_entity", "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name", "sammy");
        tvs_hset(SafeEncoder.encode("second_entity"), SafeEncoder.encode("[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]"), SafeEncoder.encode("name"), SafeEncoder.encode("tiddy"));

        long cursor = 0;
        HscanParams exhscanParams = new HscanParams();
        exhscanParams.count(1);
        exhscanParams.match("*entit*");
        ScanResult<String> result_string =  tairVectorCluster.tvsscan(index, cursor, exhscanParams);
        assert(result_string.getResult().size() >= 1);

        ScanResult<byte[]> entity_byte=  tairVectorCluster.tvsscan(SafeEncoder.encode(index), cursor, exhscanParams);
        assert(entity_byte.getResult().size() >= 1);
    }

    @Test
    public  void tvs_knnsearch() {
        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");

        tvs_hset("first_entity", "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name", "sammy");
        tvs_hset(SafeEncoder.encode("second_entity"), SafeEncoder.encode("[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]"), SafeEncoder.encode("name"), SafeEncoder.encode("tiddy"));

        long topn = 10L;
        VectorBuilderFactory.Knn<String> result_string =  tairVectorCluster.tvsknnsearch(index, topn, "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]");
        assertEquals(2, result_string.getKnnResults().size());

        VectorBuilderFactory.Knn<byte[]> entity_byte=  tairVectorCluster.tvsknnsearch(SafeEncoder.encode(index), topn, SafeEncoder.encode("[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]"));
        assertEquals(2, entity_byte.getKnnResults().size());
    }

    @Test
    public  void tvs_knnsearch_filter() {
        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");

        tvs_hset("first_entity", "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name", "sammy");
        tvs_hset(SafeEncoder.encode("second_entity"), SafeEncoder.encode("[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]"), SafeEncoder.encode("name"), SafeEncoder.encode("tiddy"));

        long topn = 10L;
        VectorBuilderFactory.Knn<String> result_string =  tairVectorCluster.tvsknnsearchfilter(index, topn, "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name == \"sammy\"");
        assertEquals(2, result_string.getKnnResults().size());

        VectorBuilderFactory.Knn<byte[]> entity_byte=  tairVectorCluster.tvsknnsearchfilter(SafeEncoder.encode(index), topn, SafeEncoder.encode("[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]"), SafeEncoder.encode("name == \"sammy\""));
        assertEquals(2, entity_byte.getKnnResults().size());
    }

    @Test
    public  void tvs_mknnsearch() {
        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");

        tvs_hset("first_entity", "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name", "sammy");
        tvs_hset(SafeEncoder.encode("second_entity"), SafeEncoder.encode("[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]"), SafeEncoder.encode("name"), SafeEncoder.encode("tiddy"));

        long topn = 10L;
        List<String> vectors = Arrays.asList("[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]");
        Collection<VectorBuilderFactory.Knn<String>> result_string =  tairVectorCluster.tvsmknnsearch(index, topn, vectors);
        assertEquals(2, result_string.size());
        result_string.forEach(one -> System.out.printf("string: %s\n", one.toString()));


        Collection<VectorBuilderFactory.Knn<byte[]>> entity_byte=  tairVectorCluster.tvsmknnsearch(SafeEncoder.encode(index), topn, vectors.stream().map(item -> SafeEncoder.encode(item)).collect(Collectors.toList()));
        assertEquals(2, entity_byte.size());
        result_string.forEach(one -> System.out.printf("byte: %s\n", one.toString()));
    }

    @Test
    public  void tvs_mknnsearch_filter() {
        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");

        tvs_hset("first_entity", "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name", "sammy");
        tvs_hset(SafeEncoder.encode("second_entity"), SafeEncoder.encode("[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]"), SafeEncoder.encode("name"), SafeEncoder.encode("tiddy"));

        long topn = 10L;
        List<String> vectors = Arrays.asList("[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]");
        String pattern = "name == \"no-sammy\"";
        Collection<VectorBuilderFactory.Knn<String>> result_string =  tairVectorCluster.tvsmknnsearchfilter(index, topn, vectors, pattern);
        assertEquals(2, result_string.size());
        result_string.forEach(one -> System.out.printf("string: %s\n", one.toString()));


        Collection<VectorBuilderFactory.Knn<byte[]>> entity_byte=  tairVectorCluster.tvsmknnsearchfilter(SafeEncoder.encode(index), topn, vectors.stream().map(item -> SafeEncoder.encode(item)).collect(Collectors.toList()), SafeEncoder.encode(pattern));
        assertEquals(2, entity_byte.size());
        result_string.forEach(one -> System.out.printf("byte: %s\n", one.toString()));
    }
}
