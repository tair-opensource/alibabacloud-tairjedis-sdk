package com.aliyun.tair.tests.tairvector;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.aliyun.tair.tairvector.factory.VectorBuilderFactory;
import com.aliyun.tair.tairvector.factory.VectorBuilderFactory.KnnItem;
import com.aliyun.tair.tairvector.params.DistanceMethod;
import com.aliyun.tair.tairvector.params.HscanParams;
import com.aliyun.tair.tairvector.params.IndexAlgorithm;

import java.util.*;
import java.util.stream.Collectors;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import com.aliyun.tair.jedis3.ScanResult;
import io.valkey.exceptions.JedisDataException;
import io.valkey.util.SafeEncoder;

public class TairVectorClusterTest extends TairVectorTestBase {
    final String index = "default_index_cluster";
    final int dims = 8;
    final IndexAlgorithm algorithm = IndexAlgorithm.HNSW;
    final DistanceMethod method = DistanceMethod.IP;
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

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private void tvs_create_index(int dims, IndexAlgorithm algorithm, DistanceMethod method) {
        assertEquals("OK", tairVectorCluster.tvscreateindex(index, dims, algorithm, method));
    }

    private void tvs_create_index_and_load_data() {
        tvs_create_index(dims, algorithm, method);
        tvs_hset("first_entity", "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name", "sammy");
        tvs_hset(SafeEncoder.encode("second_entity"),
                SafeEncoder.encode("[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]"),
                SafeEncoder.encode("name"),
                SafeEncoder.encode("tiddy"));
    }

    private void tvs_hset(
            final String entityid, final String vector, final String param_k, final String param_v) {
        long result = tairVectorCluster.tvshset(index, entityid, vector, param_k, param_v);
        assertEquals(result, 2);
    }

    private void tvs_hset(byte[] entityid, byte[] vector, byte[] param_k, byte[] param_v) {
        long result = tairVectorCluster.tvshset(SafeEncoder.encode(index), entityid, vector, param_k, param_v);
        assertEquals(result, 2);
    }

    private long tvs_del_entity(String... entity) {
        return tairVectorCluster.tvsdel(index, entity);
    }

    private long tvs_del_entity(byte[]... entity) {
        return tairVectorCluster.tvsdel(SafeEncoder.encode(index), entity);
    }

    private long tvs_del_entity(String entity) {
        return tairVectorCluster.tvsdel(index, entity);
    }

    private long tvs_del_entity(byte[] entity) {
        return tairVectorCluster.tvsdel(SafeEncoder.encode(index), entity);
    }

    @Test
    @Ignore
    public void tvs_create_index_test() {
        tairVectorCluster.tvsdelindex(index);

        assertEquals("OK", tairVectorCluster.tvscreateindex(index, dims, algorithm, method));
        thrown.expect(JedisDataException.class);
        tairVectorCluster.tvscreateindex(SafeEncoder.encode(index), dims, algorithm, method);
    }

    // @Test
    // public void tvs_scan_index() {
    // tvs_create_index(dims, algorithm, method);

    // HscanParams exhscanParams = new HscanParams();
    // exhscanParams.count(5);
    // ScanResult<String> result = tairVectorCluster.tvsscanindex(0L,
    // exhscanParams);
    // assertEquals(String.valueOf(1), result.getCursor());
    // assertEquals(1, result.getResult().size());
    // assertEquals(index, result.getResult().get(0));

    // tairVectorCluster.tvsdelindex(index);
    // }

    @Test
    public void tvs_get_index() {
        tairVectorCluster.tvsdelindex(index);
        tvs_create_index(dims, algorithm, method);

        Map<String, String> schema = tairVectorCluster.tvsgetindex(index);
        assertEquals(algorithm.name(), schema.get("algorithm"));
        assertEquals(method.name(), schema.get("distance_method"));
        assertEquals(String.valueOf(0), schema.get("data_count"));

        Map<byte[], byte[]> schema_bytecode = tairVectorCluster.tvsgetindex(SafeEncoder.encode(index));
        Iterator<Map.Entry<byte[], byte[]>> entries = schema_bytecode.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry<byte[], byte[]> entry = entries.next();
            assertEquals(
                    schema.get(SafeEncoder.encode(entry.getKey())),
                    SafeEncoder.encode(entry.getValue()));
        }

        tairVectorCluster.tvsdelindex(index);
    }

    @Test
    public void tvs_hset() {
        tairVectorCluster.tvsdelindex(index);
        tvs_create_index(dims, algorithm, method);

        tvs_hset("first_entity", "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]", "name", "sammy");
        tvs_hset(
                SafeEncoder.encode("second_entity"),
                SafeEncoder.encode("[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]"),
                SafeEncoder.encode("name"),
                SafeEncoder.encode("tiddy"));

        tairVectorCluster.tvsdelindex(index);
    }

    @Test
    public void tvs_hgetall() {
        tairVectorCluster.tvsdelindex(index);
        tvs_create_index_and_load_data();

        Map<String, String> entity_string = tairVectorCluster.tvshgetall(index, "first_entity");
        assertEquals(
                "[0.12,0.23,0.56,0.67,0.78,0.89,0.01,0.89]",
                entity_string.get(VectorBuilderFactory.VECTOR_TAG));
        assertEquals("sammy", entity_string.get("name"));

        Map<byte[], byte[]> entity_byte = tairVectorCluster.tvshgetall(SafeEncoder.encode(index),
                SafeEncoder.encode("first_entity"));
        assertEquals(
                "[0.12,0.23,0.56,0.67,0.78,0.89,0.01,0.89]",
                SafeEncoder.encode(
                        entity_byte.get(SafeEncoder.encode(VectorBuilderFactory.VECTOR_TAG))));
        assertEquals("sammy", SafeEncoder.encode(entity_byte.get(SafeEncoder.encode("name"))));

        tairVectorCluster.tvsdelindex(index);
    }

    @Test
    public void tvs_hmgetall() {
        tairVectorCluster.tvsdelindex(index);
        tvs_create_index_and_load_data();

        List<String> entity_string = tairVectorCluster.tvshmget(index, "first_entity",
                VectorBuilderFactory.VECTOR_TAG, "name");
        assertEquals("[0.12,0.23,0.56,0.67,0.78,0.89,0.01,0.89]", entity_string.get(0));
        assertEquals("sammy", entity_string.get(1));

        List<byte[]> entity_byte = tairVectorCluster.tvshmget(
                SafeEncoder.encode(index),
                SafeEncoder.encode("first_entity"),
                SafeEncoder.encode(VectorBuilderFactory.VECTOR_TAG),
                SafeEncoder.encode("name"));
        assertEquals(
                "[0.12,0.23,0.56,0.67,0.78,0.89,0.01,0.89]", SafeEncoder.encode(entity_byte.get(0)));
        assertEquals("sammy", SafeEncoder.encode(entity_byte.get(1)));

        tairVectorCluster.tvsdelindex(index);
    }

    @Test
    public void tvs_del() {
        tairVectorCluster.tvsdelindex(index);
        tvs_create_index_and_load_data();

        long count_string = tvs_del_entity("first_entity");
        assertEquals(1, count_string);

        long count_byte = tvs_del_entity(SafeEncoder.encode("second_entity"));
        assertEquals(1, count_byte);

        tairVectorCluster.tvsdelindex(index);
        tvs_create_index_and_load_data();
        count_string = tvs_del_entity("first_entity", "second_entity");
        assertEquals(2, count_string);

        tairVectorCluster.tvsdelindex(index);
        tvs_create_index_and_load_data();
        count_byte = tvs_del_entity(SafeEncoder.encode("first_entity"), SafeEncoder.encode("second_entity"));
        assertEquals(2, count_byte);

        tairVectorCluster.tvsdelindex(index);
    }

    @Test
    public void tvs_hdel() {
        tairVectorCluster.tvsdelindex(index);
        tvs_create_index_and_load_data();

        long count_string = tairVectorCluster.tvshdel(index, "first_entity", "name");
        assertEquals(1, count_string);
        Map<String, String> entity_string = tairVectorCluster.tvshgetall(index, "first_entity");
        assertTrue(entity_string.size() == 1 && (!entity_string.containsKey("name")));

        long count_byte = tairVectorCluster.tvshdel(
                SafeEncoder.encode(index),
                SafeEncoder.encode("second_entity"),
                SafeEncoder.encode(VectorBuilderFactory.VECTOR_TAG));
        assertEquals(1, count_byte);
        Map<String, String> entity_byte = tairVectorCluster.tvshgetall(index, "second_entity");
        assertTrue(
                entity_byte.size() == 1 && (!entity_byte.containsKey(VectorBuilderFactory.VECTOR_TAG)));

        tairVectorCluster.tvsdelindex(index);
    }

    @Test
    public void tvs_scan() {
        tairVectorCluster.tvsdelindex(index);
        tvs_create_index_and_load_data();

        long cursor = 0;
        HscanParams exhscanParams = new HscanParams();
        exhscanParams.count(1);
        exhscanParams.match("*entit*");
        ScanResult<String> result_string = tairVectorCluster.tvsscan(index, cursor, exhscanParams);
        assert (result_string.getResult().size() >= 1);

        ScanResult<byte[]> entity_byte = tairVectorCluster.tvsscan(SafeEncoder.encode(index), cursor,
                exhscanParams);
        assert (entity_byte.getResult().size() >= 1);

        tairVectorCluster.tvsdelindex(index);
    }

    @Test
    public void tvs_knnsearch() {
        tairVectorCluster.tvsdelindex(index);
        tvs_create_index_and_load_data();

        long topn = 10L;
        String vector = "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]";
        VectorBuilderFactory.Knn<String> result_string = tairVectorCluster.tvsknnsearch(
                index, topn, vector);
        assertEquals(2, result_string.getKnnResults().size());

        VectorBuilderFactory.Knn<byte[]> entity_byte = tairVectorCluster.tvsknnsearch(
                SafeEncoder.encode(index), topn, SafeEncoder.encode(vector));
        assertEquals(2, entity_byte.getKnnResults().size());

        Collection<String> fields = Collections.singletonList("name");
        VectorBuilderFactory.KnnField<String> entity_string_field = tairVectorCluster.tvsknnsearchfield(index, topn, vector, fields);
        assertEquals(2, entity_string_field.getKnnResults().size());
        entity_string_field.getKnnResults().forEach(knn -> {
            assertTrue(knn.getIndex().isEmpty());
            assertEquals(1, knn.getFields().size());
            assertEquals(knn.getFields().get("name"), knn.getId().equals("first_entity") ? "sammy" : "tiddy");
        });

        Collection<byte[]> byte_fields = Collections.singletonList(SafeEncoder.encode("name"));
        VectorBuilderFactory.KnnField<byte[]> entity_byte_field = tairVectorCluster.tvsknnsearchfield(SafeEncoder.encode(index),
                topn, SafeEncoder.encode(vector), byte_fields);
        assertEquals(2, entity_byte_field.getKnnResults().size());
        entity_byte_field.getKnnResults().forEach(knn -> {
            assertTrue(SafeEncoder.encode(knn.getIndex()).isEmpty());
            assertEquals(1, knn.getFields().size());
            assertArrayEquals(knn.getFields().get(SafeEncoder.encode("name")),
                    Arrays.equals(knn.getId(), SafeEncoder.encode("first_entity")) ?
                            SafeEncoder.encode("sammy") : SafeEncoder.encode("tiddy"));
        });

        tairVectorCluster.tvsdelindex(index);
    }

    @Test
    public void tvs_knnsearch_filter() {
        tairVectorCluster.tvsdelindex(index);
        tvs_create_index_and_load_data();

        long topn = 10L;
        String vector = "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]";
        String filter = "name == \"sammy\"";
        VectorBuilderFactory.Knn<String> result_string = tairVectorCluster.tvsknnsearchfilter(
                index, topn, vector, filter);
        assertEquals(1, result_string.getKnnResults().size());

        VectorBuilderFactory.Knn<byte[]> entity_byte = tairVectorCluster.tvsknnsearchfilter(
                SafeEncoder.encode(index), topn, SafeEncoder.encode(vector), SafeEncoder.encode(filter));
        assertEquals(1, entity_byte.getKnnResults().size());

        Collection<String> fields = Collections.singletonList("name");
        VectorBuilderFactory.KnnField<String> entity_string_field = tairVectorCluster.tvsknnsearchfilterfield(index, topn, vector, fields, filter);
        assertEquals(1, entity_string_field.getKnnResults().size());
        entity_string_field.getKnnResults().forEach(knn -> {
            assertTrue(knn.getIndex().isEmpty());
            assertEquals(1, knn.getFields().size());
            assertEquals(knn.getFields().get("name"), knn.getId().equals("first_entity") ? "sammy" : "tiddy");
        });

        Collection<byte[]> byte_fields = Collections.singletonList(SafeEncoder.encode("name"));
        VectorBuilderFactory.KnnField<byte[]> entity_byte_field = tairVectorCluster.tvsknnsearchfilterfield(SafeEncoder.encode(index),
                topn, SafeEncoder.encode(vector), byte_fields, SafeEncoder.encode(filter));
        assertEquals(1, entity_byte_field.getKnnResults().size());
        entity_byte_field.getKnnResults().forEach(knn -> {
            assertTrue(SafeEncoder.encode(knn.getIndex()).isEmpty());
            assertEquals(1, knn.getFields().size());
            assertArrayEquals(knn.getFields().get(SafeEncoder.encode("name")),
                    Arrays.equals(knn.getId(), SafeEncoder.encode("first_entity")) ?
                            SafeEncoder.encode("sammy") : SafeEncoder.encode("tiddy"));
        });

        tairVectorCluster.tvsdelindex(index);
    }

    @Test
    public void tvs_knnsearch_with_params() {
        tairVectorCluster.tvsdelindex(index);
        tvs_create_index_and_load_data();

        long topn = 10L;
        String query = "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]";
        VectorBuilderFactory.Knn<String> result_string = tairVectorCluster.tvsknnsearch(
                index, topn, query, ef_params.toArray(new String[0]));
        assertEquals(2, result_string.getKnnResults().size());

        VectorBuilderFactory.Knn<byte[]> entity_byte = tairVectorCluster.tvsknnsearch(
                SafeEncoder.encode(index), topn, SafeEncoder.encode(query),
                SafeEncoder.encodeMany(ef_params.toArray(new String[0])));
        assertEquals(2, entity_byte.getKnnResults().size());

        Collection<String> fields = Collections.singletonList("name");
        VectorBuilderFactory.KnnField<String> entity_string_field = tairVectorCluster.tvsknnsearchfield(index, topn, query,
                fields, ef_params.toArray(new String[0]));
        assertEquals(2, entity_string_field.getKnnResults().size());
        entity_string_field.getKnnResults().forEach(knn -> {
            assertTrue(knn.getIndex().isEmpty());
            assertEquals(1, knn.getFields().size());
            assertEquals(knn.getFields().get("name"), knn.getId().equals("first_entity") ? "sammy" : "tiddy");
        });

        Collection<byte[]> byte_fields = Collections.singletonList(SafeEncoder.encode("name"));
        VectorBuilderFactory.KnnField<byte[]> entity_byte_field = tairVectorCluster.tvsknnsearchfield(SafeEncoder.encode(index),
                topn, SafeEncoder.encode(query), byte_fields, SafeEncoder.encodeMany(ef_params.toArray(new String[0])));
        assertEquals(2, entity_byte_field.getKnnResults().size());
        entity_byte_field.getKnnResults().forEach(knn -> {
            assertTrue(SafeEncoder.encode(knn.getIndex()).isEmpty());
            assertEquals(1, knn.getFields().size());
            assertArrayEquals(knn.getFields().get(SafeEncoder.encode("name")),
                    Arrays.equals(knn.getId(), SafeEncoder.encode("first_entity")) ?
                            SafeEncoder.encode("sammy") : SafeEncoder.encode("tiddy"));
        });
        tairVectorCluster.tvsdelindex(index);
    }

    @Test
    public void tvs_mknnsearch() {
        tairVectorCluster.tvsdelindex(index);
        tvs_create_index_and_load_data();

        long topn = 10L;
        List<String> vectors = Arrays.asList(
                "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]",
                "[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]");
        Collection<VectorBuilderFactory.Knn<String>> result_string = tairVectorCluster.tvsmknnsearch(index,
                topn, vectors);
        assertEquals(2, result_string.size());
        result_string.forEach(one -> System.out.printf("string: %s\n", one.toString()));

        Collection<VectorBuilderFactory.Knn<byte[]>> entity_byte = tairVectorCluster.tvsmknnsearch(
                SafeEncoder.encode(index),
                topn,
                vectors.stream().map(item -> SafeEncoder.encode(item)).collect(Collectors.toList()));
        assertEquals(2, entity_byte.size());
        result_string.forEach(one -> System.out.printf("byte: %s\n", one.toString()));

        tairVectorCluster.tvsdelindex(index);
    }

    @Test
    public void tvs_mknnsearch_filter() {
        tairVectorCluster.tvsdelindex(index);
        tvs_create_index_and_load_data();

        long topn = 10L;
        List<String> vectors = Arrays.asList(
                "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]",
                "[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]");
        String pattern = "name == \"no-sammy\"";
        Collection<VectorBuilderFactory.Knn<String>> result_string = tairVectorCluster.tvsmknnsearchfilter(
                index, topn,
                vectors, pattern);
        assertEquals(2, result_string.size());
        result_string.forEach(one -> System.out.printf("string: %s\n", one.toString()));

        Collection<VectorBuilderFactory.Knn<byte[]>> entity_byte = tairVectorCluster.tvsmknnsearchfilter(
                SafeEncoder.encode(index),
                topn,
                vectors.stream().map(item -> SafeEncoder.encode(item)).collect(Collectors.toList()),
                SafeEncoder.encode(pattern));
        assertEquals(2, entity_byte.size());
        result_string.forEach(one -> System.out.printf("byte: %s\n", one.toString()));

        tairVectorCluster.tvsdelindex(index);
    }

    @Test
    public void tvs_mknnsearch_with_params() {
        tairVectorCluster.tvsdelindex(index);
        tvs_create_index_and_load_data();

        long topn = 10L;
        List<String> vectors = Arrays.asList(
                "[0.12, 0.23, 0.56, 0.67, 0.78, 0.89, 0.01, 0.89]",
                "[0.22, 0.33, 0.66, 0.77, 0.88, 0.89, 0.11, 0.89]");
        Collection<VectorBuilderFactory.Knn<String>> result_string = tairVectorCluster.tvsmknnsearch(index,
                topn, vectors, ef_params.toArray(new String[0]));
        assertEquals(2, result_string.size());
        result_string.forEach(one -> System.out.printf("string: %s\n", one.toString()));

        Collection<VectorBuilderFactory.Knn<byte[]>> entity_byte = tairVectorCluster.tvsmknnsearch(
                SafeEncoder.encode(index),
                topn,
                vectors.stream().map(item -> SafeEncoder.encode(item)).collect(Collectors.toList()),
                SafeEncoder.encodeMany(ef_params.toArray(new String[0])));
        assertEquals(2, entity_byte.size());
        result_string.forEach(one -> System.out.printf("byte: %s\n", one.toString()));

        tairVectorCluster.tvsdelindex(index);
    }

    @Test
    public void tvs_del_index() {
        tairVectorCluster.tvsdelindex(index);
        tvs_create_index(dims, algorithm, method);

        Map<String, String> schema = tairVectorCluster.tvsgetindex(index);
        assertEquals(algorithm.name(), schema.get("algorithm"));
        assertEquals(method.name(), schema.get("distance_method"));
        assertEquals(String.valueOf(0), schema.get("data_count"));

        assertEquals((long) tairVectorCluster.tvsdelindex(index), 1L);
    }

    @Test
    public void tvs_hincrby_tvs_hincrbyfloat() {
        tairVectorCluster.tvsdelindex(index);
        tvs_create_index_and_load_data();

        tvs_del_entity("first_entity");
        tvs_del_entity("second_entity");
        long tvshincrby = tairVectorCluster.tvshincrby(index, "first_entity", "field", 2);
        assertEquals(2, tvshincrby);
        tvshincrby = tairVectorCluster.tvshincrby(SafeEncoder.encode(index), SafeEncoder.encode("first_entity"), SafeEncoder.encode("field"), 2);
        assertEquals(4, tvshincrby);

        double tvshincrbyfloat = tairVectorCluster.tvshincrbyfloat(index, "second_entity", "field", 1.5d);
        assertEquals(Double.compare(1.5d, tvshincrbyfloat), 0);
        tvshincrbyfloat = tairVectorCluster.tvshincrbyfloat(SafeEncoder.encode(index), SafeEncoder.encode("second_entity"), SafeEncoder.encode("field"), 1.5d);
        assertEquals(Double.compare(3.0d, tvshincrbyfloat), 0);
    }

    public void tvs_getdistance() {
        tvs_create_index(2, algorithm, DistanceMethod.L2);

        for (int i = 0; i < test_data.size(); ++i) {
            String[] args = test_data.get(i);
            Long ret = tairVectorCluster.tvshset(index, String.format("key-%d", i), args[1],
                    Arrays.copyOfRange(args, 2, 6));
            assertEquals(3, ret.longValue());
        }

        List<String> keys = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            String key = "key-" + i;
            keys.add(key);
        }

        // getdistance
        {
            VectorBuilderFactory.Knn<String> result = tairVectorCluster.tvsgetdistance(index, "[0,0]", keys, null, null, null);
            assertEquals(10, result.getKnnResults().size());
        }
        {
            VectorBuilderFactory.Knn<byte[]> result = tairVectorCluster.tvsgetdistance(SafeEncoder.encode(index), SafeEncoder.encode("[0,0]"),
                    keys.stream().map(key -> SafeEncoder.encode(key)).collect(Collectors.toList()), null, null, null);
            assertEquals(10, result.getKnnResults().size());
        }

        // getdistance with TOPN
        {
            VectorBuilderFactory.Knn<String> result = tairVectorCluster.tvsgetdistance(index, "[0,0]", keys, Long.valueOf(5), null, null);
            assertEquals(5, result.getKnnResults().size());
            KnnItem<String> items[] = result.getKnnResults().toArray(new KnnItem[0]);
            for (int i = 0; i < items.length - 1; ++i) {
                assertTrue(items[i].getScore() <= items[i + 1].getScore());
            }
        }
        {
            VectorBuilderFactory.Knn<byte[]> result = tairVectorCluster.tvsgetdistance(SafeEncoder.encode(index), SafeEncoder.encode("[0,0]"),
                    keys.stream().map(key -> SafeEncoder.encode(key)).collect(Collectors.toList()), Long.valueOf(5), null, null);
            assertEquals(5, result.getKnnResults().size());
            KnnItem<byte[]> items[] = result.getKnnResults().toArray(new KnnItem[0]);
            for (int i = 0; i < items.length - 1; ++i) {
                assertTrue(items[i].getScore() <= items[i + 1].getScore());
            }
        }

        // getdistance with MAX_DIST
        {
            VectorBuilderFactory.Knn<String> result = tairVectorCluster.tvsgetdistance(index, "[0,0]", keys, null, Float.valueOf(50), null);
            assertEquals(3, result.getKnnResults().size());
            KnnItem<String> items[] = result.getKnnResults().toArray(new KnnItem[0]);
            for (int i = 0; i < items.length; ++i) {
                assertTrue(items[i].getScore() < 50);
            }
        }
        {
            VectorBuilderFactory.Knn<byte[]> result = tairVectorCluster.tvsgetdistance(SafeEncoder.encode(index), SafeEncoder.encode("[0,0]"),
                    keys.stream().map(key -> SafeEncoder.encode(key)).collect(Collectors.toList()), null, Float.valueOf(50), null);
            assertEquals(3, result.getKnnResults().size());
            KnnItem<byte[]> items[] = result.getKnnResults().toArray(new KnnItem[0]);
            for (int i = 0; i < items.length; ++i) {
                assertTrue(items[i].getScore() < 50);
            }
        }

        // getdistance with FILTER
        {
            VectorBuilderFactory.Knn<String> result = tairVectorCluster.tvsgetdistance(index, "[0,0]", keys, null, null, "name>\"H\"");
            assertEquals(3, result.getKnnResults().size());
            KnnItem<String> items[] = result.getKnnResults().toArray(new KnnItem[0]);
            for (int i = 0; i < items.length; ++i) {
                assertTrue(items[i].getId().equals("key-7") || items[i].getId().equals("key-8") || items[i].getId().equals("key-9"));
            }
        }
        {
            VectorBuilderFactory.Knn<byte[]> result = tairVectorCluster.tvsgetdistance(SafeEncoder.encode(index), SafeEncoder.encode("[0,0]"),
                    keys.stream().map(key -> SafeEncoder.encode(key)).collect(Collectors.toList()), null, null, SafeEncoder.encode("name>\"H\""));
            assertEquals(3, result.getKnnResults().size());
            KnnItem<byte[]> items[] = result.getKnnResults().toArray(new KnnItem[0]);
            for (int i = 0; i < items.length; ++i) {
                assertTrue(SafeEncoder.encode((byte[]) items[i].getId()).equals("key-7") ||
                        SafeEncoder.encode((byte[]) items[i].getId()).equals("key-8") ||
                        SafeEncoder.encode((byte[]) items[i].getId()).equals("key-9"));
            }
        }

        tairVectorCluster.tvsdelindex(index);
    }

    @Test
    public void tvs_expire() throws InterruptedException {
        tairVectorCluster.tvsdelindex(index);
        tvs_create_index_and_load_data();

        List<String> keys = new ArrayList<>();
        int keySize = 10;
        for (int i = 0; i < keySize; ++i) {
            String key = UUID.randomUUID().toString();
            String vector = generateVector(dims);
            assertEquals(3, tairVectorCluster.tvshset(index, key, vector,
                    "name", "tom", "age", String.valueOf(random.nextInt(100))).longValue());
            keys.add(key);
        }

        int expireSecond = 10;
        long unixTime = System.currentTimeMillis() + expireSecond * 1000;
        Map<String, Integer> keyCommands = new HashMap<>();
        for (String key : keys) {
            int rate = random.nextInt(4);
            if (rate == 0) {
                assertTrue(tairVectorCluster.tvshexpire(index, key, expireSecond));
            } else if (rate == 1) {
                assertTrue(tairVectorCluster.tvshpexpire(index, key, expireSecond * 1000));
            } else if (rate == 2) {
                assertTrue(tairVectorCluster.tvshexpireAt(index, key, unixTime / 1000));
            } else {
                assertTrue(tairVectorCluster.tvshpexpireAt(index, key, unixTime));
            }
            keyCommands.put(key, rate);
        }

        for (String key : keys) {
            int rate = keyCommands.get(key);
            if (rate == 0) {
                long ttl = tairVectorCluster.tvshttl(index, key);
                assertTrue(0 < ttl && ttl <= expireSecond);
            } else if (rate == 1) {
                long ttl = tairVectorCluster.tvshpttl(index, key);
                assertTrue(0 < ttl && ttl <= expireSecond * 1000);
            } else if (rate == 2) {
                assertEquals(unixTime / 1000, tairVectorCluster.tvshexpiretime(index, key).longValue());
            } else {
                assertEquals(unixTime, tairVectorCluster.tvshpexpiretime(index, key).longValue());
            }
        }

        // update all key expire after 100 milliseconds
        for (String key : keys) {
            assertTrue(tairVectorCluster.tvshpexpire(index, key, 100));
        }

        Thread.sleep(500);

        for (String key : keys) {
            assertTrue(tairVectorCluster.tvshgetall(index, key).isEmpty());
        }
    }

}
