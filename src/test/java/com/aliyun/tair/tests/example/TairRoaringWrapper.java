package com.aliyun.tair.tests.example;

import com.aliyun.tair.tairroaring.TairRoaring;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class TairRoaringWrapper {
    private static JedisPool jedisPool = null;
    private static final String HOST = "r-xxx.redis.rds.aliyuncs.com";
    private static final int PORT = 6379;

    static {
        jedisPool = new JedisPool(HOST, PORT);
    }

    public static long setbit(String key, long offset, String value) {
        try (Jedis jedis = jedisPool.getResource()) {
            TairRoaring tairRoaring = new TairRoaring(jedis);
            return tairRoaring.trsetbit(key, offset, value);
        } catch (Exception e) {
            e.printStackTrace(); // handle exception
        }
        return -1;
    }

    public static long getbit(String key, long offset) {
        try (Jedis jedis = jedisPool.getResource()) {
            TairRoaring tairRoaring = new TairRoaring(jedis);
            return tairRoaring.trgetbit(key, offset);
        } catch (Exception e) {
            e.printStackTrace(); // handle exception
        }
        return -1;
    }

    public static void main(String[] args) {
        setbit("key", 10, "1");
        System.out.println(getbit("key", 10));
    }
}
