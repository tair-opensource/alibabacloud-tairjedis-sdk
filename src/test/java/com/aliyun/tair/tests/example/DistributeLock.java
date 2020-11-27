package com.aliyun.tair.tests.example;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import com.aliyun.tair.tairstring.TairString;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.params.SetParams;

public class DistributeLock {
    // 初始化连接超时时间
    private static final int DEFAULT_CONNECTION_TIMEOUT = 5000;
    // 查询超时时间
    private static final int DEFAULT_SO_TIMEOUT = 2000;
    private static final String HOST = "r-xxx.redis.rds.aliyuncs.com";
    private static final int PORT = 6379;
    private static final String PASSWORD = "xxx";
    private static JedisPool jedisPool = null;
    private static final JedisPoolConfig config = new JedisPoolConfig();

    static {
        // 参数设置最佳实践可参考：https://help.aliyun.com/document_detail/98726.html
        config.setMaxTotal(32);
        config.setMaxIdle(32);
        config.setMaxIdle(20);

        jedisPool = new JedisPool(config, HOST, PORT, DEFAULT_CONNECTION_TIMEOUT,
            DEFAULT_SO_TIMEOUT, PASSWORD, 0, null);
    }

    public static boolean tryGetDistributedLock(String lockKey, String requestId, int expireTime) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            String result = jedis.set(lockKey, requestId, SetParams.setParams().nx().ex(expireTime));
            if ("OK".equals(result)) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (jedis != null) {
                jedis.close();  // Just return to pool
            }
        }
        return false;
    }

    public static boolean releaseDistributedLock(String lockKey, String requestId) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            TairString tairString = new TairString(jedis);
            Long ret = tairString.cad(lockKey, requestId);
            if (1 == ret) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return false;
    }

    static int total = 0;
    public static void main(String[] args) throws Exception {
        final String LOCK_KEY = "lock_key";
        final int EXPIRE_TIME = 10;
        int thread_num = 10;

        final CountDownLatch latch = new CountDownLatch(thread_num);
        for (int i = 0; i < thread_num; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    String requsetId = UUID.randomUUID().toString();
                    for (int j = 0; j < 10; j++) {
                        // 要是没拿到锁就去尝试
                        for (;;) {
                            if (tryGetDistributedLock(LOCK_KEY, requsetId, EXPIRE_TIME)) {
                                System.out.println("I am thread: " + Thread.currentThread().getName() + ", lock success, total: " + total);
                                total++;
                                releaseDistributedLock(LOCK_KEY, requsetId);
                                System.out.println("I am thread: " + Thread.currentThread().getName() + ", unlock success, total: " + total);
                                break;
                            }
                        }
                    }
                    latch.countDown();
                }
            }).start();
        }
        latch.await();
        System.out.println("Final total is: " + total);
    }
}
