package com.aliyun.tair.tests.example;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import com.aliyun.tair.tairstring.TairString;
import io.valkey.Jedis;
import io.valkey.JedisPool;
import io.valkey.JedisPoolConfig;
import io.valkey.params.SetParams;

public class DistributeLock {
    // init timeout
    private static final int DEFAULT_CONNECTION_TIMEOUT = 5000;
    // api timeout
    private static final int DEFAULT_SO_TIMEOUT = 2000;
    private static final String HOST = "r-xxx.redis.rds.aliyuncs.com";
    private static final int PORT = 6379;
    private static final String PASSWORD = "xxx";
    private static JedisPool jedisPool = null;
    private static TairString tairString = null;
    private static final JedisPoolConfig config = new JedisPoolConfig();

    static {
        // JedisPool config: https://help.aliyun.com/document_detail/98726.html
        config.setMaxTotal(32);
        config.setMaxIdle(32);
        config.setMaxIdle(20);

        jedisPool = new JedisPool(config, HOST, PORT, DEFAULT_CONNECTION_TIMEOUT,
            DEFAULT_SO_TIMEOUT, PASSWORD, 0, null);
        tairString = new TairString(jedisPool);
    }

    /**
     * locks atomically via set with NX flag
     * @param lockKey the key
     * @param requestId prevents the lock from being deleted by mistake
     * @param expireTime prevent the deadlock of business machine downtime
     * @return success: true, fail: false.
     */
    public static boolean tryGetDistributedLock(String lockKey, String requestId, int expireTime) {
        try (Jedis jedis = jedisPool.getResource()) {
            String result = jedis.set(lockKey, requestId, SetParams.setParams().nx().ex(expireTime));
            if ("OK".equals(result)) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * atomically releases the lock via the CAD command
     * @param lockKey the key
     * @param requestId ensures that the released lock is added by itself
     * @return success: true, fail: false.
     */
    public static boolean releaseDistributedLock(String lockKey, String requestId) {
        try {
            Long ret = tairString.cad(lockKey, requestId);
            if (1 == ret) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
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
