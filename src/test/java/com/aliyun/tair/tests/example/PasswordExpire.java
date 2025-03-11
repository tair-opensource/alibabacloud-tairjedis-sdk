package com.aliyun.tair.tests.example;

import com.aliyun.tair.tairhash.TairHash;
import com.aliyun.tair.tairhash.params.ExhsetParams;
import io.valkey.JedisPool;
import io.valkey.JedisPoolConfig;

public class PasswordExpire {
    // init timeout
    private static final int DEFAULT_CONNECTION_TIMEOUT = 5000;
    // api timeout
    private static final int DEFAULT_SO_TIMEOUT = 2000;
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 6379;
    private static final String PASSWORD = null;
    private static JedisPool jedisPool = null;
    private static TairHash tairHash = null;
    private static final JedisPoolConfig config = new JedisPoolConfig();

    static {
        // JedisPool config: https://help.aliyun.com/document_detail/98726.html
        config.setMaxTotal(32);
        config.setMaxIdle(32);
        config.setMaxIdle(20);

        jedisPool = new JedisPool(config, HOST, PORT, DEFAULT_CONNECTION_TIMEOUT,
            DEFAULT_SO_TIMEOUT, PASSWORD, 0, null);
        tairHash = new TairHash(jedisPool);
    }

    /**
     * Add a user and password with a timeout.
     * @param key the key
     * @param user the user
     * @param password the password
     * @param timeout the password expiration time
     * @return success: true, fail: false
     */
    public static boolean addUserPass(final String key, final String user, final String password, final int timeout) {
        try {
            Long ret = tairHash.exhset(key, user, password, ExhsetParams.ExhsetParams().ex(timeout));
            return ret == 1;
        } catch (Exception e) {
            // logger.error(e);
            return false;
        }
    }

    public static void main(String[] args) throws Exception {
        String key = "PasswordExpire";
        addUserPass(key, "user1", "password1", 5);
        addUserPass(key, "user2", "password2", 10);
        Thread.sleep(5000);
        System.out.println(tairHash.exhgetAll(key));
    }
}
