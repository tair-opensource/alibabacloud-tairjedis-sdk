package com.aliyun.tair.tests.retry;

import java.time.Duration;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.retry.JedisRetryCommand;
import com.aliyun.tair.tests.TestBase;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisException;

public class JedisRetryCommandTest extends TestBase {
    @Test
    public void retrySuccess() {
        int maxRetries = 5;
        Duration maxTotalRetriesDuration = Duration.ofSeconds(10);
        try {
            String ret = new JedisRetryCommand<String>(jedisPool, maxRetries, maxTotalRetriesDuration) {
                @Override
                public String execute(Jedis connection) {
                    return connection.set("key", "value");
                }
            }.runWithRetries();

            Assert.assertEquals("OK", ret);
        } catch (JedisException e) {
            Assert.fail();
        }
    }

    @Test
    @Ignore
    public void retrySuccessInMiddle() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                getJedis().sendCommand(ModuleCommand.DEBUG, "sleep", "5");
            }
        }).start();

        int maxRetries = 5;
        Duration maxTotalRetriesDuration = Duration.ofSeconds(10);
        try {
            String ret = new JedisRetryCommand<String>(jedisPool, maxRetries, maxTotalRetriesDuration) {
                @Override
                public String execute(Jedis connection) {
                    return connection.set("key", "value");
                }
            }.runWithRetries();

            Assert.assertEquals("OK", ret);
        } catch (JedisException e) {
            Assert.fail();
        }
    }

    @Test
    @Ignore
    public void retryFail() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                getJedis().sendCommand(ModuleCommand.DEBUG, "sleep", "15");
            }
        }).start();

        int maxRetries = 5;
        Duration maxTotalRetriesDuration = Duration.ofSeconds(10);
        try {
            String ret = new JedisRetryCommand<String>(jedisPool, maxRetries, maxTotalRetriesDuration) {
                @Override
                public String execute(Jedis connection) {
                    return connection.set("key", "value");
                }
            }.runWithRetries();

            Assert.assertEquals("OK", ret);
        } catch (JedisException e) {
            Assert.assertTrue(e.getMessage().contains("Command retry deadline exceeded"));
        }
    }
}
