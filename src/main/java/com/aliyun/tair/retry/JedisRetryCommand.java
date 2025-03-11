package com.aliyun.tair.retry;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.valkey.Jedis;
import io.valkey.JedisPool;
import io.valkey.exceptions.JedisException;

public abstract class JedisRetryCommand<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(JedisRetryCommand.class);

    private final JedisPool jedisPool;
    private final int maxRetries;
    private final Duration maxTotalRetriesDuration;

    public JedisRetryCommand(JedisPool jedisPool, int maxRetries, Duration maxTotalRetriesDuration) {
        this.jedisPool = jedisPool;
        this.maxRetries = maxRetries;
        this.maxTotalRetriesDuration = maxTotalRetriesDuration;
    }

    public abstract T execute(Jedis connection);

    /**
     * This method will retry maxRetries times and check
     * whether the time reaches maxTotalRetriesDuration
     * If any one of the conditions is met, JedisException will be thrown
     */
    public T runWithRetries() {
        Instant deadline = Instant.now().plus(maxTotalRetriesDuration);
        Exception lastException = null;

        for (int retriesLeft = maxRetries; retriesLeft > 0; retriesLeft--) {
            Jedis connection = null;
            try {
                connection = jedisPool.getResource();
                return execute(connection);
            } catch (Exception jce) {
                lastException = jce;
                LOGGER.warn("Redis throw an exception: {}", connection, jce);
                sleep(getBackoffSleepMillis(retriesLeft, deadline));
            } finally {
                releaseConnection(connection);
            }
            if (Instant.now().isAfter(deadline)) {
                throw new JedisException("Command retry deadline exceeded.");
            }
        }

        JedisException maxRetriesException
            = new JedisException("No more command retries left.");
        maxRetriesException.addSuppressed(lastException);
        throw maxRetriesException;
    }

    private static long getBackoffSleepMillis(int retriesLeft, Instant deadline) {
        if (retriesLeft <= 0) {
            return 0;
        }

        long millisLeft = Duration.between(Instant.now(), deadline).toMillis();
        if (millisLeft < 0) {
            throw new JedisException("Command retry deadline exceeded.");
        }

        /**
         * Theoretical data, the actual TIMEOUT should be considered
         * At time 0.0, 5 attempts left, sleep (10s/(5*5)) = 0.4s
         * At time 0.4, 4 attempts left, sleep (9.6s/(4*4)) = 0.6s
         * At time 1.0, 3 attempts left, sleep (9s/(3*3)) = 1.0s
         * At time 2.0, 2 attempts left, sleep (8/(2*2)) = 2.0s
         * At time 6.0, 1 attempts left, sleep (4/(1*1)) = 4.0s
         */
        return millisLeft / ((long)retriesLeft * (retriesLeft + 1));
    }

    private void sleep(long sleepMillis) {
        try {
            TimeUnit.MILLISECONDS.sleep(sleepMillis);
        } catch (InterruptedException e) {
            throw new JedisException(e);
        }
    }

    private void releaseConnection(Jedis connection) {
        if (connection != null) {
            connection.close();
        }
    }
}
