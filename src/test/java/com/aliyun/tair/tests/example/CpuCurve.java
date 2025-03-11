package com.aliyun.tair.tests.example;

import com.aliyun.tair.tairts.TairTs;
import com.aliyun.tair.tairts.results.ExtsSkeyResult;
import io.valkey.JedisPool;
import io.valkey.JedisPoolConfig;

public class CpuCurve {
    // init timeout
    private static final int DEFAULT_CONNECTION_TIMEOUT = 5000;
    // api timeout
    private static final int DEFAULT_SO_TIMEOUT = 2000;
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 6379;
    private static final String PASSWORD = null;
    private static JedisPool jedisPool = null;
    private static TairTs tairTs = null;
    private static final JedisPoolConfig config = new JedisPoolConfig();

    static {
        // JedisPool config: https://help.aliyun.com/document_detail/98726.html
        config.setMaxTotal(32);
        config.setMaxIdle(32);
        config.setMaxIdle(20);

        jedisPool = new JedisPool(config, HOST, PORT, DEFAULT_CONNECTION_TIMEOUT,
            DEFAULT_SO_TIMEOUT, PASSWORD, 0, null);
        tairTs = new TairTs(jedisPool);
    }

    /**
     * add point to CPU_LOAD series
     * @param ip machine ip
     * @param ts the timestamp
     * @param value the value
     * @return success: true, fail: false.
     */
    public static boolean addPoint(final String ip, final String ts, final double value) {
        try {
            String result = tairTs.extsadd("CPU_LOAD", ip, ts, value);
            if ("OK".equals(result)) {
                return true;
            }
        } catch (Exception e) {
            // logger.error(e);
        }
        return false;
    }

    /**
     * Range all data in a certain time series
     * @param ip machine ip
     * @param startTs start timestamp
     * @param endTs end timestamp
     * @return
     */
    public static ExtsSkeyResult rangePoint(final String ip, final String startTs, final String endTs) {
        try {
            return tairTs.extsrange("CPU_LOAD", ip, startTs, endTs);
        } catch (Exception e) {
            // logger.error(e);
        }
        return null;
    }

    public static void main(String[] args) {
        addPoint("127.0.0.1", "*", 10);
        addPoint("127.0.0.1", "*", 20);
        addPoint("127.0.0.1", "*", 30);

        rangePoint("127.0.0.1", "1587889046161", "*");
    }
}
