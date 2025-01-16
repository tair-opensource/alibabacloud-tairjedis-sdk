package com.aliyun.tair.tests.mcommand;

import com.aliyun.tair.mcommamd.AliyunRedisCommand;
import com.aliyun.tair.mcommamd.results.SlotAndNodeIndex;
import com.aliyun.tair.tests.TestBase;
import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisMonitor;
import com.aliyun.tair.jedis3.ScanResult;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class AliyunRedisCommandTest extends TestBase {

    @Test
    public void infoKeyTest() {
        try (Jedis jedis = jedisPool.getResource()) {
            AliyunRedisCommand aliyunRedisCommand = new AliyunRedisCommand(jedis);
            SlotAndNodeIndex slotAndNodeIndex = aliyunRedisCommand.infoKey("key");
            Assert.assertNotNull(slotAndNodeIndex);
            Assert.assertEquals(slotAndNodeIndex.getSlot(), 12539);
            Assert.assertEquals(slotAndNodeIndex.getNodeIndex(), 1);
        } catch (Exception e) {
            if (e.getMessage().contains("unknown command") || e.getMessage().contains("syntax error")) {
                // ignore test
            } else {
                Assert.fail();
            }
        }
    }

    @Test
    public void iinfoTest() {
        try (Jedis jedis = jedisPool.getResource()) {
            AliyunRedisCommand aliyunRedisCommand = new AliyunRedisCommand(jedis);
            String info = aliyunRedisCommand.iInfo(0);
            Assert.assertFalse(info.isEmpty());
        } catch (Exception e) {
            if (e.getMessage().contains("unknown command")) {
                // ignore test
            } else {
                Assert.fail();
            }
        }
    }

    @Test
    public void iscanTest() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.flushAll();
            for (int i = 0; i < 10; i++) {
                jedis.set(i + "", i + "");
            }

            AliyunRedisCommand aliyunRedisCommand = new AliyunRedisCommand(jedis);
            ScanResult<String> result = aliyunRedisCommand.iScan(0, "0");
            assertEquals("0", result.getCursor());
            assertFalse(result.getResult().isEmpty());
        } catch (Exception e) {
            if (e.getMessage().contains("unknown command")) {
                // ignore test
            } else {
                Assert.fail();
            }
        }
    }

    @Test
    public void imonitorTest() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del("monitor_key");
            AliyunRedisCommand aliyunRedisCommand = new AliyunRedisCommand(jedis);
            SlotAndNodeIndex slotAndNodeIndex = aliyunRedisCommand.infoKey("monitor_key");
            int nodeIndex = slotAndNodeIndex.getNodeIndex();

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        // sleep 1s to make sure that monitor thread runs first
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                    for (int i = 0; i < 5; i++) {
                        try (Jedis j = jedisPool.getResource()) {
                            j.incr("monitor_key");
                        }
                    }
                }
            }).start();

            aliyunRedisCommand.iMonitor(nodeIndex, new JedisMonitor() {
                private int count = 0;

                @Override
                public void onCommand(String command) {
                    if (command.contains("INCR")) {
                        count++;
                    }
                    if (count == 5) {
                        client.disconnect();
                    }
                }
            });
        } catch (Exception e) {
            if (e.getMessage().contains("unknown command") || e.getMessage().contains("syntax error")) {
                // ignore test
            } else {
                Assert.fail();
            }
        }
    }
}
