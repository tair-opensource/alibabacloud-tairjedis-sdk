package com.aliyun.tair.mcommamd;

import java.util.ArrayList;
import java.util.List;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.jedis3.ScanParams;
import com.aliyun.tair.jedis3.ScanResult;
import com.aliyun.tair.mcommamd.factory.AliyunRedisCommandBuilderFactory;
import com.aliyun.tair.mcommamd.results.SlotAndNodeIndex;
import io.valkey.BuilderFactory;
import io.valkey.Jedis;
import io.valkey.JedisMonitor;
import io.valkey.JedisPool;
import io.valkey.Protocol.Command;
import io.valkey.util.SafeEncoder;

public class AliyunRedisCommand {
    private Jedis jedis;
    private JedisPool jedisPool;

    public AliyunRedisCommand(Jedis jedis) {
        this.jedis = jedis;
    }

    public AliyunRedisCommand(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    private Jedis getJedis() {
        if (jedisPool != null) {
            return jedisPool.getResource();
        }
        return jedis;
    }

    private void releaseJedis(Jedis jedis) {
        if (jedisPool != null) {
            jedis.close();
        }
    }

    /**
     * Query the slot and db to which the key belongs. Redis' native info command can take at most one optional section
     * (info [section]). Currently, in ApsaraDB for Redis cluster instances, some commands restrict all keys to be in
     * the same slot. The info key command is convenient for users to query whether certain keys are in the same slot
     * or db node.
     * @param key the key
     * @return SlotAndNodeIndex
     */
    public SlotAndNodeIndex infoKey(String key) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(Command.INFO, "KEY", key);
            return AliyunRedisCommandBuilderFactory.SlotAndNodeIndexResult.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public SlotAndNodeIndex infoKey(byte[] key) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(Command.INFO, "KEY".getBytes(), key);
            return AliyunRedisCommandBuilderFactory.SlotAndNodeIndexResult.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Execute the info command on the specified Redis node.
     * @param index the node index, see {@link AliyunRedisCommand#infoKey(String)}.
     * @return the info content.
     */
    public String iInfo(int index) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.IINFO, String.valueOf(index));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public String iInfo(int index, String section) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.IINFO, String.valueOf(index), section);
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Similar to the iinfo command, but can only be used in read-write separation mode. The idx of a readonly slave
     * is added to the usage, which is used to specify the number of readonly slaves to execute the info command.
     * It can be used to execute the info command on the specified readonly slave in a read-write separation cluster.
     * If used in a non-read-write split cluster, an error will be returned.
     * @param index the node index, see {@link AliyunRedisCommand#infoKey(String)}.
     * @param roIndex the read-only node index.
     * @return the info content.
     */
    public String rIInfo(int index, int roIndex) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.RIINFO, String.valueOf(index), String.valueOf(roIndex));
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    public String rIInfo(int index, int roIndex, String section) {
        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.RIINFO, String.valueOf(index), String.valueOf(roIndex), section);
            return BuilderFactory.STRING.build(obj);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * In cluster mode, the scan command can be executed on the specified db node. Based on the scan command,
     * a parameter is extended to specify db_idx. The range of db_idx is [0, nodecount]. The nodecount can be obtained
     * through the info command or viewed from the console.
     * @param index
     * @param cursor
     * @return
     */
    public ScanResult<String> iScan(int index, String cursor) {
        return iScan(index, cursor, new ScanParams());
    }

    public ScanResult<String> iScan(int index, String cursor, ScanParams params) {
        final List<byte[]> args = new ArrayList<>();
        args.add(String.valueOf(index).getBytes());
        args.add(cursor.getBytes());
        args.addAll(params.getParams());

        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.ISCAN, args.toArray(new byte[args.size()][]));
            List<Object> result = BuilderFactory.RAW_OBJECT_LIST.build(obj);
            String newcursor = new String((byte[]) result.get(0));
            List<String> results = new ArrayList<>();
            List<byte[]> rawResults = (List<byte[]>) result.get(1);
            for (byte[] bs : rawResults) {
                results.add(SafeEncoder.encode(bs));
            }
            return new ScanResult<>(newcursor, results);
        } finally {
            releaseJedis(jedis);
        }
    }

    public ScanResult<byte[]> iScan(int index, byte[] cursor) {
        return iScan(index, cursor);
    }

    public ScanResult<byte[]> iScan(int index, byte[] cursor, ScanParams params) {
        final List<byte[]> args = new ArrayList<>();
        args.add(String.valueOf(index).getBytes());
        args.add(cursor);
        args.addAll(params.getParams());

        Jedis jedis = getJedis();
        try {
            Object obj = jedis.sendCommand(ModuleCommand.ISCAN, args.toArray(new byte[args.size()][]));
            List<Object> result = BuilderFactory.RAW_OBJECT_LIST.build(obj);
            byte[] newcursor = (byte[]) result.get(0);
            List<byte[]> rawResults = (List<byte[]>) result.get(1);
            return new ScanResult<>(newcursor, rawResults);
        } finally {
            releaseJedis(jedis);
        }
    }

    /**
     * Specifies to obtain the monitor result of a index node.
     * @param index the node index.
     * @param jedisMonitor process result from redis.
     */
    public void iMonitor(int index, JedisMonitor jedisMonitor) {
        Jedis jedis = getJedis();
        try {
            jedis.sendCommand(ModuleCommand.IMONITOR, String.valueOf(index));
            jedisMonitor.proceed(getJedis().getClient());
        } catch (Exception e) {
            throw e;
        } finally {
            releaseJedis(jedis);
        }
    }

    public void rIMonitor(int index, int roIndex, JedisMonitor jedisMonitor) {
        Jedis jedis = getJedis();
        try {
            jedis.sendCommand(ModuleCommand.RIMONITOR, String.valueOf(index), String.valueOf(roIndex));
            jedisMonitor.proceed(getJedis().getClient());
        } catch (Exception e) {
            throw e;
        } finally {
            releaseJedis(jedis);
        }
    }
}
