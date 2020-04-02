package com.aliyun.tair.mcommamd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisClusterCommand;
import redis.clients.jedis.JedisClusterHostAndPortMap;
import redis.clients.jedis.util.JedisClusterCRC16;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TairCluster extends JedisCluster {
    private static final Logger LOGGER = LoggerFactory.getLogger(TairCluster.class);

    private static int keepAliveTime = 5000;
    private static ThreadPoolExecutor backend;
    private static BlockingQueue<Runnable> blockingQueue;
    private static int blockingQueueSize = HASHSLOTS + 1;
    private static int corePoolSize = Runtime.getRuntime().availableProcessors();
    private static int maximumPoolSize = Runtime.getRuntime().availableProcessors() * 2;

    static {
        blockingQueue = new ArrayBlockingQueue<Runnable>(blockingQueueSize);
        backend = new ThreadPoolExecutor(corePoolSize, maximumPoolSize,
            keepAliveTime, TimeUnit.MILLISECONDS, blockingQueue);
        backend.prestartAllCoreThreads();
    }

    public void setThreadPoll(ThreadPoolExecutor threadPoll) {
        if (backend != threadPoll && backend != null) {
            backend.shutdown();
        }
        backend = threadPoll;
    }

    @Override
    public void close() {
        super.close();

        if (backend != null) {
            backend.shutdown();
        }
    }

    public TairCluster(HostAndPort node) {
        super(node);
    }

    public TairCluster(HostAndPort node, int timeout) {
        super(node, timeout);
    }

    public TairCluster(HostAndPort node, int timeout, int maxAttempts) {
        super(node, timeout, maxAttempts);
    }

    public TairCluster(HostAndPort node, GenericObjectPoolConfig poolConfig) {
        super(node, poolConfig);
    }

    public TairCluster(HostAndPort node, int timeout, GenericObjectPoolConfig poolConfig) {
        super(node, timeout, poolConfig);
    }

    public TairCluster(HostAndPort node, int timeout, int maxAttempts,
        GenericObjectPoolConfig poolConfig) {
        super(node, timeout, maxAttempts, poolConfig);
    }

    public TairCluster(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts,
        GenericObjectPoolConfig poolConfig) {
        super(node, connectionTimeout, soTimeout, maxAttempts, poolConfig);
    }

    public TairCluster(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, String password,
        GenericObjectPoolConfig poolConfig) {
        super(node, connectionTimeout, soTimeout, maxAttempts, password, poolConfig);
    }

    public TairCluster(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, String password,
        String clientName, GenericObjectPoolConfig poolConfig) {
        super(node, connectionTimeout, soTimeout, maxAttempts, password, clientName, poolConfig);
    }

    public TairCluster(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, String password,
        String clientName, GenericObjectPoolConfig poolConfig, boolean ssl) {
        super(node, connectionTimeout, soTimeout, maxAttempts, password, clientName, poolConfig, ssl);
    }

    public TairCluster(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, String password,
        String clientName, GenericObjectPoolConfig poolConfig, boolean ssl,
        SSLSocketFactory sslSocketFactory, SSLParameters sslParameters,
        HostnameVerifier hostnameVerifier, JedisClusterHostAndPortMap hostAndPortMap) {
        super(node, connectionTimeout, soTimeout, maxAttempts, password, clientName, poolConfig, ssl, sslSocketFactory,
            sslParameters, hostnameVerifier, hostAndPortMap);
    }

    public TairCluster(Set<HostAndPort> nodes) {
        super(nodes);
    }

    public TairCluster(Set<HostAndPort> nodes, int timeout) {
        super(nodes, timeout);
    }

    public TairCluster(Set<HostAndPort> nodes, int timeout, int maxAttempts) {
        super(nodes, timeout, maxAttempts);
    }

    public TairCluster(Set<HostAndPort> nodes, GenericObjectPoolConfig poolConfig) {
        super(nodes, poolConfig);
    }

    public TairCluster(Set<HostAndPort> nodes, int timeout,
        GenericObjectPoolConfig poolConfig) {
        super(nodes, timeout, poolConfig);
    }

    public TairCluster(Set<HostAndPort> jedisClusterNode, int timeout, int maxAttempts,
        GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, timeout, maxAttempts, poolConfig);
    }

    public TairCluster(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts,
        GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, poolConfig);
    }

    public TairCluster(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts,
        String password, GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, password, poolConfig);
    }

    public TairCluster(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts,
        String password, String clientName, GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, password, clientName, poolConfig);
    }

    public TairCluster(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts,
        String password, String clientName, GenericObjectPoolConfig poolConfig, boolean ssl) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, password, clientName, poolConfig, ssl);
    }

    public TairCluster(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts,
        String password, String clientName, GenericObjectPoolConfig poolConfig, boolean ssl,
        SSLSocketFactory sslSocketFactory, SSLParameters sslParameters,
        HostnameVerifier hostnameVerifier, JedisClusterHostAndPortMap hostAndPortMap) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, password, clientName, poolConfig, ssl,
            sslSocketFactory, sslParameters, hostnameVerifier, hostAndPortMap);
    }

    @Override
    public Long exists(final String... keys) {
        long ret = 0;
        Map<String, List<String>> map = preTreatKey(keys);
        List<FutureTask<Long>> list = new ArrayList<FutureTask<Long>>();
        for (final Map.Entry<String, List<String>> entry : map.entrySet()) {
            final String[] subkeys = new String[entry.getValue().size()];
            entry.getValue().toArray(subkeys);
            FutureTask<Long> task = new FutureTask<Long>(new Callable<Long>() {
                public Long call() throws Exception {
                    return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                        @Override
                        public Long execute(Jedis connection) {
                            return connection.texists(subkeys);
                        }
                    }.runMcommand(subkeys.length, subkeys);
                }
            });
            backend.submit(task);
            list.add(task);
        }

        Exception exc = null;
        for (FutureTask<Long> task : list) {
            if (exc == null) {
                try {
                    ret += task.get();
                } catch (Exception e) {
                    exc = e;
                    LOGGER.error("TairCluster.exists " + e.toString());
                }
            } else {
                task.cancel(false);
            }
        }
        if (exc != null) {
            if (exc.getCause() instanceof RuntimeException) {
                throw (RuntimeException)exc.getCause();
            }
            return null;
        }
        return ret;
    }

    @Override
    public Long exists(final byte[]... keys) {
        long ret = 0;
        Map<String, List<byte[]>> map = preTreatKey(keys);
        List<FutureTask<Long>> list = new ArrayList<FutureTask<Long>>();
        for (final Map.Entry<String, List<byte[]>> entry : map.entrySet()) {
            final byte[][] subkeys = new byte[entry.getValue().size()][];
            entry.getValue().toArray(subkeys);
            FutureTask<Long> task = new FutureTask<Long>(new Callable<Long>() {
                public Long call() throws Exception {
                    return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                        @Override
                        public Long execute(Jedis connection) {
                            return connection.texists(subkeys);
                        }
                    }.runBinaryMcommand(subkeys.length, subkeys);
                }
            });
            backend.submit(task);
            list.add(task);
        }

        Exception exc = null;
        for (FutureTask<Long> task : list) {
            if (exc == null) {
                try {
                    ret += task.get();
                } catch (Exception e) {
                    exc = e;
                    LOGGER.error("TairCluster.exists " + e.toString());
                }
            } else {
                task.cancel(false);
            }
        }
        if (exc != null) {
            if (exc.getCause() instanceof RuntimeException) {
                throw (RuntimeException)exc.getCause();
            }
            return null;
        }
        return ret;
    }

    @Override
    public Long del(final String... keys) {
        long ret = 0;
        Map<String, List<String>> map = preTreatKey(keys);
        List<FutureTask<Long>> list = new ArrayList<FutureTask<Long>>();
        for (final Map.Entry<String, List<String>> entry : map.entrySet()) {
            final String[] subkeys = new String[entry.getValue().size()];
            entry.getValue().toArray(subkeys);
            FutureTask<Long> task = new FutureTask<Long>(new Callable<Long>() {
                public Long call() throws Exception {
                    return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                        @Override
                        public Long execute(Jedis connection) {
                            return connection.tdel(subkeys);
                        }
                    }.runMcommand(subkeys.length, subkeys);
                }
            });
            backend.submit(task);
            list.add(task);
        }

        Exception exc = null;
        for (FutureTask<Long> task : list) {
            if (exc == null) {
                try {
                    ret += task.get();
                } catch (Exception e) {
                    exc = e;
                    LOGGER.error("TairCluster.del " + e.toString());
                }
            } else {
                task.cancel(false);
            }
        }
        if (exc != null) {
            if (exc.getCause() instanceof RuntimeException) {
                throw (RuntimeException)exc.getCause();
            }
            return null;
        }
        return ret;
    }

    @Override
    public Long del(final byte[]... keys) {
        long ret = 0;
        Map<String, List<byte[]>> map = preTreatKey(keys);
        List<FutureTask<Long>> list = new ArrayList<FutureTask<Long>>();
        for (final Map.Entry<String, List<byte[]>> entry : map.entrySet()) {
            final byte[][] subkeys = new byte[entry.getValue().size()][];
            entry.getValue().toArray(subkeys);
            FutureTask<Long> task = new FutureTask<Long>(new Callable<Long>() {
                public Long call() throws Exception {
                    return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                        @Override
                        public Long execute(Jedis connection) {
                            return connection.tdel(subkeys);
                        }
                    }.runBinaryMcommand(subkeys.length, subkeys);
                }
            });
            backend.submit(task);
            list.add(task);
        }

        Exception exc = null;
        for (FutureTask<Long> task : list) {
            if (exc == null) {
                try {
                    ret += task.get();
                } catch (Exception e) {
                    exc = e;
                    LOGGER.error("TairCluster.del " + e.toString());
                }
            } else {
                task.cancel(false);
            }
        }
        if (exc != null) {
            if (exc.getCause() instanceof RuntimeException) {
                throw (RuntimeException)exc.getCause();
            }
            return null;
        }
        return ret;
    }

    @Override
    public List<String> mget(String... keys) {
        Map<String, List<String>> map = preTreatKey(keys);
        List<String> ret = new ArrayList<String>();
        Map<String[], FutureTask<List<String>>> retMap = new HashMap<String[], FutureTask<List<String>>>();
        for (final Map.Entry<String, List<String>> entry : map.entrySet()) {
            final String[] subkeys = new String[entry.getValue().size()];
            entry.getValue().toArray(subkeys);
            FutureTask<List<String>> task = new FutureTask<List<String>>(new Callable<List<String>>() {
                public List<String> call() throws Exception {
                    return new JedisClusterCommand<List<String>>(connectionHandler, maxAttempts) {
                        @Override
                        public List<String> execute(Jedis connection) {
                            return connection.tmget(subkeys);
                        }
                    }.runMcommand(subkeys.length, subkeys);
                }
            });
            backend.submit(task);
            retMap.put(subkeys, task);
        }
        Map<String, String> resultMerge = new HashMap<String, String>();
        Exception exc = null;
        for (Map.Entry<String[], FutureTask<List<String>>> entry : retMap.entrySet()) {
            if (exc == null) {
                try {
                    List<String> subResult = entry.getValue().get();
                    for (int i = 0; i < entry.getKey().length; i++) {
                        resultMerge.put(entry.getKey()[i], subResult.get(i));
                    }
                } catch (Exception e) {
                    exc = e;
                    LOGGER.error("TairCluster.mget " + e.toString());
                }
            } else {
                entry.getValue().cancel(false);
            }
        }
        if (exc != null) {
            if (exc.getCause() instanceof RuntimeException) {
                throw (RuntimeException)exc.getCause();
            }
            return null;
        }
        for (String key : keys) {
            if (resultMerge.containsKey(key)) {
                ret.add(resultMerge.get(key));
            } else {
                ret.add(null);
            }
        }
        return ret;
    }

    @Override
    public List<byte[]> mget(final byte[]... keys) {
        Map<String, List<byte[]>> map = preTreatKey(keys);
        List<byte[]> ret = new ArrayList<byte[]>();
        Map<byte[][], FutureTask<List<byte[]>>> retMap = new HashMap<byte[][], FutureTask<List<byte[]>>>();
        for (final Map.Entry<String, List<byte[]>> entry : map.entrySet()) {
            final byte[][] subkeys = new byte[entry.getValue().size()][];
            entry.getValue().toArray(subkeys);
            FutureTask<List<byte[]>> task = new FutureTask<List<byte[]>>(new Callable<List<byte[]>>() {
                public List<byte[]> call() throws Exception {
                    return new JedisClusterCommand<List<byte[]>>(connectionHandler, maxAttempts) {
                        @Override
                        public List<byte[]> execute(Jedis connection) {
                            return connection.tmget(subkeys);
                        }
                    }.runBinaryMcommand(subkeys.length, subkeys);
                }
            });
            backend.submit(task);
            retMap.put(subkeys, task);
        }

        Map<byte[], byte[]> resultMerge = new HashMap<byte[], byte[]>();
        Exception exc = null;
        for (Map.Entry<byte[][], FutureTask<List<byte[]>>> entry : retMap.entrySet()) {
            if (exc == null) {
                try {
                    List<byte[]> subResult = entry.getValue().get();
                    for (int i = 0; i < entry.getKey().length; i++) {
                        resultMerge.put(entry.getKey()[i], subResult.get(i));
                    }
                } catch (Exception e) {
                    exc = e;
                    LOGGER.error("TairCluster.mget " + e.toString());
                }
            } else {
                entry.getValue().cancel(false);
            }
        }
        if (exc != null) {
            if (exc.getCause() instanceof RuntimeException) {
                throw (RuntimeException) exc.getCause();
            }
            return null;
        }
        for (byte[] key : keys) {
            if (resultMerge.containsKey(key)) {
                ret.add(resultMerge.get(key));
            } else {
                ret.add(null);
            }
        }
        return ret;
    }

    @Override
    public String mset(final String... keysvalues) {
        String ret = null;
        Map<String, Map<String, String>> map = preTreatKeyAndValue(keysvalues);
        List<FutureTask<String>> list = new ArrayList<FutureTask<String>>();
        for (final Map.Entry<String, Map<String, String>> entry : map.entrySet()) {
            int i = 0, j = 0;
            final String[] subkeysvalues = new String[entry.getValue().size() * 2];
            final String[] subkeys = new String[entry.getValue().size()];

            for (Map.Entry<String, String> en : entry.getValue().entrySet()) {
                subkeys[j++] = en.getKey();
                subkeysvalues[i++] = en.getKey();
                subkeysvalues[i++] = en.getValue();
            }
            FutureTask<String> task = new FutureTask<String>(new Callable<String>() {
                public String call() throws Exception {
                    return new JedisClusterCommand<String>(connectionHandler, maxAttempts) {
                        @Override
                        public String execute(Jedis connection) {
                            return connection.tmset(subkeysvalues);
                        }
                    }.runMcommand(subkeys.length, subkeys);
                }
            });
            backend.submit(task);
            list.add(task);
        }
        Exception exc = null;
        for (FutureTask<String> task : list) {
            if (exc == null) {
                try {
                    String subRet = task.get();
                    if (!"OK".equals(subRet)) {
                        ret = subRet;
                        break;
                    } else {
                        ret = "OK";
                    }
                } catch (Exception e) {
                    LOGGER.error("TairCluster.mset " + e.toString());
                    exc = e;
                }
            } else {
                task.cancel(false);
            }
        }
        if (exc != null) {
            if (exc.getCause() instanceof RuntimeException) {
                throw (RuntimeException)exc.getCause();
            }
            return null;
        }
        return ret;
    }

    @Override
    public String mset(final byte[]... keysvalues) {
        String ret = null;
        Map<String, Map<byte[], byte[]>> map = preTreatKeyAndValue(keysvalues);
        List<FutureTask<String>> list = new ArrayList<FutureTask<String>>();
        for (final Map.Entry<String, Map<byte[], byte[]>> entry : map.entrySet()) {
            int i = 0, j = 0;
            final byte[][] subkeysvalues = new byte[entry.getValue().size() * 2][];
            final byte[][] subkeys = new byte[entry.getValue().size()][];

            for (Map.Entry<byte[], byte[]> en : entry.getValue().entrySet()) {
                subkeys[j++] = en.getKey();
                subkeysvalues[i++] = en.getKey();
                subkeysvalues[i++] = en.getValue();
            }
            FutureTask<String> task = new FutureTask<String>(new Callable<String>() {
                public String call() throws Exception {
                    return new JedisClusterCommand<String>(connectionHandler, maxAttempts) {
                        @Override
                        public String execute(Jedis connection) {
                            return connection.tmset(subkeysvalues);
                        }
                    }.runBinaryMcommand(subkeys.length, subkeys);
                }
            });
            backend.submit(task);
            list.add(task);
        }
        Exception exc = null;
        for (FutureTask<String> task : list) {
            if (exc == null) {
                try {
                    String subRet = task.get();
                    if (!"OK".equals(subRet)) {
                        ret = subRet;
                        break;
                    } else {
                        ret = "OK";
                    }
                } catch (Exception e) {
                    LOGGER.error("TairCluster.mset " + e.toString());
                    exc = e;
                }
            } else {
                task.cancel(false);
            }
        }
        if (exc != null) {
            if (exc.getCause() instanceof RuntimeException) {
                throw (RuntimeException)exc.getCause();
            }
            return null;
        }
        return ret;
    }

    // =====================================================
    // Common function
    // =====================================================

    private Map<String, List<String>> preTreatKey(String... keys) {
        Map<String, List<String>> map = new HashMap<String, List<String>>();
        for (String key : keys) {
            int slot = JedisClusterCRC16.getSlot(key);
            String hostAndPort = connectionHandler.getHostAndPortFromSlot(slot);
            if (!map.containsKey(hostAndPort)) {
                map.put(hostAndPort, new ArrayList<String>());
            }
            map.get(hostAndPort).add(key);
        }
        return map;
    }

    private Map<String, List<byte[]>> preTreatKey(byte[]... keys) {
        Map<String, List<byte[]>> map = new HashMap<String, List<byte[]>>();
        for (byte[] key : keys) {
            int slot = JedisClusterCRC16.getSlot(key);
            String hostAndPort = connectionHandler.getHostAndPortFromSlot(slot);
            if (!map.containsKey(hostAndPort)) {
                map.put(hostAndPort, new ArrayList<byte[]>());
            }
            map.get(hostAndPort).add(key);
        }
        return map;
    }

    private Map<String, Map<String, String>> preTreatKeyAndValue(String... keysvalues) {
        Map<String, Map<String, String>> map = new HashMap<String, Map<String, String>>();
        for (int i = 0; i < keysvalues.length - 1; i += 2) {
            int slot = JedisClusterCRC16.getSlot(keysvalues[i]);
            String hostAndPort = connectionHandler.getHostAndPortFromSlot(slot);
            if (!map.containsKey(hostAndPort)) {
                map.put(hostAndPort, new HashMap<String, String>());
            }
            map.get(hostAndPort).put(keysvalues[i], keysvalues[i + 1]);
        }
        return map;
    }

    private Map<String, Map<byte[], byte[]>> preTreatKeyAndValue(byte[]... keysvalues) {
        Map<String, Map<byte[], byte[]>> map = new HashMap<String, Map<byte[], byte[]>>();
        for (int i = 0; i < keysvalues.length - 1; i += 2) {
            int slot = JedisClusterCRC16.getSlot(keysvalues[i]);
            String hostAndPort = connectionHandler.getHostAndPortFromSlot(slot);
            if (!map.containsKey(hostAndPort)) {
                map.put(hostAndPort, new HashMap<byte[], byte[]>());
            }
            map.get(hostAndPort).put(keysvalues[i], keysvalues[i + 1]);
        }
        return map;
    }
}
