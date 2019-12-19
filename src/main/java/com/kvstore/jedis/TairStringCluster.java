package com.kvstore.jedis;

import java.util.List;

import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.JedisCluster;

/**
 * @author bodong.ybd
 * @date 2019/12/17
 */
public class TairStringCluster {
    private JedisCluster jc;

    public TairStringCluster(JedisCluster jc) {
        this.jc = jc;
    }

    public Long cas(String sampleKey, String... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.CAS, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Long cas(byte[] sampleKey, byte[]... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.CAS, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Long cad(String sampleKey, String... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.CAD, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Long cad(byte[] sampleKey, byte[]... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.CAD, args);
        return BuilderFactory.LONG.build(obj);
    }

    public String exset(String sampleKey, String... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.EXSET, args);
        return BuilderFactory.STRING.build(obj);
    }

    public String exset(byte[] sampleKey, byte[]... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.EXSET, args);
        return BuilderFactory.STRING.build(obj);
    }

    public List<String> exget(String sampleKey, String... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.EXGET, args);
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<byte[]> exget(byte[] sampleKey, byte[]... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.EXGET, args);
        return BuilderFactory.BYTE_ARRAY_LIST.build(obj);
    }

    public Long exsetver(String sampleKey, String... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.EXSETVER, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Long exsetver(byte[] sampleKey, byte[]... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.EXSETVER, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Long exincrBy(String sampleKey, String... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.EXINCRBY, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Long exincrBy(byte[] sampleKey, byte[]... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.EXINCRBY, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Double exincrByFloat(String sampleKey, String... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.EXINCRBYFLOAT, args);
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double exincrByFloat(byte[] sampleKey, byte[]... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.EXINCRBYFLOAT, args);
        return BuilderFactory.DOUBLE.build(obj);
    }

    public List<String> excas(String sampleKey, String... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.EXCAS, args);
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<byte[]> excas(byte[] sampleKey, byte[]... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.EXCAS, args);
        return BuilderFactory.BYTE_ARRAY_LIST.build(obj);
    }

    public Long excad(String sampleKey, String... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.EXCAD, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Long excad(byte[] sampleKey, byte[]... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.EXCAD, args);
        return BuilderFactory.LONG.build(obj);
    }
}
