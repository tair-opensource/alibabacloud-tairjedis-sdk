import java.util.List;

import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * @author bodong.ybd
 * @date 2019/12/16
 */
public class TairString {
    private Jedis jedis;
    private JedisPool jedisPool;

    public TairString(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    public TairString(Jedis jedis) {
        this.jedis = jedis;
    }

    private Jedis getJedis() {
        if (jedisPool != null) {
            return jedisPool.getResource();
        }
        return jedis;
    }

    public Long cas(String... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.CAS, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Long cas(byte[]... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.CAS, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Long cad(String... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.CAD, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Long cad(byte[]... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.CAD, args);
        return BuilderFactory.LONG.build(obj);
    }

    public String exset(String... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXSET, args);
        return BuilderFactory.STRING.build(obj);
    }

    public String exset(byte[]... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXSET, args);
        return BuilderFactory.STRING.build(obj);
    }

    public List<String> exget(String... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXGET, args);
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<byte[]> exget(byte[]... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXGET, args);
        return BuilderFactory.BYTE_ARRAY_LIST.build(obj);
    }

    public Long exsetver(String... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXSETVER, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Long exsetver(byte[]... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXSETVER, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Long exincrBy(String... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXINCRBY, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Long exincrBy(byte[]... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXINCRBY, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Double exincrByFloat(String... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXINCRBYFLOAT, args);
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double exincrByFloat(byte[]... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXINCRBYFLOAT, args);
        return BuilderFactory.DOUBLE.build(obj);
    }

    public List<String> excas(String... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXCAS, args);
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<byte[]> excas(byte[]... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXCAS, args);
        return BuilderFactory.BYTE_ARRAY_LIST.build(obj);
    }

    public Long excad(String... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXCAD, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Long excad(byte[]... args) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXCAD, args);
        return BuilderFactory.LONG.build(obj);
    }
}
