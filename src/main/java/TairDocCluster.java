import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.JedisCluster;

/**
 * @author bodong.ybd
 * @date 2019/12/11
 */
public class TairDocCluster {
    private JedisCluster jc;

    public TairDocCluster(JedisCluster jc) {
        this.jc = jc;
    }

    public String jsonset(String sampleKey, String... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.JSONSET, args);
        return BuilderFactory.STRING.build(obj);
    }

    public String jsonset(byte[] sampleKey, byte[]... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.JSONSET, args);
        return BuilderFactory.STRING.build(obj);
    }

    public String jsonget(String sampleKey, String... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.JSONGET, args);
        return BuilderFactory.STRING.build(obj);
    }

    public byte[] jsonget(byte[] sampleKey, byte[]... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.JSONGET, args);
        return BuilderFactory.BYTE_ARRAY.build(obj);
    }

    // Cluster multi keys command is not support
    //public List<String> jsonmget(String... args) {
    //    getClient("").sendCommand(ModuleCommand.JSONMGET, args);
    //    return getResponse(BuilderFactory.STRING_LIST);
    //}
    //
    //public Response<List<byte[]>> jsonmget(byte[]... args) {
    //    getClient("").sendCommand(ModuleCommand.JSONMGET, args);
    //    return getResponse(BuilderFactory.BYTE_ARRAY_LIST);
    //}

    public Long jsondel(String sampleKey, String... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.JSONDEL, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsondel(byte[] sampleKey, byte[]... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.JSONDEL, args);
        return BuilderFactory.LONG.build(obj);
    }

    public String jsontype(String sampleKey, String... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.JSONTYPE, args);
        return BuilderFactory.STRING.build(obj);
    }

    public byte[] jsontype(byte[] sampleKey, byte[]... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.JSONTYPE, args);
        return BuilderFactory.BYTE_ARRAY.build(obj);
    }

    public Double jsonnumincrBy(String sampleKey, String... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.JSONNUMINCRBY, args);
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Double jsonnumincrBy(byte[] sampleKey, byte[]... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.JSONNUMINCRBY, args);
        return BuilderFactory.DOUBLE.build(obj);
    }

    public Long jsonstrAppend(String sampleKey, String... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.JSONSTRAPPEND, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonstrAppend(byte[] sampleKey, byte[]... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.JSONSTRAPPEND, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonstrlen(String sampleKey, String... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.JSONSTRLEN, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonstrlen(byte[] sampleKey, byte[]... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.JSONSTRLEN, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonarrAppend(String sampleKey, String... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.JSONARRAPPEND, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonarrAppend(byte[] sampleKey, byte[]... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.JSONARRAPPEND, args);
        return BuilderFactory.LONG.build(obj);
    }

    public String jsonarrPop(String sampleKey, String... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.JSONARRPOP, args);
        return BuilderFactory.STRING.build(obj);
    }

    public byte[] jsonarrPop(byte[] sampleKey, byte[]... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.JSONARRPOP, args);
        return BuilderFactory.BYTE_ARRAY.build(obj);
    }

    public Long jsonarrInsert(String sampleKey, String... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.JSONARRAPPEND, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonarrInsert(byte[] sampleKey, byte[]... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.JSONARRAPPEND, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonArrlen(String sampleKey, String... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.JSONARRLEN, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonArrlen(byte[] sampleKey, byte[]... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.JSONARRLEN, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonarrTrim(String sampleKey, String... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.JSONARRTRIM, args);
        return BuilderFactory.LONG.build(obj);
    }

    public Long jsonarrTrim(byte[] sampleKey, byte[]... args) {
        Object obj = jc.sendCommand(sampleKey, ModuleCommand.JSONARRTRIM, args);
        return BuilderFactory.LONG.build(obj);
    }
}
