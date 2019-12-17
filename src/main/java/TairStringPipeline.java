import java.util.List;

import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

/**
 * @author bodong.ybd
 * @date 2019/12/16
 */
public class TairStringPipeline extends Pipeline {
    public Response<Long> cas(String... args) {
        getClient("").sendCommand(ModuleCommand.CAS, args);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> cas(byte[]... args) {
        getClient("").sendCommand(ModuleCommand.CAS, args);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> cad(String... args) {
        getClient("").sendCommand(ModuleCommand.CAD, args);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> cad(byte[]... args) {
        getClient("").sendCommand(ModuleCommand.CAD, args);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<String> exset(String... args) {
        getClient("").sendCommand(ModuleCommand.EXSET, args);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<String> exset(byte[]... args) {
        getClient("").sendCommand(ModuleCommand.EXSET, args);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<List<String>> exget(String... args) {
        getClient("").sendCommand(ModuleCommand.EXGET, args);
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<byte[]>> exget(byte[]... args) {
        getClient("").sendCommand(ModuleCommand.EXGET, args);
        return getResponse(BuilderFactory.BYTE_ARRAY_LIST);
    }

    public Response<Long> exsetver(String... args) {
        getClient("").sendCommand(ModuleCommand.EXSETVER, args);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> exsetver(byte[]... args) {
        getClient("").sendCommand(ModuleCommand.EXSETVER, args);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> exincrBy(String... args) {
        getClient("").sendCommand(ModuleCommand.EXINCRBY, args);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> exincrBy(byte[]... args) {
        getClient("").sendCommand(ModuleCommand.EXINCRBY, args);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Double> exincrByFloat(String... args) {
        getClient("").sendCommand(ModuleCommand.EXINCRBY, args);
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<Double> exincrByFloat(byte[]... args) {
        getClient("").sendCommand(ModuleCommand.EXINCRBY, args);
        return getResponse(BuilderFactory.DOUBLE);
    }

    public Response<List<String>> excas(String... args) {
        getClient("").sendCommand(ModuleCommand.EXCAS, args);
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<byte[]>> excas(byte[]... args) {
        getClient("").sendCommand(ModuleCommand.EXCAS, args);
        return getResponse(BuilderFactory.BYTE_ARRAY_LIST);
    }

    public Response<Long> excad(String... args) {
        getClient("").sendCommand(ModuleCommand.EXCAD, args);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> excad(byte[]... args) {
        getClient("").sendCommand(ModuleCommand.EXCAD, args);
        return getResponse(BuilderFactory.LONG);
    }
}
