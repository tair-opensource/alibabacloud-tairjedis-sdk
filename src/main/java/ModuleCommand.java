import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.SafeEncoder;

/**
 * @author bodong.ybd
 * @date 2019/12/11
 */
public enum ModuleCommand implements ProtocolCommand {
    // TairDoc command
    JSONDEL("JSON.DEL"),
    JSONGET("JSON.GET"),
    JSONMGET("JSON.MGET"),
    JSONSET("JSON.SET"),
    JSONTYPE("JSON.TYPE"),
    JSONNUMINCRBY("JSON.NUMINCRBY"),
    JSONSTRAPPEND("JSON.STRAPPEND"),
    JSONSTRLEN("JSON.STRLEN"),
    JSONARRAPPEND("JSON.ARRAPPEND"),
    JSONARRPOP("JSON.ARRPOP"),
    JSONARRINSERT("JSON.ARRINSERT"),
    JSONARRLEN("JSON.ARRLEN"),
    JSONARRTRIM("JSON.ARRTRIM");

    // TairHash command

    private final byte[] raw;

    ModuleCommand(String alt) {
        raw = SafeEncoder.encode(alt);
    }

    public byte[] getRaw() {
        return raw;
    }
}
