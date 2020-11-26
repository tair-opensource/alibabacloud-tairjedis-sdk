package com.aliyun.tair;

import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.SafeEncoder;

public enum ModuleCommand implements ProtocolCommand {
    // com.kvstore.jedis.TairDoc command
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
    JSONARRTRIM("JSON.ARRTRIM"),

    // TairHash command
    EXHSET("EXHSET"),
    EXHSETNX("EXHSETNX"),
    EXHMSET("EXHMSET"),
    EXHMSETWITHOPTS("EXHMSETWITHOPTS"),
    EXHPEXPIREAT("EXHPEXPIREAT"),
    EXHPEXPIRE("EXHPEXPIRE"),
    EXHEXPIREAT("EXHEXPIREAT"),
    EXHEXPIRE("EXHEXPIRE"),
    EXHPTTL("EXHPTTL"),
    EXHTTL("EXHTTL"),
    EXHVER("EXHVER"),
    EXHSETVER("EXHSETVER"),
    EXHINCRBY("EXHINCRBY"),
    EXHINCRBYFLOAT("EXHINCRBYFLOAT"),
    EXHGET("EXHGET"),
    EXHGETWITHVER("EXHGETWITHVER"),
    EXHMGET("EXHMGET"),
    EXHDEL("EXHDEL"),
    EXHLEN("EXHLEN"),
    EXHEXISTS("EXHEXISTS"),
    EXHSTRLEN("EXHSTRLEN"),
    EXHKEYS("EXHKEYS"),
    EXHVALS("EXHVALS"),
    EXHGETALL("EXHGETALL"),
    EXHMGETWITHVER("EXHMGETWITHVER"),
    EXHSCAN("EXHSCAN"),

    // CAS & CAD
    CAS("CAS"),
    CAD("CAD"),

    // com.kvstore.jedis.TairString command
    EXSET("EXSET"),
    EXGET("EXGET"),
    EXSETVER("EXSETVER"),
    EXINCRBY("EXINCRBY"),
    EXINCRBYFLOAT("EXINCRBYFLOAT"),
    EXCAS("EXCAS"),
    EXCAD("EXCAD"),
    EXAPPEND("EXAPPEND"),
    EXPREPEND("EXPREPEND"),
    EXGAE("EXGAE"),

    // TairGis command
    GISADD("GIS.ADD"),
    GISGET("GIS.GET"),
    GISDEL("GIS.DEL"),
    GISSEARCH("GIS.SEARCH"),
    GISCONTAINS("GIS.CONTAINS"),
    GISINTERSECTS("GIS.INTERSECTS"),
    GISGETALL("GIS.GETALL"),

    // TairBloom command
    BFADD("BF.ADD"),
    BFMADD("BF.MADD"),
    BFEXISTS("BF.EXISTS"),
    BFMEXISTS("BF.MEXISTS"),
    BFINSERT("BF.INSERT"),
    BFRESERVE("BF.RESERVE"),
    BFDEBUG("BF.DEBUG");

    private final byte[] raw;

    ModuleCommand(String alt) {
        raw = SafeEncoder.encode(alt);
    }

    @Override
    public byte[] getRaw() {
        return raw;
    }
}
