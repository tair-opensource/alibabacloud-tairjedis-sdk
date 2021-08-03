package com.aliyun.tair.tests.tairai;

import com.aliyun.tair.tairai.TairAI;
import com.aliyun.tair.tairai.TairAICluster;
import com.aliyun.tair.tairai.TairAIPipeline;
import com.aliyun.tair.tests.TestBase;
import org.junit.BeforeClass;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.SafeEncoder;

import java.util.Iterator;
import java.util.List;


public class TairAITestBase extends TestBase {
    public static boolean AISupport = false;
    public static TairAI tairAI;
    public static TairAIPipeline tairAIPipeline;
    public static TairAICluster tairAICluster;

    @BeforeClass
    public static void setUp() {
        tairAI = new TairAI(jedis);
        tairAIPipeline = new TairAIPipeline();
        tairAIPipeline.setClient(jedis.getClient());
        tairAICluster = new TairAICluster(jedisCluster);

        List ret = (List)jedis.sendCommand(GetCommand.COMMAND);
        Iterator<Object> it = ret.iterator();
        while (it.hasNext()) {
            String str = new String((byte[]) ((List)it.next()).get(0));
            if (str.indexOf("tai.hnsw") >= 0) {
                AISupport = true;
            }
        }
    }

    enum GetCommand implements ProtocolCommand {
        COMMAND("command");

        private final byte[] raw;

        GetCommand(String alt) {
            raw = SafeEncoder.encode(alt);
        }

        @Override
        public byte[] getRaw() {
            return raw;
        }
    }

    public static Boolean isSupportTairAI() {
            return AISupport;
    }
}
