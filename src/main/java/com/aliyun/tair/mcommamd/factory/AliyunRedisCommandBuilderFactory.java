package com.aliyun.tair.mcommamd.factory;

import com.aliyun.tair.mcommamd.results.SlotAndNodeIndex;
import redis.clients.jedis.Builder;
import redis.clients.jedis.util.SafeEncoder;

public class AliyunRedisCommandBuilderFactory {
    public static final Builder<SlotAndNodeIndex> SlotAndNodeIndexResult = new Builder<SlotAndNodeIndex>() {
        @Override
        public SlotAndNodeIndex build(Object data) {
            if (data == null) {
                return null;
            }
            String result = SafeEncoder.encode(((byte[])data));
            result = result.substring(0, result.length() - 2);
            String[] strings = result.split(" ");
            if (strings.length != 2) {
                return null;
            }

            String[] slotInfo = strings[0].split(":");
            if (!slotInfo[0].equals("slot")) {
                return null;
            }
            int slot = Integer.parseInt(slotInfo[1]);

            String[] nodeIndexInfo = strings[1].split(":");
            if (!nodeIndexInfo[0].equals("node_index")) {
                return null;
            }
            int nodeIndex = Integer.parseInt(nodeIndexInfo[1]);

            return new SlotAndNodeIndex(slot, nodeIndex);
        }

        @Override
        public String toString() {
            return "SlotAndNodeIndex";
        }
    };
}
