package com.aliyun.tair.taircpc.params;

import redis.clients.jedis.params.Params;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;

import static redis.clients.jedis.Protocol.toByteArray;

public class CpcMultiArrayUpdateParams extends Params {
    public CpcMultiArrayUpdateParams() {}

    public static CpcMultiArrayUpdateParams cpcMultiArrayUpdateParams() {
        return new CpcMultiArrayUpdateParams();
    }

    public byte[][] getByteParams(ArrayList<CpcArrayData> keys) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();

        for (CpcArrayData key:keys) {
            byteParams.add(SafeEncoder.encode(key.getKey()));
            byteParams.add(toByteArray(key.getOffset()));
            byteParams.add(SafeEncoder.encode(key.getItem()));
            byteParams.add(toByteArray(key.getSize()));
            byteParams.add(SafeEncoder.encode(key.getExpStr()));
            byteParams.add(toByteArray(key.getExp()));
        }

        return byteParams.toArray(new byte[byteParams.size()][]);
    }
}