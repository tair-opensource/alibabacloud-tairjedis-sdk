package com.aliyun.tair.taircpc.params;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.taircpc.CommonResult;
import com.aliyun.tair.jedis3.Params;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;

import static com.aliyun.tair.taircpc.params.CpcDataUtil.*;
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
            byteParams.add(toByteArray(key.getTimestamp()));
            byteParams.add(SafeEncoder.encode(key.getItem()));
            byteParams.add(SafeEncoder.encode(key.getExpStr()));
            byteParams.add(toByteArray(key.getExp()));
            byteParams.add(SafeEncoder.encode(key.getSizeStr()));
            byteParams.add(toByteArray(key.getSize()));
            byteParams.add(SafeEncoder.encode(key.getWinSizeStr()));
            byteParams.add(toByteArray(key.getWinSize()));
        }

        return byteParams.toArray(new byte[byteParams.size()][]);
    }

    public byte[][] getByteMultiParams(ArrayList<CpcArrayMultiData> keys){
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();
        byteParams.add(SafeEncoder.encode(keys.get(0).getKey()));
        for (CpcArrayMultiData key : keys) {
            checkMultiData(key);

            switch (key.getType()) {
                case CPCARRAYUPDATE : {
                    checkCpcMultiData(key);
                    byteParams.add(ModuleCommand.CPCARRAYUPDATE.getRaw());
                    byteParams.add(SafeEncoder.encode(key.getKey()));
                    byteParams.add(toByteArray(key.getTimestamp()));
                    byteParams.add(SafeEncoder.encode((String) key.getValues().get(CpcDataUtil.VALUE)));
                    break;
                }
                case SUMARRAYADD : {
                    checkCpcMultiData(key);
                    byteParams.add(ModuleCommand.SUMARRAYADD.getRaw());
                    byteParams.add(SafeEncoder.encode(key.getKey()));
                    byteParams.add(toByteArray(key.getTimestamp()));
                    byteParams.add(toByteArray((Double) key.getValues().get(CpcDataUtil.VALUE)));
                    break;
                }
                case MAXARRAYADD : {
                    checkCpcMultiData(key);
                    byteParams.add(ModuleCommand.MAXARRAYADD.getRaw());
                    byteParams.add(SafeEncoder.encode(key.getKey()));
                    byteParams.add(toByteArray(key.getTimestamp()));
                    byteParams.add(toByteArray((Double) key.getValues().get(CpcDataUtil.VALUE)));
                    break;
                }
                case MINARRAYADD : {
                    checkCpcMultiData(key);
                    byteParams.add(ModuleCommand.MINARRAYADD.getRaw());
                    byteParams.add(SafeEncoder.encode(key.getKey()));
                    byteParams.add(toByteArray(key.getTimestamp()));
                    byteParams.add(toByteArray((Double) key.getValues().get(CpcDataUtil.VALUE)));
                    break;
                }
                case FIRSTARRAYADD : {
                    checkFirstMultiData(key);
                    byteParams.add(ModuleCommand.FIRSTARRAYADD.getRaw());
                    byteParams.add(SafeEncoder.encode(key.getKey()));
                    byteParams.add(toByteArray(key.getTimestamp()));
                    byteParams.add(SafeEncoder.encode((String) key.getValues().get(CONTENT)));
                    byteParams.add(toByteArray((Double) key.getValues().get(CpcDataUtil.VALUE)));
                    break;
                }
                case LASTARRAYADD : {
                    checkFirstMultiData(key);
                    byteParams.add(ModuleCommand.LASTARRAYADD.getRaw());
                    byteParams.add(SafeEncoder.encode(key.getKey()));
                    byteParams.add(toByteArray(key.getTimestamp()));
                    byteParams.add(SafeEncoder.encode((String) key.getValues().get(CONTENT)));
                    byteParams.add(toByteArray((Double) key.getValues().get(CpcDataUtil.VALUE)));
                    break;
                }
                case AVGARRAYADD : {
                    checkAvgMultiData(key);
                    byteParams.add(ModuleCommand.AVGARRAYADD.getRaw());
                    byteParams.add(SafeEncoder.encode(key.getKey()));
                    byteParams.add(toByteArray(key.getTimestamp()));
                    byteParams.add(toByteArray((Long) key.getValues().get(WEIGHT)));
                    byteParams.add(toByteArray((Double) key.getValues().get(CpcDataUtil.VALUE)));
                    break;
                }
                case STDDEVARRAYADD : {
                    checkCpcMultiData(key);
                    byteParams.add(ModuleCommand.STDDEVARRAYADD.getRaw());
                    byteParams.add(SafeEncoder.encode(key.getKey()));
                    byteParams.add(toByteArray(key.getTimestamp()));
                    byteParams.add(toByteArray((Double) key.getValues().get(CpcDataUtil.VALUE)));
                    break;
                }
                default: {
                    throw new IllegalArgumentException(CommonResult.optionIllegal);
                }
            }
            byteParams.add(SafeEncoder.encode(key.getExpStr()));
            byteParams.add(toByteArray(key.getExp()));
            byteParams.add(SafeEncoder.encode(key.getSizeStr()));
            byteParams.add(toByteArray(key.getSize()));
            byteParams.add(SafeEncoder.encode(key.getWinSizeStr()));
            byteParams.add(toByteArray(key.getWinSize()));
        }
        return byteParams.toArray(new byte[byteParams.size()][]);
    }

    private boolean checkMultiData(CpcArrayMultiData data) {
        if (null == data.getKey()){
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        return true;
    }

    private boolean checkCpcMultiData(CpcArrayMultiData data) {
        if (null == data.getValues() || null == data.getValues().get(CpcDataUtil.VALUE)){
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        return true;
    }

    private boolean checkFirstMultiData(CpcArrayMultiData data) {
        if (null == data.getValues() || null == data.getValues().get(CpcDataUtil.VALUE) || null == data.getValues().get(CONTENT)){
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        return true;
    }

    private boolean checkAvgMultiData(CpcArrayMultiData data) {
        if (null == data.getValues() || null == data.getValues().get(CpcDataUtil.VALUE) || null == data.getValues().get(WEIGHT)){
            throw new IllegalArgumentException(CommonResult.valueIsNull);
        }
        return true;
    }
}
