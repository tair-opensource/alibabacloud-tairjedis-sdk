package com.aliyun.tair.taircpc.params;

import com.aliyun.tair.jedis3.Params;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;

public class CpcMergeParams extends Params {
    private static final String LGK = "lgk";
    private static final String SEED = "seed";
    private static final String NUMCOUPONS = "numCoupons";
    private static final String MERGEFLAG = "mergeFlag";
    private static final String FICOL = "fiCol";
    private static final String WINDOWOFFSET = "windowOffset";
    private static final String KXP = "kxp";
    private static final String HIPESTACCUM = "hipEstAccum";
    private static final String SLIDINGWINDOW = "slidingWindow";
    private static final String PAIRTABLE = "pairTable";
    private static final String LGSIZEINTS = "lgSizeInts";
    private static final String VALIDBITS = "validBits";
    private static final String NUMPAIRS = "numPairs";

    public CpcMergeParams() {}

    public static CpcMergeParams CpcMergeParams() {
        return new CpcMergeParams();
    }

    public CpcMergeParams lgK(int lgK) {
        addParam(LGK, lgK);
        return this;
    }

    public CpcMergeParams seed(long seed) {
        addParam(SEED, seed);
        return this;
    }

    public CpcMergeParams numCoupons(long numCoupons) {
        addParam(NUMCOUPONS, numCoupons);
        return this;
    }

    public CpcMergeParams mergeFlag(boolean mergeFlag) {
        addParam(MERGEFLAG, mergeFlag);
        return this;
    }

    public CpcMergeParams fiCol(int fiCol) {
        addParam(FICOL, fiCol);
        return this;
    }

    public CpcMergeParams windowOffset(int windowOffset) {
        addParam(WINDOWOFFSET, windowOffset);
        return this;
    }

    public CpcMergeParams kxp(double kxp) {
        addParam(KXP, kxp);
        return this;
    }

    public CpcMergeParams hipEstAccum(double hipEstAccum) {
        addParam(HIPESTACCUM, hipEstAccum);
        return this;
    }

    public CpcMergeParams lgSizeInts(int lgSizeInts) {
        addParam(LGSIZEINTS, lgSizeInts);
        return this;
    }

    public CpcMergeParams validBits(int validBits) {
        addParam(VALIDBITS, validBits);
        return this;
    }

    public CpcMergeParams numPairs(int numPairs) {
        addParam(NUMPAIRS, numPairs);
        return this;
    }

    private void addParamWithValue(ArrayList<byte[]> byteParams, String option) {
        if (contains(option)) {
            byteParams.add(SafeEncoder.encode(option));
            byteParams.add(SafeEncoder.encode(String.valueOf((Object)getParam(option))));
        }
    }

//    public byte[][] getByteParams(byte[] key, CpcSketch cpcSketch) {
//        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();
//        byteParams.add(key);
//
//        lgK(cpcSketch.getLgK());
//        seed(cpcSketch.getSeed());
//        numCoupons(cpcSketch.getNumCoupons());
//        mergeFlag(cpcSketch.isMergeFlag());
//        fiCol(cpcSketch.getFiCol());
//        windowOffset(cpcSketch.getWindowOffset());
//        kxp(cpcSketch.getKxp());
//        hipEstAccum(cpcSketch.getHipEstAccum());
//
//        PairTable pairTable = cpcSketch.getPairTable();
//        lgSizeInts(pairTable.getLgSizeInts());
//        validBits(pairTable.getValidBits());
//        numPairs(pairTable.getNumPairs());
//
//        addParamWithValue(byteParams, LGK);
//        addParamWithValue(byteParams, SEED);
//        addParamWithValue(byteParams, NUMCOUPONS);
//        addParamWithValue(byteParams, MERGEFLAG);
//        addParamWithValue(byteParams, FICOL);
//        addParamWithValue(byteParams, WINDOWOFFSET);
//        addParamWithValue(byteParams, KXP);
//        addParamWithValue(byteParams, HIPESTACCUM);
//        addParamWithValue(byteParams, LGSIZEINTS);
//        addParamWithValue(byteParams, VALIDBITS);
//        addParamWithValue(byteParams, NUMPAIRS);
//
//        byteParams.add(SafeEncoder.encode("pairTable"));
//        int numSlots = 1 << pairTable.getLgSizeInts();
//        int[] slotsArr = pairTable.getSlotsArr();
//        for(int i = 0; i < numSlots; ++i) {
//            byteParams.add(SafeEncoder.encode(String.valueOf((Object)slotsArr[i])));
//        }
//
//        byteParams.add(SafeEncoder.encode("slidingWindow"));
//        byte[] slidingWindow = cpcSketch.getSlidingWindow();
//        for(int i = 0; i < slidingWindow.length; ++i) {
//            byteParams.add(SafeEncoder.encode(String.valueOf((Object)slidingWindow[i])));
//        }
//
//        return byteParams.toArray(new byte[byteParams.size()][]);
//    }
}
