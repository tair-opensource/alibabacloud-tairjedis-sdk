package com.aliyun.tair.tairhash.params;

import java.util.ArrayList;

import com.aliyun.tair.jedis3.Params;
import io.valkey.util.SafeEncoder;

public class ExhsetParams extends Params {
    private static final String XX = "xx";
    private static final String NX = "nx";

    private static final String PX = "px";
    private static final String EX = "ex";
    private static final String EXAT = "exat";
    private static final String PXAT = "pxat";

    private static final String VER = "ver";
    private static final String ABS = "abs";

    private static final String NOACTIVE = "noactive";
    private static final String WITHPE = "withpe";

    private static final String KEEPTTL = "keepttl";

    public ExhsetParams() {}

    public static ExhsetParams ExhsetParams() {
        return new ExhsetParams();
    }

    /**
     * Only set the key if it already exist.
     *
     * @return SetParams
     */
    public ExhsetParams xx() {
        addParam(XX);
        return this;
    }

    /**
     * Only set the key if it does not already exist.
     *
     * @return SetParams
     */
    public ExhsetParams nx() {
        addParam(NX);
        return this;
    }

    /**
     * Set the specified expire time, in seconds.
     *
     * @param secondsToExpire
     * @return SetParams
     */
    public ExhsetParams ex(int secondsToExpire) {
        addParam(EX, secondsToExpire);
        return this;
    }

    /**
     * Set the specified expire time, in milliseconds.
     *
     * @param millisecondsToExpire
     * @return SetParams
     */
    public ExhsetParams px(long millisecondsToExpire) {
        addParam(PX, millisecondsToExpire);
        return this;
    }

    /**
     * Set the specified absolute expire time, in seconds.
     *
     * @param secondsToExpire
     * @return SetParams
     */
    public ExhsetParams exat(int secondsToExpire) {
        addParam(EXAT, secondsToExpire);
        return this;
    }

    /**
     * Set the specified absolute expire time, in milliseconds.
     *
     * @param millisecondsToExpire
     * @return SetParams
     */
    public ExhsetParams pxat(long millisecondsToExpire) {
        addParam(PXAT, millisecondsToExpire);
        return this;
    }

    /**
     * Set if version equal or not exist
     *
     * @param version
     * @return SetParams
     */
    public ExhsetParams ver(long version) {
        addParam(VER, version);
        return this;
    }

    /**
     * Set version to absoluteVersion
     *
     * @param absoluteVersion
     * @return SetParams
     */
    public ExhsetParams abs(long absoluteVersion) {
        addParam(ABS, absoluteVersion);
        return this;
    }

    public ExhsetParams noactive( ) {
        addParam(NOACTIVE, "");
        return this;
    }

    public ExhsetParams withpe() {
        addParam(WITHPE);
        return this;
    }

    public ExhsetParams keeptl() {
        addParam(KEEPTTL);
        return this;
    }

    private void addParamWithValue(ArrayList<byte[]> byteParams, String option) {
        if (contains(option)) {
            byteParams.add(SafeEncoder.encode(option));
            byteParams.add(SafeEncoder.encode(String.valueOf((Object)getParam(option))));
        }
    }

    public byte[][] getByteParams(byte[]... args) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();
        for (byte[] arg : args) {
            byteParams.add(arg);
        }

        if (contains(XX)) {
            byteParams.add(SafeEncoder.encode(XX));
        }
        if (contains(NX)) {
            byteParams.add(SafeEncoder.encode(NX));
        }

        addParamWithValue(byteParams, EX);
        addParamWithValue(byteParams, PX);
        addParamWithValue(byteParams, EXAT);
        addParamWithValue(byteParams, PXAT);

        addParamWithValue(byteParams, VER);
        addParamWithValue(byteParams, ABS);

        if(contains(NOACTIVE)){
            byteParams.add(SafeEncoder.encode(NOACTIVE));
        }

        if (contains(WITHPE)) {
            byteParams.add(SafeEncoder.encode(WITHPE));
        }

        if (contains(KEEPTTL)) {
            byteParams.add(SafeEncoder.encode(KEEPTTL));
        }

        return byteParams.toArray(new byte[byteParams.size()][]);
    }
}
