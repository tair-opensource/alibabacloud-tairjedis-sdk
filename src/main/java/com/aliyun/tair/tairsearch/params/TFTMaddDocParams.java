package com.aliyun.tair.tairsearch.params;

import io.valkey.util.SafeEncoder;

import java.util.ArrayList;
import java.util.List;

public class TFTMaddDocParams {
    public byte[][] getByteParams(String key, List<DocInfo> docs) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();

        byteParams.add(SafeEncoder.encode(key));

        for (DocInfo entry : docs) {
            byteParams.add(SafeEncoder.encode(entry.docContent()));
            byteParams.add(SafeEncoder.encode(entry.docID()));
        }

        return byteParams.toArray(new byte[byteParams.size()][]);
    }

    public byte[][] getByteParams(byte[] key, List<DocInfoByte> docs) {
        ArrayList<byte[]> byteParams = new ArrayList<byte[]>();

        byteParams.add(key);

        for (DocInfoByte entry : docs) {
            byteParams.add(entry.docContent());
            byteParams.add(entry.docID());
        }
        return byteParams.toArray(new byte[byteParams.size()][]);
    }
}
