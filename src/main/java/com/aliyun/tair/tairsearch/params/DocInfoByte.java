package com.aliyun.tair.tairsearch.params;

import java.util.Arrays;

public class DocInfoByte {
    private byte[] docContent;
    private byte[] docID;

    public DocInfoByte(byte[] docContent, byte[] docID) {
        this.docContent = docContent;
        this.docID = docID;
    }

    public byte[] docContent() {
        return docContent;
    }

    public void docContent(byte[] docContent) {
        this.docContent = docContent;
    }

    public byte[] docID() {
        return docID;
    }

    public void docID(byte[] docID) {
        this.docID = docID;
    }

    @Override
    public boolean equals(Object other) {
        if(other instanceof DocInfoByte) {
            return Arrays.equals(docID, ((DocInfoByte) other).docID);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(docID);
    }
}
