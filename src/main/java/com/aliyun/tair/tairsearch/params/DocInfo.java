package com.aliyun.tair.tairsearch.params;

public class DocInfo {
    private String docContent;
    private String docID;

    public DocInfo(String docContent, String docID) {
        this.docContent = docContent;
        this.docID = docID;
    }

    public String docContent() {
        return docContent;
    }

    public void docContent(String docContent) {
        this.docContent = docContent;
    }

    public String docID() {
        return docID;
    }

    public void docID(String docID) {
        this.docID = docID;
    }

    @Override
    public boolean equals(Object other) {
        if(other instanceof DocInfo) {
            return docID == ((DocInfo) other).docID;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return docID.hashCode();
    }
}
