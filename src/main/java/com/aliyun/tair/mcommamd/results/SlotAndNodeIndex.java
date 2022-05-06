package com.aliyun.tair.mcommamd.results;

public class SlotAndNodeIndex {
    private final int slot;
    private final int nodeIndex;

    public SlotAndNodeIndex(int slot, int nodeIndex) {
        this.slot = slot;
        this.nodeIndex = nodeIndex;
    }

    public int getSlot() {
        return slot;
    }

    public int getNodeIndex() {
        return nodeIndex;
    }
}
