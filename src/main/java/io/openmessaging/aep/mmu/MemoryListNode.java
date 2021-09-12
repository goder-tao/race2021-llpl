package io.openmessaging.aep.mmu;

/**
 * A MemoryListNode indicate a pmem block space
 */
public class MemoryListNode {
    boolean used = true;
    public MemoryListNode preNode, nextNode;
    public long blockSize, blockOffset;
    public PMemMMU2 pMemMMU;

    public MemoryListNode(long offset, long size, MemoryListNode preNode, MemoryListNode nextNode, PMemMMU2 pMemMMU) {
        this.blockOffset = offset;
        this.blockSize = size;
        this.preNode = preNode;
        this.nextNode = nextNode;
        this.pMemMMU = pMemMMU;
    }

    /**
     * change Node state, used->not used, not used->used
     */
    void changeState() {
        this.used = !this.used;
    }


    public void free() {
        pMemMMU.free(this);
    }
}
