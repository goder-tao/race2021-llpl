package io.openmessaging.aep.mmu;

public interface MMU2 {
    void free(MemoryListNode listNode);

    MemoryListNode allocate(long size);

}
