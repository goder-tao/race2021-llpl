package io.openmessaging.aep.util;

import io.openmessaging.aep.mmu.MemoryListNode;

public interface Space {
    void free(MemoryListNode listNode);

    MemoryListNode write(byte[] data);

    byte[] read(MemoryListNode listNode);
}
