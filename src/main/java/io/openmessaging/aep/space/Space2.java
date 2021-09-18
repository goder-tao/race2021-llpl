package io.openmessaging.aep.space;

import io.openmessaging.aep.mmu.MemoryNode;

public interface Space2 {
    void free(MemoryNode listNode);

    MemoryNode write(byte[] data);

    byte[] read(MemoryNode listNode);
}
