package io.openmessaging.aep.space;

import io.openmessaging.aep.mmu.MemoryNode;

import java.nio.ByteBuffer;

public interface Space2 {
    void free(MemoryNode listNode);

    MemoryNode write(ByteBuffer data);

    byte[] read(MemoryNode listNode);
}
