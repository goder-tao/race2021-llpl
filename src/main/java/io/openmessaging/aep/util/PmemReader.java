package io.openmessaging.aep.util;

import java.nio.ByteBuffer;

public interface PmemReader {
    // 读指定MemoryBlock内的所有数据
    byte[] read(long handle);

}
