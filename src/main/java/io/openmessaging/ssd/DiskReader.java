package io.openmessaging.ssd;

import java.nio.ByteBuffer;

/**
 * SSD 读接口
 */
public interface DiskReader {
    ByteBuffer read(String path, long offset, int size);
}
