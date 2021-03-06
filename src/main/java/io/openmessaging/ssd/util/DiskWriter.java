package io.openmessaging.ssd.util;

import java.nio.ByteBuffer;

/**
 * SSD 写接口
 *
 * @author tao
 */
public interface DiskWriter {
    /**
     * 直接在filePath文件结尾append数据，append之前计算是哪个partition
     */
    int append(String dirPath, String fileName, byte[] data);

    int write(String dirPath, String fileName, long offset, ByteBuffer buffer);
}
