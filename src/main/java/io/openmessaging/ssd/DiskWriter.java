package io.openmessaging.ssd;

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
    int append(String dirPath, String fileName, ByteBuffer buffer);

    int write(String dirPath, String fileName, long offset, ByteBuffer buffer);
}
