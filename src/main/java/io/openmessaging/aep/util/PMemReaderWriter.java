package io.openmessaging.aep.util;

import com.intel.pmem.llpl.MemoryBlock;

/**
 * 专职负责对主MemoryBlock进行读写，需要MMU先告诉ReaderWriter内存的使用情况
 *
 * @author tao
 */
public class PMemReaderWriter implements Reader, Writer {
    private final MemoryBlock mainBlock;

    public PMemReaderWriter(MemoryBlock mainBlock) {
        this.mainBlock = mainBlock;
    }

    /**
     * 读main block
     *
     * @param offset - 读取开始的位置
     * @param size   - 读取数据的大小
     */
    @Override
    public byte[] read(long offset, int size) {
        byte[] b = new byte[size];
        mainBlock.copyToArray(offset, b, 0, size);
        return b;
    }

    /**
     * 写main block
     *
     * @param offset - 写入开始的位置
     * @param data   - 写入的数据
     */
    @Override
    public void write(long offset, byte[] data) {
        mainBlock.copyFromArray(data, 0, offset, data.length);
    }
}
