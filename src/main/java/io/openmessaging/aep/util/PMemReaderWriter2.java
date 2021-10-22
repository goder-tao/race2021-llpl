package io.openmessaging.aep.util;

import com.intel.pmem.llpl.MemoryBlock;

import java.nio.ByteBuffer;

/**
 * 非针对某个特定MemoryBlock的ReaderWriter, 减少ReaderWriter的对象创建
 * @author tao */
public class PMemReaderWriter2 {

    private static final PMemReaderWriter2 instance = new PMemReaderWriter2();

    private PMemReaderWriter2() {

    }

    /**
     * 读main block
     * @param block  - 将要读的MemoryBlock
     * @param offset - 读取开始的位置
     * @param size   - 读取数据的大小
     */
    public byte[] read(MemoryBlock block, long offset, int size) {
        byte[] b = new byte[size];
        block.copyToArray(offset, b, 0, size);
        return b;
    }

    /**
     * 写main block
     * @param block - 将要写入的MemoryBlock
     * @param offset - 写入开始的位置
     * @param data   - 写入的数据
     */
    public void write(MemoryBlock block, long offset, ByteBuffer data) {
        block.copyFromArray(data.array(), 0, offset, data.remaining());
    }

    /**
     * 单例模式*/
    public static PMemReaderWriter2 getInstance() {
        return instance;
    }

}
