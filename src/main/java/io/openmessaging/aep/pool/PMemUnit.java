package io.openmessaging.aep.pool;

import com.intel.pmem.llpl.MemoryBlock;
import io.openmessaging.aep.mmu.MemoryNode;
import io.openmessaging.aep.space.PMemThreadSpace2;
import io.openmessaging.aep.space.Space2;
import io.openmessaging.aep.util.PMemReaderWriter2;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author tao */
public class PMemUnit implements Space2 {
    // 实际保存数据的block
    private MemoryBlock block;
    // Unit的entry数量和当前数量
    private int entryNum;
    private AtomicInteger currentEntry = new AtomicInteger(0);
    // entry大小
    private short entrySize;
    // 使用标识
    private Boolean[] arrayFlag;
    // 从上次空的位置开始找位置
    private int lastPosition = 0;

    /**
     * @param block - 存储数据的MemoryBlock
     */
    public PMemUnit(MemoryBlock block) {
        this.block = block;
    }

    /**
     * 从池重新分配的时候调用的方法，表示的entry可能不同
     * @param entryNum - 新entry数量
     * @param entrySize - 新entry大小*/
    public void reset(int entryNum, int entrySize) {
        this.entryNum = entryNum;
        this.entrySize = (short) entrySize;
        currentEntry.set(0);
        arrayFlag = new Boolean[entryNum];
        for (int i = 0; i < entryNum; i++) {
            arrayFlag[i] = Boolean.FALSE;
        }
    }

    @Override
    public void free(MemoryNode listNode) {
        arrayFlag[listNode.entryPosition] = Boolean.FALSE;
        currentEntry.decrementAndGet();
    }

    @Override
    public MemoryNode write(ByteBuffer data) {
        // 已满
        if (currentEntry.get() >= entryNum) {
            return null;
        }
        MemoryNode node = null;
        // 完整遍历标记
        int mark = lastPosition;
        int last = mark;
        // 开始遍历找空的entry
        do {
            if (arrayFlag[last%entryNum] == Boolean.FALSE) {
                arrayFlag[last%entryNum] = Boolean.TRUE;
                node = new MemoryNode();
                node.entryPosition = last%entryNum;
                node.dataSize = (short) data.remaining();
                PMemReaderWriter2.getInstance().write(block, node.entryPosition*entrySize, data);
                currentEntry.incrementAndGet();
                break;
            }
            last++;
        } while (last%entryNum != mark);

        // 找不到
        if (last%entryNum != mark) {
            lastPosition = last+1;
        }

        return node;
    }

    @Override
    public byte[] read(MemoryNode listNode) {
        return PMemReaderWriter2.getInstance().read(block, listNode.entryPosition*entrySize, listNode.dataSize);
    }

    public boolean isEmpty() {
        return currentEntry.get() <= 0;
    }

}
