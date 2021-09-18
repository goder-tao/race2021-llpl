package io.openmessaging.aep.mmu;

/**
 * 无锁内存池使用的保存数据位置信息的node, 根据node信息能够将数据读取回来
 * @author tao */
public class MemoryNode {
    public short dataSize;
    // 属于的线程
    public String tName;
    // ThreadSpace中的哪个槽
    public byte slot;
    // 每个槽map的key
    public int key;
    // PMemUnit的entry位置
    public int entryPosition;

    public MemoryNode() {

    }
}
