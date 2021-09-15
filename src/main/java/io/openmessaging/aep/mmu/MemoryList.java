package io.openmessaging.aep.mmu;

/**
 * 双链表管理空间的使用情况
 * @author tao */
public class MemoryList {
    public MemoryListNode presentNode;

    public MemoryList(long size, PMemMMU2 pMemMMU){
        presentNode= new MemoryListNode(0, size, null, null, pMemMMU, Thread.currentThread().getName());
        presentNode.preNode = presentNode;
        presentNode.nextNode = presentNode;
        presentNode.changeState();
    }
}



