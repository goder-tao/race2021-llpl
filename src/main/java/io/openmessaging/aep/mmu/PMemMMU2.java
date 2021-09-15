package io.openmessaging.aep.mmu;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import sun.misc.Lock;

import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * 仅负责主MemoryBlock内存情况的记录及分配，使用MemoryList链表记录当前已经分配了
 * 和还没有分配的内存块的offset和size
 *
 * @author tao
 */
public class PMemMMU2 implements MMU2 {
    private final MemoryList memoryList;
    private Lock lock = new Lock();
    private Logger logger = LogManager.getLogger(PMemMMU.class.getName());
    private ConcurrentLinkedDeque<MemoryListNode> memoryLists = new ConcurrentLinkedDeque<>();

    public PMemMMU2(long size) {
        memoryList = new MemoryList(size, this);
    }

    /**
     * 释放mainBlock空间，主要修改MemoryListNode的使用情况，尝试和前后node进行合并，
     * 保证MemoryList的并发安全
     */
    @Override
    public void free(MemoryListNode node) {
        try {
            lock.lock();
            node.changeState();
            if (!node.preNode.used) {
                node.blockSize += node.preNode.blockSize;
                node.blockOffset = node.preNode.blockOffset;
                node.preNode = node.preNode.preNode;
                node.preNode.nextNode = node;
            }
            if (!node.nextNode.used) {
                node.blockSize += node.nextNode.blockSize;
                node.nextNode = node.nextNode.nextNode;
                node.nextNode.preNode = node;
            }
        } catch (Exception e) {
            logger.error("Free memory fail, " + e.toString());
        } finally {
            lock.unlock();
        }
    }

    /**
     * 使用下次最差算法在MeoryList中寻找空间合适的存储空间，标记对应node为使用，如果有剩余空间，
     * 需要新建一个node标记为未使用，更新新node的offset和size。保证MemoryList的并发安全
     */
    @Override
    public MemoryListNode allocate(long size) {
        try {
            lock.lock();
            // 判断是否扫描完表
            MemoryListNode flag = memoryList.presentNode;
            MemoryListNode move = memoryList.presentNode;

            while (move.used || move.blockSize < size) {
                move = move.nextNode;
                if (move == flag) {
                    return null;
                }
            }

            // use move node space
            if (move.blockSize == size) {
                move.changeState();
                memoryList.presentNode = move;
                return move;
            }
            // create new node
            MemoryListNode newNode = new MemoryListNode(move.blockOffset, size, move.preNode, move, this, "");
            move.blockOffset += size;
            move.blockSize -= size;
            move.preNode.nextNode = newNode;
            move.preNode = newNode;
            memoryList.presentNode = move;
            return newNode;
        } catch (Exception e) {
            logger.error("Allocate memory fail, " + e.toString());
            return null;
        } finally {
            lock.unlock();
        }
    }

    public void setPosition(MemoryListNode memoryListNode) {
        memoryList.presentNode = memoryListNode;
    }

}
