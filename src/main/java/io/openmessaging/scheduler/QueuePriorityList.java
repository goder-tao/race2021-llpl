package io.openmessaging.scheduler;


import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import sun.misc.Lock;

/**
 * 带头节点的双向环形队列，每次寻找queue调度数据都从队列头部开始寻找合适的node*/
public class QueuePriorityList {
    PriorityListNode head = null;
    private Lock enterLock = new Lock(), deLock = new Lock();
    private Logger logger = LogManager.getLogger(QueuePriorityList.class.getName());
    // queue数量
    int node_n = 0;
    public QueuePriorityList() {

    }

    /**
     * 进入list头*/
    public void enterListHead(PriorityListNode listNode) {
        try {
            enterLock.lock();
            if (head == null) {
                head = listNode;
                head.next = head;
                head.pre = head;
            } else {
                listNode.pre = head.pre;
                listNode.next = head;
                head.pre.next = listNode;
                head.pre = listNode;
                head = listNode;
            }
            node_n++;
        } catch (Exception e) {
            logger.error("PriorityList enterListHead(): "+e.toString());
        } finally {
            enterLock.unlock();
        }
    }

    /**
     * 从将listNode的pre和next关系安全删除*/
    public void deList(PriorityListNode listNode) {
        try {
            deLock.lock();
            if (listNode == head) {
                head = null;
            } else {
                listNode.pre.next = listNode.next;
                listNode.next.pre = listNode.pre;
                listNode.next = null;
                listNode.pre = null;
            }
            node_n--;
        } catch (Exception e) {
            logger.error("PriorityList deList(): "+e.toString());
        } finally {
            deLock.unlock();
        }
    }
}
