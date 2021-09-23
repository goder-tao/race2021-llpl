package io.openmessaging.scheduler;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 保存和冷队列相关的一些信息，包括下次可调度时间、冷队列
 * 在aep中已占空间大小、冷队列当前在aep中的最后offset是多少
 * 是否队列信息已验证、topic和对应的queueId
 */
public class PriorityListNode {
    public PriorityListNode pre = null, next = null;
    // 并发
    public AtomicLong queueDataSize;
    // 下一个不在aep中的offset
    public AtomicLong tailOffset;
    long availableTime;
    public AtomicBoolean isVerified = new AtomicBoolean(false);
    final String topic;
    final int queueId;
    final String tName;

    public PriorityListNode(String topic, int queueId, String tName) {
        this.topic = topic;
        this.queueId = queueId;
        this.tName = tName;
        availableTime = System.currentTimeMillis();
        queueDataSize = new AtomicLong(0);
        tailOffset = new AtomicLong(0);
    }
}
