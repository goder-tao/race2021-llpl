package io.openmessaging.scheduler;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 保存和冷队列相关的一些信息，包括下次可调度时间、冷队列
 * 在aep中已占空间大小、冷队列当前在aep中的最后offset是多少
 * 是否队列信息已验证、topic和对应的queueId*/
public class PriorityListNode {
    public PriorityListNode pre = null, next = null;
    // 并发
    public AtomicLong queueDataSize;
    public AtomicLong tailOffset;
    long availableTime;
    public volatile byte isVerified = 0;
    final String topic;
    final int queueId;
    public PriorityListNode(String topic, int queueId) {
        this.topic = topic;
        this.queueId = queueId;
        availableTime = System.currentTimeMillis();
    }
}
