package io.openmessaging.scheduler;

import io.openmessaging.aep.mmu.MemoryNode;
import io.openmessaging.aep.space.PMemSpace2;
import io.openmessaging.ssd.util.SSDWriterReader5MMAP;
import io.openmessaging.util.MapUtil;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 一个队列， 每次有topic+queue的消息被消费就添加入队，实现先被消费的
 * 先进行调度，根据当前queue在aep中占的空间大小给queue分等级，分为空闲，
 * 一般和拥挤，根据等级决定一次调度的消息数目多少
 * @author tao
 * @date 2021-09-23*/
public class Disk2AepScheduler2 implements Runnable {
    // 线程池
    private  final ExecutorService executorService = Executors.newFixedThreadPool(10);
    // 优先调度队列
    private final ConcurrentLinkedQueue<SchedulerTask> priorityQueue = new ConcurrentLinkedQueue();
    // 冷空间
    private final PMemSpace2 coldSpace;
    // 同步更新handle
    private final ConcurrentHashMap<String, Map<Integer, Map<Long, MemoryNode>>> coldTopicQueueOffsetHandle;
    // 记录冷队列数量
    private final AtomicInteger coldQueueCount = new AtomicInteger(0);
    // 信号量提醒调度群调度
    private volatile Semaphore waitPoint = new Semaphore(0);

    private final Logger logger = LogManager.getLogger(Disk2AepScheduler2.class.getName());

    public Disk2AepScheduler2(PMemSpace2 coldSpace, ConcurrentHashMap<String, Map<Integer, Map<Long, MemoryNode>>> topicQueueOffsetHandle) {
        this.coldSpace = coldSpace;
        this.coldTopicQueueOffsetHandle = topicQueueOffsetHandle;
    }

    /**
     * 向优先调度队列添加调度任务*/
    public void putSchedulerTask(String topic, int queueId, String tName, int fetchNum, PriorityNode node) {
        SchedulerTask task = new SchedulerTask(topic, queueId, tName, fetchNum, node);
        priorityQueue.offer(task);
        waitPoint.release();
    }

    public void increaseQueueCount() {
        coldQueueCount.incrementAndGet();
    }

    public void decreaseQueueCount() {
        coldQueueCount.decrementAndGet();
    }

    @Override
    public void run() {
        while (true) {
            try {
                waitPoint.acquire();
                SchedulerTask task = priorityQueue.poll();
                executorService.submit(new SchedulerWorker(task));
            } catch (InterruptedException e) {
                logger.error(e.toString());
            }
        }
    }

    /**
     * 内部进行具体调度任务的类*/
    class SchedulerWorker implements Runnable{
        SchedulerTask task;
        SchedulerWorker(SchedulerTask task) {
            this.task = task;
        }

        @Override
        public void run() {
            long tailOffset = task.node.tailOffset.get();
            Map<Integer, Map<Long, MemoryNode>> queueOffsetHandle = MapUtil.getOrPutDefault(coldTopicQueueOffsetHandle, task.topic, new HashMap<>());
            Map<Long, MemoryNode> offsetHandle = MapUtil.getOrPutDefault(queueOffsetHandle, task.queueId, new HashMap<>());

            // 调度分级
            if (task.node.queueDataSize.get() < 0.2 *coldSpace.getSize() / coldQueueCount.get()) {
                task.fetchNum *= 10;
            } else if (task.node.queueDataSize.get() < 0.5 *coldSpace.getSize() / coldQueueCount.get()) {
                task.fetchNum *= 5;
            } else if (task.node.queueDataSize.get() < 0.8 *coldSpace.getSize() / coldQueueCount.get()) {
                task.fetchNum *= 2;
            }

            // 尝试调度预热fetchNum条消息
            int i;
            for (i = 0; i < task.fetchNum; i++) {
                ByteBuffer bb = SSDWriterReader5MMAP.getInstance().directRead((task.topic+"#"+task.queueId+"#"+(tailOffset+i)).hashCode());
                if (bb == null) break;
                byte[] b = bb.array();
                MemoryNode handle = coldSpace.write(b, task.tName);
                if (handle != null) {  // 分配空间成功，保存
                    offsetHandle.put(tailOffset+i, handle);
                } else {  // 分配空间失败，空间不足，修改tailOffset,退出
                    break;
                }
            }
            // 更新tailOffset
            task.node.tailOffset.set(tailOffset+i);
        }
    }

    /**
     * 一个任务包含进行调度的具体数据*/
    class SchedulerTask {
        String tName;
        String topic;
        int queueId;
        PriorityNode node;
        int fetchNum;

        SchedulerTask(String topic, int queueId, String tName, int fetchNum, PriorityNode node) {
            this.topic = topic;
            this.queueId = queueId;
            this.tName = tName;
            this.fetchNum = fetchNum;
            this.node = node;
        }
    }

}



