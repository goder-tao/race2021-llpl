package io.openmessaging.scheduler;

import io.openmessaging.aep.mmu.MemoryNode;
import io.openmessaging.aep.space.PMemSpace2;
import io.openmessaging.ssd.util.SSDWriterReader2;
import io.openmessaging.ssd.util.SSDWriterReader3;
import io.openmessaging.util.MapUtil;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import sun.misc.Lock;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 在磁盘和ape之间调度冷数据，调度的策略使用一个优先队列, 平均queue在aep的空间的数据，
 * 在aep中保存数据远小于aep给所有队列分配的平均空间值的queue先调度，并且更新一个较小的
 * 不可调度时间，而aep中已经存储了接近平均分配空间的队列分配较大的不可调度时间，减少调度的
 * 优先级。对于经常被消耗的queue，插入优先队列的头部，争取早检查是否满足调度条件
 */
public class Disk2AepScheduler {
    public final QueuePriorityList queuePriorityList = new QueuePriorityList();
    private final SSDWriterReader3 ssdWriterReader = SSDWriterReader3.getInstance();
    ;
    private final PMemSpace2 pmemBlock;
    private boolean isWork = false;
    // 保存在aep中的handle
    private final ConcurrentHashMap<String, Map<Integer, Map<Long, MemoryNode>>> topicQueueOffsetHandle;
    private final Logger logger = LogManager.getLogger(Disk2AepScheduler.class.getName());
    // 单例线程锁
    private Lock lock = new Lock();
    // 单例线程只开一次
    private Thread thread = null;
    // 直接一次性预调度100条数据
    private final int fetchNum = 100;

    public Disk2AepScheduler(PMemSpace2 pmemBlock, ConcurrentHashMap<String, Map<Integer, Map<Long, MemoryNode>>> topicQueueOffsetHandle) {
        this.pmemBlock = pmemBlock;
        this.topicQueueOffsetHandle = topicQueueOffsetHandle;
    }

    /**
     * start scheduling
     */
    public void run() {
        try {
            lock.lock();
            if (thread == null) {
                thread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        PriorityListNode move;
                        // 不停调度
                        while (true) {
                            move = queuePriorityList.head;
                            // 寻找一个合适的queue进行调度
                            while (move != null) {
                                if (System.currentTimeMillis() - move.availableTime > 0) {  // 可调度
                                    new Thread(new SchedulerWorker(move)).start();
                                    break;
                                } else {  // 不可调度转到优先队列下一个节点
                                    move = move.next;
                                }
                            }
                            try {
                                // 调度间隔期
                                Thread.sleep(10);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                });
                thread.start();
            }
            lock.unlock();
        } catch (Exception e) {
            System.out.println("Scheduler new thread," + e.toString());
        } finally {
            lock.unlock();
        }
    }

    // TODO 运行一个内部类进行ssd -> aep的调度
    class SchedulerWorker implements Runnable {
        private PriorityListNode move;

        SchedulerWorker(PriorityListNode listNode) {
            this.move = listNode;
        }

        @Override
        public void run() {
            // 数据调度
            long tailOffset = move.tailOffset.get();

            Map<Integer, Map<Long, MemoryNode>> queueOffsetHandle = MapUtil.getOrPutDefault(topicQueueOffsetHandle, move.topic, new HashMap<>());
            Map<Long, MemoryNode> offsetHandle = MapUtil.getOrPutDefault(queueOffsetHandle, move.queueId, new HashMap<>());

            // 尝试调度预热fetchNum条消息
            int i;
            for (i = 0; i < fetchNum; i++) {
                ByteBuffer bb = ssdWriterReader.directRead((move.topic+move.queueId+(tailOffset+i)).hashCode());
                if (bb == null) break;
                byte[] b = bb.array();
                MemoryNode handle = pmemBlock.write(b, move.tName);
                if (handle != null) {  // 分配空间成功，保存
                    offsetHandle.put(tailOffset+i, handle);
                } else {  // 分配空间失败，空间不足，修改tailOffset,退出
                    break;
                }
            }

            move.tailOffset.set(tailOffset+i);

            if (i > 0) {
                if (move.queueDataSize.get() < pmemBlock.getSize() / queuePriorityList.node_n * 0.8) {
                    // 稀疏队列，下次可调度的时间较短
                    move.availableTime = System.currentTimeMillis() + 1000;
                } else {
                    // 拥挤队列，下次可调度的时间较长
                    move.availableTime = System.currentTimeMillis() + 3000;
                }
            }
        }
    }

}


