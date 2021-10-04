package io.openmessaging.manager;

import io.openmessaging.aep.mmu.MemoryNode;
import io.openmessaging.aep.space.PMemSpace2;
import io.openmessaging.constant.MntPath;
import io.openmessaging.constant.StorageSize;
import io.openmessaging.dramcache.DRAMCache;
import io.openmessaging.scheduler.Disk2AepScheduler2;
import io.openmessaging.scheduler.PriorityNode;
import io.openmessaging.ssd.aggregator.Aggregator;
import io.openmessaging.ssd.aggregator.Message4Flush;
import io.openmessaging.ssd.aggregator.MessagePutRequest;
import io.openmessaging.ssd.index.IndexHandle;
import io.openmessaging.ssd.util.SSDWriterReader5MMAP;
import io.openmessaging.util.MapUtil;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class Manager {
    // aep冷空间和热空间
    private final PMemSpace2 coolBlock;
    private final PMemSpace2 hotBlock;
    private final SSDWriterReader5MMAP ssdWriterReader = SSDWriterReader5MMAP.getInstance();
    // 保存每个冷queue在aep冷空间的最后一个offset值以及当前冷队列使用了多少aep空间
    private final ConcurrentHashMap<String, Map<Integer, PriorityNode>> coldTopicQueuePriMap = new ConcurrentHashMap<>();
    // 获取到topic+qid下当前插入数据数的offset
    private final ConcurrentHashMap<String, Map<Integer, Long>> topicQueueOffset = new ConcurrentHashMap<>();
    // 保存在cold aep中的handle
    private final ConcurrentHashMap<String, Map<Integer, Map<Long, MemoryNode>>> coldTopicQueueOffsetHandle = new ConcurrentHashMap<>();
    // hot space handle
    private final ConcurrentHashMap<String, Map<Integer, Map<Long, MemoryNode>>> hotTopicQueueOffsetHandle = new ConcurrentHashMap<>();
    // 记录topic的线程名字
    private final ConcurrentHashMap<String, String> topicThreadName = new ConcurrentHashMap<>();
    // scheduler
    private final Disk2AepScheduler2 scheduler;
    // memory cache
    private volatile DRAMCache dramCache = new DRAMCache();
    // 聚合器
    private final Aggregator aggregator;
    // index handle
    private final IndexHandle indexHandle;
    // 清除热队列在cold space中的数据任务的线程池
    private final ExecutorService cleanService = Executors.newFixedThreadPool(3);
    // 阶段标记
    private AtomicBoolean isStageChanged = new AtomicBoolean(false);
    private final Logger logger = LogManager.getLogger(Manager.class.getName());
    // 阶段时间s
    private long createTime;

    public Manager() {
        coolBlock = new PMemSpace2(MntPath.AEP_PATH + "cold", StorageSize.COLD_SPACE_SIZE);
        hotBlock = new PMemSpace2(MntPath.AEP_PATH + "hot", StorageSize.HOT_SPACE_SIZE);
        scheduler = new Disk2AepScheduler2(coolBlock, coldTopicQueueOffsetHandle);
        indexHandle = IndexHandle.getInstance();
        aggregator = new Aggregator(indexHandle);
        Thread aggThread = new Thread(aggregator);
        aggThread.start();
        createTime = System.nanoTime();
    }

    public AtomicLong sumPMemIO = new AtomicLong(0), sumMapTime = new AtomicLong(0),
            sumDiskIO = new AtomicLong(0), sumAppendTime = new AtomicLong(0);

    /**
     * 写方法
     */
    public long append(String topic, int queueId, ByteBuffer data) {
        long startFlag = System.nanoTime();
        long pmemIOFlag = 0, mapFlag, write2DiskFlag;

        long appendOffset;
        String tName = Thread.currentThread().getName().split("-")[1];
        topicThreadName.putIfAbsent(topic, tName);
        // append offset
        Map<Integer, Long> queueOffset;
        queueOffset = MapUtil.getOrPutDefault(topicQueueOffset, topic, new HashMap<>());
        appendOffset = queueOffset.getOrDefault(queueId, 0L);
        queueOffset.put(queueId, appendOffset + 1);

        mapFlag = System.nanoTime();

        assert data.limit() == data.capacity();

//        ByteBuffer b1 = ByteBufferUtil.copyFrom(data);

        // 生成MessageRequest
        int hashKey = (topic+"#"+queueId+"#"+appendOffset).hashCode();
        Message4Flush message4Flush = new Message4Flush(data.array(), hashKey);
        MessagePutRequest request = new MessagePutRequest(message4Flush);

        //
        aggregator.putMessageRequest(request);
        request.getResponse();

        write2DiskFlag = System.nanoTime();

        // 获取冷队列的一些信息
        Map<Integer, PriorityNode> coldQueueMap = MapUtil.getOrPutDefault(coldTopicQueuePriMap, topic, new HashMap<>());
        PriorityNode node = coldQueueMap.getOrDefault(queueId, null);

        // 分阶段
        if (!isStageChanged.get()) {  // 第一阶段
            // 尝试写aep
            MemoryNode memoryNode = writePMemOnly(coolBlock, data.array(), tName);

            pmemIOFlag = System.nanoTime();

            // 写入aep成功
            if (memoryNode != null) {
                Map<Integer, Map<Long, MemoryNode>> queueOffsetHandle = MapUtil.getOrPutDefault(coldTopicQueueOffsetHandle, topic, new HashMap<>());
                Map<Long, MemoryNode> offsetHandle = MapUtil.getOrPutDefault(queueOffsetHandle, queueId, new HashMap<>());
                offsetHandle.put(appendOffset, memoryNode);
                if (node == null) {
                    // 成功写入aep冷空间且队列信息未注册，一阶段全部视为冷队列
                    node = new PriorityNode();
                    PriorityNode preNode = coldQueueMap.putIfAbsent(queueId, node);
                    if (preNode != null) {
                        node = preNode;
                    } else {
                        // 增加冷队列数量
                        scheduler.increaseQueueCount();
                    }
                }
                // 更新队列信息
                node.queueDataSize.addAndGet(data.capacity());
                // 更新下一个不在aep中的offset， 当前offset已经加入到aep中
                if (appendOffset + 1 > node.tailOffset.get()) {
                    node.tailOffset.set(appendOffset + 1);
                }
            }
        } else {  // 第二阶段
            // 冷热队列判断, 不再有新的队列加入进来, 热队列会被删掉, 根据null值判队列冷热
            if (node == null) {  // 热队列
                if (dramCache != null && dramCache.isCacheAvailable()) {  // 缓存在dram
                    dramCache.put(topic + queueId, appendOffset, data);
                } else {  // 缓存在aep

                    MemoryNode memoryNode = writePMemOnly(hotBlock, data.array(), tName);
                    pmemIOFlag = System.nanoTime();

                    // 写入aep成功
                    if (memoryNode != null) {
                        Map<Integer, Map<Long, MemoryNode>> queueOffsetHandle = MapUtil.getOrPutDefault(hotTopicQueueOffsetHandle, topic, new HashMap<>());
                        Map<Long, MemoryNode> offsetHandle = MapUtil.getOrPutDefault(queueOffsetHandle, queueId, new HashMap<>());
                        offsetHandle.put(appendOffset, memoryNode);
                    }
                }
            }
        }

//        if (pmemIOFlag != 0) {
//            long appendTime = pmemIOFlag - startFlag;
//            long mapTime = mapFlag - startFlag;
//            long pmemIOTime = pmemIOFlag - write2DiskFlag;
//            long writeDiskTime = write2DiskFlag - mapFlag;
//            sumAppendTime.addAndGet(appendTime);
//            sumMapTime.addAndGet(mapTime);
//            sumPMemIO.addAndGet(pmemIOTime);
//            sumDiskIO.addAndGet(writeDiskTime);
//            System.out.printf("Append spend time: %dns, map time: %dns, pmem io time: %dns, disk io time: %dns\n" +
//                            "Spend time - map time: %f%%, pmem io: %f%%, hdd io: %f%%\n\n",
//                    appendTime, mapTime, pmemIOTime, writeDiskTime,
//                    (double) sumMapTime.get() / sumAppendTime.get(), (double) sumPMemIO.get() / sumAppendTime.get(),
//                    (double) sumDiskIO.get() / sumAppendTime.get());
//        }

        return appendOffset;
    }

    /**
     * 读方法实现
     */
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        // 阶段转换时需要做的一些操作，只做一次
        if (isStageChanged.compareAndSet(false, true)) {
            coolBlock.changeStage();
            hotBlock.changeStage();
            dramCache.startDetect();
            // 二阶段开始调度
            Thread schedulerThread = new Thread(scheduler);
            schedulerThread.start();
            logger.info("stage one spend time: "+(System.nanoTime()-createTime)+"ns");
            ssdWriterReader.printInfo();
        }

        // 返回结果
        Map<Integer, ByteBuffer> dataMap;
        ByteBuffer data;
        // 队列信息节点
        Map<Integer, PriorityNode> coldQueueMap = MapUtil.getOrPutDefault(coldTopicQueuePriMap, topic, new HashMap<>());
        PriorityNode node = coldQueueMap.getOrDefault(queueId, null);

        // TODO 队列分开处理
        if (node == null) {  // 热队列
            dataMap = readHotQueueData(topic, queueId, offset, fetchNum);
        } else {  // 未验证或是冷队列
            if (node.isVerified.compareAndSet(false, true)) {  // 未验证
                if (offset == 0) {  // 冷队列
                    dataMap = readColdQueueData(topic, queueId, offset, fetchNum, node);
                    scheduler.putSchedulerTask(topic, queueId, topicThreadName.get(topic), fetchNum, node);
                } else {  // 热队列，清除aep冷空间和当前queue的数据
                    // 清除优先队列中的节点
                    coldQueueMap.remove(queueId);
                    // 减少冷队列数
                    scheduler.decreaseQueueCount();
                    // 清除aep冷空间queue数据
                    Map<Integer, Map<Long, MemoryNode>> coldQueueOffsetHandle = coldTopicQueueOffsetHandle.get(topic);
                    Map<Long, MemoryNode> coldOffsetHandle = coldQueueOffsetHandle.get(queueId);
                    coldQueueOffsetHandle.remove(queueId);
                    // 开始清理热队列在coldSpace中的数据
                    cleanService.submit(new PMemCleaner(coldOffsetHandle.values().iterator()));

                    // 热队列读
                    dataMap = readHotQueueData(topic, queueId, offset, fetchNum);
                }
            } else {  // 冷队列
                dataMap = readColdQueueData(topic, queueId, offset, fetchNum, node);
            }
        }
        return dataMap;
    }

    /**
     * 完成只写aep的工作
     * @return: handle of allocated MemoryBlock
     */
    MemoryNode writePMemOnly(PMemSpace2 space, byte[] data, String tName) {
        return space.write(data, tName);
    }

//    int c = 0;

    /**
     * 获取冷队列的数据，并负责处理在disk的读情况
     * @param coldQueueNode - 冷队列的node，在确定了冷队列的前提下调用的方法
     */
    Map<Integer, ByteBuffer> readColdQueueData(String topic, int queueId, long offset, int fetchNum, PriorityNode coldQueueNode) {
        Map<Integer, ByteBuffer> dataMap = new HashMap<>();
        ByteBuffer data;

        Map<Long, MemoryNode> coldOffsetHandle = getOrNull(getOrNull(coldTopicQueueOffsetHandle, topic), queueId);

        int i;
        // 尝试都从aep取
        for (i = 0; i < fetchNum; i++) {
            MemoryNode handle = coldOffsetHandle.get(offset + i);
            if (handle == null) {
                break;
            }
//            c++;
//            System.out.println(c);

            // 从aep冷空间读取并移除消息
            data = coolBlock.readDataAndFree(handle);
            dataMap.put(i, data);
            coldQueueNode.queueDataSize.addAndGet(-data.capacity());
            coldOffsetHandle.remove(offset + i);
        }

        // 需要从ssd读
        if (i != fetchNum) {
//            System.out.println("ssd");
            for(; i < fetchNum; i++) {
                // 从indexhandle获取offset和size
                data = ssdWriterReader.directRead((topic+"#"+queueId+"#"+(offset+i)).hashCode());
                // 读取超过已经写入的范围，不再读取
                if (data == null) {
                    break;
                }
                dataMap.put(i, data);
            }
            // 重新调整调度器的起点
            coldQueueNode.tailOffset.set(offset + i);
        }
        // 冷队列信息节点加入队列head, 刚刚被消费过的队列增加调度的机会
        return dataMap;
    }

    /**
     * 获取热队列的数据
     */
    Map<Integer, ByteBuffer> readHotQueueData(String topic, int queueId, long offset, int fetchNum) {
        Map<Integer, ByteBuffer> dataMap = new HashMap<>();
        ByteBuffer data;
        if (dramCache == null) {
            logger.fatal("DRAM cache is null");
            return null;
        }

        for (int i = 0; i < fetchNum; i++) {
            data = dramCache.getAndRemove(topic + queueId, offset + i);
            // 未缓存在dram中
            if (data == null) {
                // 尝试在热aep中获得数据
                MemoryNode handle = getOrNull(getOrNull(getOrNull(hotTopicQueueOffsetHandle, topic), queueId), offset + i);
                if (handle == null) {  // aep 热空间无缓存，只能走磁盘
                    // 从indexhandle获取offset和size
                    data = ssdWriterReader.directRead((topic+"#"+queueId+"#"+(offset+i)).hashCode());
                    // 读取超过已经写入的范围，不再读取
                    if (data == null) {
                        break;
                    }
                } else {
                    data = hotBlock.readDataAndFree(handle);
                }
            }
            dataMap.put(i, data);
        }
        return dataMap;
    }

    /**
     * 内部PMem数据清除类，第二阶段清除所有热队列在aep冷空间的数据
     */
    class PMemCleaner implements Runnable {
        private Iterator<MemoryNode> handles;

        PMemCleaner(Iterator<MemoryNode> handles) {
            this.handles = handles;
        }

        @Override
        public void run() {
            while (handles.hasNext()) {
                MemoryNode handle = handles.next();
                coolBlock.free(handle);
            }
        }
    }

    /**
     * 尝试获取map, 减少多级map的null判断
     */
    private <K, V> V getOrNull(Map<K, V> map, K key) {
        if (map == null) {
            return null;
        }
        return map.get(key);
    }

    /**
     * 合并两个map
     */
    private Map<Integer, ByteBuffer> mergeMap(Map<Integer, ByteBuffer> dst, Map<Integer, ByteBuffer> map) {
        for (int key : map.keySet()) {
            dst.put(key, map.get(key));
        }
        return dst;
    }

    private void byteArrayMergeMap(Map<Integer, ByteBuffer> dst, Map<Long, byte[]> map, long offset) {
        for (long key : map.keySet()) {
            byte[] b = map.get(key);
            ByteBuffer buffer = ByteBuffer.allocate(b.length);
            buffer.put(b);
            buffer.rewind();
            dst.put((int) (key - offset), buffer);
        }
    }

}