package io.openmessaging.manager;

import io.openmessaging.aep.mmu.MemoryListNode;
import io.openmessaging.aep.util.PMemSpace;
import io.openmessaging.constant.DataFileBasicInfo;
import io.openmessaging.constant.MntPath;
import io.openmessaging.constant.StatusCode;
import io.openmessaging.constant.StorageSize;
import io.openmessaging.dramcache.DRAMCache;
import io.openmessaging.scheduler.Disk2AepScheduler;
import io.openmessaging.scheduler.PriorityListNode;
import io.openmessaging.ssd.SSDWriterReader;
import io.openmessaging.util.ByteBufferUtil;
import io.openmessaging.util.PartitionMaker;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import sun.misc.Lock;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class Manager {
    // aep冷空间和热空间
    private final PMemSpace coolBlock;
    private final PMemSpace hotBlock;
    private final Logger logger = LogManager.getLogger(Manager.class.getName());
    private final SSDWriterReader ssdWriterReader = new SSDWriterReader();
    // 保存每个冷queue在aep冷空间的最后一个offset值
    private final ConcurrentHashMap<String, Map<Integer, PriorityListNode>> coldTopicQueueMap = new ConcurrentHashMap<>();
    // 获取到topic+qid下当前插入数据数的offset
    private final ConcurrentHashMap<String, Map<Integer, Long>> topicQueueOffset = new ConcurrentHashMap<>();
    // 分配给当前queue数据的.data文件偏移量
    private final ConcurrentHashMap<String, Map<Integer, Map<String, Long>>> dataFileOffset = new ConcurrentHashMap<>();
    // 保存在cold aep中的handle
    private final ConcurrentHashMap<String, Map<Integer, Map<Long, MemoryListNode>>> coldTopicQueueOffsetHandle = new ConcurrentHashMap<>();
    // hot space handle
    private final ConcurrentHashMap<String, Map<Integer, Map<Long, MemoryListNode>>> hotTopicQueueOffsetHandle = new ConcurrentHashMap<>();
    // scheduler
    private final Disk2AepScheduler scheduler;
    // lock definition
    private final Lock writeLock = new Lock();
    // memory cache
    private volatile DRAMCache dramCache;
    private AtomicBoolean isStageChanged = new AtomicBoolean(false);

    public Manager() {
        coolBlock = new PMemSpace(MntPath.AEP_PATH+"cold", StorageSize.COLD_SPACE_SIZE);
        hotBlock = new PMemSpace(MntPath.AEP_PATH+"hot", StorageSize.HOT_SPACE_SIZE);
        scheduler = new Disk2AepScheduler(coolBlock, coldTopicQueueOffsetHandle);
    }

    public AtomicLong sumPMemIO = new AtomicLong(0), sumMapTime = new AtomicLong(0),
            sumDiskIO = new AtomicLong(0), sumAppendTime = new AtomicLong(0);

    /**
     * 写方法*/
    public long append(String topic, int queueId, ByteBuffer data){
        long sTime = System.nanoTime();
//        long pmemIOTIme = 0, mapTime, write2DiskTime;

        long appendOffset;
        long dataOffset;
        long indexOffsetC;
        String partitionPath;

        Map<Integer, Long> queueOffset;
        queueOffset = getOrPutDefault(topicQueueOffset, topic, new ConcurrentHashMap<>());
        appendOffset = queueOffset.getOrDefault(queueId, 0L);
        queueOffset.put(queueId, appendOffset+1);
        // 确定分区
        int partition = (int) (appendOffset / DataFileBasicInfo.ITEM_NUM);
        indexOffsetC = appendOffset % DataFileBasicInfo.ITEM_NUM;
        partitionPath = PartitionMaker.makePartitionPath(partition, DataFileBasicInfo.FILE_NAME_LENGTH, DataFileBasicInfo.ITEM_NUM);
        // .data文件当前offset
        Map<Integer, Map<String, Long>> dataFileQueueMap = getOrPutDefault(dataFileOffset, topic, new ConcurrentHashMap<>());
        Map<String, Long> dataFilePartitionMap = getOrPutDefault(dataFileQueueMap, queueId, new ConcurrentHashMap<>());
        dataOffset = dataFilePartitionMap.getOrDefault(partitionPath, 0L);
        dataFilePartitionMap.put(partitionPath, dataOffset+data.capacity());

//        mapTime = System.nanoTime();

//        try {
//            writeLock.lock();
//            // 互斥获取当前appendOffset
//            Map<Integer, Long> queueOffset;
//            queueOffset = getOrPutDefault(topicQueueOffset, topic, new ConcurrentHashMap<>());
//            appendOffset = queueOffset.getOrDefault(queueId, 0L);
//            queueOffset.put(queueId, appendOffset+1);
//            // 确定分区
//            int partition = (int) (appendOffset / DataFileBasicInfo.ITEM_NUM);
//            indexOffsetC = appendOffset % DataFileBasicInfo.ITEM_NUM;
//            partitionPath = PartitionMaker.makePartitionPath(partition, DataFileBasicInfo.FILE_NAME_LENGTH, DataFileBasicInfo.ITEM_NUM);
//            // .data文件当前offset
//            Map<Integer, Map<String, Long>> dataFileQueueMap = getOrPutDefault(dataFileOffset, topic, new ConcurrentHashMap<>());
//            Map<String, Long> dataFilePartitionMap = getOrPutDefault(dataFileQueueMap, queueId, new ConcurrentHashMap<>());
//            dataOffset = dataFilePartitionMap.getOrDefault(partitionPath, 0L);
//            dataFilePartitionMap.put(partitionPath, dataOffset+data.capacity());
//            writeLock.unlock();
//        } catch (Exception e) {
//            logger.error("Append writeLock: "+e.toString());
//            return -1L;
//        }finally {
//            writeLock.unlock();
//        }

        Map<Integer, PriorityListNode> coldQueueMap = getOrPutDefault(coldTopicQueueMap, topic, new ConcurrentHashMap<>());
        PriorityListNode node = coldQueueMap.getOrDefault(queueId, null);

        // .index文件数据
        ByteBuffer indexData = ByteBuffer.allocate(10);
        indexData.putLong(dataOffset);
        indexData.putShort((short) data.capacity());

        // 多线程双写, buffer并发不安全
        ByteBuffer b1 = ByteBufferUtil.copyFrom(data);

        // .index .data并发双写
        Thread writeSSDData = new Thread(new Runnable() {
            @Override
            public void run() {
                int writeStatus = ssdWriterReader.write(MntPath.SSD_PATH+topic+"/"+queueId+"/", partitionPath+".data", dataOffset, data);
                if (writeStatus == StatusCode.ERROR) {
                    logger.error("Manager write disk data fail, status code:" + writeStatus);
                }
            }
        }), writeSSDIndex = new Thread(new Runnable() {
            @Override
            public void run() {
                int writeStatus = ssdWriterReader.write(MntPath.SSD_PATH+topic+"/"+queueId+"/", partitionPath+".index", indexOffsetC*10, indexData);
                if (writeStatus == StatusCode.ERROR) {
                    logger.error("Manager write disk index fail, status code:" + writeStatus);
                }
            }
        });
        writeSSDData.start();
        writeSSDIndex.start();

//        write2DiskTime = System.nanoTime();

        // 分阶段
        if (!isStageChanged.get()) {  // 第一阶段
            // 尝试写aep
            FutureTask<MemoryListNode> writePMemFutureTask = new FutureTask<>(new Callable<MemoryListNode>() {
                @Override
                public MemoryListNode call() throws Exception {
                    return writePMemOnly(coolBlock, topic, queueId, b1);
                }
            });
            Thread writePMem = new Thread(writePMemFutureTask);
            writePMem.start();
            try {
                writePMem.join();
            } catch (Exception e) {
                logger.error("Write aep thread: "+e.toString());
            }

            // 尝试获取写入ape的handle
            MemoryListNode memoryListNode;
            try {
                memoryListNode = writePMemFutureTask.get();
            } catch (Exception e) {
                logger.error("Write aep future task get: "+e.toString());
                return -1;
            }

//            pmemIOTIme = System.nanoTime();

            // 写入aep成功
            if (memoryListNode != null) {
                Map<Integer, Map<Long, MemoryListNode>> queueOffsetHandle = getOrPutDefault(coldTopicQueueOffsetHandle, topic, new ConcurrentHashMap<>());
                Map<Long, MemoryListNode> offsetHandle = getOrPutDefault(queueOffsetHandle, queueId, new ConcurrentHashMap<>());
                offsetHandle.put(appendOffset, memoryListNode);
                if (node == null) {
                    // 成功写入aep冷空间且队列信息未注册，一阶段全部视为冷队列
                    node = new PriorityListNode(topic, queueId);
                    scheduler.queuePriorityList.enterListHead(node);
                    coldQueueMap.put(queueId, node);
                }
                // 更新队列信息
                node.queueDataSize.addAndGet(data.capacity());
                if (appendOffset+1 > node.tailOffset.get()) {
                    node.tailOffset.set(appendOffset+1);
                }
            }
        } else {  // 第二阶段
            // 冷热队列判断, 不再有新的队列加入进来, 热队列会被删掉, 根据null值判队列冷热
            if(node == null) {  // 热队列
                if(dramCache.isCacheAvailable()) {  // 缓存在dram
                    dramCache.put(topic+queueId, appendOffset, data);
                } else {  // 缓存在aep
                    MemoryListNode memoryListNode = writePMemOnly(hotBlock, topic, queueId, data);
                    // 写入aep成功
                    if (memoryListNode != null) {
                        Map<Integer, Map<Long, MemoryListNode>> queueOffsetHandle = getOrPutDefault(hotTopicQueueOffsetHandle, topic, new ConcurrentHashMap<>());
                        Map<Long, MemoryListNode> offsetHandle = getOrPutDefault(queueOffsetHandle, queueId, new ConcurrentHashMap<>());
                        offsetHandle.put(appendOffset, memoryListNode);
                    }
                }
            }
        }

//        // System.out.println("Append spend time: "+(write2DiskTime-sTime)+"ns, map time: "+(mapTime-sTime)+"ns, pmem io time: "+(pmemIOTIme-mapTime)+"ns, disk io time: "+(write2DiskTime-mapTime)+"ns");
//        sumAppendTime.addAndGet(pmemIOTIme-sTime);
//        sumMapTime.addAndGet(mapTime-sTime);
//        sumPMemIO.addAndGet(pmemIOTIme-write2DiskTime);
//        sumDiskIO.addAndGet(write2DiskTime-mapTime);
//        System.out.printf("Append spend time: %dns, map time: %dns, pmem io time: %dns, disk io time: %dns\n" +
//                        "Spend time - map time: %f%%, pmem io: %f%%, hdd io: %f%%\n\n",
//                pmemIOTIme-sTime, mapTime-sTime, pmemIOTIme-write2DiskTime, write2DiskTime-mapTime,
//                (double)sumMapTime.get()/sumAppendTime.get(), (double)sumPMemIO.get()/sumAppendTime.get(),
//                (double)sumDiskIO.get()/sumAppendTime.get());

        try {
            writeSSDData.join();
            writeSSDIndex.join();
        } catch (Exception e) {
            logger.error("Write SSD thread: "+e.toString());
            return -1L;
        }
        return appendOffset;
    }

    /**
     * 读方法实现*/
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        // 阶段转换时需要做的一些操作，只做一次
        if (!isStageChanged.get()) {
            isStageChanged.set(true);
            isStageChanged.set(true);
            scheduler.run();
        }

        dramCache = DRAMCache.createOrGetCache();

        // 返回结果
        Map<Integer, ByteBuffer> dataMap;
        ByteBuffer data;
        // 队列信息节点
        Map<Integer, PriorityListNode> coldQueueMap = getOrPutDefault(coldTopicQueueMap, topic, new ConcurrentHashMap<>());
        PriorityListNode node = coldQueueMap.getOrDefault(queueId, null);

        // TODO 队列分开处理
        if (node == null) {  // 热队列
            dataMap = readHotQueueData(topic, queueId, offset, fetchNum);
        } else {  // 未验证或是冷队列
            if(node.isVerified == 0) {  // 未验证
                node.isVerified = 1;
                if(offset == 0) {  // 冷队列
                    dataMap = readColdQueueData(topic, queueId, offset, fetchNum, node);
                } else {  // 热队列，清除aep冷空间和当前queue的数据
                    // 清除优先队列中的节点
                    coldQueueMap.remove(queueId);
                    scheduler.queuePriorityList.deList(node);

                    // 清除aep冷空间queue数据
                    Map<Integer, Map<Long, MemoryListNode>> coldQueueOffsetHandle = coldTopicQueueOffsetHandle.get(topic);
                    Map<Long, MemoryListNode> coldOffsetHandle = coldQueueOffsetHandle.get(queueId);
                    coldQueueOffsetHandle.remove(queueId);
                    new Thread(new PMemCleaner(coldOffsetHandle.values().iterator())).start();

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
     * @return: handle of allocated MemoryBlock*/
    MemoryListNode writePMemOnly(PMemSpace space, String topic, int queueId, ByteBuffer data) {
        return space.write(data.array());
    }

    /**
     * 获取冷队列的数据，并负责处理在disk的读情况
     * @param coldQueueNode - 冷队列的node，在确定了冷队列的前提下调用的方法*/
    Map<Integer, ByteBuffer> readColdQueueData(String topic, int queueId, long offset, int fetchNum, PriorityListNode coldQueueNode) {
        Map<Integer, ByteBuffer> dataMap = new HashMap<>();
        ByteBuffer data;


        Map<Long, MemoryListNode> coldOffsetHandle = getOrNull(getOrNull(coldTopicQueueOffsetHandle, topic), queueId);

        int i;
        for(i = 0; i < fetchNum; i++) {
            MemoryListNode handle = coldOffsetHandle.get(offset+i);
            if (handle == null) {
                break;
            }
            // 从aep冷空间读取并移除消息
            data = coolBlock.readDataAndFree(handle);
            dataMap.put(i, data);
            coldQueueNode.queueDataSize.addAndGet(-data.capacity());
            coldOffsetHandle.remove(offset+i);

        }
        // 需要从ssd读
        if (i != fetchNum) {
            // 存在aep冷空间没有保存的数据，从ssd中取顺便做一些调整
            Map<Long, byte[]> map = ssdWriterReader.directRead(topic, queueId, offset+i, fetchNum-i);
            byteArrayMergeMap(dataMap, map, offset);
            // 重新调整调度器的起点
            coldQueueNode.tailOffset.set(offset+fetchNum+1);
        }
        // 冷队列信息节点加入队列head, 刚刚被消费过的队列增加调度的机会
        scheduler.queuePriorityList.deList(coldQueueNode);
        scheduler.queuePriorityList.enterListHead(coldQueueNode);
        return dataMap;
    }

    /**
     * 获取热队列的数据*/
    Map<Integer, ByteBuffer> readHotQueueData(String topic, int queueId, long offset, int fetchNum) {
        Map<Integer, ByteBuffer> dataMap = new HashMap<>();
        ByteBuffer data;

        for(int i = 0; i < fetchNum; i++) {
            if (dramCache == null) {
                logger.fatal("DRAM cache is null");
                return null;
            }

            data = dramCache.getAndRemove(topic+queueId, offset+i);
            // 未缓存在dram中
            if(data == null) {
                // 尝试在热aep中获得数据
                MemoryListNode handle = getOrNull(getOrNull(getOrNull(hotTopicQueueOffsetHandle, topic),queueId),offset+i);
                if (handle == null) {  // aep 热空间无缓存，只能走磁盘
                    // 寻找连续不在aep的offset
                    int j;
                    for(j = i+1; j < fetchNum;j++) {
                        handle = getOrNull(getOrNull(getOrNull(hotTopicQueueOffsetHandle, topic),queueId),offset+j);
                        if (handle != null) break;
                    }
                    Map<Long, byte[]> map = ssdWriterReader.directRead(topic, queueId, offset+i, j-i);
                    byteArrayMergeMap(dataMap, map, offset);
                    // 恢复循环的正常进行
                    i = j - 1;
                    continue;
                } else {
                    data = hotBlock.readDataAndFree(handle);
                }
            }
            dataMap.put(i, data);
        }
        return dataMap;
    }

    /**
     * 内部PMem数据清除类，第二阶段清除所有热队列在aep冷空间的数据*/
    class PMemCleaner implements Runnable{
        private Iterator<MemoryListNode> handles;
        PMemCleaner(Iterator<MemoryListNode> handles) {
            this.handles = handles;
        }
        @Override
        public void run() {
           while (handles.hasNext()) {
               MemoryListNode handle = handles.next();
               coolBlock.free(handle);
           }
        }
    }

    /**
     * 尝试获取map, 减少多级map的null判断*/
    private <K,V> V getOrNull(Map<K,V> map, K key) {
        if (map == null) {
            return null;
        }
        return map.get(key);
    }
    /**
     * 合并两个map*/
    private Map<Integer, ByteBuffer> mergeMap(Map<Integer, ByteBuffer> dst, Map<Integer, ByteBuffer> map) {
        for (int key:map.keySet()) {
            dst.put(key, map.get(key));
        }
        return dst;
    }

    private void byteArrayMergeMap(Map<Integer, ByteBuffer> dst, Map<Long, byte[]> map, long offset) {
        for (long key:map.keySet()) {
            byte[] b = map.get(key);
            ByteBuffer buffer = ByteBuffer.allocate(b.length);
            buffer.put(b);
            buffer.rewind();
            dst.put((int) (key-offset), buffer);
        }
    }

    private <K, V> V getOrPutDefault(Map<K, V> map, K key, V defaultValue){
        V retObj = map.get(key);
        if(retObj != null){
            return retObj;
        }
        map.put(key, defaultValue);
        return defaultValue;
    }

}
