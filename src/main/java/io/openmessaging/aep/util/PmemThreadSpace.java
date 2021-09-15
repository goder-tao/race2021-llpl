package io.openmessaging.aep.util;

import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryBlock;
import io.openmessaging.aep.mmu.MemoryListNode;
import io.openmessaging.constant.StorageSize;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * 创建一个线程的pmem空间，每个thread space中有若干fixed size的MemoryBlock，
 * 初始化时先分配一个固定大小的MemoryBlock，当前MemoryBlock读失败后再尝试向Heap
 * 请求分配新的MemoryBlock
 * @author tao */
public class PmemThreadSpace implements Space {
    // 总空间heap
    Heap heap;
    // 已经分配了的MemoryBlock ArrayList
    private final ArrayList<PMemPartitionSpace> partitionSpacesList = new ArrayList<>();
    // 当前partition的标记
    private byte currentPart = 0;
    // 阶段标记, 先向heap请求分配至heap满，满了之后再循环再已分配的partition中寻找合适的空间存储
    private byte stage = 1;
    private boolean full = false;
    public PmemThreadSpace(Heap heap) {
        this.heap = heap;
        MemoryBlock firstBlock = heap.allocateMemoryBlock(StorageSize.DEFAULT_PARTITION_SIZE);
        PMemPartitionSpace firstPartitionSpace = new PMemPartitionSpace(firstBlock, (byte) 0);
        partitionSpacesList.add(firstPartitionSpace);
    }

    @Override
    public void free(MemoryListNode listNode) {
        partitionSpacesList.get(listNode.partiotion).free(listNode);
    }

    /**
     * 两个阶段，第一阶段尝试从heap分配新的partition，第二阶段heap已经不能分配新partition，
     * 循环从已经分配的partitionList中寻找能够写入的partition*/
    @Override
    public MemoryListNode write(byte[] data) {
        MemoryListNode memoryListNode = null;
        memoryListNode = partitionSpacesList.get(currentPart).write(data);
        // 上一块partitionSpace已满
        if (memoryListNode == null) {
            if (!full) {
                // 尝试分配新的partition
                MemoryBlock newBlock = null;
                try {
                    newBlock =  heap.allocateMemoryBlock(StorageSize.DEFAULT_PARTITION_SIZE);
                } catch (Exception e) {
                    // 捕捉heap无法再分配的错误， 标记满
                    full = true;
                    System.out.println();
                }
                if (newBlock != null) {
                    currentPart++;
                    PMemPartitionSpace newPartitionSpace = new PMemPartitionSpace(newBlock, currentPart);
                    partitionSpacesList.add(newPartitionSpace);
                    memoryListNode = newPartitionSpace.write(data);
                }
            } else {
                // 二阶段再partition中循环寻找合适的空间
                if (stage == 2) {
                    int current = currentPart;
                    int move = currentPart+1;
                    // 寻找
                    while (move%partitionSpacesList.size() != current) {
                        memoryListNode = partitionSpacesList.get(move).write(data);
                        // 找到合适的partition
                        if (memoryListNode != null) {
                            break;
                        }
                    }
                }
            }
        }
        return memoryListNode;
    }

    @Override
    public byte[] read(MemoryListNode listNode) {
        return partitionSpacesList.get(listNode.partiotion).read(listNode);
    }

    public ByteBuffer readDataAndFree(MemoryListNode memoryListNode) {
        return partitionSpacesList.get(memoryListNode.partiotion).readDataAndFree(memoryListNode);
    }

    /**
     * 阶段变换*/
    public void changeStage() {
        stage = 2;
    }

    public void setStage(byte stage) {
        this.stage = stage;
    }
}
