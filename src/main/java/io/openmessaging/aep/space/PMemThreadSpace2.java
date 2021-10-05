package io.openmessaging.aep.space;

import io.openmessaging.aep.mmu.MemoryNode;
import io.openmessaging.aep.pool.PMemUnit;
import io.openmessaging.aep.pool.PMemUnitPool;
import io.openmessaging.constant.StorageSize;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import sun.misc.Lock;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class PMemThreadSpace2 implements Space2{
    PMemUnitPool pool;
    // 每个线程pmem空间的槽
    // 每个槽表示的单项大小分别为136, 272, 544, 1088, 2176, 4352, 8704, 17408
    private HashMap<Integer, PMemUnit>[] slots = new HashMap[8];
    // 生成每个槽map的key, 表示上次分配的Unit的key
    private Integer[] keys = new Integer[8];
    private int[] entrySizes = new int[8];
    private int[] entryNums = new int[8];
    // 保证内存池分配的原子性锁
    private final Logger logger = LogManager.getLogger(PMemThreadSpace2.class.getName());

    public PMemThreadSpace2(PMemUnitPool pool) {
        this.pool = pool;
        for (int i = 0; i < 8; i++) {
            slots[i] = new HashMap<Integer, PMemUnit>();
            // 初始值-1是因为保证后面开始分配的时候key能从0开始
            keys[i] = -1;
            entrySizes[i] = (int) (17*Math.pow(2, i+3));
            entryNums[i] = (int) (StorageSize.DEFAULT_UNIT_SIZE/entrySizes[i]);
        }
    }

    /**
     * 找到对对应的PMemUnit，释放对应的位置的数据并判断是否可以回收回内存池中*/
    @Override
    public void free(MemoryNode listNode) {
        PMemUnit unit = slots[listNode.slot].get(listNode.key);
        unit.free(listNode);
        // 可以放回内存池中
        if (unit.isEmpty()) {
            slots[listNode.slot].remove(listNode.key);
            pool.deAllocate(unit);
        }
    }

    /**
     * 根据data的大小，定位到合适的slot，尝试向上一个分配的PMem写入数据，
     * 不成功向pool申请新的pmem unit，如果内存池已满则pool申请失败，尝试
     * 遍历当前slot中的所有pmem unit写入*/
    @Override
    public MemoryNode write(byte[] data) {
        MemoryNode node = null;
        PMemUnit unit = null;
        int slot = 0;
        // 定位slot
        for (int i = 0; i < 8; i++) {
            if (data.length <= entrySizes[i]) {
                slot = i;
                break;
            }
        }
        // 尝试向上一个分配的Unit写入
        int key = keys[slot];
        unit = slots[slot].getOrDefault(key, null);
        if (unit != null) {
            node = unit.write(data);
        }

        // 上一个分配的unit写入失败，尝试向pool请求unit
        if (node == null) {
            unit = pool.allocate();
            if (unit != null) {
                key = ++keys[slot];
                slots[slot].putIfAbsent(key, unit);
                unit.reset(entryNums[slot], entrySizes[slot]);
                node = unit.write(data);
            }
        }

        // 遍历slot map
        if (node == null) {
            for(Integer k : slots[slot].keySet()) {
                node = slots[slot].get(k).write(data);
                if (node != null) {
                    key = k;
                    break;
                }
            }
        }
        // 附加上存储位置的
        if (node != null) {
            node.slot = (byte) slot;
            node.key = key;
        }
        return node;
    }

    @Override
    public byte[] read(MemoryNode listNode) {
        try {
            return slots[listNode.slot].get(listNode.key).read(listNode);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("slot: "+listNode.slot+", key: "+listNode.key);
            return null;
        }
    }
}
