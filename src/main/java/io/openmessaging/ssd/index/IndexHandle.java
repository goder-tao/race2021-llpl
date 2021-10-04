package io.openmessaging.ssd.index;

import io.openmessaging.constant.IndexField;
import io.openmessaging.constant.MntPath;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 内包含一个indexfile的mmap，提供对mmap的update, force，
 * 对indexfile三个字段 - head, slot以及linked index的
 * 相关操作，屏蔽底层操作的细节
 * @author tao
 * @date 2021-09-21*/
public class IndexHandle {
    private final Logger logger;
    private volatile MappedByteBuffer mmapedIndex = null;
    private volatile Head head;

    private static IndexHandle instance = new IndexHandle(MntPath.INDEX_FILE_DIR);

    private IndexHandle(String indexFileDir) {
        logger = LogManager.getLogger(IndexHandle.class.getName());
        try {
            File dir = new File(indexFileDir);
            if (!dir.exists()) {
                dir.mkdirs();
            }
            RandomAccessFile raf = new RandomAccessFile(indexFileDir+"index", "rw");
            mmapedIndex = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, IndexField.INDEX_FILE_SIZE);
            head = new Head(mmapedIndex);
        } catch (Exception e) {
            logger.fatal("Create IndexHandle fail, "+e.toString());
        }
    }

    /**
     * 添加一个新的index, 更新slot中的值*/
    public void newIndex(int hashKey, long phyOffset, short size) {
        int slot = Math.abs(hashKey) % IndexField.SLOT_SUM;
        // slot中存的值，也可能是0
        int slot_internal = mmapedIndex.getInt(IndexField.HEAD_SIZE + IndexField.SLOT_SIZE * slot);
        // 获取到最新的一个位置
        int index = head.getAndAddCurrent();

        if (index >= IndexField.INDEX_SUM) {
            // 严重错误，Index的数量超出最大值
            logger.fatal("index count out of range!!!!!!!!!!!!!!!!!!!!!");
        }

        // 针对这个位置设置当前linked index的各个字段值
        int index_pos = IndexField.HEAD_SIZE + IndexField.SLOT_SUM*IndexField.SLOT_SIZE+IndexField.INDEX_SIZE*index;

        // hashKey
        mmapedIndex.putLong(index_pos, hashKey);
        // phyOffset
        mmapedIndex.putLong(index_pos+8, phyOffset);
        // size
        mmapedIndex.putShort(index_pos+16, size);
        // pre index
        mmapedIndex.putInt(index_pos+18, slot_internal);
        mmapedIndex.putInt(IndexField.HEAD_SIZE + IndexField.SLOT_SIZE * slot, index);
    }

    /**
     * 返回一个包含数据offset和size的ByteBuffer*/
    public ByteBuffer getPhyOffsetAndSize(int hashKey) {
        // 槽
        int slot = Math.abs(hashKey) % IndexField.SLOT_SUM;
        // 槽中的数据
        int slot_internal = mmapedIndex.getInt(IndexField.HEAD_SIZE + IndexField.SLOT_SIZE * slot);
        // 当前linked index的第一个index开始位置
        int index_pos;
        // 返回的数据
        byte[] b = new byte[10];
        // 是否找到
        boolean find = false;
        // 模拟一个虚拟节点，这个虚拟节点的preIndex是slot中存的index，方便组织遍历的形式
        int preIndex = slot_internal;
        // 开始遍历链表
        while (preIndex != 0) {
            index_pos = IndexField.HEAD_SIZE + IndexField.SLOT_SUM*IndexField.SLOT_SIZE+IndexField.INDEX_SIZE*preIndex;
            for (int i = 0; i < 10; i++) {
                b[i] = mmapedIndex.get(index_pos+8+i);
            }
            preIndex = mmapedIndex.getInt(index_pos+18);
            if (hashKey == mmapedIndex.getLong(index_pos)) {
                find = true;
                break;
            }
        }

        // 不存在
        if (!find) {
            return null;
        } else {
            return ByteBuffer.wrap(b);
        }
    }

    /**
     * force mmap*/
    public void force() {
        mmapedIndex.force();
    }

    public Head getHead() {
        return head;
    }

    /**
     * 一个head类，负责head位置的数据更新*/
    public class Head {
        private MappedByteBuffer mmapedIndex = null;
        // 当前可用的index位置，最小是1，0号index由于特殊性不进行使用
        private AtomicInteger currentIndex;

        public Head(MappedByteBuffer mmapedIndex) {
            this.mmapedIndex  = mmapedIndex;
            // slot count
            this.mmapedIndex.putInt(0, IndexField.SLOT_SUM);
            // index count
            this.mmapedIndex.putInt(4, IndexField.INDEX_SUM);

            currentIndex = new AtomicInteger(this.mmapedIndex.getInt(8));
            if (currentIndex.get() == 0) {
                // 设1
                this.mmapedIndex.putInt(8, currentIndex.incrementAndGet());
            }
        }
        
        public int getSlotSum() {
            return mmapedIndex.getInt(0);
        }

        public int getIndexSum() {
            return mmapedIndex.getInt(4);
        }

        public int getAndAddCurrent() {
            int curr = currentIndex.getAndIncrement();

            this.mmapedIndex.putInt(8, curr+1);
            // 返回当前可用的index位置
            return curr;
        }
    }

    public static IndexHandle getInstance() {
        return instance;
    }
}
