package io.openmessaging.util;

import io.openmessaging.constant.StorageSize;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * DirectByteBuffer池，减少jvm的gc压力，并且复用ByteBuffer，
 * 省去堆内外Buffer的复制，减少读写耗时
 * @author tao
 * @date 2021-10-08*/
public class DirectBufferPool {
    // 单例
    private static final DirectBufferPool instance = new DirectBufferPool();
    // 每个buffer的最大size 24K
    private int defaultUnitSize = (int) (StorageSize.KB * 24);
    // 池大小
    private int poolSize = 500;
    // 用队列保存池
    private ConcurrentLinkedQueue<ByteBuffer> pool = new ConcurrentLinkedQueue<>();

    private DirectBufferPool() {
        for (int i = 0; i < poolSize; i++) {
            ByteBuffer buffer = ByteBuffer.allocateDirect(defaultUnitSize);
            pool.offer(buffer);
        }
    }

    // 回收一个buffer
    public void deAllocate(ByteBuffer buffer) {
        pool.offer(buffer);
    }

    // 分配一个buffer
    public ByteBuffer allocate() {
        return pool.poll();
    }

    public static DirectBufferPool getInstance() {
        return instance;
    }
}
