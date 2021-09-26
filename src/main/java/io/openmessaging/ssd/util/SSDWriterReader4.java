package io.openmessaging.ssd.util;

import io.openmessaging.constant.MntPath;
import io.openmessaging.constant.StorageSize;
import io.openmessaging.util.PartitionMaker;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 多线程方式和Aggregator对接， 按点位并发写入
 * @author tao
 * @date 2021-09-25*/
public class SSDWriterReader4 {
    // 单例
    private static SSDWriterReader4 instance = new SSDWriterReader4();
    // 记录所有datafile的偏移量之和, 计算当前的写入点
    private AtomicLong finalPhyOffset = new AtomicLong(0);
    // datafile保存的根目录
    private final String dataFileDir = MntPath.DATA_FILE_DIR;

    /*改多点位并发写后使用的数据结构*/
    // 一个map记录打开的文件对象， key按照文件创建的顺序递增
    private ConcurrentHashMap<Integer, RandomAccessFile> filesMap = new ConcurrentHashMap<>();
    // 记录每个data file的累积Offset，用于read时候的精确定位
    private ConcurrentHashMap<Integer, Long> accPhyOffset = new ConcurrentHashMap<>();

    private final Logger logger = LogManager.getLogger(SSDWriterReader4.class.getName());

    private SSDWriterReader4() {
        File dir = new File(dataFileDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        String[] fileNames = dir.list();
        // 文件名排序
        if (fileNames != null) {
            Arrays.sort(fileNames);
        }
        // 构造list
        try {
            if (fileNames != null) {
                // 若是已经存在data file, 进行一些必要的预加载
                for (int i = 0; i < fileNames.length; i++) {
                    RandomAccessFile file = new RandomAccessFile(dataFileDir+fileNames[i], "rws");
                    finalPhyOffset.addAndGet(file.length());
                    // key表示第几个datafile, 思维逻辑上的第几
                    accPhyOffset.put(i+1, finalPhyOffset.get());
                    filesMap.put(i+1, file);
                }
            }
        } catch (Exception e) {
            logger.fatal("Open partition data fail, "+e.toString());
        }
        // 0号文件不存在，所以累积offset为0，为了后面write中判断是否新写入一个datafile语句的统一
        accPhyOffset.put(0, 0L);
    }

    /**
     * 根据hashKey直接获取到消息*/
    public ByteBuffer directRead(int hashKey) {
        ByteBuffer offAndSize =  IndexHandle.getInstance().getPhyOffsetAndSize(hashKey);
        if (offAndSize == null) return null;
        offAndSize.rewind();
        long off = offAndSize.getLong();
        short size = offAndSize.getShort();
        return read(off, size);
    }

    /**
     * 根据phyOffset定位到指定的文件，根据一个分区文件的默认大小(1G)
     * 模糊定位，再进行一次文件的确认*/
    public ByteBuffer read(long phyOffset, short size) {
        byte[] b = new byte[size];
        // 读数据的起点
        int off;
        // 粗略定位
        int fileIndex = (int) (phyOffset/ StorageSize.GB)+1;

        if (accPhyOffset.getOrDefault(fileIndex, null) != null && accPhyOffset.get(fileIndex) < phyOffset) {
            fileIndex++;
        }

        // 存放消息的哪个datafile的偏移
        off = (int) (phyOffset - accPhyOffset.get(fileIndex - 1));

        try {
            filesMap.get(fileIndex).seek(off);
            filesMap.get(fileIndex).read(b, 0, b.length);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return ByteBuffer.wrap(b);
    }

    /**
     * 按照当前点位写入，相当于能够按不同点位并发写入
     * @return: 当前线程的开始写入点位*/
    public long write(byte[] data) {
        long writeOff = finalPhyOffset.getAndAdd(data.length);
        int mapSize = filesMap.size();
        RandomAccessFile raf = null;
        RandomAccessFile tag = null;

        try {
            // 第一个datafile || 和上一个datafile的累积offset相比较
            if (mapSize == 0 || writeOff+data.length-accPhyOffset.get(mapSize-1) > StorageSize.DEFAULT_DATA_FILE_SIZE) {

                // 记录当前datafile的累积offset
                if (mapSize >0 && writeOff+data.length-accPhyOffset.get(mapSize-1) > StorageSize.DEFAULT_DATA_FILE_SIZE) {
                    Long off = accPhyOffset.putIfAbsent(mapSize, writeOff);
                    // 并发下可能多个进入，只有最小的writeOff才是正确的上一个datafile的最终大小
                    if (off != null && writeOff < off) {
                        accPhyOffset.put(mapSize, writeOff);
                    }
                }

                String fileName = PartitionMaker.makePartitionPath(mapSize+1, 5, 1)+".data";
                raf = new RandomAccessFile(dataFileDir+"/"+fileName, "rws");

                // 并发put
                tag = filesMap.putIfAbsent(mapSize+1, raf);
                if (tag != null) {
                    raf.close();
                    raf = tag;
                }
            }

            // 是否新建raf
            if (raf == null) {
                raf = filesMap.get(mapSize);
            }
            FileChannel channel = raf.getChannel();
            channel.write(ByteBuffer.wrap(data), writeOff);
            channel.force(true);
//            raf.seek(writeOff);
//            raf.write(data);
        } catch (Exception e) {
            logger.error("try to get file length fail, "+e.toString());
        }
        return writeOff;
    }

    public static SSDWriterReader4 getInstance() {
        return instance;
    }
}
