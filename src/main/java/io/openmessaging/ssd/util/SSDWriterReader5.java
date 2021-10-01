package io.openmessaging.ssd.util;

import io.openmessaging.constant.MntPath;
import io.openmessaging.constant.StorageSize;
import io.openmessaging.ssd.index.IndexHandle;
import io.openmessaging.util.PartitionMaker;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 每次重新打开一个RandomAccessFile和对应的channel根据点位写
 * @author tao
 * @date 2021-09-30*/
public class SSDWriterReader5 {
    // 单例
    private static SSDWriterReader5 instance = new SSDWriterReader5();
    // 每个文件的文件名按照次序递增，用一个list记录所有打开的文件对象，留给read复用
    private ArrayList<RandomAccessFile> fileList = new ArrayList<>();
    // 记录到当前datafile的总偏移量，read时的精确定位文件
    private ArrayList<Long> accumulativePhyOffset = new ArrayList<>();
    // 记录所有datafile的偏移量之和, 计算当前的写入点
    private AtomicLong finalPhyOffset = new AtomicLong(0);
    // datafile保存的根目录
    private final String dataFileDir = MntPath.DATA_FILE_DIR;

    private static final Logger logger = LogManager.getLogger(SSDWriterReader5.class.getName());

    private SSDWriterReader5() {
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
                for (String filename: fileNames) {
                    RandomAccessFile file = new RandomAccessFile(dataFileDir+filename, "rw");
                    finalPhyOffset.addAndGet(file.length());
                    accumulativePhyOffset.add(finalPhyOffset.get());
                    fileList.add(file);
                }
            }
        } catch (Exception e) {
            logger.fatal("Open partition data fail, "+e.toString());
        }
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
        int fileIndex = (int) (phyOffset/ StorageSize.GB);

        if (!accumulativePhyOffset.isEmpty() && accumulativePhyOffset.get(fileIndex) < phyOffset) {
            fileIndex++;
        }

        off = fileIndex == 0 ? (int) phyOffset : (int) (phyOffset - accumulativePhyOffset.get(fileIndex - 1));

        try {
            fileList.get(fileIndex).seek(off);
            fileList.get(fileIndex).read(b, 0, b.length);
        } catch (Exception e) {
            logger.error("fileList get fail, "+e.toString());
        }
        return ByteBuffer.wrap(b);
    }

    /**
     * 单线程顺序写入
     * @return: 本次写入的起点*/
    public AppendRes append(byte[] data) {
        // 写入点
        long writeStartOffset = finalPhyOffset.getAndAdd(data.length);
        int listSize = fileList.size();
        FileChannel channel;
        RandomAccessFile raf = null;

        try {
            // 超出默认的一个data partition的大小或首个partition，新建一个分区
            if (listSize == 0 || fileList.get(listSize-1).length()+data.length > StorageSize.GB) {
                // 记录上一个datafile的累计phyOffset
                if (listSize != 0 && fileList.get(listSize-1).length()+data.length > StorageSize.GB) {
                    accumulativePhyOffset.add(finalPhyOffset.get());
                }
                String fileName = PartitionMaker.makePartitionPath(listSize, 5, 1)+".data";
                raf = new RandomAccessFile(dataFileDir+"/"+fileName, "rw");
                fileList.add(raf);
                listSize++;
            }

            // 写入
            String fileName = PartitionMaker.makePartitionPath(listSize-1, 5, 1)+".data";
            raf = new RandomAccessFile(dataFileDir+"/"+fileName, "rw");
            channel = raf.getChannel();
            channel.write(ByteBuffer.wrap(data), writeStartOffset);
        } catch (IOException e) {
            logger.error("try to get file length fail, "+e.toString());
        }

        return new AppendRes(raf, writeStartOffset);
    }

    public static SSDWriterReader5 getInstance() {
        return instance;
    }
}