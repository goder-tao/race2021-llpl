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
 * 单线程方式和Aggregator对接, 顺序写盘，聚合多条batch的byte[] 一次force
 * @version 1.1
 * @author tao
 * @date 2021-09-26*/
public class SSDWriterReader3 {
    // 单例
    private static SSDWriterReader3 instance = new SSDWriterReader3();
    // 每个文件的文件名按照次序递增， 用一个list记录所有打开的文件对象
    private ArrayList<RandomAccessFile> fileList = new ArrayList<>();
    // 记录到当前datafile的总偏移量，read时的精确定位文件
    private ArrayList<Long> accumulativePhyOffset = new ArrayList<>();
    // 记录所有datafile的偏移量之和, 计算当前的写入点
    private AtomicLong finalPhyOffset = new AtomicLong(0);
    // datafile保存的根目录
    private final String dataFileDir = MntPath.DATA_FILE_DIR;

    private static final Logger logger = LogManager.getLogger(SSDWriterReader3.class.getName());

    private SSDWriterReader3() {
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
        int fileIndex = (int) (phyOffset/StorageSize.GB);

        if (!accumulativePhyOffset.isEmpty() && accumulativePhyOffset.get(fileIndex) < phyOffset) {
            fileIndex++;
        }

        off = fileIndex == 0 ? (int) phyOffset : (int) (phyOffset - accumulativePhyOffset.get(fileIndex - 1));

        try {
            fileList.get(fileIndex).seek(off);
            fileList.get(fileIndex).read(b, 0, b.length);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return ByteBuffer.wrap(b);
    }
    /**
     * 多batch一force*/
    public long append(ArrayList<byte[]> bytesArray) {
        // 写入点
        long writeStartOffset = finalPhyOffset.get();
        // 这批数据的总大小
        int dataSize = 0;
        // 当前已经创建的datafile数量
        int listSize = fileList.size();
        RandomAccessFile raf;
        FileChannel channel = null;

        try {
            for (byte[] data : bytesArray) {
                dataSize += data.length;
                // 超出默认的一个data partition的大小或首个partition，新建一个分区
                if (listSize == 0 || fileList.get(listSize-1).length()+data.length > StorageSize.GB) {
                    // 记录上一个datafile的累计phyOffset
                    if (listSize != 0 && fileList.get(listSize-1).length()+data.length > StorageSize.GB) {
                        // 将上一个channel中的数据force
                        channel.force(true);
                        accumulativePhyOffset.add(finalPhyOffset.get());
                    }
                    String fileName = PartitionMaker.makePartitionPath(listSize, 5, 1)+".data";
                    raf = new RandomAccessFile(dataFileDir+"/"+fileName, "rw");
                    fileList.add(raf);
                    channel = raf.getChannel();
                } else {
                    raf = fileList.get(listSize-1);
                    channel = fileList.get(listSize-1).getChannel();
                }
                channel.write(ByteBuffer.wrap(data));
            }
            channel.force(true);
        } catch (Exception e) {
            logger.error("try to get file length fail, "+e.toString());
        }

        finalPhyOffset.addAndGet(dataSize);
        return writeStartOffset;
    }

    /**
     * 单线程顺序写入
     * @return: 本次写入的起点*/
    public long append(byte[] data) {
        // 写入点
        long writeStartOffset = finalPhyOffset.getAndAdd(data.length);
        int listSize = fileList.size();
        FileChannel channel;
        RandomAccessFile raf;

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
                channel = raf.getChannel();
            } else {
                raf = fileList.get(listSize-1);
                channel = fileList.get(listSize-1).getChannel();
            }
            // 持久化
            channel.write(ByteBuffer.wrap(data));
            channel.force(true);
        } catch (IOException e) {
            logger.error("try to get file length fail, "+e.toString());
        }

        return writeStartOffset;
    }

    public static SSDWriterReader3 getInstance() {
        return instance;
    }
}
