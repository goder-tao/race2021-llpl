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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * 打开文件的mmap写
 * @author tao
 * @date 2021-09-30*/
public class SSDWriterReader5MMAP {
    // 单例
    private static final SSDWriterReader5MMAP instance = new SSDWriterReader5MMAP();
    // 用一个list保存按创建顺序添加的文件raf对象，留给read复用
    private ArrayList<RandomAccessFile> fileList = new ArrayList<>();
    // 记录到当前datafile的总偏移量，read时的精确定位文件
    private ArrayList<Long> accumulativePhyOffset = new ArrayList<>();
    // 记录所有datafile的偏移量之和, 计算当前的写入点
    private long finalPhyOffset = 0L;
    // datafile保存的根目录
    private final String dataFileDir = MntPath.DATA_FILE_DIR;
    // 当前datafile的mmap
    private  MappedByteBuffer mmap;

    private final Logger logger;

    private SSDWriterReader5MMAP() {
        File dir = new File(dataFileDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        String[] fileNames = dir.list();
        // 文件名排序
        if (fileNames != null) {
            Arrays.sort(fileNames);
        }
        logger = LogManager.getLogger(SSDWriterReader5MMAP.class.getName());
        // 构造list
        try {
            // 模拟第0个文件，大小为0，使得List的index逻辑上和文件的排序相同，比如index-1对应第一个文件
            accumulativePhyOffset.add(0L);
            fileList.add(null);
            if (fileNames != null) {
                for (int i = 0; i < fileNames.length; i++) {
                    logger.info("open file "+fileNames[i]);
                    RandomAccessFile file = new RandomAccessFile(dataFileDir+fileNames[i], "rw");
                    finalPhyOffset += file.length();
                    accumulativePhyOffset.add(finalPhyOffset);
                    fileList.add(file);
                    // 获取最新文件的mmap
                    if (i == fileNames.length-1) {
                        mmap = file.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, StorageSize.GB);
                    }
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
        // 粗略定位逻辑上的第几个文件
        int fileIndex = (int) (phyOffset/ StorageSize.GB)+1;

        // 精确定位，看读取的offset和对应datafile的累积offset之间的关系
        if (accumulativePhyOffset.size() > fileIndex && accumulativePhyOffset.get(fileIndex) != null && accumulativePhyOffset.get(fileIndex) < phyOffset) {
            fileIndex++;
        }

        off = (int) (phyOffset - accumulativePhyOffset.get(fileIndex-1));

        try {
            String fileName = PartitionMaker.makePartitionPath(fileIndex, 5, 1)+".data";
            RandomAccessFile raf = new RandomAccessFile(dataFileDir+"/"+fileName, "r");
            raf.seek(off);
            raf.read(b, 0, b.length);
            raf.close();
        } catch (Exception e) {
            logger.error("fileList get fail, "+e.toString());
        }
        return ByteBuffer.wrap(b);
    }

    /**
     * 单线程顺序写入
     * @return: 本次写入的起点和用于force的mmap对象*/
    public AppendRes2 append(ByteBuffer data) {
        data.rewind();
        // 写入点
        long writeStartOffset = finalPhyOffset;
        finalPhyOffset += data.remaining();
        // listSize总是比accSize大一，因为acc只有当前datafile满的时候才会新增，而list在当前文件未满的时候就已经存在datafile的raf对象了
        int listSize = fileList.size();
        int accSize = accumulativePhyOffset.size();
        RandomAccessFile raf = null;
        try {
            // 当前上写入点+写入的长度-上一个文件的累积offset，计算当前文件的大小，超出默认的一个data partition的大小，新建一个分区
            if (listSize == 1 || (writeStartOffset-accumulativePhyOffset.get(accSize-1))+data.remaining() > StorageSize.GB) {

                if (listSize != 1) {
                    // 记录上一个datafile的累计phyOffset
                    accumulativePhyOffset.add(writeStartOffset);
                    accSize++;
                }

                // 截掉上一个datafile因为mmap创建出来多余的部分
                if (fileList.get(listSize-1) != null) {
                    fileList.get(listSize-1).getChannel().truncate(accumulativePhyOffset.get(accSize-1)-accumulativePhyOffset.get(accSize-2));
                }

                // 新建一个datafile
                String fileName = PartitionMaker.makePartitionPath(listSize, 5, 1)+".data";
                raf = new RandomAccessFile(dataFileDir+"/"+fileName, "rw");
                fileList.add(raf);

                // 建立新的mmap
                mmap = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, StorageSize.GB);
            }

            // 写入
            mmap.position((int) (writeStartOffset-accumulativePhyOffset.get(accSize-1)));
            mmap.put(data);
        } catch (IOException e) {
            logger.error("try to get file length fail, "+e.toString());
        }
        return new AppendRes2(mmap, writeStartOffset);
    }

    public void printInfo() {
        System.out.println("final off: "+(float)finalPhyOffset/StorageSize.GB+", list size: "+fileList.size()+", acc size: "+accumulativePhyOffset.size());
    }

    public static SSDWriterReader5MMAP getInstance() {
        return instance;
    }
}
