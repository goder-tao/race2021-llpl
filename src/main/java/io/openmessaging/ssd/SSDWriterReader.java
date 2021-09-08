package io.openmessaging.ssd;

import io.openmessaging.constant.DataFileBasicInfo;
import io.openmessaging.constant.MntPath;
import io.openmessaging.constant.StatusCode;
import io.openmessaging.util.PartitionMaker;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

public class SSDWriterReader implements DiskReader, DiskWriter {
    private Logger logger = LogManager.getLogger(SSDWriterReader.class.getName());
    @Override
    public ByteBuffer read(String path, long offset, int size) {
        ByteBuffer data = null;
        try {
            RandomAccessFile file = new RandomAccessFile(path, "rw");
            byte[] b = new byte[size];
            file.seek(offset);
            file.read(b);
            data = ByteBuffer.allocate(size);
            data.put(b);
            data.rewind();
        } catch (Exception e) {
            logger.error("Read from disk fail, " + e.toString());
            return data;
        }
        return data;
    }

    @Override
    public int append(String dirPath, String fileName, ByteBuffer buffer) {
        try {
            File f = new File(dirPath);
            if (!f.exists()) {
                boolean b = f.mkdirs();
                if (!b) {
                    logger.error("Create dir fail!!");
                }
            }
            RandomAccessFile file = new RandomAccessFile(dirPath + fileName, "rw");

            buffer.rewind();
            FileChannel channel = file.getChannel();
            ByteBuffer puter = channel.map(FileChannel.MapMode.READ_WRITE, file.length(), buffer.capacity());
            puter.put(buffer);
            channel.close();
            file.close();
        } catch (Exception e) {
            logger.error("Write to disk fail, " + e.toString());
            return StatusCode.ERROR;
        }
        return StatusCode.SUCCESS;
    }

    @Override
    public int write(String dirPath, String fileName, long offset, ByteBuffer buffer) {
        try {
            File f = new File(dirPath);
            if (!f.exists()) {
                boolean b = f.mkdirs();
                if (!b) {
                    logger.error("Create dir fail!!");
                }
            }
            RandomAccessFile file = new RandomAccessFile(dirPath + fileName, "rw");
            //file.seek(offset);

            buffer.rewind();
            FileChannel channel = file.getChannel();
            ByteBuffer puter = channel.map(FileChannel.MapMode.READ_WRITE, offset, buffer.capacity());
            puter.put(buffer);
            channel.close();
            file.close();
        } catch (Exception e) {
            logger.error("Write to disk fail, " + e.toString());
            return StatusCode.ERROR;
        }
        return StatusCode.SUCCESS;
    }

    /**直接从磁盘读，屏蔽.index文件细节*/
    public Map<Long, byte[]> directRead(String topic, int queueId, long offset, int fetchNum) {
        Map<Long, byte[]> map = new HashMap<>();
        int partition = (int) (offset / DataFileBasicInfo.ITEM_NUM);
        long indexFileOffset = offset % DataFileBasicInfo.ITEM_NUM;
        String partitionPath = PartitionMaker.makePartitionPath(partition, DataFileBasicInfo.FILE_NAME_LENGTH, DataFileBasicInfo.ITEM_NUM);
        // 读索引文件
        ByteBuffer indexData = read(MntPath.SSD_PATH + topic+"/"+queueId+"/"+partitionPath+".index", indexFileOffset*10, fetchNum*10);
        long SSDDataStartOffset = -1L;
        int SSDReadBlockSize = 0;
        int[] size = new int[fetchNum];
        // 寻找.data文件的起止点
        for (int i = 0; i < fetchNum; i++) {
            if (i == 0) {
                SSDDataStartOffset = indexData.getLong();
                size[i] = indexData.getShort();
            } else {
                indexData.getLong();
                size[i] = indexData.getShort();
            }
            SSDReadBlockSize += size[i];
        }
        // 从ssd读数据
        ByteBuffer SSDData = read(MntPath.SSD_PATH+topic+"/"+queueId+"/"+partitionPath+".data", SSDDataStartOffset, SSDReadBlockSize);
        for (int i = 0; i < fetchNum; i++) {
            byte[] bytes = new byte[size[i]];
            SSDData.get(bytes);
            map.put(offset+i, bytes);
        }
        return map;
    }
}
