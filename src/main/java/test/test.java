package test;

import io.openmessaging.constant.MntPath;
import io.openmessaging.constant.StorageSize;
import io.openmessaging.dramcache.DRAMCache;
import io.openmessaging.ssd.SSDWriterReader;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


public class test {
    public static void main(String[] args) throws IOException {
        final String path = MntPath.SSD_PATH+"test";
        RandomAccessFile rfile = null;
        BufferedOutputStream outputStream = null;
        try {
            outputStream = new BufferedOutputStream(new FileOutputStream(path));
            rfile = new RandomAccessFile(path, "r");
            for (int i = 0; i < 100000; i++) {
                byte[] b = new byte[10];
                outputStream.write(b);
                outputStream.flush();
                if (rfile.length() != (i+1)*10) {
                    System.out.println("in time file length not equal");
                }
                
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                outputStream.close();
                rfile.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }


    /**
     * 测试flush不关闭流使用rfile实时获取文件长度*/
    static void testInTimeFileLength() throws IOException {
        final String path = MntPath.SSD_PATH+"test";
        for (int i = 0; i < 10; i++) {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    RandomAccessFile rfile;
                    BufferedOutputStream outputStream = null;
                    Random random = new Random();
                    int r = random.nextInt(1000);
                    try {
                        outputStream = new BufferedOutputStream(new FileOutputStream(path+r));
                        for (int i = 0; i < 1000000; i++) {
                            byte[] b = new byte[10];
                            outputStream.write(b);
                            outputStream.flush();
                            rfile = new RandomAccessFile(path+r, "r");
                            if (rfile.length() != (i+1)*10) {
                                System.out.println("in time file length not equal");
                            }
                            rfile.close();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            outputStream.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
            thread.start();
        }

    }

    /**
     * 使用CAS保证并发的正确性测试*/
    static void atomicTest() {
        AtomicBoolean atomicBoolean = new AtomicBoolean(false);
        final int[]  i = new int[1];
        for (int j = 0; j < 1000000; j++) {
            atomicBoolean.set(false);
            i[0] = 0;
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    if (atomicBoolean.compareAndSet(false, true)) {
                        atomicBoolean.set(true);
                        i[0]++;
                    }
                }
            }), thread1 = new Thread(new Runnable() {
                @Override
                public void run() {
                    if (atomicBoolean.compareAndSet(false, true)) {
                        atomicBoolean.set(true);
                        i[0]++;
                    }
                }
            });
            try {
                thread.start();
                thread1.start();
                thread1.join();
                thread.join();
            } catch (Exception e) {
                System.out.println(e.toString());
            }
            if (i[0] == 2) {
                System.out.println("!!!");
            }
        }
    }

    /**
     * 并发写磁盘*/
    static void diskPressureTest() {
        SSDWriterReader ssdWriterReader = new SSDWriterReader();
        for (int i = 0; i < 10; i++) {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    Random random = new Random();
                    long startTime = System.nanoTime();
                    int fi = random.nextInt(10000);
                    for (int i = 0; i < 10000; i++) {
                        ssdWriterReader.append(MntPath.SSD_PATH, "test"+fi, ByteBuffer.allocate((int) StorageSize.KB).array());
                    }
                    System.out.println("Average write time: "+(System.nanoTime()-startTime)/10000+"ns");
                }
            });
            thread.start();
        }
    }


    public static void test() {
        ConcurrentHashMap<Integer, Integer> map = new ConcurrentHashMap<>();
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                if (map.get(1) == null) {
                    if (map.putIfAbsent(1, 1) != null) {
                        System.out.println("!!!");
                    }
                }
            }
        }), thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                if (map.get(1) == null) {
                    if (map.putIfAbsent(1, 2) != null) {
                        System.out.println("!!!");
                    }
                }
            }
        });
        thread.start();
        thread1.start();
        try {
            thread.join();
            thread1.join();
        } catch (Exception e) {

        }
//        System.out.println(map.get(1));
    }
    static void parallelDir() {
        for(int i = 0; i < 100; i++) {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    File f = new File("/home/tao/Data/sim_SSD/test2");
                    if (!f.exists()) {
                        boolean b = f.mkdirs();
                        if (!b) {
                            System.out.println("Create dir fail!!");
                        }
                    }
                }
            });
            thread.start();
        }
    }
}
