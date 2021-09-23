package test;

import io.openmessaging.constant.MntPath;
import io.openmessaging.constant.StorageSize;
import io.openmessaging.ssd.util.IndexHandle;
import io.openmessaging.ssd.util.SSDWriterReader;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;


public class test {
    public static void main(String[] args) throws IOException {
        File dir = new File("/home/tao/Data/sim_SSD");
        String[] filename = dir.list();
        Arrays.sort(filename);
        for (String fn:filename) {
            System.out.println(fn);
        }
    }

    /**
     * 读超过RandomAccessFile范围测试*/
    static void outOfRandomAccessFileRangeRead() {
        try {
            RandomAccessFile file = new RandomAccessFile(MntPath.SSD_PATH+"test", "r");
            byte[] b = new byte[10];
            file.read(b);
            ByteBuffer buffer = ByteBuffer.allocate(b.length);
            buffer.put(b);
            buffer.rewind();
            System.out.println(buffer.getLong());
            System.out.println(buffer.getShort());
        } catch (Exception e) {
            e.printStackTrace();
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
     * 测试flush不关闭流使用rfile实时获取文件长度*/
    static void testForceWithoutClose() throws IOException {
        final String path = MntPath.SSD_PATH+"test";
        for (int i = 0; i < 1; i++) {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    int writeTime = 1000;
                    RandomAccessFile rfile = null;
                    BufferedOutputStream outputStream = null;
                    Random random = new Random();
                    int r = random.nextInt(1000);
                    try {
                        rfile = new RandomAccessFile(path+r, "rw");
                        FileChannel channel = rfile.getChannel();
                        long t = System.nanoTime();
                        for (int i = 0; i < writeTime; i++) {
                            byte[] b = new byte[1024*2];
                            rfile.seek(random.nextInt(1000));
                            rfile.read(b);
                        }
                        System.out.println("average write time: "+(System.nanoTime()-t)/writeTime+"ns");
                    } catch (Exception e) {
                        e.printStackTrace();
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
