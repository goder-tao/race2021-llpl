package test;

import io.openmessaging.constant.MntPath;
import io.openmessaging.constant.StorageSize;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class test {

    // 半秒顺序写 300+MB文件  600+MB/s
    //
    static void writeTest() {
        try {
            RandomAccessFile raf = new RandomAccessFile(MntPath.SSD_PATH+"test", "rw");
            FileChannel channel = raf.getChannel();
            int writeTimes = 5000;
            int writeSize = 32768;
//            int writeSize = 8192;
            long t = System.nanoTime();
            long ft = 0;
            for (int i = 0; i < writeTimes; i++) {
                channel.write(ByteBuffer.wrap(new byte[writeSize]));
                long t1 = System.nanoTime();
                channel.force(true);
                ft += System.nanoTime()-t1;
            }

            long totalTime = System.nanoTime() - t;

            System.out.println("total time: "+totalTime);
            System.out.println("total time of writing: "+(totalTime-ft));
            System.out.println("total time of force: "+(ft));
            System.out.println("average time of writing: "+(totalTime-ft)/writeTimes);
            System.out.println("average time of force: "+ft/writeTimes);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 并发写磁盘
     * 1、利用点位并发写同一个文件+force -> 2MB/s
     * 2、一个文件8kb写+每次force -> 15MB/s
     * 3、并发点位并发写同一个文件不force，并发非顺序写 -> 600MB/s
     * */
    static void testParallelWriteSame() {
        AtomicLong off = new AtomicLong(0);
        AtomicLong I = new AtomicLong(0);
        int threadCount = 20;
        Thread[] threads = new Thread[threadCount];
        long t = System.nanoTime();
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    Random random = new Random();
                    long startTime = System.nanoTime();
                    int fi = random.nextInt(8192);
                    int writeTimes = 10000;
                    int writeSize = 8100;

                    try {
                        RandomAccessFile raf = new RandomAccessFile(MntPath.SSD_PATH+"test", "rw");
                        FileChannel channel = raf.getChannel();
                        for (int i = 0; i < writeTimes; i++) {
                            long offset = off.getAndAdd(writeSize);
                            channel.write(ByteBuffer.wrap(new byte[writeSize]), offset);
                            channel.force(true);
                            I.incrementAndGet();
                            if ((I.get()+1)%5==0){
                                channel.force(true);
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    System.out.println("Average write time: "+(System.nanoTime()-startTime)/writeTimes+"ns");
                }
            });
            threads[i].start();
        }

        for (int i = 0; i < threads.length; i++) {
            try {
                threads[i].join();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        System.out.println("total time: "+(System.nanoTime()-t));
    }

    // 并发顺序写多个文件 -> 10MB/s
    static void testParallelWriteMultiFile() {
        int threadCount = 4;
        Thread[] threads = new Thread[threadCount];
        long t = System.nanoTime();
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    Random random = new Random();
                    long startTime = System.nanoTime();
                    int fi = random.nextInt(8192);
                    int writeTimes = 5000;
                    int writeSize = 8100;

                    try {
                        RandomAccessFile raf = new RandomAccessFile(MntPath.SSD_PATH+"test"+fi, "rw");
                        FileChannel channel = raf.getChannel();
                        for (int i = 0; i < writeTimes; i++) {
                            channel.write(ByteBuffer.wrap(new byte[writeSize]));
                            channel.force(true);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    System.out.println("Average write time: "+(System.nanoTime()-startTime)/writeTimes+"ns");
                }
            });
            threads[i].start();
        }

        for (int i = 0; i < threads.length; i++) {
            try {
                threads[i].join();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        System.out.println("total time:"+(System.nanoTime()-t));

    }

    // 并发写同一个mmap -> 10MB/s
    // 并发写同一个mmap, 每次写入只map对应写入范围的区域 -> 10MB/s
    static void testParallelWriteSameMMap() {
        AtomicLong off = new AtomicLong(0);
        int threadCount = 4;
        Thread[] threads = new Thread[threadCount];
        long t = System.nanoTime();
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    Random random = new Random();
                    long startTime = System.nanoTime();
                    int fi = random.nextInt(8192);
                    int writeTimes = 5000;
                    int writeSize = 8100;

                    try {
                        RandomAccessFile raf = new RandomAccessFile(MntPath.SSD_PATH+"test", "rw");
                        FileChannel channel = raf.getChannel();
                        MappedByteBuffer mmap = channel.map(FileChannel.MapMode.READ_WRITE, 0, 500*StorageSize.MB);
                        for (int i = 0; i < writeTimes; i++) {
                            long offset = off.getAndAdd(writeSize);
                            mmap.position((int) offset);
                            mmap.put(new byte[writeSize]);
                            mmap.force();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    System.out.println("Average write time: "+(System.nanoTime()-startTime)/writeTimes+"ns");
                }
            });
            threads[i].start();
        }

        for (int i = 0; i < threads.length; i++) {
            try {
                threads[i].join();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        System.out.println("total time:"+(System.nanoTime()-t));
    }

    // 使用mmap并发写多个文件, 每次只map对应要写入的范围 -> 20MB/s
    static void testParallelWriteMultiMmapFile() {
        int threadCount = 10;
        Thread[] threads = new Thread[threadCount];
        long t = System.nanoTime();
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    Random random = new Random();
                    long startTime = System.nanoTime();
                    int fi = random.nextInt(1000000);
                    int writeTimes = 5000;
                    int writeSize = 8100;

                    try {
                        RandomAccessFile raf = new RandomAccessFile(MntPath.SSD_PATH+"test"+fi, "rw");
                        FileChannel channel = raf.getChannel();
                        for (int i = 0; i < writeTimes; i++) {
                            MappedByteBuffer mmap = channel.map(FileChannel.MapMode.READ_WRITE, i*writeSize, (i+1)*writeSize);
                            mmap.put(new byte[writeSize]);
                            mmap.force();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    System.out.println("Average write time: "+(System.nanoTime()-startTime)/writeTimes+"ns");
                }
            });
            threads[i].start();
        }

        for (int i = 0; i < threads.length; i++) {
            try {
                threads[i].join();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("total time: "+(System.nanoTime()-t));
    }

    // 随机写测试 -> 600MB/s
    static void randomWrite() {
        int threadCount = 1;
        Thread[] threads = new Thread[threadCount];
        long t = System.nanoTime();
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    Random random = new Random();
                    long startTime = System.nanoTime();
                    int fi = random.nextInt(1000000);
                    int writeTimes = 400000;
                    int writeSize = 8100;

                    try {
                        RandomAccessFile raf = new RandomAccessFile(MntPath.SSD_PATH+"test"+fi, "rw");
                        FileChannel channel = raf.getChannel();
                        for (int i = 0; i < writeTimes; i++) {
                            int roff = random.nextInt((int) (2000*StorageSize.MB));
                            channel.write(ByteBuffer.wrap(new byte[writeSize]), roff);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    System.out.println("Average write time: "+(System.nanoTime()-startTime)/writeTimes+"ns");
                }
            });
            threads[i].start();
        }

        for (int i = 0; i < threads.length; i++) {
            try {
                threads[i].join();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("total time: "+(System.nanoTime()-t));
    }

    // 测试每次读取文件不关闭和关闭在效率上的差距
    // close each time -> 9k-1W
    // no close -> 4700
    static void testFileReadWithNoClose() {
        RandomAccessFile raf;
        int writeTimes = 100000;
        int writeSize = 8192;
        long t = System.nanoTime();
        try {
            raf = new RandomAccessFile(MntPath.SSD_PATH+"data/00001.data", "r");
            for (int i = 0; i < writeTimes; i++) {
                raf.seek(i*writeSize);
                raf.read(new byte[writeSize], 0, writeSize);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("average time: "+(System.nanoTime()-t)/writeTimes);
    }

    // 测试force时候会不会让出cpu
    static void testForceCpu() {
        Thread thread1, thread2;
        thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                Random random = new Random();
                int fi = random.nextInt(8192);
                int writeTimes = 100;
                int writeSize = 8100;

                try {
                    RandomAccessFile raf = new RandomAccessFile(MntPath.SSD_PATH+"test"+fi, "rw");
                    FileChannel channel = raf.getChannel();
                    for (int i = 0; i < writeTimes; i++) {
                        System.out.println("start write");
                        channel.write(ByteBuffer.wrap(new byte[writeSize]));
                        System.out.println("start force");
                        channel.force(true);
                        System.out.println("force finish");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 1000; i++) {
                    System.out.println("just out put");
                }
            }
        });
        thread1.start();
        thread2.start();
    }

    // 测试不同写入大小force的时间开销
    // ->结论：和size非线性关系，10*size的force时间只增大了差不多两倍
    static void testDifferentSizeForceTime() {
        try {
            RandomAccessFile raf = new RandomAccessFile(MntPath.SSD_PATH+"test", "rw");
            FileChannel channel = raf.getChannel();
            int writeTimes = 2000;
            int writeSize = 8192;
//             int writeSize = 16384;
            long t = System.nanoTime();
            for (int i = 0; i < writeTimes; i++) {
                channel.write(ByteBuffer.wrap(new byte[writeSize]));
                if (i % 3 == 0) {
                    channel.force(true);
                }
            }
            System.out.println("average force time: "+(System.nanoTime()-t)/writeTimes);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        testDifferentSizeForceTime();
    }
}
