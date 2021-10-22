package io.openmessaging.util;

import io.openmessaging.constant.StorageSize;

import java.io.InputStreamReader;
import java.io.LineNumberReader;

public class SystemMemory {
    static Process process = null;
    static String[] cmd = new String[]{"/bin/sh", "-c", "free -b | sed '2p' -n | awk '{print $7}'"};
    static String bytesMemory = null;
    static Runtime runtime = Runtime.getRuntime();

    public static long getSystemAvailableMemory() {
        // TODO Auto-generated method stub
        try {
            process = runtime.exec(cmd);
            InputStreamReader ir = new InputStreamReader(process.getInputStream());
            LineNumberReader input = new LineNumberReader(ir);
            bytesMemory = input.readLine();
            input.close();
            ir.close();
            process.destroy();
        } catch (Exception e) {
            e.printStackTrace();
        }
        assert bytesMemory != null;
        return Long.parseLong(bytesMemory);
    }

    public static void main(String[] args) {
        long t = System.nanoTime();
        for (int i = 0; i < 20000; i++) {
            System.out.println(getSystemAvailableMemory() / StorageSize.GB);
            try {
                Thread.sleep(500);
            } catch (Exception e) {

            }
        }
        System.out.println("average time: "+(System.nanoTime()-t)/20000);
    }
}
