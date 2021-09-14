package test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

public class FileChannelTest {
    public static void main(String[] args) throws IOException {
        String input_file_path = "/home/tao/Data/file1";
        String output_file_path = "/home/tao/Data/file2";

        FileInputStream input_file_stream = new FileInputStream(new File(input_file_path));
        FileOutputStream output_file_stream = new FileOutputStream(new File(output_file_path));
        FileChannel input_channel = input_file_stream.getChannel();
        FileChannel output_channel = output_file_stream.getChannel();

        try {
            input_channel.transferTo(0, input_channel.size(), output_channel);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
