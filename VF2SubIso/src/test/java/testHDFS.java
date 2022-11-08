import SharedStorage.HDFSStorage;
import Util.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

public class testHDFS {



    public static void main(String []args) throws IOException {

//
//
//        Configuration conf = new Configuration();
//        System.out.println("Connecting to -- "+conf.get("fs.defaultFS"));
//
//        FileSystem fs = FileSystem.get(URI.create("hdfs://130.113.158.134:9000/test5/"), conf);
//        OutputStream out = fs.create(new Path("/test5/"));


        HDFSStorage.createDirectory("/test3/");
        HDFSStorage.upload("/test3/","testFromMyPC.txt","Hello World");
    }

}
