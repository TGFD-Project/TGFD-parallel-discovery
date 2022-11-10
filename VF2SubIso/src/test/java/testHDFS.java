import SharedStorage.HDFSStorage;

import java.io.IOException;
public class testHDFS {



    public static void main(String []args) throws IOException {

//
//
//        Configuration conf = new Configuration();
//        System.out.println("Connecting to -- "+conf.get("fs.defaultFS"));
//
//        FileSystem fs = FileSystem.get(URI.create("hdfs://130.113.158.134:9000/test5/"), conf);
//        OutputStream out = fs.create(new Path("/test5/"));


        //HDFSStorage.createDirectory("/hadoop/test/");
        HDFSStorage.upload("/dir1/","testFromMyPC.txt","Hello World");
    }

}
