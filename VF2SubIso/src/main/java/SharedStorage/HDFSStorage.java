package SharedStorage;


import Util.Config;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class HDFSStorage {

    public static void createDirectory(String directoryName){
        try {
            Configuration configuration = new Configuration();
            configuration.set(Config.HDFSName, Config.HDFSAddress);
            FileSystem fileSystem = FileSystem.get(configuration);
            //String directoryName = "javadeveloperzone/javareadwriteexample";
            Path path = new Path(directoryName);
            fileSystem.mkdirs(path);
        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
        }
    }

    public static boolean upload(String directoryName, String fileName, Object obj, boolean removeTemporaryFile)
    {
        boolean done = false;
        String tempFileName="./"+fileName+"_"+Util.Config.generateRandomString(4)+".ser";
        try {
            FileOutputStream file = new FileOutputStream(tempFileName);
            ObjectOutputStream out = new ObjectOutputStream(file);
            out.writeObject(obj);
            out.close();
            file.close();
            System.out.println("Object has been serialized.");

            System.out.println("Uploading to HDFS");

            Configuration configuration = new Configuration();
            configuration.set(Config.HDFSName, Config.HDFSAddress);
            FileSystem fileSystem = FileSystem.get(configuration);

            //Input stream for the file in local file system to be written to HDFS
            InputStream inputStream = new BufferedInputStream(new FileInputStream(tempFileName));

            //Destination file in HDFS
            Path hdfsWritePath = new Path(directoryName + fileName);
            OutputStream outputStream = fileSystem.create(hdfsWritePath, true);

            //Copy file from local to HDFS
            IOUtils.copy(inputStream, outputStream);

            fileSystem.close();

            System.out.println("Uploading Done. [directory name: " + directoryName + "] [file name: " + fileName + "]");
            done = true;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        if(removeTemporaryFile)
        {
            System.out.println("Cleaning up the temporary storage...");
            File fileToBeUploaded = new File(tempFileName);
            boolean deleted = fileToBeUploaded.delete();
            if (deleted)
                System.out.println("Deleted the temporary file.");
            else
                System.out.println("Couldn't delete the temporary file: '" + tempFileName + "' ");
        }
        return done;
    }

    public static boolean upload(String directoryName, String fileName, String textToBeUploaded) {
        try {
            //directoryName = "/user/javadeveloperzone/javareadwriteexample/";
            //String fileName = "read_write_hdfs_example.txt";

            Configuration configuration = new Configuration();
            configuration.set(Config.HDFSName, Config.HDFSAddress);
            FileSystem fileSystem = FileSystem.get(configuration);
            Path hdfsWritePath = new Path(directoryName + fileName);
            FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfsWritePath, true);
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
            bufferedWriter.write(textToBeUploaded);
            bufferedWriter.newLine();
            bufferedWriter.close();
            fileSystem.close();
            return true;
        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
            return false;
        }
    }

    public static StringBuilder downloadWholeTextFile(String directoryName, String fileName) {

        StringBuilder sb=new StringBuilder();
        try
        {
            Configuration configuration = new Configuration();
            configuration.set(Config.HDFSName, Config.HDFSAddress);
            FileSystem fileSystem = FileSystem.get(configuration);
            //Create a path
            Path hdfsReadPath = new Path(directoryName+ fileName);
            //Init input stream
            FSDataInputStream inputStream = fileSystem.open(hdfsReadPath);

            //Classical input stream usage
            //String out= IOUtils.toString(inputStream, "UTF-8");

            // Buffered input stream
            BufferedReader bufferedReader = new BufferedReader(
                    new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            String line;
            while ((line=bufferedReader.readLine())!=null){
                sb.append(line);
            }
            inputStream.close();
            fileSystem.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return sb;
    }

    public static Object downloadObject(String directoryName, String fileName) {
        Object obj = null;
        try {
            Configuration configuration = new Configuration();
            configuration.set(Config.HDFSName, Config.HDFSAddress);
            FileSystem fileSystem = FileSystem.get(configuration);

            FSDataInputStream inputStream = fileSystem.open(new Path(directoryName, fileName));
            ObjectInputStream in = new ObjectInputStream(inputStream);
            obj = in.readObject();

            in.close();
            inputStream.close();
        } catch (Exception e) {
            return e.getMessage();
        }
        return obj;
    }
}
