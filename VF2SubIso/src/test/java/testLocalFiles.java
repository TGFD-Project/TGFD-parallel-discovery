import Infra.PatternTreeNode;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.ArrayList;

public class testLocalFiles {

    public static void main(String []args) throws IOException, ClassNotFoundException {
        ArrayList<String>files = new ArrayList<>();
        files.add("0_732d.ser");
        files.add("1_17de.ser");
        files.add("2_9bfd.ser");
        files.add("3_6c48.ser");
        files.add("4_0fc7.ser");
        files.add("5_b738.ser");
        files.add("6_4ed1.ser");
        for (String file:files) {
            InputStream inputStream = new BufferedInputStream(new FileInputStream("C:\\Users\\student\\IdeaProjects\\TGFD-parallel-discovery\\out\\artifacts\\TGFD_jar\\"+file));
            ObjectInputStream in = new ObjectInputStream(inputStream);
            Object obj = in.readObject();
            PatternTreeNode node = (PatternTreeNode) obj;
            System.out.println(node.getPattern().getPattern());
        }
    }

}
