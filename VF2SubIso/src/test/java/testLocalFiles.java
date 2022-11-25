import Infra.ConstantLiteral;
import Infra.PatternTreeNode;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class testLocalFiles {

    public static void main(String []args) throws IOException, ClassNotFoundException {
        ArrayList<String>files = new ArrayList<>();
        ArrayList<String>files2 = new ArrayList<>();
        //files.add("matchesPerTimestamps_0_b5ec.ser");
        files.add("matchesPerTimestamps_1_141b.ser");
        files.add("matchesPerTimestamps_2_0cf5.ser");
        files.add("matchesPerTimestamps_3_2f63.ser");
        files.add("matchesPerTimestamps_4_098a.ser");

        //files2.add("patternTreeNode_0_170e.ser");
        files2.add("patternTreeNode_1_f6bb.ser");
        files2.add("patternTreeNode_2_bb5c.ser");
        files2.add("patternTreeNode_3_8feb.ser");
        files2.add("patternTreeNode_4_5f0d.ser");
        //files.add("1_17de.ser");
        for (int i=0;i<files.size();i++) {


            InputStream inputStream = new BufferedInputStream(new FileInputStream("C:\\Users\\student\\IdeaProjects\\TGFD-parallel-discovery\\"+files2.get(i)));
            ObjectInputStream in = new ObjectInputStream(inputStream);
            Object obj = in.readObject();
            PatternTreeNode patternTreeNode = (PatternTreeNode) obj;


            String fileName="C:\\Users\\student\\IdeaProjects\\TGFD-parallel-discovery\\"+files.get(i);
             inputStream = new BufferedInputStream(new FileInputStream(fileName));
             in = new ObjectInputStream(inputStream);
             obj = in.readObject();
            List<Set<Set<ConstantLiteral>>> matchesPerTimestamps = (List<Set<Set<ConstantLiteral>>>) obj;

            System.out.println("");
        }
    }

}
