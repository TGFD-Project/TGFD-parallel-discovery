import ICs.TGFD;
import Infra.DataVertex;
import Infra.Delta;
import Infra.RelationshipEdge;
import Infra.VF2DataGraph;
import SharedStorage.HDFSStorage;

import java.io.IOException;
import java.time.Period;

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
        HDFSStorage.upload("/dir1/","testFromMyPC_2.txt","112212");

        System.out.println("File uploaded successfully");

        System.out.println("Now, trying to read the file!");

        StringBuilder sb = HDFSStorage.downloadWholeTextFile("/dir1/","testFromMyPC_2.txt");

        System.out.println("File received...");
        System.out.println(sb);

        System.out.println("Now trying to upload an object");

        HDFSStorage.upload("/dir1/","tempGraph", generateDataGraph());

        System.out.println("upload done.");


    }

    public static VF2DataGraph generateDataGraph()  {
        VF2DataGraph graph=new VF2DataGraph();

        DataVertex v1=new DataVertex("Frank_Lampard","player");
        v1.addAttribute("name","lampard");
        v1.addAttribute("age","34");
        v1.addAttribute("number","11");
        graph.addVertex(v1);

        DataVertex v3=new DataVertex("Didier_Drogba","player");
        v3.addAttribute("name","Drogba");
        v3.addAttribute("age","36");
        graph.addVertex(v3);

        DataVertex v2=new DataVertex("Team_Chelsea","team");
        v2.addAttribute("name","Chelsea");
        v2.addAttribute("league","Premiere League");
        graph.addVertex(v2);

        graph.addEdge(v1,v2,new RelationshipEdge("playing"));
        graph.addEdge(v3,v2,new RelationshipEdge("play"));

        return graph;
    }

}
