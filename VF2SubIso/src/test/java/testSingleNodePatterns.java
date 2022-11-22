import Discovery.TGFDDiscovery;
import Discovery.Util;
import Infra.PatternTreeNode;
import SharedStorage.HDFSStorage;
import org.apache.commons.math3.analysis.function.Exp;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class testSingleNodePatterns {


    public static void main(String []args) throws IOException {
        System.out.println("testSingleNodePatterns");
        Util.printToLogFile = true;
        TGFDDiscovery tgfdDiscovery = new TGFDDiscovery(args);
        tgfdDiscovery.loadGraphsAndComputeHistogram2();
//        tgfdDiscovery.initialize();
//
//        Util.divertOutputToSummaryFile();
//        System.out.println("---------------------------------------------------------------");
//        System.out.println("                          Summary                              ");
//        System.out.println("---------------------------------------------------------------");
//        Util.printTimeStatistics();

        tgfdDiscovery.vSpawnInit();

        if (Util.generatek0Tgfds) {
            Util.printTgfdsToFile(Util.experimentName, Util.discoveredTgfds.get(Util.currentVSpawnLevel));
        }
        Util.kRuntimes.add(System.currentTimeMillis() - Util.discoveryStartTime);
        Util.printSupportStatisticsForThisSnapshot();
        Util.printTimeStatisticsForThisSnapshot(Util.currentVSpawnLevel);
//
//        int id =0;
//        for (PatternTreeNode node:singlePatternTreeNodes) {
//
//            System.out.println(node.toString());
//            try {
//                HDFSStorage.upload("/dir1/", String.valueOf(id++), node, false);
//            }
//            catch (Exception e)
//            {
//                System.out.println(e.getMessage());
//            }
//        }

        for (Map.Entry<String, Integer> map:Util.sortedFrequentEdgesHistogram) {

            String candidateEdgeString = map.getKey();
            String sourceVertexType = candidateEdgeString.split(" ")[0];
            String targetVertexType = candidateEdgeString.split(" ")[2];

            System.out.println(sourceVertexType + " -> " + candidateEdgeString + " -> " + targetVertexType);


        }

        //HDFSStorage.upload("/dir1/","sortedFrequentEdgesHistogram_1", Util.sortedFrequentEdgesHistogram, false);
    }

}
