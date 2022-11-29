import Discovery.TGFDDiscovery;
import Discovery.Util;
import Infra.MapEntry;
import Infra.PatternTreeNode;
import SharedStorage.HDFSStorage;
import org.apache.commons.math3.analysis.function.Exp;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class testSingleNodePatterns {


    public static void main(String []args) throws IOException {
        System.out.println("testSingleNodePatterns");
        Util.printToLogFile = true;
        Util.isStoreInMemory = false;
        TGFDDiscovery tgfdDiscovery = new TGFDDiscovery(args);
        tgfdDiscovery.loadGraphsAndComputeHistogram2();

//        HDFSStorage.upload("/dir1/", "vertexTypesToAvgInDegreeMap", Util.vertexTypesToAvgInDegreeMap,true);
//        HDFSStorage.upload("/dir1/", "activeAttributesSet", Util.activeAttributesSet,true);
//        HDFSStorage.upload("/dir1/", "vertexTypesToActiveAttributesMap", Util.vertexTypesToActiveAttributesMap,true);

//        List<MapEntry> listSortedFrequentEdgesHistogram = new ArrayList<>();
//        for (Map.Entry<String, Integer> m:Util.sortedFrequentEdgesHistogram) {
//            listSortedFrequentEdgesHistogram.add(new MapEntry(m.getKey(), m.getValue()));
//        }
//
//        HDFSStorage.upload("/dir1/", "sortedFrequentEdgesHistogram", listSortedFrequentEdgesHistogram,true);
//
//        List<MapEntry> listSortedVertexHistogram = new ArrayList<>();
//        for (Map.Entry<String, Integer> m:Util.sortedVertexHistogram) {
//            listSortedVertexHistogram.add(new MapEntry(m.getKey(), m.getValue()));
//        }
//        HDFSStorage.upload("/dir1/", "sortedVertexHistogram", listSortedVertexHistogram,true);
//        HDFSStorage.upload("/dir1/", "vertexHistogram", Util.vertexHistogram,true);
//        HDFSStorage.upload("/dir1/", "totalHistogramTime", Util.totalHistogramTime,true);
//        HDFSStorage.upload("/dir1/", "typeChangeURIs", Util.typeChangeURIs,true);
        

        tgfdDiscovery.start();

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
