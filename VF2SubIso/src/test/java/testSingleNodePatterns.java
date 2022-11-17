import Discovery.TGFDDiscovery;
import Discovery.Util;
import Infra.PatternTreeNode;
import SharedStorage.HDFSStorage;
import org.apache.commons.math3.analysis.function.Exp;

import java.io.IOException;
import java.util.List;

public class testSingleNodePatterns {


    public static void main(String []args) throws IOException {
        Util.printToLogFile = false;
        TGFDDiscovery tgfdDiscovery = new TGFDDiscovery(args);
        tgfdDiscovery.loadGraphsAndComputeHistogram2();

        List<PatternTreeNode> singlePatternTreeNodes = tgfdDiscovery.vSpawnSinglePatternTreeNode();

        int id =0;
        for (PatternTreeNode node:singlePatternTreeNodes) {

            System.out.println(node.toString());
            try {
                HDFSStorage.upload("/dir1/", String.valueOf(id++), node);
            }
            catch (Exception e)
            {
                System.out.println(e.getMessage());
            }
        }
    }

}
