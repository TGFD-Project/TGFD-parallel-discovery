import Discovery.TGFDDiscovery;
import Discovery.Util;
import ICs.TGFD;
import Loader.DBPediaLoader;
import Loader.IMDBLoader;
import Partitioner.DBPediaPartitioner_2;
import Partitioner.IMDBPartitioner;
import Util.Config;

import java.io.FileNotFoundException;
import java.util.ArrayList;

public class testDBPediaPartitioner {

    public static void main(String []args)
    {


        TGFDDiscovery tgfdDiscovery = new TGFDDiscovery(args);
        tgfdDiscovery.loadGraphsAndComputeHistogram2();

        String[] info = {
                String.join("=", "loader", Util.loader),
                String.join("=", "|G|", Util.graphSize),
                String.join("=", "t", Integer.toString(Util.T)),
                String.join("=", "k", Integer.toString(Util.k)),
                String.join("=", "pTheta", Double.toString(Util.patternTheta)),
                String.join("=", "theta", Double.toString(Util.tgfdTheta)),
                String.join("=", "gamma", Double.toString(Util.gamma)),
                String.join("=", "frequentSetSize", Double.toString(Util.frequentSetSize)),
                String.join("=", "interesting", Boolean.toString(Util.onlyInterestingTGFDs)),
                String.join("=", "literalMax", Integer.toString(Util.maxNumOfLiterals)),
                String.join("=", "noMinimalityPruning", Boolean.toString(!Util.hasMinimalityPruning)),
                String.join("=", "noSupportPruning", Boolean.toString(!Util.hasSupportPruning)),
                String.join("=", "fastMatching", Boolean.toString(Util.fastMatching)),
                String.join("=", "interestLabels", Util.interestLabelsSet.toString()),
        };

        System.out.println(String.join(", ", info));

        DBPediaPartitioner_2 partitioner=new DBPediaPartitioner_2((DBPediaLoader) Util.graphs.get(0),2);
        partitioner.partition("C:\\Users\\student\\IdeaProjects\\TGFD-parallel-discovery\\out\\artifacts\\TGFD_jar\\dbpedia-50000\\2014\\2014-50000.ttl","./");
    }
}
