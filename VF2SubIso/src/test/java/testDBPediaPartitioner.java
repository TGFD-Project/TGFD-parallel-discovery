import Discovery.TGFDDiscovery;
import Discovery.Util;
import ICs.TGFD;
import Infra.DataVertex;
import Loader.DBPediaLoader;
import Loader.IMDBLoader;
import Partitioner.DBPediaPartitioner_2;
import Partitioner.IMDBPartitioner;
import Util.Config;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;

public class testDBPediaPartitioner {

    public static void main(String[] args) {


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

        DBPediaPartitioner_2 partitioner = new DBPediaPartitioner_2((DBPediaLoader) Util.graphs.get(0), 2);
        HashMap<DataVertex, Integer> mapping = partitioner.partition();
        // Local
        partitioner.partition("/Users/roy/Desktop/TGFD/datasets/dbpedia/dbpedia-1000/2014/2014-1000.ttl", "/Users/roy/Desktop/TGFD/datasets/dbpedia/", mapping, 2014);
        partitioner.partition("/Users/roy/Desktop/TGFD/datasets/dbpedia/dbpedia-1000/2015/2015-1000.ttl", "/Users/roy/Desktop/TGFD/datasets/dbpedia/", mapping, 2015);
        partitioner.partition("/Users/roy/Desktop/TGFD/datasets/dbpedia/dbpedia-1000/2016/2016-1000.ttl", "/Users/roy/Desktop/TGFD/datasets/dbpedia/", mapping, 2016);
        partitioner.partition("/Users/roy/Desktop/TGFD/datasets/dbpedia/dbpedia-1000/2017/2017-1000.ttl", "/Users/roy/Desktop/TGFD/datasets/dbpedia/", mapping, 2017);

        // Server
//        partitioner.partition("/home/wang851/partition/dbpedia-200000/2014/2014-200000.ttl","./", mapping,2014);
//        partitioner.partition("/home/wang851/partition/dbpedia-200000/2015/2015-200000.ttl","./", mapping,2015);
//        partitioner.partition("/home/wang851/partition/dbpedia-200000/2016/2016-200000.ttl","./", mapping,2016);
//        partitioner.partition("/home/wang851/partition/dbpedia-200000/2017/2017-200000.ttl","./", mapping,2017);
    }
}
