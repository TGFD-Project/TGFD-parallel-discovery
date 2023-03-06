

import Discovery.TGFDDiscovery;
import Discovery.Util;
import Infra.DataVertex;
import Partitioner.IMDBPartitioner;
import Loader.IMDBLoader;
import java.util.HashMap;

public class testIMDBPartitioner {

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

        IMDBPartitioner partitioner = new IMDBPartitioner((IMDBLoader) Util.graphs.get(0), 2);
        HashMap<DataVertex, Integer> mapping = partitioner.partition();

        partitioner.partition("/Users/roy/Desktop/TGFD/datasets/imdb/imdb-test//test/imdb-1111.nt", "/Users/roy/Desktop/TGFD/datasets/imdb/imdb-test/", mapping, 170929);
    }
}
