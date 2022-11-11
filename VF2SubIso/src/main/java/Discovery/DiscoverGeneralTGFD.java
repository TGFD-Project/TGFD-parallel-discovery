package Discovery;

import ICs.TGFD;
import Infra.*;

import java.time.Duration;
import java.time.Period;
import java.util.*;
import java.util.stream.Collectors;

public class DiscoverGeneralTGFD {

    PatternTreeNode patternTreeNode;
    double patternSupport;
    AttributeDependency literalPath;
    int entitiesSize;
    Map<Util.Pair, ArrayList<TreeSet<Util.Pair>>> deltaToPairsMap;
    LiteralTreeNode literalTreeNode;

    public DiscoverGeneralTGFD(PatternTreeNode patternTreeNode, double patternSupport, AttributeDependency literalPath, int entitiesSize, Map<Discovery.Util.Pair, ArrayList<TreeSet<Discovery.Util.Pair>>> deltaToPairsMap, LiteralTreeNode literalTreeNode)
    {
        this.patternTreeNode = patternTreeNode;
        this.patternSupport = patternSupport;
        this.entitiesSize = entitiesSize;
        this.deltaToPairsMap = deltaToPairsMap;
        this.literalTreeNode = literalTreeNode;
    }

    public ArrayList<TGFD> discover()
    {
        ArrayList<TGFD> tgfds = new ArrayList<>();

        System.out.println("Number of delta: " + deltaToPairsMap.keySet().size());
        for (Discovery.Util.Pair deltaPair : deltaToPairsMap.keySet()) {
            System.out.println("Constant delta: " + deltaPair);
        }

        System.out.println("Delta to Pairs map...");
        int numOfEntitiesWithDeltas = 0;
        int numOfPairs = 0;
        for (Map.Entry<Util.Pair, ArrayList<TreeSet<Util.Pair>>> deltaToPairsEntry : deltaToPairsMap.entrySet()) {
            numOfEntitiesWithDeltas += deltaToPairsEntry.getValue().size();
            for (TreeSet<Discovery.Util.Pair> pairSet : deltaToPairsEntry.getValue()) {
                System.out.println(deltaToPairsEntry.getKey()+":"+pairSet);
                numOfPairs += pairSet.size();
            }
        }
        System.out.println("Number of entities with deltas: " + numOfEntitiesWithDeltas);
        System.out.println("Number of pairs: " + numOfPairs);


        // Find intersection delta
        HashMap<Util.Pair, ArrayList<Util.Pair>> intersections = new HashMap<>();
        int currMin = 0;
        int currMax = Util.numOfSnapshots - 1;
        // TODO: Verify if TreeSet<Pair> is being sorted correctly
        // TODO: Does this method only produce intervals (x,y), where x == y ?
        ArrayList<Util.Pair> currSatisfyingAttrValues = new ArrayList<>();
        for (Util.Pair deltaPair: deltaToPairsMap.keySet().stream().sorted().collect(Collectors.toList())) {
            if (Math.max(currMin, deltaPair.min()) <= Math.min(currMax, deltaPair.max())) {
                currMin = Math.max(currMin, deltaPair.min());
                currMax = Math.min(currMax, deltaPair.max());
//				currSatisfyingAttrValues.add(satisfyingPairsSet.get(index)); // By axiom 4
                continue;
            }
            for (Map.Entry<Util.Pair, ArrayList<TreeSet<Util.Pair>>> deltaToPairsEntry : deltaToPairsMap.entrySet()) {
                for (TreeSet<Util.Pair> satisfyingPairSet : deltaToPairsEntry.getValue()) {
                    for (Util.Pair satisfyingPair : satisfyingPairSet) {
                        if (satisfyingPair.max() - satisfyingPair.min() >= currMin && satisfyingPair.max() - satisfyingPair.min() <= currMax) {
                            currSatisfyingAttrValues.add(new Util.Pair(satisfyingPair.min(), satisfyingPair.max()));
                        }
                    }
                }
            }
            intersections.putIfAbsent(new Util.Pair(currMin, currMax), currSatisfyingAttrValues);
            currSatisfyingAttrValues = new ArrayList<>();
            currMin = 0;
            currMax = Util.numOfSnapshots - 1;
            if (Math.max(currMin, deltaPair.min()) <= Math.min(currMax, deltaPair.max())) {
                currMin = Math.max(currMin, deltaPair.min());
                currMax = Math.min(currMax, deltaPair.max());
//				currSatisfyingAttrValues.add(satisfyingPairsSet.get(index));
            }
        }
        for (Map.Entry<Util.Pair, ArrayList<TreeSet<Util.Pair>>> deltaToPairsEntry : deltaToPairsMap.entrySet()) {
            for (TreeSet<Util.Pair> satisfyingPairSet : deltaToPairsEntry.getValue()) {
                for (Util.Pair satisfyingPair : satisfyingPairSet) {
                    if (satisfyingPair.max() - satisfyingPair.min() >= currMin && satisfyingPair.max() - satisfyingPair.min() <= currMax) {
                        currSatisfyingAttrValues.add(new Util.Pair(satisfyingPair.min(), satisfyingPair.max()));
                    }
                }
            }
        }
        intersections.putIfAbsent(new Util.Pair(currMin, currMax), currSatisfyingAttrValues);

        ArrayList<Map.Entry<Util.Pair, ArrayList<Util.Pair>>> sortedIntersections = new ArrayList<>(intersections.entrySet());
        sortedIntersections.sort(new Comparator<Map.Entry<Util.Pair, ArrayList<Util.Pair>>>() {
            @Override
            public int compare(Map.Entry<Util.Pair, ArrayList<Util.Pair>> o1, Map.Entry<Util.Pair, ArrayList<Util.Pair>> o2) {
                return o2.getValue().size() - o1.getValue().size();
            }
        });

        System.out.println("Candidate deltas for general TGFD:");
        for (Map.Entry<Util.Pair, ArrayList<Util.Pair>> intersection : sortedIntersections) {
            System.out.println(intersection.getKey());
        }

        System.out.println("Evaluating candidate deltas for general TGFD...");
        for (Map.Entry<Util.Pair, ArrayList<Util.Pair>> intersection : sortedIntersections) {
            Util.Pair candidateDelta = intersection.getKey();
            int generalMin = candidateDelta.min();
            int generalMax = candidateDelta.max();
            System.out.println("Calculating support for candidate general TGFD candidate delta: " + intersection.getKey());

            // Compute general support
            int numberOfSatisfyingPairs = intersection.getValue().size();

            System.out.println("Number of satisfying pairs: " + numberOfSatisfyingPairs);
            System.out.println("Satisfying pairs: " + intersection.getValue());
            double support = TGFDDiscovery.calculateSupport(numberOfSatisfyingPairs, entitiesSize, Util.T);
            System.out.println("Candidate general TGFD support: " + support);
            Util.generalTgfdSupportsListForThisSnapshot.add(support);

            Delta delta = new Delta(Period.ofYears(generalMin), Period.ofYears(generalMax), Duration.ofDays(365));

            DataDependency generalDependency = new DataDependency();
            String yVertexType = literalPath.getRhs().getVertexType();
            String yAttrName = literalPath.getRhs().getAttrName();
            VariableLiteral y = new VariableLiteral(yVertexType, yAttrName, yVertexType, yAttrName);
            generalDependency.addLiteralToY(y);
            for (ConstantLiteral x : literalPath.getLhs()) {
                String xVertexType = x.getVertexType();
                String xAttrName = x.getAttrName();
                VariableLiteral varX = new VariableLiteral(xVertexType, xAttrName, xVertexType, xAttrName);
                generalDependency.addLiteralToX(varX);
            }

            if (support < Util.tgfdTheta) {
                System.out.println("Support for candidate general TGFD is below support threshold");
            } else {
                System.out.println("Creating new general TGFD...");
                TGFD tgfd = new TGFD(patternTreeNode.getPattern(), delta, generalDependency, support, patternSupport, "");
                System.out.println("TGFD: " + tgfd);
                tgfds.add(tgfd);
            }
        }
        return tgfds;
    }

}
