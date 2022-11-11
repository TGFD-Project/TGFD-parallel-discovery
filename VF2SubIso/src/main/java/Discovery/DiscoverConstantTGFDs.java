package Discovery;

import ICs.TGFD;
import Infra.*;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.time.Period;
import java.util.*;

public class DiscoverConstantTGFDs {

    private PatternTreeNode patternNode;
    private ConstantLiteral yLiteral;
    private Map<Set<ConstantLiteral>, ArrayList<Map.Entry<ConstantLiteral, List<Integer>>>>  entities;
    private Map<Util.Pair, ArrayList<TreeSet<Util.Pair>>> deltaToPairsMap;

    public DiscoverConstantTGFDs (PatternTreeNode patternNode, ConstantLiteral yLiteral, Map<Set<ConstantLiteral>, ArrayList<Map.Entry<ConstantLiteral, List<Integer>>>> entities, Map<Util.Pair, ArrayList<TreeSet<Util.Pair>>> deltaToPairsMap)
    {
        this.patternNode = patternNode;
        this.yLiteral = yLiteral;
        this.entities = entities;
        this.deltaToPairsMap = deltaToPairsMap;

    }

    public ArrayList<TGFD> discover()
    {
        long discoverConstantTGFDsTime = System.currentTimeMillis(); long supersetPathCheckingTimeForThisDependency = 0;
        ArrayList<TGFD> tgfds = new ArrayList<>();
        String yVertexType = yLiteral.getVertexType();
        String yAttrName = yLiteral.getAttrName();
        for (Map.Entry<Set<ConstantLiteral>, ArrayList<Map.Entry<ConstantLiteral, List<Integer>>>> entityEntry : entities.entrySet()) {
            VF2PatternGraph newPattern = patternNode.getPattern().copy();
            DataDependency newDependency = new DataDependency();
            AttributeDependency constantPath = new AttributeDependency();
            for (Vertex v : newPattern.getPattern().vertexSet()) {
                String vType = new ArrayList<>(v.getTypes()).get(0);
                if (vType.equalsIgnoreCase(yVertexType)) { // TODO: What if our pattern has duplicate vertex types?
                    v.putAttributeIfAbsent(new Attribute(yAttrName));
                    if (newDependency.getY().size() == 0) {
                        VariableLiteral newY = new VariableLiteral(yVertexType, yAttrName, yVertexType, yAttrName);
                        newDependency.addLiteralToY(newY);
                    }
                }
                for (ConstantLiteral xLiteral : entityEntry.getKey()) {
                    if (xLiteral.getVertexType().equalsIgnoreCase(vType)) {
                        v.putAttributeIfAbsent(new Attribute(xLiteral.getAttrName(), xLiteral.getAttrValue()));
                        ConstantLiteral newXLiteral = new ConstantLiteral(vType, xLiteral.getAttrName(), xLiteral.getAttrValue());
                        newDependency.addLiteralToX(newXLiteral);
                        constantPath.addToLhs(newXLiteral);
                    }
                }
            }
            constantPath.setRhs(new ConstantLiteral(yVertexType, yAttrName, null));

            System.out.println("Performing Constant TGFD discovery");
            System.out.println("Pattern: " + newPattern);
            System.out.println("Entity: " + newDependency);

            System.out.println("Candidate RHS values for entity...");
            ArrayList<Map.Entry<ConstantLiteral, List<Integer>>> rhsAttrValuesTimestampsSortedByFreq = entityEntry.getValue();
            for (Map.Entry<ConstantLiteral, List<Integer>> entry : rhsAttrValuesTimestampsSortedByFreq) {
                System.out.println(entry.getKey() + ":" + entry.getValue());
            }

            System.out.println("Computing candidate delta for RHS value...\n" + rhsAttrValuesTimestampsSortedByFreq.get(0).getKey());
            ArrayList<Util.Pair> candidateDeltas = new ArrayList<>();
            if (rhsAttrValuesTimestampsSortedByFreq.size() == 1) {
                Util.numOfConsistentRHS += 1;
                List<Integer> timestampCounts = rhsAttrValuesTimestampsSortedByFreq.get(0).getValue();
                Util.Pair candidateDelta = getMinMaxPair(timestampCounts);
                if (candidateDelta == null) continue;
                candidateDeltas.add(candidateDelta);
            } else if (rhsAttrValuesTimestampsSortedByFreq.size() > 1) {
                Util.rhsInconsistencies.add(rhsAttrValuesTimestampsSortedByFreq.size());
                findCandidateDeltasForMostFreqRHS(rhsAttrValuesTimestampsSortedByFreq, candidateDeltas);
            }
            if (candidateDeltas.size() == 0) {
                System.out.println("Could not find any deltas for entity: " + entityEntry.getKey());
                continue;
            }

            // Compute TGFD support
            Delta candidateTGFDdelta;
            double candidateTGFDsupport = 0;
            Util.Pair mostSupportedDelta = null;
            TreeSet<Util.Pair> mostSupportedSatisfyingPairs = null;
            for (Util.Pair candidateDelta : candidateDeltas) {
                int minDistance = candidateDelta.min();
                int maxDistance = candidateDelta.max();
                if (minDistance <= maxDistance) {
                    System.out.println("Calculating support for candidate delta ("+minDistance+","+maxDistance+")");
                    double numerator;
                    List<Integer> timestampCounts = rhsAttrValuesTimestampsSortedByFreq.get(0).getValue();
                    TreeSet<Util.Pair> satisfyingPairs = new TreeSet<>();
                    for (int index = 0; index < timestampCounts.size(); index++) {
                        if (timestampCounts.get(index) == 0)
                            continue;
                        else if (timestampCounts.get(index) > 1 && 0 >= minDistance && 0 <= maxDistance)
                            satisfyingPairs.add(new Util.Pair(index, index));
                        for (int j = index + 1; j < timestampCounts.size(); j++) {
                            if (timestampCounts.get(j) > 0) {
                                if (j - index >= minDistance && j - index <= maxDistance) {
                                    satisfyingPairs.add(new Util.Pair(index, j));
                                }
                            }
                        }
                    }

                    System.out.println("Satisfying pairs: " + satisfyingPairs);

                    numerator = satisfyingPairs.size();
                    double candidateSupport = TGFDDiscovery.calculateSupport(numerator, entities.size(), Util.T);

                    if (candidateSupport > candidateTGFDsupport) {
                        candidateTGFDsupport = candidateSupport;
                        mostSupportedDelta = candidateDelta;
                        mostSupportedSatisfyingPairs = satisfyingPairs;
                    }
                }
            }
            if (mostSupportedDelta == null) {
                System.out.println("Could not come up with mostSupportedDelta for entity: " + entityEntry.getKey());
                continue;
            }
            System.out.println("Entity satisfying attributes:" + mostSupportedSatisfyingPairs);
            System.out.println("Entity delta = " + mostSupportedDelta);
            System.out.println("Entity support = " + candidateTGFDsupport);

            // All entities are considered in general TGFD, regardless of their support
            if (!deltaToPairsMap.containsKey(mostSupportedDelta)) {
                deltaToPairsMap.put(mostSupportedDelta, new ArrayList<>());
            }
            deltaToPairsMap.get(mostSupportedDelta).add(mostSupportedSatisfyingPairs);

            Util.constantTgfdSupportsListForThisSnapshot.add(candidateTGFDsupport); // Statistics

            int minDistance = mostSupportedDelta.min();
            int maxDistance = mostSupportedDelta.max();
            candidateTGFDdelta = new Delta(Period.ofYears(minDistance), Period.ofYears(maxDistance), Duration.ofDays(365));
            System.out.println("Constant TGFD delta: "+candidateTGFDdelta);
            constantPath.setDelta(candidateTGFDdelta);

            long supersetPathCheckingTime = System.currentTimeMillis();
            boolean isNotMinimal = false;
            if (Util.hasMinimalityPruning && constantPath.isSuperSetOfPathAndSubsetOfDelta(patternNode.getAllMinimalConstantDependenciesOnThisPath())) { // Ensures we don't expand constant TGFDs from previous iterations
                System.out.println("Candidate constant TGFD " + constantPath + " is a superset of an existing minimal constant TGFD");
                isNotMinimal = true;
            }
            supersetPathCheckingTime = System.currentTimeMillis()-supersetPathCheckingTime;
            supersetPathCheckingTimeForThisDependency += supersetPathCheckingTime;
            Util.printWithTime("supersetPathCheckingTime", supersetPathCheckingTime);
            Util.addToTotalSupersetPathCheckingTime(supersetPathCheckingTime);

            if (isNotMinimal)
                continue;

            // Only output constant TGFDs that satisfy support
            if (candidateTGFDsupport < Util.tgfdTheta) {
                System.out.println("Could not satisfy TGFD support threshold for entity: " + entityEntry.getKey());
            } else {
                System.out.println("Creating new constant TGFD...");
                TGFD entityTGFD = new TGFD(newPattern, candidateTGFDdelta, newDependency, candidateTGFDsupport, patternNode.getPatternSupport(), "");
                System.out.println("TGFD: " + entityTGFD);
                tgfds.add(entityTGFD);
                if (Util.hasMinimalityPruning)
                    patternNode.addMinimalConstantDependency(constantPath);
            }
        }

        discoverConstantTGFDsTime = System.currentTimeMillis() - discoverConstantTGFDsTime - supersetPathCheckingTimeForThisDependency;
        Util.printWithTime("discoverConstantTGFDsTime", discoverConstantTGFDsTime);
        Util.addToTotalDiscoverConstantTGFDsTime(discoverConstantTGFDsTime);

        return tgfds;
    }

    private void findCandidateDeltasForMostFreqRHS(ArrayList<Map.Entry<ConstantLiteral, List<Integer>>> rhsAttrValuesTimestampsSortedByFreq, ArrayList<Util.Pair> candidateDeltas) {
        List<Integer> timestampCountOfMostFreqRHS = rhsAttrValuesTimestampsSortedByFreq.get(0).getValue();
        Integer minExclusionDistance = null;
        Integer maxExclusionDistance = null;
        for (Map.Entry<ConstantLiteral, List<Integer>> timestampCountEntryOfOtherRHS : rhsAttrValuesTimestampsSortedByFreq.subList(1, rhsAttrValuesTimestampsSortedByFreq.size())) {
            List<Integer> timestampCountOfOtherRHS = timestampCountEntryOfOtherRHS.getValue();
            for (int i = 0; i < timestampCountOfOtherRHS.size(); i++) {
                int otherTimestampCount = timestampCountOfOtherRHS.get(i);
                if (otherTimestampCount == 0)
                    continue;
                for (int j = 0; j < timestampCountOfMostFreqRHS.size(); j++) {
                    Integer refTimestampCount = timestampCountOfMostFreqRHS.get(j);
                    if (refTimestampCount == 0)
                        continue;
                    int distance = Math.abs(i - j);
                    minExclusionDistance = minExclusionDistance != null ? Math.min(minExclusionDistance, distance) : distance;
                    maxExclusionDistance = maxExclusionDistance != null ? Math.max(maxExclusionDistance, distance) : distance;
                }
            }
            if (minExclusionDistance != null && minExclusionDistance == 0
                    && maxExclusionDistance != null && maxExclusionDistance == (Util.numOfSnapshots-1))
                break;
        }
        if (minExclusionDistance != null && minExclusionDistance > 0) {
            candidateDeltas.add(new Util.Pair(0, minExclusionDistance-1));
        }
        if (maxExclusionDistance != null && maxExclusionDistance < Util.numOfSnapshots-1) {
            candidateDeltas.add(new Util.Pair(maxExclusionDistance+1, Util.numOfSnapshots-1));
        }
    }

    @Nullable
    private Util.Pair getMinMaxPair(List<Integer> timestampCounts) {
        Integer minDistance = null;
        Integer maxDistance = null;
        if (timestampCounts.stream().anyMatch(count -> count > 1)) {
            minDistance = 0;
        } else {
            Integer indexOfPreviousOccurence = null;
            for (int index = 0; index < timestampCounts.size(); index++) {
                if (timestampCounts.get(index) > 0) {
                    if (indexOfPreviousOccurence == null) {
                        indexOfPreviousOccurence = index;
                    } else {
                        minDistance = minDistance != null ? Math.min(minDistance, index - indexOfPreviousOccurence) : (index - indexOfPreviousOccurence);
                        if (minDistance == 0) break;
                    }
                }
            }
        }
        if (minDistance == null)
            return null;
        Integer indexOfFirstOccurence = null;
        for (int index = 0; index < timestampCounts.size(); index++) {
            if (timestampCounts.get(index) > 0) {
                indexOfFirstOccurence = index;
                break;
            }
        }
        Integer indexOfFinalOccurence = null;
        for (int index = timestampCounts.size()-1; index >= 0; index--) {
            if (timestampCounts.get(index) > 0) {
                indexOfFinalOccurence = index;
                break;
            }
        }
        if (indexOfFirstOccurence != null && indexOfFinalOccurence != null) {
            if (indexOfFirstOccurence.equals(indexOfFinalOccurence) && timestampCounts.get(indexOfFirstOccurence) > 1) {
                maxDistance = 0;
            } else {
                maxDistance = indexOfFinalOccurence - indexOfFirstOccurence;
            }
        }
        if (maxDistance == null)
            return null;
        if (minDistance > maxDistance) {
            System.out.println("Not enough timestamped matches found for entity.");
            return null;
        }
        return new Util.Pair(minDistance, maxDistance);
    }

}
