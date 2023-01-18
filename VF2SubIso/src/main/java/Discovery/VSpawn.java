package Discovery;

import Infra.*;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class VSpawn {

    public VSpawnedPatterns perform(boolean setCenterVertexAutomatically) {

        VSpawnedPatterns ret = new VSpawnedPatterns();
        long vSpawnTime = System.currentTimeMillis();

        if (Util.candidateEdgeIndex > Util.sortedFrequentEdgesHistogram.size() - 1) {
            Util.candidateEdgeIndex = 0;
            Util.previousLevelNodeIndex = Util.previousLevelNodeIndex + 1;
        }

        if (Util.previousLevelNodeIndex >= Util.patternTree.getLevel(Util.currentVSpawnLevel - 2).size()) {
            Util.kRuntimes.add(System.currentTimeMillis() - Util.discoveryStartTime);
//            Util.printTgfdsToFile(Util.experimentName, Util.discoveredTgfds.get(Util.currentVSpawnLevel));
            if (Util.kExperiment) Util.printExperimentRuntimestoFile();
//            Util.printSupportStatisticsForThisSnapshot();
//            Util.printTimeStatisticsForThisSnapshot(Util.currentVSpawnLevel);
            Util.addToTotalVSpawnTime(System.currentTimeMillis() - vSpawnTime);
            Util.setCurrentVSpawnLevel(Util.currentVSpawnLevel + 1);
            vSpawnTime = System.currentTimeMillis();
            if (Util.currentVSpawnLevel > Util.k) {
                Util.addToTotalVSpawnTime(System.currentTimeMillis() - vSpawnTime);
                return new VSpawnedPatterns();
            }
            Util.patternTree.addLevel();
            Util.previousLevelNodeIndex = 0;
            Util.candidateEdgeIndex = 0;
        }

        System.out.println("Performing VSpawn");
        System.out.println("VSpawn Level " + Util.currentVSpawnLevel);

        ArrayList<PatternTreeNode> previousLevel = Util.patternTree.getLevel(Util.currentVSpawnLevel - 2);
        if (previousLevel.size() == 0) {
            System.out.println("Previous level of vSpawn contains no pattern nodes.");
            Util.previousLevelNodeIndex = (Util.previousLevelNodeIndex + 1);
            Util.addToTotalVSpawnTime(System.currentTimeMillis() - vSpawnTime);
            return new VSpawnedPatterns();
        }
        PatternTreeNode previousLevelNode = previousLevel.get(Util.previousLevelNodeIndex);
        System.out.println("Processing previous level node " + Util.previousLevelNodeIndex + "/" + (previousLevel.size() - 1));
        System.out.println("Performing VSpawn on pattern: " + previousLevelNode.getPattern());

        System.out.println("Level " + (Util.currentVSpawnLevel - 2) + " pattern: " + previousLevelNode.getPattern());
        if (Util.hasSupportPruning && previousLevelNode.isPruned()) {
            System.out.println("Marked as pruned. Skip.");
            Util.previousLevelNodeIndex = (Util.previousLevelNodeIndex + 1);
            Util.addToTotalVSpawnTime(System.currentTimeMillis() - vSpawnTime);
            return new VSpawnedPatterns();
        }

        System.out.println("Processing candidate edge " + Util.candidateEdgeIndex + "/" + (Util.sortedFrequentEdgesHistogram.size() - 1));
        Map.Entry<String, Integer> candidateEdge = Util.sortedFrequentEdgesHistogram.get(Util.candidateEdgeIndex);
        String candidateEdgeString = candidateEdge.getKey();
        System.out.println("Candidate edge:" + candidateEdgeString);


        String sourceVertexType = candidateEdgeString.split(" ")[0];
        String targetVertexType = candidateEdgeString.split(" ")[2];

        if (Util.getVertexTypesToActiveAttributesMap().get(targetVertexType).size() == 0) {
            System.out.println("Target vertex in candidate edge does not contain active attributes");
            Util.candidateEdgeIndex = (Util.candidateEdgeIndex + 1);
            return new VSpawnedPatterns();
        }

        // TODO: We should add support for duplicate vertex types in the future
        if (sourceVertexType.equals(targetVertexType)) {
            System.out.println("Candidate edge contains duplicate vertex types. Skip.");
            Util.candidateEdgeIndex = (Util.candidateEdgeIndex + 1);
            Util.addToTotalVSpawnTime(System.currentTimeMillis() - vSpawnTime);
            return new VSpawnedPatterns();
        }
        String edgeType = candidateEdgeString.split(" ")[1];

        // Check if candidate edge already exists in pattern
        if (Util.isDuplicateEdge(previousLevelNode.getPattern(), edgeType, sourceVertexType, targetVertexType)) {
            System.out.println("Candidate edge: " + candidateEdge.getKey());
            System.out.println("already exists in pattern");
            Util.candidateEdgeIndex = (Util.candidateEdgeIndex + 1);
            Util.addToTotalVSpawnTime(System.currentTimeMillis() - vSpawnTime);
            return new VSpawnedPatterns();
        }

        if (Util.isMultipleEdge(previousLevelNode.getPattern(), sourceVertexType, targetVertexType)) {
            System.out.println("We do not support multiple edges between existing vertices.");
            Util.candidateEdgeIndex = (Util.candidateEdgeIndex + 1);
            Util.addToTotalVSpawnTime(System.currentTimeMillis() - vSpawnTime);
            return new VSpawnedPatterns();
        }

        // Checks if candidate edge extends pattern
        PatternVertex sourceVertex = isDuplicateVertex(previousLevelNode.getPattern(), sourceVertexType);
        PatternVertex targetVertex = isDuplicateVertex(previousLevelNode.getPattern(), targetVertexType);
        if (sourceVertex == null && targetVertex == null) {
            System.out.println("Candidate edge: " + candidateEdge.getKey());
            System.out.println("does not extend from pattern");
            Util.candidateEdgeIndex = (Util.candidateEdgeIndex + 1);
            Util.addToTotalVSpawnTime(System.currentTimeMillis() - vSpawnTime);
            return new VSpawnedPatterns();
        }

        PatternTreeNode patternTreeNode = null;
        // TODO: FIX label conflict. What if an edge has same vertex type on both sides?
        for (Vertex v : previousLevelNode.getGraph().vertexSet()) {
            System.out.println("Looking to add candidate edge to vertex: " + v.getTypes());
            PatternVertex pv = (PatternVertex) v;
            if (pv.isMarked()) {
                System.out.println("Skip vertex. Already added candidate edge to vertex: " + pv.getTypes());
                continue;
            }
            if (!pv.getTypes().contains(sourceVertexType) && !pv.getTypes().contains(targetVertexType)) {
                System.out.println("Skip vertex. Candidate edge does not connect to vertex: " + pv.getTypes());
                pv.setMarked(true);
                continue;
            }

            // Create unmarked copy of k-1 pattern
            ret.setOldPattern(previousLevelNode);
            VF2PatternGraph newPattern = previousLevelNode.getPattern().copy();
            if (targetVertex == null) {
                targetVertex = new PatternVertex(targetVertexType);
                newPattern.addVertex(targetVertex);
            } else {
                for (Vertex vertex : newPattern.getPattern().vertexSet()) {
                    if (vertex.getTypes().contains(targetVertexType)) {
                        targetVertex.setMarked(true);
                        targetVertex = (PatternVertex) vertex;
                        break;
                    }
                }
            }
            RelationshipEdge newEdge = new RelationshipEdge(edgeType);
            if (sourceVertex == null) {
                sourceVertex = new PatternVertex(sourceVertexType);
                newPattern.addVertex(sourceVertex);
            } else {
                for (Vertex vertex : newPattern.getPattern().vertexSet()) {
                    if (vertex.getTypes().contains(sourceVertexType)) {
                        sourceVertex.setMarked(true);
                        sourceVertex = (PatternVertex) vertex;
                        break;
                    }
                }
            }
            newPattern.addEdge(sourceVertex, targetVertex, newEdge);

            System.out.println("Created new pattern: " + newPattern);

            // TODO: Debug - Why does this work with strings but not subgraph isomorphism???
            if (!isIsomorphicPattern(newPattern, Util.patternTree)) {
                pv.setMarked(true);
                System.out.println("Skip. Candidate pattern is an isomorph of existing pattern");
                continue;
            }

            if (Util.hasSupportPruning && isSuperGraphOfPrunedPattern(newPattern, Util.patternTree)) {
                pv.setMarked(true);
                System.out.println("Skip. Candidate pattern is a supergraph of pruned pattern");
                continue;
            }
//            if (Util.currentVSpawnLevel == 1) {
//                if (setCenterVertexAutomatically)
//                    newPattern.assignOptimalCenterVertex(Util.vertexTypesToAvgInDegreeMap, Util.fastMatching);
//                patternTreeNode = new PatternTreeNode(newPattern, previousLevelNode, candidateEdgeString);
//                // TODO: currentVSpawnLevel + 1, then add treeNode
//                Util.patternTree.getTree().get(Util.currentVSpawnLevel-1).add(patternTreeNode);
//                Util.patternTree.findSubgraphParents(Util.currentVSpawnLevel-1, patternTreeNode);
//                Util.patternTree.findCenterVertexParent(Util.currentVSpawnLevel-1, patternTreeNode, true);
//            } else {
            if (setCenterVertexAutomatically)
                newPattern.assignOptimalCenterVertex(Util.vertexTypesToAvgInDegreeMap, Util.fastMatching);
            boolean considerAlternativeParents = true;
            if (Util.fastMatching && Util.currentVSpawnLevel > 2) {
                if (newPattern.getPatternType() == PatternType.Line) {
                    considerAlternativeParents = false;
                }
            }
            patternTreeNode = Util.patternTree.createNodeAtLevel(Util.currentVSpawnLevel - 1, newPattern, previousLevelNode, candidateEdgeString, considerAlternativeParents);
//            }
            System.out.println("Marking vertex " + pv.getTypes() + "as expanded.");
            break;
        }
        if (patternTreeNode == null) {
            for (Vertex v : previousLevelNode.getGraph().vertexSet()) {
                System.out.println("Unmarking all vertices in current pattern for the next candidate edge");
                v.setMarked(false);
            }
            Util.candidateEdgeIndex = (Util.candidateEdgeIndex + 1);
        }
        Util.addToTotalVSpawnTime(System.currentTimeMillis() - vSpawnTime);
        ret.setNewPattern(patternTreeNode);
        return ret;
    }

    public class VSpawnedPatterns {
        private PatternTreeNode oldPattern = null;
        private PatternTreeNode newPattern = null;

        public void setNewPattern(PatternTreeNode newPattern) {
            this.newPattern = newPattern;
        }

        public void setOldPattern(PatternTreeNode oldPattern) {
            this.oldPattern = oldPattern;
        }

        public PatternTreeNode getNewPattern() {
            return newPattern;
        }

        public PatternTreeNode getOldPattern() {
            return oldPattern;
        }
    }


    private PatternVertex isDuplicateVertex(VF2PatternGraph newPattern, String vertexType) {
        for (Vertex v : newPattern.getPattern().vertexSet()) {
            if (v.getTypes().contains(vertexType)) {
                return (PatternVertex) v;
            }
        }
        return null;
    }

    private boolean isIsomorphicPattern(VF2PatternGraph newPattern, PatternTree patternTree) {
        final long isIsomorphicPatternCheckStartTime = System.currentTimeMillis();
        System.out.println("Checking if the pattern is isomorphic...");
        ArrayList<String> newPatternEdges = new ArrayList<>();
        newPattern.getPattern().edgeSet().forEach((edge) -> {
            newPatternEdges.add(edge.toString());
        });
        boolean isIsomorphic = false;
        for (PatternTreeNode otherPattern : patternTree.getLevel(Util.currentVSpawnLevel - 2)) {
            ArrayList<String> otherPatternEdges = new ArrayList<>();
            otherPattern.getGraph().edgeSet().forEach((edge) -> {
                otherPatternEdges.add(edge.toString());
            });
            if (newPatternEdges.containsAll(otherPatternEdges) && otherPatternEdges.size() != 0) {
                System.out.println("Candidate pattern: " + newPattern);
                System.out.println("is an isomorph of current VSpawn level pattern: " + otherPattern.getPattern());
                isIsomorphic = true;
                break;
            } else if (otherPatternEdges.size() == 0) {
                Set<String> types = new ArrayList<>(otherPattern.getGraph().vertexSet()).get(0).getTypes();
                Set<Set<String>> collect = newPattern.getPattern().vertexSet().stream()
                        .map(Vertex::getTypes).collect(Collectors.toSet());
                if (collect.contains(types)) {
                    System.out.println("Candidate pattern: " + newPattern);
                    System.out.println("is an isomorph of current VSpawn level pattern: " + otherPattern.getPattern());
                    isIsomorphic = true;
                    break;
                }
            }
        }
        final long isomorphicCheckingTime = System.currentTimeMillis() - isIsomorphicPatternCheckStartTime;
        Util.printWithTime("isIsomorphicPatternCheck", isomorphicCheckingTime);
        Util.addToTotalSupergraphCheckingTime(isomorphicCheckingTime);
        return isIsomorphic;
    }

    // TODO: Should this be done using real subgraph isomorphism instead of strings?
    private boolean isSuperGraphOfPrunedPattern(VF2PatternGraph newPattern, PatternTree patternTree) {
        final long supergraphCheckingStartTime = System.currentTimeMillis();
        ArrayList<String> newPatternEdges = new ArrayList<>();
        newPattern.getPattern().edgeSet().forEach((edge) -> {
            newPatternEdges.add(edge.toString());
        });
        int i = Util.currentVSpawnLevel - 2;
        boolean isSupergraph = false;
        while (i >= 0) {
            for (PatternTreeNode treeNode : patternTree.getLevel(i)) {
                if (treeNode.isPruned()) {
                    if (treeNode.getPattern().getCenterVertexType().equals(newPattern.getCenterVertexType())) {
                        if (i == 0) {
                            isSupergraph = true;
                        } else {
                            ArrayList<String> otherPatternEdges = new ArrayList<>();
                            treeNode.getGraph().edgeSet().forEach((edge) -> {
                                otherPatternEdges.add(edge.toString());
                            });
                            if (newPatternEdges.containsAll(otherPatternEdges)) {
                                isSupergraph = true;
                            }
                        }
                        if (isSupergraph) {
                            System.out.println("Candidate pattern: " + newPattern);
                            System.out.println("is a supergraph of pruned subgraph pattern: " + treeNode.getPattern());
                            break;
                        }
                    }
                }
            }
            if (isSupergraph) break;
            i--;
        }
        final long supergraphCheckingTime = System.currentTimeMillis() - supergraphCheckingStartTime;
        Util.printWithTime("Supergraph checking", supergraphCheckingTime);
        Util.addToTotalSupergraphCheckingTime(supergraphCheckingTime);
        return isSupergraph;
    }

}
