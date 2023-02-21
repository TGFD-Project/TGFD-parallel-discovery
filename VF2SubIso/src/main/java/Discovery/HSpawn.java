package Discovery;

import ICs.TGFD;
import Infra.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HSpawn {

    private PatternTreeNode patternTreeNode;
    private List<Set<Set<ConstantLiteral>>> matchesPerTimestamps;

    public HSpawn(PatternTreeNode patternTreeNode, List<Set<Set<ConstantLiteral>>> matchesPerTimestamps) {
        this.matchesPerTimestamps = matchesPerTimestamps;
        this.patternTreeNode = patternTreeNode;
    }

    public ArrayList<TGFD> performHSPawn() {
        ArrayList<TGFD> tgfds = new ArrayList<>();

        System.out.println("Performing HSpawn for " + patternTreeNode.getPattern());

        List<ConstantLiteral> activeAttributesInPattern = new ArrayList<>(Util.getActiveAttributesInPattern(patternTreeNode.getGraph().vertexSet(), false));

        LiteralTree literalTree = new LiteralTree();
        int hSpawnLimit;
        if (Util.onlyInterestingTGFDs) {
            hSpawnLimit = Math.max(patternTreeNode.getGraph().vertexSet().size(), Util.maxNumOfLiterals);
        } else {
            hSpawnLimit = Util.maxNumOfLiterals;
        }
        for (int j = 0; j < hSpawnLimit; j++) {

            System.out.println("HSpawn level " + j + "/" + (hSpawnLimit - 1));

            if (j == 0) {
                literalTree.addLevel();
                for (int index = 0; index < activeAttributesInPattern.size(); index++) {
                    ConstantLiteral literal = activeAttributesInPattern.get(index);
                    literalTree.createNodeAtLevel(j, literal, null);
                    System.out.println("Created root " + index + "/" + activeAttributesInPattern.size() + " of literal forest.");
                }
            } else {
                ArrayList<LiteralTreeNode> literalTreePreviousLevel = literalTree.getLevel(j - 1);
                if (literalTreePreviousLevel.size() == 0) {
                    System.out.println("Previous level of literal tree is empty. Nothing to expand. End HSpawn");
                    break;
                }
                literalTree.addLevel();
                HashSet<AttributeDependency> visitedPaths = new HashSet<>();
                ArrayList<TGFD> currentLevelTGFDs = new ArrayList<>();
                for (int literalTreePreviousLevelIndex = 0; literalTreePreviousLevelIndex < literalTreePreviousLevel.size(); literalTreePreviousLevelIndex++) {
                    System.out.println("Expanding previous level literal tree path " + (literalTreePreviousLevelIndex + 1) + "/" + literalTreePreviousLevel.size() + "...");

                    LiteralTreeNode previousLevelLiteral = literalTreePreviousLevel.get(literalTreePreviousLevelIndex);
                    ArrayList<ConstantLiteral> parentsPathToRoot = previousLevelLiteral.getPathToRoot(); //TODO: Can this be implemented as HashSet to improve performance?
                    System.out.println("Literal path: " + parentsPathToRoot);

                    if (previousLevelLiteral.isPruned()) {
                        System.out.println("Could not expand pruned literal path.");
                        continue;
                    }
                    for (int index = 0; index < activeAttributesInPattern.size(); index++) {
                        ConstantLiteral literal = activeAttributesInPattern.get(index);
                        System.out.println("Adding active attribute " + (index + 1) + "/" + activeAttributesInPattern.size() + " to path...");
                        System.out.println("Literal: " + literal);
                        if (Util.onlyInterestingTGFDs && j < patternTreeNode.getGraph().vertexSet().size()) { // Ensures all vertices are involved in dependency
                            if (Util.isUsedVertexType(literal.getVertexType(), parentsPathToRoot))
                                continue;
                        }

                        if (parentsPathToRoot.contains(literal)) {
                            System.out.println("Skip. Literal already exists in path.");
                            continue;
                        }

                        // Check if path to candidate leaf node is unique
                        AttributeDependency newPath = new AttributeDependency(parentsPathToRoot, literal);
                        System.out.println("New candidate literal path: " + newPath);

                        long visitedPathCheckingTime = System.currentTimeMillis();
                        boolean isVistedPath = false;
                        if (visitedPaths.contains(newPath)) { // TODO: Is this relevant anymore?
                            System.out.println("Skip. Duplicate literal path.");
                            isVistedPath = true;
                        }
                        visitedPathCheckingTime = System.currentTimeMillis() - visitedPathCheckingTime;
                        Util.printWithTime("visitedPathChecking", visitedPathCheckingTime);
                        Util.addToTotalVisitedPathCheckingTime(visitedPathCheckingTime);

                        if (isVistedPath)
                            continue;

                        long supersetPathCheckingTime = System.currentTimeMillis();
                        boolean isSuperSetPath = false;
                        if (Util.hasSupportPruning && newPath.isSuperSetOfPath(patternTreeNode.getZeroEntityDependenciesOnThisPath())) { // Ensures we don't re-explore dependencies whose subsets have no entities
                            System.out.println("Skip. Candidate literal path is a superset of zero-entity dependency.");
                            isSuperSetPath = true;
                        } else if (Util.hasMinimalityPruning && newPath.isSuperSetOfPath(patternTreeNode.getAllMinimalDependenciesOnThisPath())) { // Ensures we don't re-explore dependencies whose subsets have already have a general dependency
                            System.out.println("Skip. Candidate literal path is a superset of minimal dependency.");
                            isSuperSetPath = true;
                        }
                        supersetPathCheckingTime = System.currentTimeMillis() - supersetPathCheckingTime;
                        Util.printWithTime("supersetPathCheckingTime", supersetPathCheckingTime);
                        Util.addToTotalSupersetPathCheckingTime(supersetPathCheckingTime);

                        if (isSuperSetPath)
                            continue;

                        // Add leaf node to tree
                        LiteralTreeNode literalTreeNode = literalTree.createNodeAtLevel(j, literal, previousLevelLiteral);
                        System.out.println("Added candidate literal path to tree.");

                        visitedPaths.add(newPath);

                        if (Util.onlyInterestingTGFDs) { // Ensures all vertices are involved in dependency
                            if (Util.literalPathIsMissingTypesInPattern(literalTreeNode.getPathToRoot(), patternTreeNode.getGraph().vertexSet())) {
                                System.out.println("Skip Delta Discovery. Literal path does not involve all pattern vertices.");
                                continue;
                            }
                        }

                        System.out.println("Performing Delta Discovery at HSpawn level " + j);
                        final long deltaDiscoveryTime = System.currentTimeMillis();
                        DeltaDiscovery deltaDiscovery = new DeltaDiscovery(patternTreeNode, literalTreeNode, newPath, matchesPerTimestamps);
                        ArrayList<TGFD> discoveredTGFDs = deltaDiscovery.perform();
                        Util.printWithTime("deltaDiscovery", System.currentTimeMillis() - deltaDiscoveryTime);
                        currentLevelTGFDs.addAll(discoveredTGFDs);
                    }
                }
                System.out.println("TGFDs generated at HSpawn level " + j + ": " + currentLevelTGFDs.size());
                if (currentLevelTGFDs.size() > 0) {
                    tgfds.addAll(currentLevelTGFDs);
                }
            }
            System.out.println("Generated new literal tree nodes: " + literalTree.getLevel(j).size());
        }
        System.out.println("For pattern " + patternTreeNode.getPattern());
        System.out.println("HSpawn TGFD count: " + tgfds.size());
        return tgfds;
    }

}
