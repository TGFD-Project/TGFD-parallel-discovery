package Discovery;

import ICs.TGFD;
import Infra.*;

import java.util.*;

public class DeltaDiscovery {


    private PatternTreeNode patternNode;
    private LiteralTreeNode literalTreeNode;
    private AttributeDependency literalPath;
    private List<Set<Set<ConstantLiteral>>> matchesPerTimestamps;
    private EntityDiscovery entityDiscovery;

    public DeltaDiscovery(PatternTreeNode patternNode, LiteralTreeNode literalTreeNode, AttributeDependency literalPath, List<Set<Set<ConstantLiteral>>> matchesPerTimestamps) {
        this.patternNode = patternNode;
        this.literalTreeNode = literalTreeNode;
        this.literalPath = literalPath;
        this.matchesPerTimestamps = matchesPerTimestamps;
    }

    public ArrayList<ArrayList<TGFD>> perform() {
        ArrayList<ArrayList<TGFD>> tgfds = new ArrayList<>();
        tgfds.add(new ArrayList<>());
        tgfds.add(new ArrayList<>());

        // Add dependency attributes to pattern
        // TODO: Fix - when multiple vertices in a pattern have the same type, attribute values get overwritten
        VF2PatternGraph patternForDependency = patternNode.getPattern().copy();
        Set<ConstantLiteral> attributesSetForDependency = new HashSet<>(literalPath.getLhs());
        attributesSetForDependency.add(literalPath.getRhs());
        for (Vertex v : patternForDependency.getPattern().vertexSet()) {
            String vType = new ArrayList<>(v.getTypes()).get(0);
            for (ConstantLiteral attribute : attributesSetForDependency) {
                if (vType.equals(attribute.getVertexType())) {
                    v.putAttributeIfAbsent(new Attribute(attribute.getAttrName()));
                }
            }
        }

        System.out.println("Pattern: " + patternForDependency);
        System.out.println("Dependency: " + "\n\tY=" + literalPath.getRhs() + ",\n\tX={" + literalPath.getLhs() + "\n\t}");

        System.out.println("Performing Entity Discovery");

        // Discover entities
        long findEntitiesTime = System.currentTimeMillis();
        entityDiscovery = new EntityDiscovery(literalPath, matchesPerTimestamps);
        Map<Set<ConstantLiteral>, ArrayList<Map.Entry<ConstantLiteral, List<Integer>>>> entities = entityDiscovery.findEntities();
        findEntitiesTime = System.currentTimeMillis() - findEntitiesTime;
        Util.printWithTime("findEntitiesTime", findEntitiesTime);
        Util.addToTotalFindEntitiesTime(findEntitiesTime);
        if (entities == null) {
            System.out.println("No entities found during entity discovery.");
            if (Util.hasSupportPruning) {
                literalTreeNode.setIsPruned();
                System.out.println("Marked as pruned. Literal path " + literalTreeNode.getPathToRoot());
                patternNode.addZeroEntityDependency(literalPath);
            }
            return tgfds;
        }
        System.out.println("Number of entities discovered: " + entities.size());

        System.out.println("Discovering constant TGFDs");

        // Find Constant TGFDs
        Map<Util.Pair, ArrayList<TreeSet<Util.Pair>>> deltaToPairsMap = new HashMap<>();
        DiscoverConstantTGFDs discoverConstantTGFDs = new DiscoverConstantTGFDs(patternNode, literalPath.getRhs(), entities, deltaToPairsMap);
        ArrayList<TGFD> constantTGFDs = discoverConstantTGFDs.discover();

        // TODO: Try discover general TGFD even if no constant TGFD candidate met support threshold
        System.out.println("Constant TGFDs discovered: " + constantTGFDs.size());
        tgfds.get(0).addAll(constantTGFDs);

        System.out.println("Discovering general TGFDs");

        // Find general TGFDs
        if (!deltaToPairsMap.isEmpty()) {
            Util.numOfCandidateGeneralTGFDs += 1;
            long discoverGeneralTGFDTime = System.currentTimeMillis();

            DiscoverGeneralTGFD discoverGeneralTGFD = new DiscoverGeneralTGFD(patternNode, patternNode.getPatternSupport(), literalPath, entities.size(), deltaToPairsMap, literalTreeNode);
            ArrayList<TGFD> generalTGFDs = discoverGeneralTGFD.discover();
            discoverGeneralTGFDTime = System.currentTimeMillis() - discoverGeneralTGFDTime;
            Util.printWithTime("discoverGeneralTGFDTime", discoverGeneralTGFDTime);
            Util.addToTotalDiscoverGeneralTGFDTime(discoverGeneralTGFDTime);
            if (generalTGFDs.size() > 0) {
                System.out.println("Discovered " + generalTGFDs.size() + " general TGFDs for this dependency.");
                if (Util.hasMinimalityPruning) {
                    literalTreeNode.setIsPruned();
                    System.out.println("Marked as pruned. Literal path " + literalTreeNode.getPathToRoot());
                    patternNode.addMinimalDependency(literalPath);
                }
            }
            tgfds.get(1).addAll(generalTGFDs);
        }

        return tgfds;
    }

}
