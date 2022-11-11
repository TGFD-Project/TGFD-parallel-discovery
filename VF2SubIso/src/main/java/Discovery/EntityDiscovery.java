package Discovery;

import Infra.AttributeDependency;
import Infra.ConstantLiteral;

import java.util.*;

public class EntityDiscovery {

    AttributeDependency attributes;
    List<Set<Set<ConstantLiteral>>> matchesPerTimestamps;

    public EntityDiscovery(AttributeDependency attributes, List<Set<Set<ConstantLiteral>>> matchesPerTimestamps)
    {
        this.attributes = attributes;
        this.matchesPerTimestamps = matchesPerTimestamps;
    }

    public Map<Set<ConstantLiteral>, ArrayList<Map.Entry<ConstantLiteral, List<Integer>>>> findEntities()
    {
        String yVertexType = attributes.getRhs().getVertexType();
        String yAttrName = attributes.getRhs().getAttrName();
        Set<ConstantLiteral> xAttributes = attributes.getLhs();
        Map<Set<ConstantLiteral>, Map<ConstantLiteral, List<Integer>>> entitiesWithRHSvalues = new HashMap<>();

        // TO-DO: Add support for schemaless graphs
        for (int timestamp = 0; timestamp < matchesPerTimestamps.size(); timestamp++) {
            Set<Set<ConstantLiteral>> matchesInOneTimeStamp = matchesPerTimestamps.get(timestamp);
            System.out.println("---------- Attribute values in t = " + timestamp + " ---------- ");
            int numOfMatches = 0;
            if (matchesInOneTimeStamp.size() > 0) {
                for(Set<ConstantLiteral> match : matchesInOneTimeStamp) {
                    if (match.size() < attributes.size())
                        continue;
                    Set<ConstantLiteral> entity = new HashSet<>();
                    ConstantLiteral rhs = null;
                    for (ConstantLiteral literalInMatch : match) {
                        if (literalInMatch.getVertexType().equals(yVertexType) && literalInMatch.getAttrName().equals(yAttrName)) {
                            rhs = literalInMatch;
                            continue;
                        }
                        for (ConstantLiteral attribute : xAttributes) {
                            if (literalInMatch.getVertexType().equals(attribute.getVertexType()) && literalInMatch.getAttrName().equals(attribute.getAttrName()))
                                entity.add(literalInMatch);
                        }
                    }
                    if (entity.size() < xAttributes.size() || rhs == null)
                        continue;

                    if (!entitiesWithRHSvalues.containsKey(entity))
                        entitiesWithRHSvalues.put(entity, new HashMap<>());

                    if (!entitiesWithRHSvalues.get(entity).containsKey(rhs))
                        entitiesWithRHSvalues.get(entity).put(rhs, Util.createEmptyArrayListOfSize(matchesPerTimestamps.size()));

                    entitiesWithRHSvalues.get(entity).get(rhs).set(timestamp, entitiesWithRHSvalues.get(entity).get(rhs).get(timestamp)+1);
                    numOfMatches++;
                }
            }
            System.out.println("Number of matches: " + numOfMatches);
        }
        if (entitiesWithRHSvalues.size() == 0)
            return null;

//        Comparator<Map.Entry<ConstantLiteral, List<Integer>>> comparator = new Comparator<Map.Entry<ConstantLiteral, List<Integer>>>() {
//            @Override
//            public int compare(Map.Entry<ConstantLiteral, List<Integer>> o1, Map.Entry<ConstantLiteral, List<Integer>> o2) {
//                return o2.getValue().stream().reduce(0, Integer::sum) - o1.getValue().stream().reduce(0, Integer::sum);
//            }
//        };

        Comparator<Map.Entry<ConstantLiteral, List<Integer>>> comparator =
                (o1, o2) ->
                        o2.getValue()
                        .stream()
                        .reduce(0, Integer::sum) -
                        o1.getValue()
                        .stream()
                        .reduce(0, Integer::sum);

        Map<Set<ConstantLiteral>, ArrayList<Map.Entry<ConstantLiteral, List<Integer>>>> entitiesWithSortedRHSvalues = new HashMap<>();
        for (Set<ConstantLiteral> entity : entitiesWithRHSvalues.keySet()) {
            Map<ConstantLiteral, List<Integer>> rhsMapOfEntity = entitiesWithRHSvalues.get(entity);
            ArrayList<Map.Entry<ConstantLiteral, List<Integer>>> sortedRhsMapOfEntity = new ArrayList<>(rhsMapOfEntity.entrySet());
            sortedRhsMapOfEntity.sort(comparator);
            entitiesWithSortedRHSvalues.put(entity, sortedRhsMapOfEntity);
        }

        return entitiesWithSortedRHSvalues;
    }


}
