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
        Map<Set<String>, Map<String, List<Integer>>> entitiesWithRHSvalues_Shit = new HashMap<>();
        Map<Set<String>, Set<ConstantLiteral>> reverseMap_entity = new HashMap<>();
        Map<String, HashMap<Set<String> ,ConstantLiteral>> reverseMap_rhs = new HashMap<>();

        // TO-DO: Add support for schemaless graphs
        for (int timestamp = 0; timestamp < matchesPerTimestamps.size(); timestamp++) {
            Set<Set<ConstantLiteral>> matchesInOneTimeStamp = matchesPerTimestamps.get(timestamp);
            System.out.println("---------- Attribute values in t = " + timestamp + " ---------- ");
            int numOfMatches = 0;
            if (matchesInOneTimeStamp.size() > 0) {
                for(Set<ConstantLiteral> match : matchesInOneTimeStamp) {
                    if (match.size() < attributes.size())
                        continue;
                    //TODO: Fix this shit

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

                    Set<String> signature = ConstantLiteral.getSignature(entity);
                    if (!entitiesWithRHSvalues_Shit.containsKey(signature))
                    {
                        entitiesWithRHSvalues.put(entity, new HashMap<>());
                        entitiesWithRHSvalues_Shit.put(signature, new HashMap<>());
                        reverseMap_entity.put(signature, entity);
                    }

                    String rhsSignature = ConstantLiteral.getSignature(rhs);
                    Set<ConstantLiteral> actualEntityInHashMap = reverseMap_entity.get(signature);

                    if (!entitiesWithRHSvalues_Shit.get(signature).containsKey(rhsSignature))
                    {
                        entitiesWithRHSvalues_Shit.get(signature).put(rhsSignature, null);
                        entitiesWithRHSvalues.get(actualEntityInHashMap).put(rhs, Util.createEmptyArrayListOfSize(matchesPerTimestamps.size()));
                        if(!reverseMap_rhs.containsKey(rhsSignature))
                            reverseMap_rhs.put(rhsSignature, new HashMap<>());
                        reverseMap_rhs.get(rhsSignature).put(signature,rhs);
                    }

                    ConstantLiteral actualRhsInHashMap = reverseMap_rhs.get(rhsSignature).get(signature);
                    try {
                        entitiesWithRHSvalues.get(actualEntityInHashMap).get(actualRhsInHashMap).set(timestamp, entitiesWithRHSvalues.get(actualEntityInHashMap).get(actualRhsInHashMap).get(timestamp) + 1);
                    }
                    catch (Exception e)
                    {
                        System.out.println(e.getMessage());
                    }
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
