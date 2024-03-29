package Infra;

import Discovery.Util;
import QPathBasedWorkload.VertexMapping;
import org.jgrapht.GraphMapping;

import java.time.Duration;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

/**
 * Represents a match.
 * @note We do not need the edges of a match
 */
public final class Match {
    //region --[Fields: Private]---------------------------------------
    /** Intervals where the match exists. */
    private List<Interval> intervals;

    /** Graph mapping from pattern graph to match graph. */
//    private GraphMapping<Vertex, RelationshipEdge> matchMapping;

    /** Graph mapping from pattern graph to match graph using vertexMapping for QPath based subgraph isomorphism. */
//    private VertexMapping matchVertexMapping;

    /** Signature of the match computed from X. */
    private String signatureX;

    /** Signature of the match computed from X. */
    private String signatureY;

    /** Signature of the match computed from the pattern. */
    private String signatureFromPattern;

    /** Signature of the match computed from Y with different intervals. */
    //private HashMap<String, List<Interval>> signatureYWithInterval = new HashMap<>();

//    private TemporalGraph<Vertex> temporalGraph;
    //endregion

    //region --[Constructors]------------------------------------------

    //region --[Constructors]------------------------------------------
    private Match(
            String signatureX,
            String signatureY,
            String signatureFromPattern,
            List<Interval> intervals)
    {
        this.signatureX = signatureX;
        this.intervals = intervals;
        this.signatureY = signatureY;
        this.signatureFromPattern = signatureFromPattern;

//        this.matchMapping = null;
//        this.matchVertexMapping=matchVertexMapping;
    }

    /**
     * Create a new Match.
     * @param signatureX Signature of the match computed from X.
     */
    public Match(
            String signatureX,
            String signatureY,
            String signatureFromPattern)
    {
        // TODO: FIXME: can we get away with using initalTimepoint for the TemporalGraph? [2021-02-24]
        this(signatureX, signatureY,  signatureFromPattern, new ArrayList<Interval>());
    }

    /**
     * Creates a new Match with the given intervals.
     * @param intervals Intervals of the match.
     */
    public Match WithIntervals(List<Interval> intervals)
    {
            return new Match(
                    this.signatureX,
                    this.signatureY,
                    this.signatureFromPattern,
                    intervals);
    }
    //endregion

    //region --[Methods: Public]---------------------------------------
    /**
     * Adds a timepoint to the match.
     *
     * Will either extend the latest interval to include the new timepoint, or
     * add a new interval (break in intervals represents that no match occurred).
     *
     * @param timepoint Timepoint of match.
     * @param granularity Minimum timespan between matches.
     * @exception IllegalArgumentException if timepoint is before the latest interval's end.
     * @exception IllegalArgumentException if timepoint is less than the granularity away from the latest interval end.
     */
    public void addTimepoint(LocalDate timepoint, Duration granularity)
    {
        if (intervals.isEmpty())
        {
            intervals.add(new Interval(timepoint, timepoint));
            return;
        }

        Interval latestInterval = intervals.get(intervals.size()-1);
        //var latestInterval = intervals.stream().max(Comparator.comparing(Interval::getEnd)).orElseThrow();

        LocalDate latestEnd = latestInterval.getEnd();
        if (timepoint.isBefore(latestEnd) || timepoint.isEqual(latestEnd))
            return;
//            throw new IllegalArgumentException(String.format(
//                "Timepoint `%s` is <= the latest interval's end `%s`",
//                timepoint.toString(), latestEnd.toString()));

        Duration sinceEnd = Duration.between(latestEnd.atStartOfDay(), timepoint.atStartOfDay());
        int comparison = sinceEnd.compareTo(granularity);
        if (comparison > 0)
        {
            // Time since end is greater than the granularity so add a new interval.
            // This represents that the match did not exist between the latestInterval.end and newInterval.start.
            intervals.add(new Interval(timepoint, timepoint));
        }
        else if (comparison == 0)
        {
            // Time since end is the granularity so extend the last interval.
            // This represents that the match continued existing for this interval.
            latestInterval.setEnd(timepoint);
        }
        else
        {
            //System.out.println("Match already exists at the same timestamp, signatureX: " + signatureX);
            //For now, I ignore throwing this error to figure out how to anchor matches together
//            throw new IllegalArgumentException(String.format(
//                "Timepoint `%s` is less than the granularity `%s` away from the latest interval end `%s`",
//                timepoint.toString(), granularity.toString(), latestEnd.toString()));
        }
    }

//    /**
//     * Adds a timepoint to the match.
//     *
//     * Will either extend the latest interval to include the new timepoint, or
//     * add a new interval (break in intervals represents that no match occurred).
//     *
//     * @param timepoint Timepoint of match.
//     * @param granularity Minimum timespan between matches.
//     * @param signatureY Signature of the match derived form Y.
//     * @exception IllegalArgumentException if timepoint is before the latest interval's end.
//     * @exception IllegalArgumentException if timepoint is less than the granularity away from the latest interval end.
//     */
//    public void addSignatureY(LocalDate timepoint, Duration granularity, String signatureY)
//    {
//        if (!signatureYWithInterval.containsKey(signatureY))
//        {
//            signatureYWithInterval.put(signatureY,new ArrayList<>());
//            signatureYWithInterval.get(signatureY).add(new Interval(timepoint, timepoint));
//            return;
//        }
//
//        var latestInterval = signatureYWithInterval.get(signatureY)
//                .get(signatureYWithInterval.get(signatureY).size()-1);
//
//        //var latestInterval = signatureYWithInterval.get(signatureY).stream()
//        //                .max(Comparator.comparing(Interval::getEnd))
//        //                .orElseThrow();
//
//        var latestEnd = latestInterval.getEnd();
//        if (timepoint.isBefore(latestEnd))
//            return;
////            throw new IllegalArgumentException(String.format(
////                    "Timepoint `%s` is < the latest interval's end `%s`",
////                    timepoint.toString(), latestEnd.toString()));
//
//        var sinceEnd = Duration.between(latestEnd.atStartOfDay(), timepoint.atStartOfDay());
//        var comparison = sinceEnd.compareTo(granularity);
//        if (comparison > 0)
//        {
//            // Time since end is greater than the granularity so add a new interval.
//            // This represents that the match did not exist between the latestInterval.end and newInterval.start.
//            signatureYWithInterval.get(signatureY).add(new Interval(timepoint, timepoint));
//        }
//        else if (comparison == 0)
//        {
//            // Time since end is the granularity so extend the last interval.
//            // This represents that the match continued existing for this interval.
//            latestInterval.setEnd(timepoint);
//        }
//        else
//        {
//            //throw new IllegalArgumentException("Timepoint is less than the granularity away from the latest interval end");
//        }
//    }

//    public void addSignatureYBasedOnTimestap(LocalDate timepoint, String signatureY)
//    {
//        if(!allSignatureY.containsKey(timepoint))
//            allSignatureY.put(timepoint,signatureY);
//    }

    public String getSignatureY()
    {
        return signatureY;
    }

    /**
     * Gets the signature of a match for comparison across time w.r.t. the X of the dependency.
     * @param pattern Pattern of the match.
     * @param mapping Mapping of the match.
     * @param xLiterals Literals of the X dependency.
     */
    public static String signatureFromX(
            VF2PatternGraph pattern,
            GraphMapping<Vertex, RelationshipEdge> mapping,
            ArrayList<Literal> xLiterals)
    {
        // We assume that all x variable literals are also defined in the pattern? [2021-02-13]
        StringBuilder builder = new StringBuilder();
        AtomicBoolean violateALiteralInX= new AtomicBoolean(false);

        // TODO: consider collecting (type, name, attr) and sorting at the end [2021-02-14]

        // NOTE: Ensure stable sorting of vertices [2021-02-13]
        Stream sortedPatternVertices = pattern.getPattern().vertexSet().stream().sorted();
        sortedPatternVertices.forEach(patternVertex ->
        {
            Vertex matchVertex = mapping.getVertexCorrespondence((Vertex) patternVertex, false);
            if (matchVertex == null)
                return;

            // NOTE: Ensure stable sorting of attributes [2021-02-13]
            //var sortedAttributes = matchVertex.getAllAttributesList().stream().sorted();
            //sortedAttributes.forEach(attribute ->{});
            for (Literal literal : xLiterals)
            {
                // We can ignore constant literals because a Match is for a single TGFD which has constant defined in the pattern
                if (literal instanceof VariableLiteral)
                {
                    VariableLiteral varLiteral = (VariableLiteral)literal;
                    Set<String> matchVertexTypes = matchVertex.getTypes();
                    if ((matchVertexTypes.contains(varLiteral.getVertexType_1()) && matchVertex.hasAttribute(varLiteral.getAttrName_1())))
                    {
                        builder.append(varLiteral.getVertexType_1()).append("_1.")
                                .append(varLiteral.getAttrName_1()).append(": ");
                        builder.append(matchVertex.getAttributeValueByName(varLiteral.getAttrName_1()));
                        builder.append(",");
                    }
                    if(matchVertexTypes.contains(varLiteral.getVertexType_2()) && matchVertex.hasAttribute((varLiteral.getAttrName_2())))
                    {
                        builder.append(varLiteral.getVertexType_2()).append("_2.")
                                .append(varLiteral.getAttrName_2()).append(": ");
                        builder.append(matchVertex.getAttributeValueByName(varLiteral.getAttrName_2()));
                        builder.append(",");
                    }
                }
                else if(literal instanceof ConstantLiteral)
                {
                    //TODO: Check for constant literals on X
                    ConstantLiteral constantLiteral = (ConstantLiteral)literal;
                    if (!matchVertex.getTypes().contains(constantLiteral.getVertexType()))
                        continue;
                    if (!matchVertex.hasAttribute(constantLiteral.getAttrName()))
                        continue;
                    if (!matchVertex.getAttributeValueByName(constantLiteral.getAttrName()).equals(constantLiteral.getAttrValue())) {
                        violateALiteralInX.set(true);
                    }
                }
            }
        });
        // TODO: consider returning a hash [2021-02-13]
        if(violateALiteralInX.get())
            return null;
        else
            return builder.toString();
    }

    /**
     * Gets the signature of a match for comparison across time w.r.t. the X of the dependency.
     * @param pattern Pattern of the match.
     * @param mapping VertexMapping of the match.
     * @param xLiterals Literals of the X dependency.
     */
    public static String signatureFromX(
            VF2PatternGraph pattern,
            VertexMapping mapping,
            ArrayList<Literal> xLiterals)
    {
        // We assume that all x variable literals are also defined in the pattern? [2021-02-13]
        StringBuilder builder = new StringBuilder();
        AtomicBoolean violateALiteralInX= new AtomicBoolean(false);

        // TODO: consider collecting (type, name, attr) and sorting at the end [2021-02-14]

        // NOTE: Ensure stable sorting of vertices [2021-02-13]
        Stream sortedPatternVertices = pattern.getPattern().vertexSet().stream().sorted();
        sortedPatternVertices.forEach(patternVertex ->
        {
            Vertex matchVertex = mapping.getVertexCorrespondence((Vertex) patternVertex);
            if (matchVertex == null)
                return;

            // NOTE: Ensure stable sorting of attributes [2021-02-13]
            //var sortedAttributes = matchVertex.getAllAttributesList().stream().sorted();
            //sortedAttributes.forEach(attribute ->{});
            for (Literal literal : xLiterals)
            {
                // We can ignore constant literals because a Match is for a single TGFD which has constant defined in the pattern
                if (literal instanceof VariableLiteral)
                {
                    VariableLiteral varLiteral = (VariableLiteral)literal;
                    Set<String> matchVertexTypes = matchVertex.getTypes();
                    if ((matchVertexTypes.contains(varLiteral.getVertexType_1()) && matchVertex.hasAttribute(varLiteral.getAttrName_1())))
                    {
                        builder.append(matchVertex.getAttributeValueByName(varLiteral.getAttrName_1()));
                        builder.append(",");
                    }
                    if(matchVertexTypes.contains(varLiteral.getVertexType_2()) && matchVertex.hasAttribute((varLiteral.getAttrName_2())))
                    {
                        builder.append(matchVertex.getAttributeValueByName(varLiteral.getAttrName_2()));
                        builder.append(",");
                    }
                }
                else if(literal instanceof ConstantLiteral)
                {
                    //TODO: Check for constant literals on X
                    ConstantLiteral constantLiteral = (ConstantLiteral)literal;
                    if (!matchVertex.getTypes().contains(constantLiteral.getVertexType()))
                        continue;
                    if (!matchVertex.hasAttribute(constantLiteral.getAttrName()))
                        continue;
                    if (!matchVertex.getAttributeValueByName(constantLiteral.getAttrName()).equals(constantLiteral.getAttrValue())) {
                        violateALiteralInX.set(true);
                    }
                }
            }
        });
        // TODO: consider returning a hash [2021-02-13]
        if(violateALiteralInX.get())
            return null;
        else
            return builder.toString();
    }

    /**
     * Gets the signature of a match for comparison across time w.r.t. the Y of the dependency.
     * @param pattern Pattern of the match.
     * @param mapping Mapping of the match.
     * @param yLiterals TGFD dependency.
     */
    public static String signatureFromY(
            VF2PatternGraph pattern,
            GraphMapping<Vertex, RelationshipEdge> mapping,
            ArrayList<Literal> yLiterals)
    {
        // We assume that all x variable literals are also defined in the pattern? [2021-02-13]
        StringBuilder builder = new StringBuilder();

        // NOTE: Ensure stable sorting of vertices [2021-02-13]
        Stream sortedPatternVertices = pattern.getPattern().vertexSet().stream().sorted();
        sortedPatternVertices.forEach(patternVertex ->
        {
            Vertex matchVertex = mapping.getVertexCorrespondence((Vertex) patternVertex, false);
            if (matchVertex == null)
                return;

            // NOTE: Ensure stable sorting of attributes [2021-02-13]
            //var sortedAttributes = matchVertex.getAllAttributesList().stream().sorted();
            //sortedAttributes.forEach(attribute ->{});
            for (Literal literal : yLiterals)
            {
                if (literal instanceof ConstantLiteral)
                {
                    ConstantLiteral constantLiteral = (ConstantLiteral)literal;
                    if (!matchVertex.getTypes().contains(constantLiteral.getVertexType()))
                        continue;
                    if (!matchVertex.hasAttribute(constantLiteral.getAttrName()))
                        continue;
                    if (!matchVertex.getAttributeValueByName(constantLiteral.getAttrName()).equals(constantLiteral.getAttrValue()))
                        continue;

                    builder.append(matchVertex.getAttributeValueByName(constantLiteral.getAttrName()));
                    builder.append(",");
                }
                else if (literal instanceof VariableLiteral)
                {
                    VariableLiteral varLiteral = (VariableLiteral)literal;
                    Set<String> matchVertexTypes = matchVertex.getTypes();
                    if ((matchVertexTypes.contains(varLiteral.getVertexType_1()) && matchVertex.hasAttribute(varLiteral.getAttrName_1())))
                    {
                        builder.append(matchVertex.getAttributeValueByName(varLiteral.getAttrName_1()));
                        builder.append(",");
                    }
//                    if(matchVertexTypes.contains(varLiteral.getVertexType_2()) && matchVertex.hasAttribute((varLiteral.getAttrName_2())))
//                    {
//                        builder.append(matchVertex.getAttributeValueByName(varLiteral.getAttrName_2()));
//                        builder.append(",");
//                    }
                }
            }
        });
        String ret = builder.toString();
        if(ret.endsWith(","))
            ret = ret.substring(0, ret.length()-1);
        // TODO: consider returning a hash [2021-02-13]
        return ret;
    }

    /**
     * Gets the signature of a match for comparison across time w.r.t. the Y of the dependency.
     * @param pattern Pattern of the match.
     * @param mapping VertexMapping of the match.
     * @param yLiterals TGFD dependency.
     */
    public static String signatureFromY(
            VF2PatternGraph pattern,
            VertexMapping mapping,
            ArrayList<Literal> yLiterals)
    {
        // We assume that all x variable literals are also defined in the pattern? [2021-02-13]
        StringBuilder builder = new StringBuilder();

        // NOTE: Ensure stable sorting of vertices [2021-02-13]
        Stream sortedPatternVertices = pattern.getPattern().vertexSet().stream().sorted();
        sortedPatternVertices.forEach(patternVertex ->
        {
            Vertex matchVertex = mapping.getVertexCorrespondence((Vertex) patternVertex);
            if (matchVertex == null)
                return;

            // NOTE: Ensure stable sorting of attributes [2021-02-13]
            //var sortedAttributes = matchVertex.getAllAttributesList().stream().sorted();
            //sortedAttributes.forEach(attribute ->{});
            for (Literal literal : yLiterals)
            {
                if (literal instanceof ConstantLiteral)
                {
                    ConstantLiteral constantLiteral = (ConstantLiteral)literal;
                    if (!matchVertex.getTypes().contains(constantLiteral.getVertexType()))
                        continue;
                    if (!matchVertex.hasAttribute(constantLiteral.getAttrName()))
                        continue;
                    if (!matchVertex.getAttributeValueByName(constantLiteral.getAttrName()).equals(constantLiteral.getAttrValue()))
                        continue;

                    builder.append(matchVertex.getAttributeValueByName(constantLiteral.getAttrName()));
                    builder.append(",");
                }
                else if (literal instanceof VariableLiteral)
                {
                    VariableLiteral varLiteral = (VariableLiteral)literal;
                    Set<String> matchVertexTypes = matchVertex.getTypes();
                    if ((matchVertexTypes.contains(varLiteral.getVertexType_1()) && matchVertex.hasAttribute(varLiteral.getAttrName_1())))
                    {
                        builder.append(matchVertex.getAttributeValueByName(varLiteral.getAttrName_1()));
                        builder.append(",");
                    }
//                    if(matchVertexTypes.contains(varLiteral.getVertexType_2()) && matchVertex.hasAttribute((varLiteral.getAttrName_2())))
//                    {
//                        builder.append(matchVertex.getAttributeValueByName(varLiteral.getAttrName_2()));
//                        builder.append(",");
//                    }
                }
            }
        });
        String ret = builder.toString();
        if(ret.endsWith(","))
            ret = ret.substring(0, ret.length()-1);
        // TODO: consider returning a hash [2021-02-13]
        return ret;
    }

    /**
     * Gets the signature of a match w.r.t the input pattern.
     * @param pattern Pattern of the match.
     * @param mapping Mapping of the match.
     */
    public static String signatureFromPattern(
            VF2PatternGraph pattern,
            GraphMapping<Vertex, RelationshipEdge> mapping)
    {
        StringBuilder builder = new StringBuilder();

        // NOTE: Ensure stable sorting of vertices [2021-02-13]
        Stream sortedPatternVertices = pattern.getPattern().vertexSet().stream().sorted();
        sortedPatternVertices.forEach(patternVertex ->
        {
            DataVertex matchVertex = (DataVertex)mapping.getVertexCorrespondence((Vertex) patternVertex, false);
            if (matchVertex == null)
                return;
            builder.append(matchVertex.getVertexURI());
            builder.append(",");
            for (Attribute patternAttr:((Vertex)patternVertex).getAllAttributesList()) {
                if(!patternAttr.getAttrName().equals("uri"))
                {
                    builder.append(matchVertex.getAttributeValueByName(patternAttr.getAttrName()));
                    builder.append(",");
                }
            }
        });
        // TODO: consider returning a hash [2021-02-13]
        return builder.toString();
    }

    /**
     * Gets the signature of a match w.r.t the input pattern.
     * @param pattern Pattern of the match.
     * @param mapping VertexMapping of the match.
     */
    public static String signatureFromPattern(
            VF2PatternGraph pattern,
            VertexMapping mapping)
    {
        StringBuilder builder = new StringBuilder();

        // NOTE: Ensure stable sorting of vertices [2021-02-13]
        Stream sortedPatternVertices = pattern.getPattern().vertexSet().stream().sorted();
        sortedPatternVertices.forEach(patternVertex ->
        {
            DataVertex matchVertex = (DataVertex)mapping.getVertexCorrespondence((Vertex) patternVertex);
            if (matchVertex == null)
                return;
            builder.append(matchVertex.getVertexURI());
            builder.append(",");
            for (Attribute patternAttr:((Vertex) patternVertex).getAllAttributesList()) {
                if(!patternAttr.getAttrName().equals("uri"))
                {
                    builder.append(matchVertex.getAttributeValueByName(patternAttr.getAttrName()));
                    builder.append(",");
                }
            }
        });
        // TODO: consider returning a hash [2021-02-13]
        return builder.toString();
    }

    public static Set<ConstantLiteral> extractMatch(VF2PatternGraph pattern, GraphMapping<Vertex, RelationshipEdge> mapping) {
        Set<ConstantLiteral> match = new HashSet<>();
        for (Vertex v: pattern.getPattern().vertexSet()) {
            Vertex currentMatchedVertex = mapping.getVertexCorrespondence(v, false);
            if (currentMatchedVertex == null) continue;
            String patternVertexType = v.getTypes().iterator().next();
            for (String matchedAttrName : currentMatchedVertex.getAllAttributesNames()) {
                String matchedAttrValue = currentMatchedVertex.getAttributeValueByName(matchedAttrName);
                ConstantLiteral xLiteral = new ConstantLiteral(patternVertexType, matchedAttrName, matchedAttrValue);
                match.add(xLiteral);
            }
        }
        return match;
    }

    public static String extractMatch(GraphMapping<Vertex, RelationshipEdge> result, PatternTreeNode patternTreeNode, HashSet<ConstantLiteral> match, Map<String, Integer> interestingnessMap) {
        String entityURI = null;
        for (Vertex v : patternTreeNode.getGraph().vertexSet()) {
            Vertex currentMatchedVertex = result.getVertexCorrespondence(v, false);
            if (currentMatchedVertex == null) continue;
            String patternVertexType = v.getTypes().iterator().next();
            if (entityURI == null) {
                entityURI = extractAttributes(patternTreeNode, patternVertexType, match, currentMatchedVertex, interestingnessMap);
            } else {
                extractAttributes(patternTreeNode, patternVertexType, match, currentMatchedVertex, interestingnessMap);
            }
        }
        return entityURI;
    }

    public static int extractMatches(Iterator<GraphMapping<Vertex, RelationshipEdge>> iterator, HashSet<HashSet<ConstantLiteral>> matches, PatternTreeNode patternTreeNode, Map<String, List<Integer>> entityURIs, int timestamp) {
        int numOfMatches = 0;
        while (iterator.hasNext()) {
            numOfMatches++;
            GraphMapping<Vertex, RelationshipEdge> result = iterator.next();
            HashSet<ConstantLiteral> literalsInMatch = new HashSet<>();
            Map<String, Integer> interestingnessMap = new HashMap<>();
            String entityURI = Match.extractMatch(result, patternTreeNode, literalsInMatch, interestingnessMap);
            // ensures that the match is not empty and contains more than just the uri attribute
            if (Util.onlyInterestingTGFDs && interestingnessMap.values().stream().anyMatch(n -> n < 2)) {
                continue;
            } else if (!Util.onlyInterestingTGFDs && literalsInMatch.size() < patternTreeNode.getGraph().vertexSet().size()) {
                continue;
            }
            if (entityURI != null) {
                entityURIs.putIfAbsent(entityURI, Util.createEmptyArrayListOfSize(Util.numOfSnapshots));
                entityURIs.get(entityURI).set(timestamp, entityURIs.get(entityURI).get(timestamp)+1);
            }
            matches.add(literalsInMatch);
        }
//        matches.sort(new Comparator<HashSet<ConstantLiteral>>() {
//            @Override
//            public int compare(HashSet<ConstantLiteral> o1, HashSet<ConstantLiteral> o2) {
//                return o1.size() - o2.size();
//            }
//        });
        return numOfMatches;
    }

    public static String extractAttributes(PatternTreeNode patternTreeNode, String patternVertexType, HashSet<ConstantLiteral> match, Vertex currentMatchedVertex, Map<String, Integer> interestingnessMap) {
        String entityURI = null;
        String centerVertexType = patternTreeNode.getPattern().getCenterVertexType();
        Set<String> matchedVertexTypes = currentMatchedVertex.getTypes();
        for (ConstantLiteral activeAttribute : getActiveAttributesInPattern(patternTreeNode.getGraph().vertexSet(),true)) {
            if (!matchedVertexTypes.contains(activeAttribute.getVertexType())) continue;
            for (String matchedAttrName : currentMatchedVertex.getAllAttributesNames()) {
                if (matchedVertexTypes.contains(centerVertexType) && matchedAttrName.equals("uri")) {
                    entityURI = currentMatchedVertex.getAttributeValueByName(matchedAttrName);
                }
                if (!activeAttribute.getAttrName().equals(matchedAttrName)) continue;
                String matchedAttrValue = currentMatchedVertex.getAttributeValueByName(matchedAttrName);
                ConstantLiteral xLiteral = new ConstantLiteral(patternVertexType, matchedAttrName, matchedAttrValue);
                interestingnessMap.merge(patternVertexType, 1, Integer::sum);
                match.add(xLiteral);
            }
        }
        return entityURI;
    }

    public static HashSet<ConstantLiteral> getActiveAttributesInPattern(Set<Vertex> vertexSet, boolean considerURI) {
        HashMap<String, HashSet<String>> patternVerticesAttributes = new HashMap<>();
        for (Vertex vertex : vertexSet) {
            for (String vertexType : vertex.getTypes()) {
                patternVerticesAttributes.put(vertexType, new HashSet<>());
                Set<String> attrNameSet = Util.getVertexTypesToActiveAttributesMap().get(vertexType);
                for (String attrName : attrNameSet) {
                    patternVerticesAttributes.get(vertexType).add(attrName);
                }
            }
        }
        HashSet<ConstantLiteral> literals = new HashSet<>();
        for (String vertexType : patternVerticesAttributes.keySet()) {
            if (considerURI) literals.add(new ConstantLiteral(vertexType,"uri",null));
            for (String attrName : patternVerticesAttributes.get(vertexType)) {
                ConstantLiteral literal = new ConstantLiteral(vertexType, attrName, null);
                literals.add(literal);
            }
        }
        return literals;
    }

    //endregion

    //region --[Properties: Public]------------------------------------
    /** Gets the intervals of the match. */
    public List<Interval> getIntervals() { return this.intervals; }

    /** Gets the vertices of the match. */
//    public GraphMapping<Vertex, RelationshipEdge> getMatchMapping() { return this.matchMapping; }

    /** Gets the vertices of the match using VertexMapping. */
//    public VertexMapping getMatchVertexMapping() {
//        return matchVertexMapping;
//    }

    /** Gets the signature of the match computed from X. */
    public String getSignatureX() { return signatureX; }

    /** Sets the signature of the match computed from X. */
    public void setSignatureX(String signatureX) {
        this.signatureX = signatureX;
    }

    /** Gets the signature of the match computed from the pattern. */
    public String getSignatureFromPattern() { return signatureFromPattern; }

    /** Sets the signature of the match computed from the pattern. */
//    public void setSignatureFromPattern(LocalDate date, String signatureFromPattern) {
//        this.signatureFromPattern.put(date,signatureFromPattern);
//    }

    /** Gets the signature Y of the match along with different time intervals. */
//    public String getSignatureY() {
//        return signatureY;
//    }
    //endregion

    //region --[Methods: Override]-------------------------------------

    @Override
    public String toString() {
        return "Match{" +
                "intervals=" + intervals +
                ", signatureX='" + signatureX + '\'' +
                ", signatureY=" + signatureY +
                ", signatureFromPattern=" + signatureFromPattern +
                '}';
    }

    //endregion
}