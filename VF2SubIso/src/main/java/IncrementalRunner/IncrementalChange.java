package IncrementalRunner;

import Infra.*;
import org.jgrapht.GraphMapping;

import java.util.*;

public class IncrementalChange {

    //region Fields: Private
    private VF2PatternGraph pattern;
    private HashMap <String, GraphMapping <Vertex, RelationshipEdge>> newMatches;
    private HashMap<String, Set<ConstantLiteral>> newMatchesInConstantLiteralFormat;
    private HashMap<String, Set<ConstantLiteral>> removedMatchesInConstantLiteralFormat;
    private ArrayList <String> removedMatchesSignatures;
    private HashMap<String, GraphMapping<Vertex, RelationshipEdge>> afterMatches;
    private HashMap<String, Set<ConstantLiteral>> beforeMatches;
    //endregion

    //region Constructors
    public IncrementalChange(Iterator<GraphMapping<Vertex, RelationshipEdge>> beforeMatchIterator,VF2PatternGraph pattern)
    {
        newMatches=new HashMap<>();
        newMatchesInConstantLiteralFormat=new HashMap<>();
        removedMatchesInConstantLiteralFormat = new HashMap<>();
        removedMatchesSignatures=new ArrayList <>();
        this.pattern=pattern;
        computeBeforeMatches(beforeMatchIterator);
    }
    //endregion

    //region Public Functions

    public String addAfterMatches(Iterator<GraphMapping<Vertex, RelationshipEdge>> afterMatchIterator)
    {
        afterMatches=new HashMap<>();
        if(afterMatchIterator!=null) {
            while (afterMatchIterator.hasNext()) {
                var mapping = afterMatchIterator.next();
                var signatureFromPattern = Match.signatureFromPattern(pattern, mapping);

                afterMatches.put(signatureFromPattern, mapping);
            }
        }

        for (String key:afterMatches.keySet()) {
            if(!beforeMatches.containsKey(key))
                newMatches.put(key,afterMatches.get(key));
        }
        for (String key:beforeMatches.keySet()) {
            if(!afterMatches.containsKey(key)) {
                removedMatchesSignatures.add(key);
                removedMatchesInConstantLiteralFormat.put(key,beforeMatches.get(key));
            }
        }
        return beforeMatches.keySet().size() + " - " +afterMatches.size();
        //System.out.print(beforeMatchesSignatures.size() + " -- " + newMatches.size() + " -- " + removedMatchesSignatures.size());
    }
    //endregion

    //region Private Functions
    private void computeBeforeMatches(Iterator<GraphMapping<Vertex, RelationshipEdge>> beforeMatchIterator)
    {
        beforeMatches=new HashMap <>();
        if (beforeMatchIterator!=null)
        {
            while (beforeMatchIterator.hasNext())
            {
                GraphMapping<Vertex, RelationshipEdge> mapping = beforeMatchIterator.next();
                beforeMatches.put(Match.signatureFromPattern(pattern, mapping), Match.extractMatch(pattern, mapping));
            }
        }
    }
    //endregion

    //region Getters
    public HashMap <String, GraphMapping <Vertex, RelationshipEdge>> getNewMatches() {
        return newMatches;
    }

    public ArrayList<String> getRemovedMatchesSignatures() {
        return removedMatchesSignatures;
    }

    public HashMap<String, Set<ConstantLiteral>> getNewMatchesInConstantLiteralFormat() {
        newMatchesInConstantLiteralFormat.clear();
        for (String signature:newMatches.keySet()) {
            newMatchesInConstantLiteralFormat.put(signature,Match.extractMatch(pattern,newMatches.get(signature)));
        }
        return newMatchesInConstantLiteralFormat;
    }

    public HashMap<String, Set<ConstantLiteral>> getRemovedMatches() {
        return removedMatchesInConstantLiteralFormat;
    }

    //endregion
}
