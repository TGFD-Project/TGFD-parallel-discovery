package Discovery;

import ICs.TGFD;
import Infra.*;
import org.jgrapht.Graph;

import java.util.ArrayList;

public class Job {

    private int id;
    private int diameter;
    private DataVertex centerNode; //a ->b //10,000 joblets -> 10,000 instances of a// 50 of them had a match
    private int fragmentID;        //a->b and b->c -> start off with 50 joblets, then extract the subgraph within diameter 2 and then do the matching
    private ArrayList<RelationshipEdge> edges;
    private VF2DataGraph subgraph;
    private PatternTreeNode patternTreeNode;

    public Job(int id, DataVertex centerNode, int diameter, int fragmentID, PatternTreeNode patternTreeNode)
    {
        this.id=id;
        this.diameter=diameter;
        this.centerNode=centerNode;
        this.fragmentID=fragmentID;
        this.patternTreeNode=patternTreeNode;
    }

    public void setEdges(ArrayList<RelationshipEdge> edges) {
        this.edges = edges;
        edges.forEach(edge -> {
            edge.getSource().addJobletID(id);
            edge.getTarget().addJobletID(id);
        });
    }

    public void setSubgraph(Graph<Vertex, RelationshipEdge> inducedGraph) {
        this.subgraph=new VF2DataGraph(inducedGraph);
        this.subgraph.getGraph().vertexSet().forEach(vertex -> vertex.addJobletID(id));
    }

    public VF2DataGraph getSubgraph() {
        return subgraph;
    }

    public ArrayList<RelationshipEdge> getEdges() {
        return edges;
    }

    public int getDiameter() {
        return diameter;
    }

    public DataVertex getCenterNode() {
        return centerNode;
    }

    public int getId() {
        return id;
    }

    public int getFragmentID() {
        return fragmentID;
    }

    public PatternTreeNode getPatternTreeNode() {
        return patternTreeNode;
    }
}
