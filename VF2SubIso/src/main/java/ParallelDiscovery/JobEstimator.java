package ParallelDiscovery;

import ChangeExploration.*;
import Discovery.Job;
import Infra.*;
import Loader.GraphLoader;
import Partitioner.RangeBasedPartitioner;
import SharedStorage.HDFSStorage;
import SharedStorage.S3Storage;
import Util.Config;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Function;
import java.util.stream.IntStream;

public class JobEstimator {

    private GraphLoader loader;
    private HashMap<DataVertex,Integer> fragments;
    private HashMap<String,Integer> fragmentsByVertexURI;
    private HashMap<DataVertex, HashSet<Integer>> copiedVertices;
    private HashMap<Integer, ArrayList<Job>> jobsByFragmentID;
    private HashMap<Integer, Job> jobsByID;
    private HashMap<PatternTreeNode, HashSet<String>> alreadyDefinedJobsForVertices = new HashMap<>();
    private int numberOfProcessors;
    private int diameter;

    public JobEstimator(GraphLoader loader, int numberOfProcessors, HashMap<String,Integer> fragmentsByVertexURI, int fixedDiameter)
    {
        this.fragmentsByVertexURI = fragmentsByVertexURI;
        this.loader = loader;
        this.fragments=new HashMap<>();
        for (Vertex v:loader.getGraph().getGraph().vertexSet()) {
            DataVertex dataVertex = (DataVertex) v;
            if(fragmentsByVertexURI.containsKey(dataVertex.getVertexURI()))
                fragments.put(dataVertex,fragmentsByVertexURI.get(dataVertex.getVertexURI()));
            else
            {
                fragments.put(dataVertex,0);
                this.fragmentsByVertexURI.put(dataVertex.getVertexURI(),0);
            }
        }
        this.numberOfProcessors=numberOfProcessors;
        copiedVertices=new HashMap<>();
        this.diameter = fixedDiameter;
    }

    public JobEstimator(GraphLoader loader, int numberOfProcessors, int fixedDiameter)
    {
        fragmentsByVertexURI = new HashMap<>();
        this.loader = loader;
        this.numberOfProcessors=numberOfProcessors;
        RangeBasedPartitioner partitioner=new RangeBasedPartitioner(loader.getGraph());
        this.fragments=partitioner.fragment(numberOfProcessors);
        fragments.keySet()
                .forEach(v -> fragmentsByVertexURI
                        .put(v.getVertexURI(), fragments.get(v)));
        copiedVertices=new HashMap<>();
        this.diameter = fixedDiameter;
    }

    public void defineJobs(Set<PatternTreeNode> singlePatternTreeNodes)
    {
        jobsByID=new HashMap<>();
        jobsByFragmentID= new HashMap<>();
        int jobID=0;
        IntStream.range(0, numberOfProcessors)
                .forEach(i -> jobsByFragmentID.put(i, new ArrayList<>()));

        for (PatternTreeNode ptn:singlePatternTreeNodes) {
            alreadyDefinedJobsForVertices.put(ptn,new HashSet<>());
            System.out.println("PatternTreeNode with the center type: " + ptn.getPattern().getCenterVertexType());
            String centerNodeType=ptn.getPattern().getCenterVertexType();
            for (Vertex v: loader.getGraph().getGraph().vertexSet()) {
                if(v.getTypes().contains(centerNodeType))
                {
                    jobID++;
                    DataVertex dataVertex=(DataVertex) v;
                    Job job=new Job(jobID,dataVertex,diameter,fragments.get(dataVertex),ptn);
                    ArrayList<RelationshipEdge> edges = loader.getGraph().getEdgesWithinDiameter(dataVertex, diameter);
                    job.setEdges(edges);
                    jobsByID.put(jobID,job);
                    jobsByFragmentID.get(fragments.get(dataVertex)).add(job);
                    alreadyDefinedJobsForVertices.get(ptn).add(dataVertex.getVertexURI());
                    if(jobID%100==0)
                        System.out.println("Jobs so far: " + jobID + "  **  " + LocalDateTime.now());
                }
            }
        }
    }

    public void defineNewJobs(List<PatternTreeNode> singlePatternTreeNodes, HashMap<PatternTreeNode, HashSet<String>> previouslyDefinedJobsForVertices)
    {
        jobsByID=new HashMap<>();
        jobsByFragmentID= new HashMap<>();
        int jobID=0;
        IntStream.range(0, numberOfProcessors)
                .forEach(i -> jobsByFragmentID.put(i, new ArrayList<>()));

        for (PatternTreeNode ptn:singlePatternTreeNodes) {
            alreadyDefinedJobsForVertices.put(ptn,new HashSet<>());
            System.out.println("PatternTreeNode with the center type: " + ptn.getPattern().getCenterVertexType());
            String centerNodeType=ptn.getPattern().getCenterVertexType();
            for (Vertex v: loader.getGraph().getGraph().vertexSet()) {
                if(v.getTypes().contains(centerNodeType))
                {
                    DataVertex dataVertex=(DataVertex) v;
                    if(previouslyDefinedJobsForVertices.containsKey(ptn))
                        if(previouslyDefinedJobsForVertices.get(ptn).contains(dataVertex.getVertexURI()))
                            continue;
                    jobID++;
                    Job job=new Job(jobID,dataVertex,diameter,fragments.get(dataVertex),ptn);
                    ArrayList<RelationshipEdge> edges = loader.getGraph().getEdgesWithinDiameter(dataVertex, diameter);
                    job.setEdges(edges);
                    jobsByID.put(jobID,job);
                    jobsByFragmentID.get(fragments.get(dataVertex)).add(job);
                    alreadyDefinedJobsForVertices.get(ptn).add(dataVertex.getVertexURI());
                    if(jobID%100==0)
                        System.out.println("Jobs so far: " + jobID + "  **  " + LocalDateTime.now());
                }
            }
        }
    }

    public void partitionWorkload()
    {
        JobPartitioner partitioner=new JobPartitioner(this);
        this.jobsByFragmentID =  partitioner.partition();
    }

    public int communicationCost()
    {
        System.out.println("Computing the data that needs to be shipped");
        int count=0;
        for (int fragment:jobsByFragmentID.keySet()) {
            count += jobsByFragmentID
                    .get(fragment)
                    .stream()
                    .flatMap(job -> job
                            .getEdges()
                            .stream())
                    .filter(edge -> fragments.get((DataVertex) edge.getTarget()) != fragment || fragments.get((DataVertex) edge.getSource()) != fragment)
                    .count();
        }
        return count;
    }

    public HashMap<Integer,HashMap<Integer,ArrayList<SimpleEdge>>> dataToBeShipped()
    {
        HashMap<Integer,HashMap<Integer,ArrayList<SimpleEdge>>> dataToBeShipped=new HashMap<>();
        for(int i:jobsByFragmentID.keySet())
        {
            dataToBeShipped.put(i,new HashMap<>());
            for(int j:jobsByFragmentID.keySet())
                dataToBeShipped.get(i).put(j,new ArrayList<>());
        }
        for (int fragmentID:jobsByFragmentID.keySet()) {
            for (Job job :jobsByFragmentID.get(fragmentID)) {
                for (RelationshipEdge edge:job.getEdges()) {
                    DataVertex srcVertex=(DataVertex) edge.getSource();
                    DataVertex dstVertex=(DataVertex) edge.getTarget();
                    if(fragments.get(srcVertex)!=fragmentID)
                    {
                        if(fragments.get(srcVertex).equals(fragments.get(dstVertex)))
                        {
                            dataToBeShipped.get(fragments.get(srcVertex))
                                    .get(fragmentID)
                                    .add(new SimpleEdge(edge));
                        }
                        else if(fragments.get(dstVertex)!=fragmentID)
                        {
                            dataToBeShipped.get(fragments.get(dstVertex))
                                    .get(fragmentID)
                                    .add(new SimpleEdge(edge));
                        }
                    }
                    else if(fragments.get(dstVertex)!=fragmentID)
                    {
                        dataToBeShipped.get(fragments.get(dstVertex))
                                .get(fragmentID)
                                .add(new SimpleEdge(edge));
                    }
                }
            }
        }
        return dataToBeShipped;
    }

    public HashMap<Integer,HashMap<Integer,ArrayList<SimpleEdge>>> dataToBeShipped(List<Change> changes)
    {
        HashMap<Integer,HashMap<Integer,ArrayList<SimpleEdge>>> dataToBeShipped=new HashMap<>();
        for(int i:jobsByFragmentID.keySet())
        {
            dataToBeShipped.put(i,new HashMap<>());
            for(int j:jobsByFragmentID.keySet())
                dataToBeShipped.get(i).put(j,new ArrayList<>());
        }
        for (Change change:changes) {
            if(change instanceof EdgeChange)
            {
                EdgeChange edgeChange=(EdgeChange) change;
                if(edgeChange.getTypeOfChange()== ChangeType.insertEdge)
                {
                    DataVertex src= (DataVertex) loader.getGraph().getNode(edgeChange.getSrc());
                    DataVertex dst= (DataVertex) loader.getGraph().getNode(edgeChange.getSrc());
                    if(!fragments.get(src).equals(fragments.get(dst)))
                    {
                        if(!src.getJobletID().isEmpty())
                        {
                            dataToBeShipped.get(fragments.get(dst))
                                    .get(fragments.get(src))
                                    .add(new SimpleEdge(src.getVertexURI(),dst.getVertexURI(),edgeChange.getLabel()));
                            if(!dst.getJobletID().isEmpty())
                            {
                                if(!copiedVertices.containsKey(dst))
                                    copiedVertices.put(dst,new HashSet<>());
                                copiedVertices.get(dst).add(fragments.get(src));
                                change.addJobletID(dst.getJobletID());
                            }
                        }
                        if(!dst.getJobletID().isEmpty())
                        {
                            dataToBeShipped.get(fragments.get(src))
                                    .get(fragments.get(dst))
                                    .add(new SimpleEdge(src.getVertexURI(),dst.getVertexURI(),edgeChange.getLabel()));
                            if(!src.getJobletID().isEmpty())
                            {
                                if(!copiedVertices.containsKey(src))
                                    copiedVertices.put(src,new HashSet<>());
                                copiedVertices.get(src).add(fragments.get(dst));
                                change.addJobletID(src.getJobletID());
                            }
                        }
                    }
                }
            }
        }
        return dataToBeShipped;
    }

    public HashMap<Integer,List<Change>> changesToBeSent(List<Change> changes)
    {
        HashMap<Integer,List<Change>> changesByFragmentID=new HashMap<>();

        for(int i:jobsByFragmentID.keySet())
            changesByFragmentID.put(i,new ArrayList<>());

        for (Change change:changes) {
            if (change instanceof EdgeChange) {
                EdgeChange edgeChange = (EdgeChange) change;
                DataVertex src = (DataVertex) loader.getGraph().getNode(edgeChange.getSrc());
                DataVertex dst = (DataVertex) loader.getGraph().getNode(edgeChange.getSrc());
                if (!src.getJobletID().isEmpty())
                {
                    change.addJobletID(src.getJobletID());
                    changesByFragmentID.get(fragments.get(src)).add(change);
                }
                if (!dst.getJobletID().isEmpty())
                {
                    change.addJobletID(dst.getJobletID());
                    changesByFragmentID.get(fragments.get(dst)).add(change);
                }
                if(copiedVertices.containsKey(src))
                {
                    copiedVertices
                            .get(src)
                            .stream()
                            .mapToInt(f -> f)
                            .filter(f -> fragments.get(src) != f)
                            .forEach(f -> changesByFragmentID.get(f).add(change));
                }
                if(copiedVertices.containsKey(dst))
                {
                    copiedVertices
                            .get(dst)
                            .stream()
                            .mapToInt(f -> f)
                            .filter(f -> fragments.get(dst) != f)
                            .forEach(f -> changesByFragmentID.get(f).add(change));
                }
            }
            else if(change instanceof AttributeChange)
            {
                AttributeChange attributeChange = (AttributeChange) change;
                DataVertex vertex = (DataVertex) loader.getGraph().getNode(attributeChange.getUri());
                if (!vertex.getJobletID().isEmpty())
                {
                    change.addJobletID(vertex.getJobletID());
                    changesByFragmentID.get(fragments.get(vertex)).add(change);
                }
                if(copiedVertices.containsKey(vertex))
                {
                    copiedVertices
                            .get(vertex)
                            .stream()
                            .mapToInt(f -> f)
                            .filter(f -> fragments.get(vertex) != f)
                            .forEach(f -> {
                                changesByFragmentID.get(f).add(change);
                            } );
                }
            }
            else if(change instanceof VertexChange)
            {
                VertexChange vertexChange = (VertexChange) change;
                if(vertexChange.getTypeOfChange()==ChangeType.deleteVertex) {
                    if (!vertexChange.getVertex().getJobletID().isEmpty())
                    {
                        change.addJobletID(vertexChange.getVertex().getJobletID());
                        changesByFragmentID.get(fragments.get(vertexChange.getVertex())).add(change);
                    }
                    if(copiedVertices.containsKey(vertexChange.getVertex()))
                    {
                        copiedVertices
                                .get(vertexChange.getVertex())
                                .stream()
                                .mapToInt(f -> f)
                                .filter(f -> fragments.get(vertexChange.getVertex()) != f)
                                .forEach(f -> changesByFragmentID.get(f).add(change));
                    }
                }

            }
        }
        return changesByFragmentID;
    }

    public HashMap<Integer, String> sendChangesToWorkers(HashMap<Integer,List<Change>> changes, int snapshotID)
    {
        HashMap<Integer, String> listOfFiles=new HashMap<>();
        LocalDateTime now = LocalDateTime.now();
        String date=now.getHour() + "_" + now.getMinute() + "_" + now.getSecond();

        for (int id:changes.keySet()) {
            if(Config.sharedStorage == Config.SharedStorage.S3)
                S3Storage.upload(Config.S3BucketName, date + "_Change[" + snapshotID + "]_" + id + ".ser", changes.get(id));
            else if (Config.sharedStorage == Config.SharedStorage.HDFS)
                HDFSStorage.upload(Config.HDFSDirectory, date + "_Change[" + snapshotID + "]_" + id + ".ser", changes.get(id),true);

            listOfFiles.put(id, date + "_Change[" + snapshotID + "]_" + id + ".ser");
        }
        return listOfFiles;
    }

    public HashMap<Integer, ArrayList<String>> sendEdgesToWorkersForShipment(HashMap<Integer, HashMap<Integer,ArrayList<SimpleEdge>>> dataToBeShipped)
    {
        HashMap<Integer, ArrayList<String>> listOfFiles=new HashMap<>();
        LocalDateTime now = LocalDateTime.now();
        String date=now.getHour() + "_" + now.getMinute() + "_" + now.getSecond();

        for (int id:dataToBeShipped.keySet()) {
            StringBuilder sb = new StringBuilder();
            listOfFiles.put(id,new ArrayList<>());
            for (int key : dataToBeShipped.get(id).keySet()) {
                if (key != id) {
                    sb.append(key).append("\n");
                    for (SimpleEdge edge : dataToBeShipped.get(id).get(key))
                        sb.append(edge.getSrc()).append("\t").append(edge.getDst()).append("\n");
                    if(Config.sharedStorage == Config.SharedStorage.S3)
                        S3Storage.upload(Config.S3BucketName,date + "_F" + id + "_to_" +key + ".txt",sb.toString());
                    else if (Config.sharedStorage == Config.SharedStorage.HDFS)
                        HDFSStorage.upload(Config.HDFSDirectory,date + "_F" + id + "_to_" +key + ".txt",sb.toString());
                    listOfFiles.get(id).add(date + "_F" + id + "_to_" +key + ".txt");
                    //saveEdges("./Fragment" + id + "_to_" +key + ".txt", sb);
                }
            }
        }
        return listOfFiles;
    }

    public double computeJobsSize(int fragmentID)
    {
        return jobsByFragmentID
                .get(fragmentID)
                .stream()
                .mapToDouble(Job::getSize)
                .sum();
    }

    public double computeTotalSize()
    {
        return jobsByFragmentID
                .keySet()
                .stream()
                .mapToInt(fragmentID -> fragmentID)
                .mapToObj(fragmentID -> jobsByFragmentID
                        .get(fragmentID)
                        .stream())
                .flatMap(Function.identity())
                .mapToDouble(Job::getSize)
                .sum();
    }

    public HashMap<Integer, ArrayList<Job>> getJobsByFragmentID() {
        return jobsByFragmentID;
    }

    public GraphLoader getLoader() {
        return loader;
    }

    public HashMap<DataVertex, Integer> getFragments() {
        return fragments;
    }

    public HashMap<PatternTreeNode, HashSet<String>> getAlreadyDefinedJobsForVertices() {
        return alreadyDefinedJobsForVertices;
    }

    public void setAlreadyDefinedJobsForVertices(HashMap<PatternTreeNode, HashSet<String>> alreadyDefinedJobsForVertices) {
        this.alreadyDefinedJobsForVertices = alreadyDefinedJobsForVertices;
    }

    public int getNumberOfProcessors() {
        return numberOfProcessors;
    }

    public void setJobsByFragmentID(HashMap<Integer, ArrayList<Job>> jobsByFragmentID) {
        this.jobsByFragmentID = jobsByFragmentID;
    }

    public HashMap<String, Integer> getFragmentsByVertexURI() {
        return fragmentsByVertexURI;
    }

    private void saveEdges(String path, StringBuilder stringBuilder)
    {
        try {
            FileWriter file = new FileWriter(path);
            file.write(stringBuilder.toString());
            file.close();
            System.out.println("Successfully wrote to the file: " + path);
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }
}
