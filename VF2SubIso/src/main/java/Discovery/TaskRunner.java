package Discovery;

import ChangeExploration.Change;
import ICs.TGFD;
import IncrementalRunner.IncUpdates;
import IncrementalRunner.IncrementalChange;
import Infra.*;
import Loader.*;
import Util.Config;
import VF2BasedWorkload.Joblet;
import VF2Runner.VF2SubgraphIsomorphism;
import org.jgrapht.Graph;
import org.jgrapht.GraphMapping;
import org.jgrapht.alg.isomorphism.VF2AbstractIsomorphismInspector;

import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class TaskRunner {

    private GraphLoader loader=null;
    private long wallClockTime=0;
    private HashMap<Integer, Job> assignedJobs;
    private String jobsInRawString;

    private List<Set<Set<ConstantLiteral>>> matchesPerTimestamps = new ArrayList<>();


//    private HashMap <String, MatchCollection> matchCollectionHashMap;

    public TaskRunner()
    {
        System.out.println("Incremental algorithm for the "+ Config.datasetName +" dataset from taskRunner");
        assignedJobs=new HashMap<>();
    }

    public void load()
    {
        long startTime=System.currentTimeMillis();

        //Load the first timestamp
        System.out.println("===========Snapshot 1 (" + Config.getTimestamps().get(1) + ")===========");

        if(Config.datasetName== Config.dataset.dbpedia)
            loader = new DBPediaLoader(new ArrayList<>(), Config.getFirstTypesFilePath(), Config.getFirstDataFilePath());
        else if(Config.datasetName == Config.dataset.synthetic)
            loader = new SyntheticLoader(new ArrayList<>(), Config.getFirstDataFilePath());
        else if(Config.datasetName == Config.dataset.pdd)
            loader = new PDDLoader(new ArrayList<>(), Config.getFirstDataFilePath());
        else if(Config.datasetName == Config.dataset.imdb) // default is imdb
            loader = new IMDBLoader(new ArrayList<>(), Config.getFirstDataFilePath());

        printWithTime("Load graph 1 (" + Config.getTimestamps().get(1) + ")", System.currentTimeMillis()-startTime);
        wallClockTime+=System.currentTimeMillis()-startTime;
    }

    public void setJobsInRawString(String jobsInRawString) {
        this.jobsInRawString = jobsInRawString;
    }

    public void generateJobs(List<PatternTreeNode> singlePatternTreeNodes)
    {
        if(jobsInRawString!=null)
        {
            String []temp=jobsInRawString.split("\n");
            for (int i=1;i<temp.length;i++)
            {
                String []arr=temp[i].split("#");
                if(arr.length==3)
                {
                    Job job=new Job(Integer.parseInt(arr[0]),(DataVertex) loader.getGraph().getNode(arr[1]),Integer.valueOf(arr[2]),0, singlePatternTreeNodes.get(0));
                    assignedJobs.put(job.getId(), job);
                }
            }
        }
    }

    public void runTheFirstSnapshot(List<PatternTreeNode> singlePatternTreeNodes)
    {
        if(loader==null)
        {
            System.out.println("Graph is not loaded yet");
            return;
        }
        StringBuilder msg=new StringBuilder();

        long startTime, functionWallClockTime=System.currentTimeMillis();
        LocalDate currentSnapshotDate= Config.getTimestamps().get(1);

        //Create the match collection for all the TGFDs in the list
//        matchCollectionHashMap=new HashMap <>();
//        for (PatternTreeNode ptn:singlePatternTreeNodes) {
//            matchCollectionHashMap.put(ptn.getName(),new MatchCollection(ptn.getPattern(),ptn.getDependency(),tgfd.getDelta().getGranularity()));
//        }

        // Now, we need to find the matches for the first snapshot.
        System.out.println("Retrieving matches for all the joblets.");
        VF2SubgraphIsomorphism VF2 = new VF2SubgraphIsomorphism();

        Map<String, List<Integer>> entityURIs = new HashMap<>();

        startTime=System.currentTimeMillis();
        for (Job job:assignedJobs.values()) {
            Graph<Vertex, RelationshipEdge> subgraph = loader.getGraph().getSubGraphWithinDiameter(job.getCenterNode(), job.getDiameter(),new TGFD()); // Fix
            job.setSubgraph(subgraph);
            ArrayList<HashSet<ConstantLiteral>> matches = new ArrayList<>();
            int numOfMatchesInTimestamp = 0;
            VF2AbstractIsomorphismInspector<Vertex, RelationshipEdge> results = new VF2SubgraphIsomorphism().execute2(subgraph, job.getPatternTreeNode().getPattern(), false);
            if (results.isomorphismExists()) {
                numOfMatchesInTimestamp = Match.extractMatches(results.getMappings(), matches, job.getPatternTreeNode(), entityURIs, 2016);
            }
            matchesPerTimestamps.get(2016).addAll(matches);
        }
        printWithTime("Match retrieval", System.currentTimeMillis()-startTime);
    }

    public void runTheNextTimestamp(List<Change> changes, int superstep)
    {
        //Load the change files
        System.out.println("===========Snapshot "+superstep+" (" + Config.getTimestamps().get(superstep) + ")===========");

        long startTime=System.currentTimeMillis();
        LocalDate currentSnapshotDate= Config.getTimestamps().get(superstep);
        System.out.println("Total number of changes: " + changes.size());

        System.out.println("Updating the graph");

        HashMap<String, ArrayList <String>> newMatchesSignaturesByTGFD=new HashMap <>();
        HashMap<String,ArrayList<String>> removedMatchesSignaturesByTGFD=new HashMap <>();
        HashMap<String,TGFD> tgfdsByName=new HashMap <>();
//        for (TGFD tgfd:tgfds) {
//            newMatchesSignaturesByTGFD.put(tgfd.getName(), new ArrayList <>());
//            removedMatchesSignaturesByTGFD.put(tgfd.getName(), new ArrayList <>());
//            tgfdsByName.put(tgfd.getName(),tgfd);
//        }
//
//        for (Change change:changes) {
//            for (int jobletID:change.getJobletIDs()) {
//                if(assignedJoblets.containsKey(jobletID))
//                {
//                    IncUpdates incUpdatesOnDBpedia=new IncUpdates(assignedJoblets.get(jobletID).getSubgraph(),tgfds);
//                    HashMap<String, IncrementalChange> incrementalChangeHashMap=incUpdatesOnDBpedia.updateGraph(change,tgfdsByName);
//                    if(incrementalChangeHashMap==null)
//                        continue;
//                    for (String tgfdName:incrementalChangeHashMap.keySet()) {
//                        newMatchesSignaturesByTGFD.get(tgfdName).addAll(incrementalChangeHashMap.get(tgfdName).getNewMatches().keySet());
//                        removedMatchesSignaturesByTGFD.get(tgfdName).addAll(incrementalChangeHashMap.get(tgfdName).getRemovedMatchesSignatures());
//                        matchCollectionHashMap.get(tgfdName).addMatches(currentSnapshotDate,incrementalChangeHashMap.get(tgfdName).getNewMatches());
//                    }
//                }
//            }
//        }
//        for (TGFD tgfd:tgfds) {
//            matchCollectionHashMap.get(tgfd.getName())
//                    .addTimestamp(currentSnapshotDate, newMatchesSignaturesByTGFD.get(tgfd.getName()),removedMatchesSignaturesByTGFD.get(tgfd.getName()));
//
//            System.out.println("New matches ("+tgfd.getName()+"): " +
//                    newMatchesSignaturesByTGFD.get(tgfd.getName()).size() + " ** " + removedMatchesSignaturesByTGFD.get(tgfd.getName()).size());
//        }
        printWithTime("Update and retrieve matches ", System.currentTimeMillis()-startTime);
    }

    public GraphLoader getLoader() {
        return loader;
    }


    private void printWithTime(String message, long runTimeInMS)
    {
        System.out.println(message + " time: " + runTimeInMS + "(ms) ** " +
                TimeUnit.MILLISECONDS.toSeconds(runTimeInMS) + "(sec) ** " +
                TimeUnit.MILLISECONDS.toMinutes(runTimeInMS) +  "(min)");
    }

}
