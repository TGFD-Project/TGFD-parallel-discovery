package Discovery;

import ICs.TGFD;
import Infra.*;
import Loader.*;
import Util.Config;
import VF2Runner.VF2SubgraphIsomorphism;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.jgrapht.Graph;
import org.jgrapht.alg.isomorphism.VF2AbstractIsomorphismInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TaskRunner {

    private static final Logger logger = LoggerFactory.getLogger(TaskRunner.class);
    private GraphLoader []loaders;
    private long wallClockTime=0;
    private HashMap<Integer, HashMap<Integer, Job>> assignedJobsBySnapshot;
    private String jobsInRawString;

    private HashMap<PatternTreeNode, List<Set<Set<ConstantLiteral>>>> matchesPerTimestampsByPTN = new HashMap<>();
    private HashMap<PatternTreeNode, Map<String, List<Integer>>> entityURIsByPTN = new HashMap<>();

    private VSpawn vSpawn;

    private int jobIDForNewJobs = 9000000;



//    private HashMap <String, MatchCollection> matchCollectionHashMap;

    public TaskRunner(int numberOfSnapshots, String[] args)
    {
        System.out.println("Task Runner for the "+ Config.datasetName +" dataset");
        assignedJobsBySnapshot=new HashMap<>();
        loaders = new GraphLoader[numberOfSnapshots];
        IntStream.range(0, numberOfSnapshots).forEach(i -> loaders[i] = null);
        IntStream.range(0, numberOfSnapshots).forEach(i -> assignedJobsBySnapshot.put(i, new HashMap<>()));
        vSpawn = new VSpawn();
        Util.config(args);
    }

    public void load(int snapShotID)
    {
        int snapShotIndex = snapShotID-1;
        if(loaders[snapShotIndex] == null)
        {
            long startTime=System.currentTimeMillis();

            //Load the first timestamp
            System.out.println("===========Snapshot "+snapShotID+" (" + Config.getTimestamps().get(snapShotID) + ")===========");

            Map.Entry<String, List<String>> timestampToPathEntry = Util.timestampToFilesMap.get(snapShotIndex);
            Model dataModel = ModelFactory.createDefaultModel();
            for (String path : timestampToPathEntry.getValue()) {
                if (!path.toLowerCase().endsWith(".ttl") && !path.toLowerCase().endsWith(".nt"))
                    continue;
                if (path.toLowerCase().contains("types"))
                    continue;
                Path input = Paths.get(path);
                System.out.println("Reading data graph: " + path);
                dataModel.read(input.toUri().toString());
            }

            if(Config.datasetName== Config.dataset.dbpedia)
                // TODO: needs to be fixed
                loaders[snapShotIndex] = new DBPediaLoader(new ArrayList<>(), Collections.singletonList(dataModel), Collections.singletonList(dataModel));
            else if(Config.datasetName == Config.dataset.synthetic)
                loaders[snapShotIndex] = new SyntheticLoader(new ArrayList<>(), Config.getFirstDataFilePath());
            else if(Config.datasetName == Config.dataset.pdd)
                loaders[snapShotIndex] = new PDDLoader(new ArrayList<>(), Config.getFirstDataFilePath());
            else if(Config.datasetName == Config.dataset.imdb) // default is imdb
                loaders[snapShotIndex] = new IMDBLoader(new ArrayList<>(), Config.getFirstDataFilePath());

            printWithTime("Load graph "+snapShotID+" (" + Config.getTimestamps().get(snapShotID) + ")", System.currentTimeMillis()-startTime);
            wallClockTime+=System.currentTimeMillis()-startTime;
        }
        else
        {
            System.out.println("Snapshot "+snapShotID+" is already loaded.");
        }
    }

    public void setJobsInRawString(String jobsInRawString) {
        this.jobsInRawString = jobsInRawString;
    }

    public void generateJobs(List<PatternTreeNode> patternTreeNodes)
    {
        matchesPerTimestampsByPTN = new HashMap<>();
        for (PatternTreeNode ptn:patternTreeNodes) {
            matchesPerTimestampsByPTN.put(ptn,new ArrayList<>());
            for (int timestamp = 0; timestamp < Util.numOfSnapshots; timestamp++) {
                matchesPerTimestampsByPTN.get(ptn).add(new HashSet<>());
            }
            entityURIsByPTN.put(ptn, new HashMap<>());
        }

        HashMap<String, PatternTreeNode> typeToPattern = patternTreeNodes
                .stream()
                .collect(Collectors
                        .toMap(ptn -> ptn.getGraph().vertexSet().iterator().next().getTypes().iterator().next(),
                                ptn -> ptn,
                                (a, b) -> b,
                                HashMap::new));
        if(jobsInRawString!=null)
        {
            String []temp=jobsInRawString.split("\n");
            for (int i=1;i<temp.length;i++)
            {
                String []arr=temp[i].split("#");
                if(arr.length==5)
                {
                    // A job is in the form of the following
                    // id # CenterNodeVertexID # diameter # FragmentID # Type
                    // TODO: Some of Jobs' pattern are null
                    Job job=new Job(Integer.parseInt(arr[0]),(DataVertex) loaders[0].getGraph().getNode(arr[1]),Integer.valueOf(arr[2]),0, typeToPattern.getOrDefault(arr[4], null));
                    assignedJobsBySnapshot.get(0).put(job.getId(), job);
                }
            }
        }
    }

    public void appendNewJobs(List<PatternTreeNode> patternTreeNodes, String newJobsInRawString, int superStep)
    {
        int superStepIndex = superStep-1;
        HashMap<String, PatternTreeNode> typeToPattern = patternTreeNodes
                .stream()
                .collect(Collectors
                        .toMap(ptn -> ptn.getGraph().vertexSet().iterator().next().getTypes().iterator().next(),
                                ptn -> ptn,
                                (a, b) -> b,
                                HashMap::new));
        if(newJobsInRawString!=null)
        {
            String []temp=newJobsInRawString.split("\n");
            for (int i=1;i<temp.length;i++)
            {
                String []arr=temp[i].split("#");
                if(arr.length==4)
                {
                    // A job is in the form of the following
                    // id # CenterNodeVertexID # diameter # FragmentID # Type
                    Job job=new Job(Integer.parseInt(arr[0]),(DataVertex) loaders[superStepIndex].getGraph().getNode(arr[1]),Integer.valueOf(arr[2]),0, typeToPattern.get(arr[3]));
                    assignedJobsBySnapshot.get(superStepIndex).put(job.getId(), job);
                }
            }
        }
    }

    public void vSpawn()
    {
        while (Util.currentVSpawnLevel <= Util.k) {

            PatternTreeNode newPattern = null;
            VSpawn.VSpawnedPatterns vSpawnedPatterns = null;
            while (newPattern == null && Util.currentVSpawnLevel <= Util.k) {
                vSpawnedPatterns = vSpawn.perform(false);
                newPattern = vSpawnedPatterns.getNewPattern();
            }

            if (Util.currentVSpawnLevel > Util.k)
                break;

            if (vSpawnedPatterns.getNewPattern() == null)
                throw new NullPointerException("patternTreeNode == null");

            matchesPerTimestampsByPTN.put(newPattern,new ArrayList<>());
            for (int timestamp = 0; timestamp < Util.numOfSnapshots; timestamp++) {
                matchesPerTimestampsByPTN.get(newPattern).add(new HashSet<>());
            }
            entityURIsByPTN.put(newPattern, new HashMap<>());

            HashMap<Integer, ArrayList<Job>> newJobsList = new HashMap<>();

            for (int index : assignedJobsBySnapshot.keySet()) {
                newJobsList.put(index, new ArrayList<>());
                for (Job job : assignedJobsBySnapshot.get(index).values()) {
                    if(job.getPatternTreeNode().equals(vSpawnedPatterns.getOldPattern()))
                    {
                        Job newJob=new Job(jobIDForNewJobs++,job.getCenterNode(),1,0, newPattern);
                        assignedJobsBySnapshot.get(index).put(newJob.getId(), newJob);
                        newJobsList.get(index).add(newJob);
                    }
                }
            }

            long matchingTime = System.currentTimeMillis();
            for (int superstep =1; superstep<=Config.supersteps;superstep++)
            {
                runSnapshot(superstep, newJobsList);
            }

            matchingTime = System.currentTimeMillis() - matchingTime;
            Util.printWithTime("Pattern matching", (matchingTime));
            Util.addToTotalMatchingTime(matchingTime);

            calculateSupport(newPattern);

            if (Util.doesNotSatisfyTheta(newPattern)) {
                System.out.println("Mark as pruned. Real pattern support too low for pattern " + newPattern.getPattern());
                if (Util.hasSupportPruning)
                    newPattern.setIsPruned();
                continue;
            }

            if (Util.skipK1 && Util.currentVSpawnLevel == 1)
                continue;

            final long hSpawnStartTime = System.currentTimeMillis();
            HSpawn hspawn = new HSpawn(newPattern, matchesPerTimestampsByPTN.get(newPattern));
            ArrayList<TGFD> tgfds = hspawn.performHSPawn();
            Util.printWithTime("hSpawn", (System.currentTimeMillis() - hSpawnStartTime));
            Util.discoveredTgfds.get(Util.currentVSpawnLevel).addAll(tgfds);
        }
    }

    public void runSnapshot(int snapShotID, HashMap<Integer, ArrayList<Job>> newJobsList)
    {
        int snapShotIndex = snapShotID-1;
        if(loaders[snapShotIndex]==null)
        {
            System.out.println("Graph is not loaded yet");
            return;
        }

        LocalDate currentSnapshotDate= Config.getTimestamps().get(snapShotID);

        System.out.println("Retrieving matches for all the joblets.");
        VF2SubgraphIsomorphism VF2 = new VF2SubgraphIsomorphism();

        long startTime=System.currentTimeMillis();
        for (int index=0; index<=snapShotIndex; index++)
        {
            for (Job job: newJobsList.get(index)) {
                Set<String> validTypes = new HashSet<>();
                for (Vertex v: job.getPatternTreeNode().getGraph().vertexSet()) {
                    validTypes.addAll(v.getTypes());
                }
                Graph<Vertex, RelationshipEdge> subgraph = loaders[snapShotIndex].getGraph().getSubGraphWithinDiameter(job.getCenterNode(), job.getDiameter(),validTypes); // Fix
                job.setSubgraph(subgraph);
                ArrayList<HashSet<ConstantLiteral>> matches = new ArrayList<>();
                int numOfMatchesInTimestamp = 0;
                VF2AbstractIsomorphismInspector<Vertex, RelationshipEdge> results = VF2.execute2(subgraph, job.getPatternTreeNode().getPattern(), false);
                if (results.isomorphismExists()) {
                    numOfMatchesInTimestamp = Match.extractMatches(results.getMappings(), matches, job.getPatternTreeNode(), entityURIsByPTN.get(job.getPatternTreeNode()), snapShotIndex);
                }
                System.out.println("Number of matches found: " + numOfMatchesInTimestamp);
                System.out.println("Number of matches found that contain active attributes: " + matches.size());
                matchesPerTimestampsByPTN.get(job.getPatternTreeNode()).get(snapShotIndex).addAll(matches);
            }
        }

        final long matchingEndTime = System.currentTimeMillis() - startTime;
        Util.printWithTime("matchingTime for Snapshot "+snapShotID, matchingEndTime);
        Util.addToTotalMatchingTime(matchingEndTime);
    }

    public void runSnapshot(int snapShotID)
    {
        int snapShotIndex = snapShotID-1;
        if(loaders[snapShotIndex]==null)
        {
            System.out.println("Graph is not loaded yet");
            return;
        }

        LocalDate currentSnapshotDate= Config.getTimestamps().get(snapShotID);

        System.out.println("Retrieving matches for all the joblets.");
        VF2SubgraphIsomorphism VF2 = new VF2SubgraphIsomorphism();

        long startTime=System.currentTimeMillis();
        for (int index=0; index<=snapShotIndex; index++)
        {
            // TODO: Some of jobs' attribute are null
            for (Job job: assignedJobsBySnapshot.get(index).values()) {
                Set<String> validTypes = new HashSet<>();
                PatternTreeNode JobPatternTreeNode = job.getPatternTreeNode();
                DataVertex centerNode = job.getCenterNode();
                if (JobPatternTreeNode == null || centerNode == null) {
                    continue;
                }
                for (Vertex v: job.getPatternTreeNode().getGraph().vertexSet()) {
                    validTypes.addAll(v.getTypes());
                }
                Graph<Vertex, RelationshipEdge> subgraph = loaders[snapShotIndex].getGraph().getSubGraphWithinDiameter(centerNode, job.getDiameter(),validTypes); // Fix
                job.setSubgraph(subgraph);
                ArrayList<HashSet<ConstantLiteral>> matches = new ArrayList<>();
                int numOfMatchesInTimestamp = 0;
                VF2AbstractIsomorphismInspector<Vertex, RelationshipEdge> results = VF2.execute2(subgraph, job.getPatternTreeNode().getPattern(), false);
                if (results.isomorphismExists()) {
                    numOfMatchesInTimestamp = Match.extractMatches(results.getMappings(), matches, job.getPatternTreeNode(), entityURIsByPTN.get(job.getPatternTreeNode()), snapShotIndex);
                }
                System.out.println("Number of matches found: " + numOfMatchesInTimestamp);
                System.out.println("Number of matches found that contain active attributes: " + matches.size());
                matchesPerTimestampsByPTN.get(job.getPatternTreeNode()).get(snapShotIndex).addAll(matches);
            }
        }

        final long matchingEndTime = System.currentTimeMillis() - startTime;
        Util.printWithTime("matchingTime for Snapshot "+snapShotID, matchingEndTime);
        Util.addToTotalMatchingTime(matchingEndTime);
    }

    public void calculateSupport()
    {
        for (PatternTreeNode ptn: matchesPerTimestampsByPTN.keySet()) {
            int numberOfMatchesFound = 0;
            for (Set<Set<ConstantLiteral>> matchesInOneTimestamp : matchesPerTimestampsByPTN.get(ptn)) {
                numberOfMatchesFound += matchesInOneTimestamp.size();
            }
            System.out.println("Total number of matches found across all snapshots:" + numberOfMatchesFound);

            for (Map.Entry<String, List<Integer>> entry: entityURIsByPTN.get(ptn).entrySet()) {
                System.out.println(entry);
            }
            double S = Util.vertexHistogram.get(ptn.getPattern().getCenterVertexType());
            double patternSupport = Util.calculatePatternSupport(entityURIsByPTN.get(ptn), S, Util.T);
            Util.patternSupportsListForThisSnapshot.add(patternSupport);
            ptn.setPatternSupport(patternSupport);
        }
    }

    public void calculateSupport(PatternTreeNode newPattern)
    {
        int numberOfMatchesFound = 0;
        for (Set<Set<ConstantLiteral>> matchesInOneTimestamp : matchesPerTimestampsByPTN.get(newPattern)) {
            numberOfMatchesFound += matchesInOneTimestamp.size();
        }
        System.out.println("Total number of matches found across all snapshots:" + numberOfMatchesFound);

        for (Map.Entry<String, List<Integer>> entry: entityURIsByPTN.get(newPattern).entrySet()) {
            System.out.println(entry);
        }
        double S = Util.vertexHistogram.get(newPattern.getPattern().getCenterVertexType());
        double patternSupport = Util.calculatePatternSupport(entityURIsByPTN.get(newPattern), S, Util.T);
        Util.patternSupportsListForThisSnapshot.add(patternSupport);
        newPattern.setPatternSupport(patternSupport);
    }

    public void vSpawnInit() {
        Util.patternTree = new PatternTree();
        Util.patternTree.addLevel();

        System.out.println("VSpawn Level 0");

        for (PatternTreeNode ptn: matchesPerTimestampsByPTN.keySet()) {
            Util.patternTree.createNodeAtLevel(Util.currentVSpawnLevel, ptn.getPattern());

            if (Util.doesNotSatisfyTheta(ptn))
                ptn.setIsPruned();
            else if (Util.generatek0Tgfds) {
                final long hSpawnStartTime = System.currentTimeMillis();
                HSpawn hspawn = new HSpawn(ptn, matchesPerTimestampsByPTN.get(ptn));
                ArrayList<TGFD> tgfds = hspawn.performHSPawn();
                Util.printWithTime("hSpawn", (System.currentTimeMillis() - hSpawnStartTime));
                Util.discoveredTgfds.get(0).addAll(tgfds);
            }
        }
        System.out.println("GenTree Level " + Util.currentVSpawnLevel + " size: " + Util.patternTree.getLevel(Util.currentVSpawnLevel).size());
        for (PatternTreeNode node : Util.patternTree.getLevel(Util.currentVSpawnLevel)) {
            System.out.println("Pattern: " + node.getPattern());
        }
    }

    public GraphLoader[] getLoaders() {
        return loaders;
    }


    private void printWithTime(String message, long runTimeInMS)
    {
        System.out.println(message + " time: " + runTimeInMS + "(ms) ** " +
                TimeUnit.MILLISECONDS.toSeconds(runTimeInMS) + "(sec) ** " +
                TimeUnit.MILLISECONDS.toMinutes(runTimeInMS) +  "(min)");
    }

}
