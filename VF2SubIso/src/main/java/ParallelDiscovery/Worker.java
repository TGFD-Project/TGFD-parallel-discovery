package ParallelDiscovery;

import Discovery.TGFDDiscovery;
import Discovery.TaskRunner;
import Infra.*;
import MPI.Consumer;
import MPI.Producer;
import Partitioner.Util;
import SharedStorage.HDFSStorage;
import SharedStorage.S3Storage;
import Util.Config;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;

import java.time.LocalDateTime;
import java.util.*;

public class Worker {

    //region --[Fields: Private]---------------------------------------

    private String nodeName = "";
    private TaskRunner runner;
    private String workingBucketName="";
    private HashMap<Integer, ArrayList<SimpleEdge>> dataToBeShipped;
    private List<PatternTreeNode> singlePatternTreeNodes;
    private TGFDDiscovery tgfdDiscovery;

    //endregion

    //region --[Constructor]-----------------------------------------

    public Worker()  {
        tgfdDiscovery = new TGFDDiscovery();
        this.nodeName= Config.nodeName;
//        workingBucketName = Config
//                .getFirstDataFilePath()
//                .get(0)
//                .substring(0, Config.getFirstDataFilePath().get(0).lastIndexOf("/"));
    }

    //endregion

    //region --[Public Methods]-----------------------------------------

    public void start()
    {
        sendStatusToCoordinator();

        receiveSingleNodePatterns();

        receiveHistogramStats();

        runner=new TaskRunner(Config.supersteps);

        this.runFirstSuperstep();

        for (int superstep =2; superstep<=Config.supersteps;superstep++)
        {
            runNextSuperSteps(superstep);
        }

        runner.calculateSupport();

        runner.vSpawnInit();

        runner.vSpawn();

        System.out.println("All Done!");
    }

    //endregion

    //region --[Private Methods]-----------------------------------------

    private void receiveSingleNodePatterns()
    {
        boolean singlePatternTreeNodesRecieved=false;

        Consumer consumer=new Consumer();
        consumer.connect(nodeName);

        while (!singlePatternTreeNodesRecieved)
        {
            String msg=consumer.receive();
            if (msg !=null) {
                if(msg.startsWith("#singlePattern"))
                {
                    String fileName = msg.split("\t")[1];
                    if(Config.sharedStorage == Config.SharedStorage.HDFS)
                        singlePatternTreeNodes = (List<PatternTreeNode>) HDFSStorage.downloadObject(Config.HDFSDirectory,fileName);
                    else if(Config.sharedStorage == Config.SharedStorage.S3)
                        singlePatternTreeNodes = (List<PatternTreeNode>) S3Storage.downloadObject(Config.S3BucketName,fileName);
                    System.out.println("All single PatternTreeNodes have been received.");
                    singlePatternTreeNodesRecieved=true;
                }
            }
            else
                System.out.println("*SINGLE PATTERNTREENODE  RECEIVER*: Error happened - message is null");
        }
        consumer.close();
    }

    private void receiveHistogramStats()
    {
        boolean histogramStatsReceived=false;

        Consumer consumer=new Consumer();
        consumer.connect(nodeName);

        while (!histogramStatsReceived)
        {
            String msg=consumer.receive();
            if (msg !=null) {
                if(msg.startsWith("#histogram"))
                {
                    List<MapEntry> listSortedFrequentEdgesHistogram = new ArrayList<>();
                    List<MapEntry> listSortedVertexHistogram = new ArrayList<>();
                    if(Config.sharedStorage == Config.SharedStorage.HDFS)
                    {
                        Discovery.Util.vertexTypesToAvgInDegreeMap = (Map<String, Double>) HDFSStorage.downloadObject(Config.HDFSDirectory, "vertexTypesToAvgInDegreeMap");
                        Discovery.Util.activeAttributesSet = (Set<String>) HDFSStorage.downloadObject(Config.HDFSDirectory, "activeAttributesSet");
                        Discovery.Util.vertexTypesToActiveAttributesMap = (Map<String, Set<String>>) HDFSStorage.downloadObject(Config.HDFSDirectory, "vertexTypesToActiveAttributesMap");
                        Discovery.Util.vertexHistogram = (Map<String, Integer>) HDFSStorage.downloadObject(Config.HDFSDirectory, "vertexHistogram");
                        Discovery.Util.typeChangeURIs = (Map<String, Set<String>>) HDFSStorage.downloadObject(Config.HDFSDirectory, "typeChangeURIs");
                        listSortedFrequentEdgesHistogram = (List<MapEntry>) HDFSStorage.downloadObject(Config.HDFSDirectory, "sortedFrequentEdgesHistogram");
                        listSortedVertexHistogram = (List<MapEntry>) HDFSStorage.downloadObject(Config.HDFSDirectory, "sortedVertexHistogram");

                    }
                    else if(Config.sharedStorage == Config.SharedStorage.S3)
                    {
                        Discovery.Util.vertexTypesToAvgInDegreeMap = (Map<String, Double>) S3Storage.downloadObject(Config.S3BucketName, "vertexTypesToAvgInDegreeMap");
                        Discovery.Util.activeAttributesSet = (Set<String>) S3Storage.downloadObject(Config.S3BucketName, "activeAttributesSet");
                        Discovery.Util.vertexTypesToActiveAttributesMap = (Map<String, Set<String>>) S3Storage.downloadObject(Config.S3BucketName, "vertexTypesToActiveAttributesMap");
                        Discovery.Util.vertexHistogram = (Map<String, Integer>) S3Storage.downloadObject(Config.S3BucketName, "vertexHistogram");
                        Discovery.Util.typeChangeURIs = (Map<String, Set<String>>) S3Storage.downloadObject(Config.S3BucketName, "typeChangeURIs");
                        listSortedFrequentEdgesHistogram = (List<MapEntry>) S3Storage.downloadObject(Config.S3BucketName, "sortedFrequentEdgesHistogram");
                        listSortedVertexHistogram = (List<MapEntry>) S3Storage.downloadObject(Config.S3BucketName, "sortedVertexHistogram");
                    }
                    Discovery.Util.sortedFrequentEdgesHistogram = new ArrayList<>();
                    Discovery.Util.sortedVertexHistogram = new ArrayList<>();
                    for (MapEntry entry: listSortedFrequentEdgesHistogram) {
                        Discovery.Util.sortedFrequentEdgesHistogram.add(new AbstractMap.SimpleEntry<>(entry.key, entry.value));
                    }
                    for (MapEntry entry: listSortedVertexHistogram) {
                        Discovery.Util.sortedVertexHistogram.add(new AbstractMap.SimpleEntry<>(entry.key, entry.value));
                    }

                    System.out.println("All Stats for the Histogram have been received.");
                    histogramStatsReceived=true;
                }
            }
            else
                System.out.println("*Histogram Stats  RECEIVER*: Error happened - message is null");
        }
        consumer.close();
    }

    private void runFirstSuperstep()
    {
        runner.load(1);

        dataToBeShipped=new HashMap<>();
        boolean jobsRecieved=false, datashipper=false;
        Consumer consumer=new Consumer();
        consumer.connect(nodeName);

        while (!jobsRecieved || !datashipper)
        {
            String msg=consumer.receive();
            if (msg !=null) {
                if(msg.startsWith("#jobs"))
                {
                    runner.setJobsInRawString(msg);
                    System.out.println("The jobs have been received.");
                    jobsRecieved=true;
                }
                else if(msg.startsWith("#datashipper"))
                {
                    readEdgesToBeShipped(msg);
                    datashipper=true;
                }
            }
            else
                System.out.println("*JOB RECEIVER*: Error happened - message is null");
        }
        consumer.close();

        for (int workerID:dataToBeShipped.keySet()) {

            Graph<Vertex, RelationshipEdge> graphToBeSent = extractGraphToBeSent(workerID,0);
            LocalDateTime now = LocalDateTime.now();
            String date=now.getHour() + "_" + now.getMinute() + "_" + now.getSecond();
            String key=date + "_G_" + nodeName + "_to_" +Config.workers.get(workerID) + ".ser";

            if(Config.sharedStorage == Config.SharedStorage.S3)
                S3Storage.upload(Config.S3BucketName,key,graphToBeSent);
            else if (Config.sharedStorage == Config.SharedStorage.HDFS)
                HDFSStorage.upload(Config.HDFSDirectory,key,graphToBeSent,true);

            Producer messageProducer=new Producer();
            messageProducer.connect();
            messageProducer.send(Config.workers.get(workerID)+"_data",key);
            System.out.println("*DATA SENDER*: Graph object has been sent to '" + Config.workers.get(workerID) + "' successfully");
            messageProducer.close();
        }

        int receivedData=0;

        consumer=new Consumer();
        consumer.connect(nodeName+"_data");

        while (receivedData<Config.workers.size()-1)
        {
            System.out.println("*WORKER*: Start reading data from other workers...");
            String msg = consumer.receive();
            System.out.println("*WORKER*: Received a new message.");
            if (msg!=null) {
                Object obj = null;
                if(Config.sharedStorage == Config.SharedStorage.S3)
                    obj=S3Storage.downloadObject(Config.S3BucketName,msg);
                else if(Config.sharedStorage == Config.SharedStorage.HDFS)
                    obj = HDFSStorage.downloadObject(Config.HDFSDirectory,msg);

                if(obj!=null)
                {
                    Graph<Vertex, RelationshipEdge> receivedGraph =(Graph<Vertex, RelationshipEdge>) obj;
                    Util.mergeGraphs(runner.getLoaders()[0].getGraph(),receivedGraph);
                }
                else
                    System.out.println("*WORKER*: Object was null!");
            } else
                System.out.println("*WORKER*: Error happened - msg is null");
            receivedData++;
        }
        consumer.close();

        runner.generateJobs(singlePatternTreeNodes);
        runner.runSnapshot(1);

        Producer messageProducer=new Producer();
        messageProducer.connect();
        messageProducer.send("results",nodeName+"@1");
        System.out.println("*WORKER*: Superstep 1 is done successfully");
        messageProducer.close();
    }

    private void runNextSuperSteps(int superStepNumber)
    {
        runner.load(superStepNumber);

        dataToBeShipped=new HashMap<>();
        boolean newJobsRecieved=false, datashipper=false;
        Consumer consumer=new Consumer();
        consumer.connect(nodeName);

        while (!newJobsRecieved || !datashipper)
        {
            String msg=consumer.receive();
            if (msg !=null) {
                if(msg.startsWith("#newjobs"))
                {
                    runner.appendNewJobs(singlePatternTreeNodes,msg,superStepNumber);
                    System.out.println("New jobs have been received.");
                    newJobsRecieved=true;
                }
                else if(msg.startsWith("#datashipper"))
                {
                    readEdgesToBeShipped(msg);
                    datashipper=true;
                }
            }
            else
                System.out.println("*NEW JOBS RECEIVER*: Error happened - message is null");
        }
        consumer.close();

        for (int workerID:dataToBeShipped.keySet()) {

            Graph<Vertex, RelationshipEdge> graphToBeSent = extractGraphToBeSent(workerID,superStepNumber-1);
            LocalDateTime now = LocalDateTime.now();
            String date=now.getHour() + "_" + now.getMinute() + "_" + now.getSecond();
            String key=date + "_G_" + nodeName + "_to_" +Config.workers.get(workerID) + ".ser";

            if(Config.sharedStorage == Config.SharedStorage.S3)
                S3Storage.upload(Config.S3BucketName,key,graphToBeSent);
            else if (Config.sharedStorage == Config.SharedStorage.HDFS)
                HDFSStorage.upload(Config.S3BucketName,key,graphToBeSent,true);

            Producer messageProducer=new Producer();
            messageProducer.connect();
            messageProducer.send(Config.workers.get(workerID)+"_data",key);
            System.out.println("*DATA SENDER*: Graph object has been sent to '" + Config.workers.get(workerID) + "' successfully");
            messageProducer.close();
        }

        int receivedData=0;

        consumer=new Consumer();
        consumer.connect(nodeName+"_data");

        while (receivedData<Config.workers.size()-1)
        {
            System.out.println("*WORKER*: Start reading data from other workers...");
            String msg = consumer.receive();
            System.out.println("*WORKER*: Received a new message.");
            if (msg!=null) {
                Object obj = null;
                if(Config.sharedStorage == Config.SharedStorage.S3)
                    obj=S3Storage.downloadObject(Config.S3BucketName,msg);
                else if(Config.sharedStorage == Config.SharedStorage.HDFS)
                    obj = HDFSStorage.downloadObject(Config.HDFSDirectory,msg);

                if(obj!=null)
                {
                    Graph<Vertex, RelationshipEdge> receivedGraph =(Graph<Vertex, RelationshipEdge>) obj;
                    Util.mergeGraphs(runner.getLoaders()[superStepNumber-1].getGraph(),receivedGraph);
                }
                else
                    System.out.println("*WORKER*: Object was null!");
            } else
                System.out.println("*WORKER*: Error happened - msg is null");
            receivedData++;
        }
        consumer.close();

        runner.runSnapshot(superStepNumber);

        Producer messageProducer=new Producer();
        messageProducer.connect();
        messageProducer.send("results",nodeName+"@" + superStepNumber);
        System.out.println("*WORKER*: SupersStep "+superStepNumber+" is done successfully");
        messageProducer.close();
    }

    private void readEdgesToBeShipped(String msg)
    {
        // #datashipper\nfile1.txt\nfile2.txt
        String []temp=msg.split("\n");
        for (int i=1;i<temp.length;i++)
        {
            StringBuilder sb = new StringBuilder();
            if(Config.sharedStorage == Config.SharedStorage.S3)
                sb = S3Storage.downloadWholeTextFile(Config.S3BucketName,temp[i]);
            else if (Config.sharedStorage == Config.SharedStorage.HDFS)
                sb = HDFSStorage.downloadWholeTextFile(Config.HDFSDirectory,temp[i]);
            String []arr = sb.toString().split("\n");
            //2 \n v1 \t v2 \t name \n v3 \t t v5 \t age
            int workerID=Integer.parseInt(arr[0]);
            if(!dataToBeShipped.containsKey(workerID))
                dataToBeShipped.put(workerID,new ArrayList<>());
            for (int j=1;j<arr.length;j++)
            {
                String []arr2=arr[j].split("\t");
                dataToBeShipped.get(workerID).add(new SimpleEdge(arr2[0],arr2[1],arr2[2]));
            }
        }
    }

    private Graph<Vertex, RelationshipEdge> extractGraphToBeSent(int workerID, int superStepIndex)
    {
        Graph<Vertex, RelationshipEdge> graphToBeSent = new DefaultDirectedGraph<>(RelationshipEdge.class);
        HashSet<String> visited=new HashSet<>();
        for (SimpleEdge edge:dataToBeShipped.get(workerID)) {
            Vertex src=null,dst=null;
            if(!visited.contains(edge.getSrc()))
            {
                src = runner.getLoaders()[superStepIndex].getGraph().getNode(edge.getSrc());
                if(src!=null)
                {
                    graphToBeSent.addVertex(src);
                    visited.add(edge.getSrc());
                }
            }
            if(!visited.contains(edge.getDst()))
            {
                dst = runner.getLoaders()[superStepIndex].getGraph().getNode(edge.getDst());
                if(dst!=null)
                {
                    graphToBeSent.addVertex(dst);
                    visited.add(edge.getDst());
                }
            }
            if(src!=null && dst!=null)
            {
                graphToBeSent.addEdge(src,dst,new RelationshipEdge(edge.getLabel()));
            }
        }
        return graphToBeSent;
    }

    private void sendStatusToCoordinator()
    {
        System.out.println("Worker '"+nodeName+"' is up and send status to the Coordinator");
        Producer producer=new Producer();
        producer.connect();
        producer.send("status","up " + nodeName);
        System.out.println("Status sent to the Coordinator successfully.");
        producer.close();
    }

    //endregion
}
