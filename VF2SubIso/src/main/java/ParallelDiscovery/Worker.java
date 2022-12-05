package ParallelDiscovery;

import ChangeExploration.Change;
import Discovery.TGFDDiscovery;
import Discovery.TaskRunner;
import Infra.PatternTreeNode;
import Infra.RelationshipEdge;
import Infra.SimpleEdge;
import Infra.Vertex;
import MPI.Consumer;
import MPI.Producer;
import Partitioner.Util;
import SharedStorage.HDFSStorage;
import SharedStorage.S3Storage;
import Util.Config;
import VF2BasedWorkload.JobletRunner;
import org.apache.kerby.config.Conf;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

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
                    // TODO: Add S3 option
                    singlePatternTreeNodes = (List<PatternTreeNode>) HDFSStorage.downloadHDFSFile(Config.HDFSDirectory,fileName);
                    System.out.println("All single PatternTreeNodes have been received.");
                    singlePatternTreeNodesRecieved=true;
                }
            }
            else
                System.out.println("*SINGLE PATTERNTREENODE  RECEIVER*: Error happened - message is null");
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
