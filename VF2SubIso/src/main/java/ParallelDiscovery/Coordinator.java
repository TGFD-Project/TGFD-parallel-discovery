package ParallelDiscovery;

import ChangeExploration.Change;
import ChangeExploration.ChangeLoader;
import Discovery.Job;
import Discovery.TGFDDiscovery;
import Discovery.Util;
import ICs.TGFD;
import Infra.PatternTreeNode;
import Infra.SimpleEdge;
import Loader.GraphLoader;
import Loader.SimpleDBPediaLoader;
import Loader.SimpleIMDBLoader;
import Loader.TGFDGenerator;
import MPI.Consumer;
import MPI.Producer;
import ParalleRunner.Status;
import SharedStorage.HDFSStorage;
import Util.Config;
import VF2BasedWorkload.Joblet;
import VF2BasedWorkload.WorkloadEstimator;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Coordinator {

    //region --[Fields: Private]---------------------------------------

    private String nodeName = "coordinator";
    private GraphLoader loader=null;
    private JobEstimator []estimator=null;

    private AtomicBoolean workersStatusChecker=new AtomicBoolean(true);
    private AtomicBoolean workersResultsChecker =new AtomicBoolean(false);
    private AtomicBoolean allDone=new AtomicBoolean(false);

    private HashMap<Integer, HashMap<Integer, ArrayList<String>>> edgesToBeShippedToOtherWorkers;
    private HashMap<Integer, HashMap<Integer, String>> changesToBeSentToOtherWorkers;

    private HashMap<String,Boolean> workersStatus=new HashMap <>();
    private HashMap<String,List<String>> results=new HashMap <>();

    private AtomicInteger superstep=new AtomicInteger(0);

    private String []args;

    private TGFDDiscovery tgfdDiscovery;

    private List<PatternTreeNode> singlePatternTreeNodes;

    //endregion

    //region --[Constructor]-----------------------------------------

    public Coordinator(String []args)
    {
        for (String worker: Config.workers) {
            workersStatus.put(worker,false);
        }
        edgesToBeShippedToOtherWorkers=new HashMap<>();
        changesToBeSentToOtherWorkers=new HashMap<>();
        this.args = args;
    }

    //endregion

    //region --[Public Methods]-----------------------------------------

    public void start() throws IOException {

        tgfdDiscovery = new TGFDDiscovery(args);
        tgfdDiscovery.loadGraphsAndComputeHistogram2();

        estimator = new JobEstimator[Util.T];

        this.singlePatternTreeNodes = tgfdDiscovery.vSpawnSinglePatternTreeNode();

        String tmpName = "SinglePatterns_" + Config.generateRandomString(10);
        HDFSStorage.upload(Config.HDFSDirectory,tmpName,singlePatternTreeNodes, true);

        Producer messageProducer=new Producer();
        messageProducer.connect();
        StringBuilder message;
        for (String worker: Config.workers) {
            message= new StringBuilder();
            message.append("#singlePattern").append("\t").append(tmpName);
            messageProducer.send(worker,message.toString());
            System.out.println("*VSPawn Starter*: Message for singlePattern nodes sent to '" + worker + "' successfully");
        }
        messageProducer.close();
        System.out.println("*VSPawn Starter*: All Single Node Patterns are assigned.");

        Thread setupThread = new Thread(new Setup());
        setupThread.setDaemon(false);
        setupThread.start();

        Set<PatternTreeNode> hSet = new HashSet<>(singlePatternTreeNodes);
        hSet.addAll(singlePatternTreeNodes);
        loadTheWorkload(hSet);

        Thread dataAndChangeFilesGeneratorThread = new Thread(new ShippedDataGenerator());
        dataAndChangeFilesGeneratorThread.setDaemon(false);
        dataAndChangeFilesGeneratorThread.start();
    }

    public void stop()
    {
        this.workersStatusChecker.set(false);
        this.workersResultsChecker.set(false);
    }

    public void assignJobs()
    {
        Thread jobAssignerThread = new Thread(new JobAssigner());
        jobAssignerThread.setDaemon(false);
        jobAssignerThread.start();

        Thread dataShipperThread = new Thread(new DataShipper());
        dataShipperThread.setDaemon(false);
        dataShipperThread.start();
    }

    public void waitForResults()
    {
        Thread ResultsGetterThread = new Thread(new ResultsGetter());
        ResultsGetterThread.setDaemon(false);
        ResultsGetterThread.start();
    }

    public HashMap<String,List<String>> getResults()
    {
        if(getStatus()== Status.Coordinator_Is_Done)
            return results;
        else
            return null;
    }

    public Status getStatus()
    {
        if(workersStatusChecker.get())
            return Status.Coordinator_Waits_For_Workers_Status;
        else if(workersResultsChecker.get())
            return Status.Coordinator_Waits_For_Workers_Results;
        else if(allDone.get())
            return Status.Coordinator_Is_Done;
        else
            return Status.Coordinator_Assigns_jobs_To_Workers;
    }

    //endregion

    //region --[Private Methods]-----------------------------------------

    private void loadTheWorkload(Set<PatternTreeNode> singlePatternTreeNodes)
    {
        if(Config.datasetName== Config.dataset.dbpedia)
            loader = new SimpleDBPediaLoader(singlePatternTreeNodes, Config.getFirstTypesFilePath(), Config.getFirstDataFilePath());
        else if(Config.datasetName== Config.dataset.synthetic) {
            //loader = new SyntheticLoader(tgfds, Config.getFirstDataFilePath());
        }
        else if(Config.datasetName== Config.dataset.imdb) // default is imdb
        {
            //loader = new SimpleIMDBLoader(tgfds, Config.getFirstDataFilePath());
        }

        System.out.println("Number of edges: " + loader.getGraph().getGraph().edgeSet().size());

        estimator[0] =new JobEstimator(Util.graphs.get(0),Config.workers.size(),2);
        estimator[0].defineJobs(singlePatternTreeNodes);
        estimator[0].partitionWorkload();
        //System.out.println("Number of edges to be shipped: " + estimator.communicationCost());
        HashMap<Integer, HashMap<Integer, ArrayList<SimpleEdge>>> dataToBeShipped = estimator[0].dataToBeShipped();
        HashMap<Integer, ArrayList<String>> filesOnSharedStorage = estimator[0].sendEdgesToWorkersForShipment(dataToBeShipped);
        edgesToBeShippedToOtherWorkers.put(1,filesOnSharedStorage);
    }

    private class Setup implements Runnable, ExceptionListener
    {
        @Override
        public void run() {
            try {
                Consumer consumer=new Consumer();
                consumer.connect("status");

                while (workersStatusChecker.get()) {

                    System.out.println("*SETUP*: Listening for new messages to get workers' status...");
                    String msg = consumer.receive();
                    System.out.println("*SETUP*: Received a new message.");
                    if (msg!=null) {
                        if(msg.startsWith("up"))
                        {
                            String []temp=msg.split(" ");
                            if(temp.length==2)
                            {
                                String worker_name=temp[1];
                                if(workersStatus.containsKey(worker_name))
                                {
                                    System.out.println("*SETUP*: Status update: '" + worker_name + "' is up");
                                    workersStatus.put(worker_name,true);
                                }
                                else
                                {
                                    System.out.println("*SETUP*: Unable to find the worker name: '" + worker_name + "' in workers list. " +
                                            "Please update the list in the Config file.");
                                }
                            }
                            else
                                System.out.println("*SETUP*: Message corrupted: " + msg);
                        }
                        else
                            System.out.println("*SETUP*: Message corrupted: " + msg);
                    } else
                        System.out.println("*SETUP*: Error happened.");

                    boolean done=true;
                    for (Boolean worker_status:workersStatus.values()) {
                        if(!worker_status)
                        {
                            done=false;
                            break;
                        }
                    }
                    if(done)
                    {
                        System.out.println("*SETUP*: All workers are up and ready to start.");
                        workersStatusChecker.set(false);
                    }
                }
                consumer.close();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void onException(JMSException e) {
            System.out.println("JMS Exception occurred.  Shutting down coordinator.");
        }
    }

    private class JobAssigner implements Runnable, ExceptionListener
    {
        @Override
        public void run() {
            System.out.println("*JOB ASSIGNER*: Jobs are received to be assigned to the workers");
            try {
                while(getStatus()==Status.Coordinator_Waits_For_Workers_Status) {
                    System.out.println("*JOB ASSIGNER*: Coordinator waits for these workers to be online: ");
                    for (String worker : workersStatus.keySet()) {
                        if (!workersStatus.get(worker))
                            System.out.print(worker + " - ");
                    }
                    Thread.sleep(Config.threadsIdleTime);
                }
                Producer messageProducer=new Producer();
                messageProducer.connect();
                StringBuilder message;
                for (int workerID:estimator[0].getJobsByFragmentID().keySet()) {
                    message= new StringBuilder();
                    message.append("#jobs").append("\n");
                    for (Job job:estimator[0].getJobsByFragmentID().get(workerID)) {
                        // A job is in the form of the following
                        // id # CenterNodeVertexID # diameter # FragmentID # Type
                        message.append(job.getId()).append("#")
                                .append(job.getCenterNode()).append("#")
                                .append(job.getDiameter()).append("#")
                                .append(job.getFragmentID()).append("#")
                                .append(job.getCenterNode().getTypes().iterator().next())
                                .append("\n");
                    }
                    messageProducer.send(Config.workers.get(workerID),message.toString());
                    System.out.println("*JOB ASSIGNER*: jobs assigned to '" + Config.workers.get(workerID) + "' successfully");
                }
                messageProducer.close();
                System.out.println("*JOB ASSIGNER*: All jobs are assigned.");
                superstep.set(1);
                workersResultsChecker.set(true);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        @Override
        public void onException(JMSException e) {
            System.out.println("*JOB ASSIGNER*: JMS Exception occurred (JobAssigner).  Shutting down coordinator.");
        }
    }

    private class DataShipper implements Runnable, ExceptionListener
    {
        @Override
        public void run() {
            System.out.println("*DATA SHIPPER*: Edges are received to be shipped to the workers");
            try {
                while (true)
                {
                    int currentSuperstep=superstep.get();
                    while(!edgesToBeShippedToOtherWorkers.containsKey(currentSuperstep)) {
                        System.out.println("*DataShipper*: Wait for the new superstep: ");
                        Thread.sleep(Config.threadsIdleTime);
                        currentSuperstep=superstep.get();
                    }

                    Producer messageProducer=new Producer();
                    messageProducer.connect();
                    StringBuilder message;
                    for (int workerID:estimator[currentSuperstep-1].getJobsByFragmentID().keySet()) {
                        message= new StringBuilder();
                        message.append("#newjobs").append("\n");
                        for (Job job:estimator[currentSuperstep-1].getJobsByFragmentID().get(workerID)) {
                            // A job is in the form of the following
                            // id # CenterNodeVertexID # diameter # FragmentID # Type
                            message.append(job.getId()).append("#")
                                    .append(job.getCenterNode()).append("#")
                                    .append(job.getDiameter()).append("#")
                                    .append(job.getFragmentID()).append("#")
                                    .append(job.getCenterNode().getTypes().iterator().next())
                                    .append("\n");
                        }
                        messageProducer.send(Config.workers.get(workerID),message.toString());
                        System.out.println("*JOB ASSIGNER*: jobs assigned to '" + Config.workers.get(workerID) + "' successfully");
                    }

                    for (int workerID:edgesToBeShippedToOtherWorkers.get(currentSuperstep).keySet()) {
                        message= new StringBuilder();
                        message.append("#datashipper").append("\n");
                        for (String path:edgesToBeShippedToOtherWorkers.get(currentSuperstep).get(workerID)) {
                            message.append(path).append("\n");
                        }
                        messageProducer.send(Config.workers.get(workerID),message.toString());
                        System.out.println("*DataShipper*: Shipping files have been shared with '" + Config.workers.get(workerID) + "' successfully");
                    }
                    messageProducer.close();
                    System.out.println("*DataShipper*: All files are shared for the superstep: " + currentSuperstep);
                    edgesToBeShippedToOtherWorkers.remove(currentSuperstep);
                    changesToBeSentToOtherWorkers.remove(currentSuperstep);
                    if(currentSuperstep==Config.getAllDataPaths().keySet().size()+1)
                        return;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void onException(JMSException e) {
            System.out.println("*DataShipper: JMS Exception occurred (DataShipper).  Shutting down coordinator.");
        }
    }

    private class ShippedDataGenerator implements Runnable, ExceptionListener
    {
        @Override
        public void run() {
            System.out.println("*DATA NEEDS TO BE SHIPPED*: Generating files to upload to S3 to send to workers later");
            try {
                for (int i=1;i<=Util.T;i++) {
                    estimator[i] =new JobEstimator(Util.graphs.get(i),Config.workers.size(),estimator[i-1].getFragmentsByVertexURI(),2);

                    HashMap<PatternTreeNode, HashSet<String>> previouslyDefinedJobsForVertices = new HashMap<>();
                    for (int j=0; j<i;j++)
                    {
                        HashMap<PatternTreeNode, HashSet<String>> temp = estimator[j].getAlreadyDefinedJobsForVertices();
                        for (PatternTreeNode ptn:temp.keySet()) {
                            if(!previouslyDefinedJobsForVertices.containsKey(ptn))
                                previouslyDefinedJobsForVertices.put(ptn,new HashSet<>());
                            previouslyDefinedJobsForVertices.get(ptn).addAll(temp.get(ptn));
                        }
                    }

                    estimator[i].defineNewJobs(singlePatternTreeNodes,previouslyDefinedJobsForVertices);
                    estimator[i].partitionWorkload();
                    //System.out.println("Number of edges to be shipped: " + estimator.communicationCost());
                    HashMap<Integer, HashMap<Integer, ArrayList<SimpleEdge>>> dataToBeShipped = estimator[i].dataToBeShipped();
                    HashMap<Integer, ArrayList<String>> filesOnSharedStorage = estimator[i].sendEdgesToWorkersForShipment(dataToBeShipped);
                    edgesToBeShippedToOtherWorkers.put(i+1,filesOnSharedStorage);
                    System.out.println("*DATA NEEDS TO BE SHIPPED*: Generating files for snapshot (" + (i+1) + ")");

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void onException(JMSException e) {
            System.out.println("*DataShipper: JMS Exception occurred (DataShipper).  Shutting down coordinator.");
        }
    }

    private class ResultsGetter implements Runnable, ExceptionListener
    {
        @Override
        public void run() {
            System.out.println("*RESULTS GETTER*: Coordinator listens to get the results back from the workers");
            for (String worker_name : workersStatus.keySet()) {
                if (!results.containsKey(worker_name))
                    results.put(worker_name, new ArrayList<>());
            }
            try {
                while (true) {
                    Consumer consumer = new Consumer();
                    consumer.connect("results"+superstep.get());

                    System.out.println("*RESULTS GETTER*: Listening for new messages to get the results...");
                    String msg = consumer.receive();
                    System.out.println("*RESULTS GETTER*: Received a new message.");
                    if (msg != null) {
                        String[] temp = msg.split("@");
                        if (temp.length == 2) {
                            String worker_name = temp[0].toLowerCase();
                            if (workersStatus.containsKey(worker_name)) {
                                System.out.println("*RESULTS GETTER*: Results received from: '" + worker_name + "'");
                                results.get(worker_name).add(temp[1]);
                            } else {
                                System.out.println("*RESULTS GETTER*: Unable to find the worker name: '" + worker_name + "' in workers list. " +
                                        "Please update the list in the Config file.");
                            }
                        } else {
                            System.out.println("*RESULTS GETTER*: Message corrupted: " + msg);
                        }
                    } else
                        System.out.println("*RESULTS GETTER*: Error happened. message is null");

                    boolean done = true;
                    for (String worker_name : workersStatus.keySet()) {
                        if (results.get(worker_name).size() != superstep.get()) {
                            done = false;
                            break;
                        }
                    }
                    if (done) {
                        System.out.println("*RESULTS GETTER*: All workers have sent the results for superstep: " + superstep.get());
                        if(superstep.get() > Config.getDiffFilesPath().keySet().size())
                        {
                            System.out.println("*RESULTS GETTER*: All done! No superstep remained.");
                            superstep.set(superstep.get() + 1);
                            allDone.set(true);
                            break;
                        }
                        else
                        {
                            superstep.set(superstep.get() + 1);
                            System.out.println("*RESULTS GETTER*: Starting the new superstep! -> " + superstep.get() );
                        }
                    }
                    consumer.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void onException(JMSException e) {
            System.out.println("*RESULTS GETTER*: JMS Exception occurred. Shutting down coordinator.");
        }
    }

    //endregion

}
