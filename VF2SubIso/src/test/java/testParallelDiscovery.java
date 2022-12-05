import ParalleRunner.MediumCoordinator;
import ParalleRunner.MediumWorker;
import ParallelDiscovery.Coordinator;
import ParallelDiscovery.Worker;
import Util.Config;

import java.io.FileNotFoundException;
import java.io.IOException;

public class testParallelDiscovery {

    public static void main(String []args) throws IOException {

        Config.parse(args[0]);
        if(Config.nodeName.equalsIgnoreCase("coordinator"))
        {

            // [Module 1]
            // Load the graph and create Histogram of the graph
            // [OPT]: Instead of loading the graph, let's load the Histogram after preprocess the input graph once!
            // Find the set of active attributes in the graph and send to the workers

            // [Module 2]
            // Create the VSpawn Tree

            // [Module 3]
            // Iterate the VSpawn Tree (up to K^2 candidates)
            // Get a candidate PatternTreeNode

            // [Module 3.1]
            // Define Jobs, Send Jobs to the workers, Send information about the data to be shipped to the workers, and Wait for the workers to find the matches of the first snapshot
            // Send the changes (preprocessed) to the corresponding workers in T-1 loop

            // [Module 3.2]
            // Receive discovered ConstantTGFDs from all workers along with Entities and DeltaToPair
            // Validate the Constant TGFDs [need to be coded]
            // Call GeneralTGFD function by merging all the received files (Entities and DeltaToPair) [Partially coded]
            // Send feedback to the workers about the results

            Coordinator coordinator =new Coordinator(args);
            coordinator.start();
            coordinator.assignJobs();
            coordinator.waitForResults();
        }
        else // Worker
        {

            //[Module1]
            // Send a readyToWorkFlag to the coordinator
            // Receive the list of active attributes
            // Receive the list of jobs for the current worker
            // Prepare n-1 subgraphs that needs to be sent to other n-1 workers
            // Send the data (subgraphs) to the workers (either through HDFS or Amazon S3)
            // Receive data from other n-1 workers and merge with local graph
            // Find the matches of the first snapshot for the given jobs
            // Let the coordinator to send the changes of the next snapshot!
            // In T-1 loop, receive the change files and find the matches

            // [Module 2]
            // Create the LiteralTree locally and start HSpawn

            // [Module 2.1]
            // Traverse the LiteralTree and generate a candidate dependency

            // [Module 2.2]
            // Call DeltaDiscovery function
            //      Determine the Entities
            //          Call ConstantTGFDDiscovery
            //              Compute the DeltaToPair set

            // [Module 2.3]
            // Send the discovered ConstantTGFDs to the coordinator along with Entities and DeltaToPair
            // Wait for the feedback
            // Unprune the LiteralTree, if needed based on the received feedback (For verification)




            System.out.println("Worker '"+ Config.nodeName+"' is starting...");
            Worker worker=new Worker();
            worker.start();
        }
    }

}
