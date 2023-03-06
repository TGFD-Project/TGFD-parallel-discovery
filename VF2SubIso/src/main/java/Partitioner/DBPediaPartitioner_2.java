package Partitioner;

import Infra.DataVertex;
import Loader.DBPediaLoader;
import Loader.GraphLoader;
import Util.Config;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.HashMap;

public class DBPediaPartitioner_2 {

    private GraphLoader dbpedia;
    private final int numberOfPartitions;

    public DBPediaPartitioner_2(DBPediaLoader dbpedia, int numberOfPartitions) {
        this.dbpedia = dbpedia;
        this.numberOfPartitions = numberOfPartitions;
    }

    public HashMap<DataVertex, Integer> partition() {
        System.out.println("Start partitioning...");
        RangeBasedPartitioner partitioner = new RangeBasedPartitioner(dbpedia.getGraph());
        HashMap<DataVertex, Integer> partitionMapping = partitioner.fragment(numberOfPartitions);
        System.out.println("Partitioning done.");

        return partitionMapping;
    }

    public void partition(String pathToFile, String savingDirectory, HashMap<DataVertex, Integer> partitionMapping, int time) {

        StringBuilder[] data = new StringBuilder[numberOfPartitions];
        for (int i = 0; i < data.length; i++)
            data[i] = new StringBuilder();

        loadFile(pathToFile, partitionMapping, data);

        try {
            for (int i = 0; i < data.length; i++) {
                FileWriter file = new FileWriter(savingDirectory + "DBPedia_data" + i + "_" + time + ".nt");
                file.write(data[i].toString());
                file.flush();
                file.close();
            }
            System.out.println("Done.");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    //region --[Methods: Private]---------------------------------------

    /**
     * Load file in the format of (subject, predicate, object)
     *
     * @param filePath Path to dbpedia file (data or type files)
     */
    private void loadFile(String filePath, HashMap<DataVertex, Integer> partitionMapping, StringBuilder[] sb) {

        if (filePath == null || filePath.length() == 0) {
            System.out.println("No Input Node Types File Path!");
            return;
        }
        S3Object fullObject = null;
        BufferedReader br;
        try {
            System.out.println("Loading File: " + filePath);
            if (Config.Amazon) {
                AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                        .withRegion(Config.region)
                        //.withCredentials(new ProfileCredentialsProvider())
                        //.withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                        .build();
                //TODO: Need to check if the path is correct (should be in the form of bucketName/Key )
                String bucketName = filePath.substring(0, filePath.lastIndexOf("/"));
                String key = filePath.substring(filePath.lastIndexOf("/") + 1);
                System.out.println("Downloading the object from Amazon S3 - Bucket name: " + bucketName + " - Key: " + key);
                fullObject = s3Client.getObject(new GetObjectRequest(bucketName, key));

                br = new BufferedReader(new InputStreamReader(fullObject.getObjectContent()));
            } else {
                br = new BufferedReader(new FileReader(filePath));
            }

            String line = br.readLine().toLowerCase();
            int partitionID = -1;
            while (line != null) {
                if (line.startsWith("<http://dbpedia.org/resource/")) {
                    String nodeURI = line.toLowerCase().substring(line.lastIndexOf("/") + 1, line.lastIndexOf(">"));
                    DataVertex v = (DataVertex) dbpedia.getGraph().getNode(nodeURI);
                    if (partitionMapping.containsKey(v)) {
                        partitionID = partitionMapping.get(v);
                        sb[partitionID].append(line).append("\n");
                        //partitionData.get(partitionID).append(line).append("\n");
                    } else {
                        sb[sb.length - 1].append(line).append("\n");
                        partitionID = -1;
                    }
                } else if (partitionID != -1)
                    sb[partitionID].append(line).append("\n");
                else
                    sb[sb.length - 1].append(line).append("\n");
                line = br.readLine().toLowerCase();
            }
            if (fullObject != null) {
                fullObject.close();
            }
            br.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    //endregion
}
