package Loader;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import Infra.Attribute;
import Infra.DataVertex;
import Infra.RelationshipEdge;
import ICs.TGFD;
import org.apache.jena.rdf.model.*;
import Util.Config;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class IMDBLoader extends GraphLoader {

    public IMDBLoader(List<TGFD> alltgfd, List<?> paths) {

        super(alltgfd);
        for (Object path : paths) {
            if (path instanceof String) {
                loadIMDBGraph((String) path);
            } else if (path instanceof Model) {
                loadIMDBGraph((Model) path);
            }
        }
    }

    private void loadIMDBGraph(String dataGraphFilePath) {

        if (dataGraphFilePath == null || dataGraphFilePath.length() == 0) {
            System.out.println("No Input Graph Data File Path!");
            return;
        }
        System.out.println("Loading IMDB Graph: " + dataGraphFilePath);

        S3Object fullObject = null;
        BufferedReader br = null;
        try {
            HashSet<String> types = new HashSet<>();
            Model model = ModelFactory.createDefaultModel();

            if (Config.Amazon) {
                AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                        .withRegion(Config.region)
                        //.withCredentials(new ProfileCredentialsProvider())
                        //.withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                        .build();
                //TODO: Need to check if the path is correct (should be in the form of bucketName/Key )
                String bucketName = dataGraphFilePath.substring(0, dataGraphFilePath.lastIndexOf("/"));
                String key = dataGraphFilePath.substring(dataGraphFilePath.lastIndexOf("/") + 1);
                System.out.println("Downloading the object from Amazon S3 - Bucket name: " + bucketName + " - Key: " + key);
                fullObject = s3Client.getObject(new GetObjectRequest(bucketName, key));

                br = new BufferedReader(new InputStreamReader(fullObject.getObjectContent()));
                model.read(br, null, Config.language);
            } else {
                Path input = Paths.get(dataGraphFilePath);
                model.read(input.toUri().toString());
            }

            StmtIterator dataTriples = model.listStatements();
            while (dataTriples.hasNext()) {

                Statement stmt = dataTriples.nextStatement();
                String subjectNodeURL = stmt.getSubject().getURI().toLowerCase();
                if (subjectNodeURL.length() > 16) {
                    subjectNodeURL = subjectNodeURL.substring(16);
                }

                String[] temp = subjectNodeURL.split("/");
                if (temp.length != 2) {
                    // Error!
                    continue;
                }
                String subjectType = temp[0];
                String subjectID = temp[1];

                // ignore the node if the type is not in the validTypes and
                // optimizedLoadingBasedOnTGFD is true
                if (Config.optimizedLoadingBasedOnTGFD && !validTypes.contains(subjectType))
                    continue;

                types.add(subjectType);
                //int nodeId = subject.hashCode();
                DataVertex subjectVertex = (DataVertex) graph.getNode(subjectID);

                if (subjectVertex == null) {
                    subjectVertex = new DataVertex(subjectID, subjectType);
                    graph.addVertex(subjectVertex);
                } else {
                    subjectVertex.addType(subjectType);
                }

                String predicate = stmt.getPredicate().getLocalName().toLowerCase();
                RDFNode object = stmt.getObject();
                String objectNodeURI;
                if (object.isLiteral()) {
                    objectNodeURI = object.asLiteral().getString().toLowerCase();
                    if (Config.optimizedLoadingBasedOnTGFD && validAttributes.contains(predicate)) {
                        subjectVertex.addAttribute(new Attribute(predicate, objectNodeURI));
                        graphSize++;
                    }
                } else {
                    objectNodeURI = object.toString().toLowerCase();
                    if (objectNodeURI.length() > 16)
                        objectNodeURI = objectNodeURI.substring(16);

                    temp = objectNodeURI.split("/");
                    if (temp.length != 2) {
                        // Error!
                        continue;
                    }

                    String objectType = temp[0];
                    String objectID = temp[1];

                    // ignore the node if the type is not in the validTypes and
                    // optimizedLoadingBasedOnTGFD is true
                    if (Config.optimizedLoadingBasedOnTGFD && !validTypes.contains(objectType))
                        continue;

                    types.add(objectType);
                    DataVertex objectVertex = (DataVertex) graph.getNode(objectID);
                    if (objectVertex == null) {
                        objectVertex = new DataVertex(objectID, objectType);
                        graph.addVertex(objectVertex);
                    } else {
                        objectVertex.addType(objectType);
                    }
                    graph.addEdge(subjectVertex, objectVertex, new RelationshipEdge(predicate));
                    graphSize++;
                }
            }
            System.out.println("Done. Nodes: " + graph.getGraph().vertexSet().size() + ",  Edges: " + graph.getGraph().edgeSet().size());
            System.out.println("Number of types: " + types.size() + "\n");
            types.forEach(type -> System.out.print(type + " - "));
            //System.out.println("Done Loading DBPedia Graph.");
            //System.out.println("Number of subjects not found: " + numberOfSubjectsNotFound);
            //System.out.println("Number of loops found: " + numberOfLoops);

            if (fullObject != null) {
                fullObject.close();
            }
            if (br != null) {
                br.close();
            }
            model.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private void loadIMDBGraph(Model model) {
        try {
            StmtIterator dataTriples = model.listStatements();
            while (dataTriples.hasNext()) {

                Statement stmt = dataTriples.nextStatement();
                String subjectNodeURL = stmt.getSubject().getURI().toLowerCase();
                if (subjectNodeURL.length() > 16) {
                    subjectNodeURL = subjectNodeURL.substring(16);
                }

                String[] temp = subjectNodeURL.split("/");
                if (temp.length != 2) {
                    // Error!
                    continue;
                }
                String subjectType = temp[0];
                String subjectID = temp[1];

                // ignore the node if the type is not in the validTypes and
                // optimizedLoadingBasedOnTGFD is true
                if (Config.optimizedLoadingBasedOnTGFD && !validTypes.contains(subjectType))
                    continue;

                types.add(subjectType);
                //int nodeId = subject.hashCode();
                DataVertex subjectVertex = (DataVertex) graph.getNode(subjectID);

                if (subjectVertex == null) {
                    subjectVertex = new DataVertex(subjectID, subjectType);
                    graph.addVertex(subjectVertex);
                } else {
                    subjectVertex.addType(subjectType);
                }

                String predicate = stmt.getPredicate().getLocalName().toLowerCase();
                RDFNode object = stmt.getObject();
                String objectNodeURI;
                if (object.isLiteral()) {
                    objectNodeURI = object.asLiteral().getString().toLowerCase();
                    subjectVertex.addAttribute(new Attribute(predicate, objectNodeURI));
                    graphSize++;
//                    if (Config.optimizedLoadingBasedOnTGFD && validAttributes.contains(predicate)) {
//                        subjectVertex.addAttribute(new Attribute(predicate, objectNodeURI));
//                        graphSize++;
//                    }
                } else {
                    objectNodeURI = object.toString().toLowerCase();
                    if (objectNodeURI.length() > 16)
                        objectNodeURI = objectNodeURI.substring(16);

                    temp = objectNodeURI.split("/");
                    if (temp.length != 2) {
                        // Error!
                        continue;
                    }

                    String objectType = temp[0];
                    String objectID = temp[1];

                    // ignore the node if the type is not in the validTypes and
                    // optimizedLoadingBasedOnTGFD is true
                    if (Config.optimizedLoadingBasedOnTGFD && !validTypes.contains(objectType))
                        continue;

                    types.add(objectType);
                    DataVertex objectVertex = (DataVertex) graph.getNode(objectID);
                    if (objectVertex == null) {
                        objectVertex = new DataVertex(objectID, objectType);
                        graph.addVertex(objectVertex);
                    } else {
                        objectVertex.addType(objectType);
                    }
                    graph.addEdge(subjectVertex, objectVertex, new RelationshipEdge(predicate));
                    graphSize++;
                }
            }
            System.out.println("Done. Nodes: " + graph.getGraph().vertexSet().size() + ",  Edges: " + graph.getGraph().edgeSet().size());
            System.out.println("Number of types: " + types.size() + "\n");
            types.forEach(type -> System.out.print(type + " - "));
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private static void printWithTime(String message, long runTimeInMS) {
        System.out.println(message + " time: " + runTimeInMS + "(ms) ** " +
                TimeUnit.MILLISECONDS.toSeconds(runTimeInMS) + "(sec) ** " +
                TimeUnit.MILLISECONDS.toMinutes(runTimeInMS) + "(min)");
    }
}
