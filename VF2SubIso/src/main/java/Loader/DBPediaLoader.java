package Loader;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import Infra.Attribute;
import Infra.DataVertex;
import Infra.RelationshipEdge;
import ICs.TGFD;
import com.github.jsonldjava.utils.Obj;
import org.apache.jena.rdf.model.*;
import Util.Config;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class DBPediaLoader extends GraphLoader {

    //region --[Methods: Private]---------------------------------------

    /**
     * @param alltgfd List of TGFDs
     * @param typesPath Path to the DBPedia type file
     * @param dataPath Path to the DBPedia graph file
     */
    public DBPediaLoader(List<TGFD> alltgfd,List<?> typesPath, List<?> dataPath)
    {
        super(alltgfd);

        for (Object typePath:typesPath) {
            if (typePath instanceof String)
                loadNodeMap((String)typePath);
            else if (typePath instanceof Model)
                loadNodeMap((Model)typePath);
            else
                throw new IllegalArgumentException("unsupported object type");
        }

        for (Object d:dataPath) {
            if (d instanceof String)
                loadDataGraph((String)d);
            else if (d instanceof Model)
                loadDataGraph((Model)d);
            else
                throw new IllegalArgumentException("unsupported object type");
        }
    }

    //endregion

    //region --[Methods: Private]---------------------------------------

    /**
     * Load file in the format of (subject, predicate, object)
     * This will load the type file and create a DataVertex for each different subject with type of object
     * @param nodeTypesPath Path to the Type file
     */
    private void loadNodeMap(String nodeTypesPath) {

        if (nodeTypesPath == null || nodeTypesPath.length() == 0) {
            System.out.println("No Input Node Types File Path!");
            return;
        }
        S3Object fullObject = null;
        BufferedReader br=null;
        try
        {
            Model model = ModelFactory.createDefaultModel();
            System.out.println("Loading Node Types: " + nodeTypesPath);

            if(Config.Amazon)
            {
                AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                        .withRegion(Config.region)
                        //.withCredentials(new ProfileCredentialsProvider())
                        //.withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                        .build();

                //TODO: Need to check if the path is correct (should be in the form of bucketName/Key )
                String bucketName=nodeTypesPath.substring(0,nodeTypesPath.lastIndexOf("/"));
                String key=nodeTypesPath.substring(nodeTypesPath.lastIndexOf("/")+1);
                System.out.println("Downloading the object from Amazon S3 - Bucket name: " + bucketName +" - Key: " + key);
                fullObject = s3Client.getObject(new GetObjectRequest(bucketName, key));

                br = new BufferedReader(new InputStreamReader(fullObject.getObjectContent()));
                model.read(br,null, Config.language);
            }
            else
            {
                Path input= Paths.get(nodeTypesPath);
                model.read(input.toUri().toString());
            }

            StmtIterator typeTriples = model.listStatements();

            while (typeTriples.hasNext()) {
                Statement stmt = typeTriples.nextStatement();

                String nodeURI = stmt.getSubject().getURI().toLowerCase();
                if (nodeURI.length() > 28) {
                    nodeURI = nodeURI.substring(28);
                }
                String nodeType = stmt.getObject().asResource().getLocalName().toLowerCase();

                // ignore the node if the type is not in the validTypes and
                // optimizedLoadingBasedOnTGFD is true
                if(Config.optimizedLoadingBasedOnTGFD && !validTypes.contains(nodeType))
                    continue;
                //int nodeId = subject.hashCode();
                DataVertex v= (DataVertex) graph.getNode(nodeURI);

                if (v==null) {
                    v=new DataVertex(nodeURI,nodeType);
                    graph.addVertex(v);
                }
                else {
                    v.addType(nodeType);
                }
            }
            System.out.println("Done. Number of Types: " + graph.getSize());
            if (fullObject != null) {
                fullObject.close();
            }
            if (br != null) {
                br.close();
            }
            model.close();
        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
        }
    }

    private void loadNodeMap(Model model) {

        try {

            StmtIterator typeTriples = model.listStatements();

            while (typeTriples.hasNext()) {
                Statement stmt = typeTriples.nextStatement();

                if (!stmt.getPredicate().getURI().equals(Discovery.Util.TYPE_PREDICATE_URI))
                    continue;

                String nodeURI = stmt.getSubject().getURI().toLowerCase(); //TODO: URI in some data can be case sensitive. Should we handle this?
                if (nodeURI.length() > 28) {
                    nodeURI = nodeURI.substring(28);
                }
                String nodeType = stmt.getObject().asResource().getLocalName().toLowerCase();
                if (nodeType.trim().length() == 0)
                    continue;

                // ignore the node if the type is not in the validTypes and
                // optimizedLoadingBasedOnTGFD is true
                if(Config.optimizedLoadingBasedOnTGFD && !validTypes.contains(nodeType))
                    continue;
                //int nodeId = subject.hashCode();
                DataVertex v= (DataVertex) graph.getNode(nodeURI);

                if (v==null) {
                    v=new DataVertex(nodeURI,nodeType);
                    graph.addVertex(v);
                }
                else {
                    v.addType(nodeType);
                }
                types.add(nodeType);
            }
            System.out.println("Done. Number of Types: " + graph.getSize());
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * This method will load DBPedia graph file
     * @param dataGraphFilePath Path to the graph file
     */
    private void loadDataGraph(String dataGraphFilePath) {

        if (dataGraphFilePath == null || dataGraphFilePath.length() == 0) {
            System.out.println("No Input Graph Data File Path!");
            return;
        }
        System.out.println("Loading DBPedia Graph: "+dataGraphFilePath);
        int numberOfObjectsNotFound=0,numberOfSubjectsNotFound=0;

        S3Object fullObject = null;
        BufferedReader br=null;
        try
        {
            Model model = ModelFactory.createDefaultModel();
            if(Config.Amazon)
            {
                AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                        .withRegion(Config.region)
                        //.withCredentials(new ProfileCredentialsProvider())
                        //.withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                        .build();

                //TODO: Need to check if the path is correct (should be in the form of bucketName/Key )
                String bucketName=dataGraphFilePath.substring(0,dataGraphFilePath.lastIndexOf("/"));
                String key=dataGraphFilePath.substring(dataGraphFilePath.lastIndexOf("/")+1);
                System.out.println("Downloading the object from Amazon S3 - Bucket name: " + bucketName +" - Key: " + key);
                fullObject = s3Client.getObject(new GetObjectRequest(bucketName, key));

                br = new BufferedReader(new InputStreamReader(fullObject.getObjectContent()));
                model.read(br,null, Config.language);
            }
            else
            {
                Path input= Paths.get(dataGraphFilePath);
                model.read(input.toUri().toString());
            }

            StmtIterator dataTriples = model.listStatements();

            while (dataTriples.hasNext()) {

                Statement stmt = dataTriples.nextStatement();
                String subjectNodeURI = stmt.getSubject().getURI().toLowerCase();
                if (subjectNodeURI.length() > 28) {
                    subjectNodeURI = subjectNodeURI.substring(28);
                }

                String predicate = stmt.getPredicate().getLocalName().toLowerCase();
                RDFNode object = stmt.getObject();
                String objectNodeURI;

                if (object.isLiteral()) {
                    objectNodeURI = object.asLiteral().getString().toLowerCase();
                } else {
                    objectNodeURI = object.toString().substring(object.toString().lastIndexOf("/")+1).toLowerCase();
                }

                DataVertex subjVertex= (DataVertex) graph.getNode(subjectNodeURI);

                if (subjVertex==null) {

                    //System.out.println("Subject node not found: " + subjectNodeURI);
                    numberOfSubjectsNotFound++;
                    continue;
                }

                if (!object.isLiteral()) {
                    DataVertex objVertex= (DataVertex) graph.getNode(objectNodeURI);
                    if(objVertex==null)
                    {
                        //System.out.println("Object node not found: " + subjectNodeURI + "  ->  " + predicate + "  ->  " + objectNodeURI);
                        numberOfObjectsNotFound++;
                        continue;
                    }
                    else if (subjectNodeURI.equals(objectNodeURI)) {
                        //System.out.println("Loop found: " + subjectNodeURI + " -> " + objectNodeURI);
                        continue;
                    }
                    graph.addEdge(subjVertex, objVertex, new RelationshipEdge(predicate));
                    graphSize++;
                }
                else
                {
                    if(!Config.optimizedLoadingBasedOnTGFD || validAttributes.contains(predicate))
                    {
                        subjVertex.addAttribute(new Attribute(predicate,objectNodeURI));
                        graphSize++;
                    }
                }
            }
            System.out.println("Subjects and Objects not found: " + numberOfSubjectsNotFound + " ** " + numberOfObjectsNotFound);
            System.out.println("Done. Nodes: " + graph.getGraph().vertexSet().size() + ",  Edges: " +graph.getGraph().edgeSet().size());
            //System.out.println("Number of subjects not found: " + numberOfSubjectsNotFound);
            //System.out.println("Number of loops found: " + numberOfLoops);

            if (fullObject != null) {
                fullObject.close();
            }
            if (br != null) {
                br.close();
            }
            model.close();
        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
        }
    }

    private void loadDataGraph(Model model) {

        int numberOfObjectsNotFound = 0, numberOfSubjectsNotFound = 0;

        try {
            StmtIterator dataTriples = model.listStatements();

            while (dataTriples.hasNext()) {

                Statement stmt = dataTriples.nextStatement();

                if (stmt.getPredicate().getURI().equals(Discovery.Util.TYPE_PREDICATE_URI))
                    continue;

                String predicate = stmt.getPredicate().getLocalName().toLowerCase();

                String subjectNodeURI = stmt.getSubject().getURI().toLowerCase();
                if (subjectNodeURI.length() > 28) {
                    subjectNodeURI = subjectNodeURI.substring(28);
                }

                RDFNode object = stmt.getObject();
                String objectNodeURI;

                if (object.isLiteral()) {
                    objectNodeURI = object.asLiteral().getString().toLowerCase();
                } else {
                    objectNodeURI = object.toString().substring(object.toString().lastIndexOf("/") + 1).toLowerCase();
                }

                DataVertex subjVertex = (DataVertex) graph.getNode(subjectNodeURI);

                if (subjVertex == null) {

                    //System.out.println("Subject node not found: " + subjectNodeURI);
                    numberOfSubjectsNotFound++;
                    continue;
                }

                if (!object.isLiteral()) {
                    DataVertex objVertex = (DataVertex) graph.getNode(objectNodeURI);
                    if (objVertex == null) {
                        //System.out.println("Object node not found: " + subjectNodeURI + "  ->  " + predicate + "  ->  " + objectNodeURI);
                        numberOfObjectsNotFound++;
                        continue;
                    } else if (subjectNodeURI.equals(objectNodeURI)) {
                        //System.out.println("Loop found: " + subjectNodeURI + " -> " + objectNodeURI);
                        continue;
                    }
                    boolean edgeAdded = graph.addEdge(subjVertex, objVertex, new RelationshipEdge(predicate));
                    if (edgeAdded) graphSize++;
                } else {
//                    if (!Config.optimizedLoadingBasedOnTGFD || validAttributes.contains(predicate)) {
                    subjVertex.putAttributeIfAbsent(new Attribute(predicate, objectNodeURI));
                    graphSize++;
//                    }
                }
            }
            System.out.println("Subjects and Objects not found: " + numberOfSubjectsNotFound + " ** " + numberOfObjectsNotFound);
            System.out.println("Done. Nodes: " + graph.getGraph().vertexSet().size() + ",  Edges: " + graph.getGraph().edgeSet().size());
            //System.out.println("Number of subjects not found: " + numberOfSubjectsNotFound);
            //System.out.println("Number of loops found: " + numberOfLoops);

        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    //endregion

}
