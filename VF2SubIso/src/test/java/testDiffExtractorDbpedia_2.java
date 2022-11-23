import ChangeExploration.*;
import Discovery.TGFDDiscovery;
import Discovery.Util;
import ICs.TGFD;
import Loader.DBPediaLoader;
import Loader.TGFDGenerator;
import Util.Config;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class testDiffExtractorDbpedia_2 {

    public static void main(String []args) throws FileNotFoundException {

        System.out.println("Test extract diffs over DBPedia graph");

        String []args2 = new String[args.length-1];
        for (int i=1;i<args.length;i++)
            args2[i-1] = args[i];

        TGFDDiscovery tgfdDiscovery = new TGFDDiscovery(args2);
        tgfdDiscovery.loadGraphsAndComputeHistogram2();

        String[] info = {
                String.join("=", "loader", Util.loader),
                String.join("=", "|G|", Util.graphSize),
                String.join("=", "t", Integer.toString(Util.T)),
                String.join("=", "k", Integer.toString(Util.k)),
                String.join("=", "pTheta", Double.toString(Util.patternTheta)),
                String.join("=", "theta", Double.toString(Util.tgfdTheta)),
                String.join("=", "gamma", Double.toString(Util.gamma)),
                String.join("=", "frequentSetSize", Double.toString(Util.frequentSetSize)),
                String.join("=", "interesting", Boolean.toString(Util.onlyInterestingTGFDs)),
                String.join("=", "literalMax", Integer.toString(Util.maxNumOfLiterals)),
                String.join("=", "noMinimalityPruning", Boolean.toString(!Util.hasMinimalityPruning)),
                String.join("=", "noSupportPruning", Boolean.toString(!Util.hasSupportPruning)),
                String.join("=", "fastMatching", Boolean.toString(Util.fastMatching)),
                String.join("=", "interestLabels", Util.interestLabelsSet.toString()),
        };

        Config.parse(args[0]);

        System.out.println(String.join(", ", info));

        System.out.println(Config.getAllDataPaths().keySet() + " *** " + Config.getAllDataPaths().values());
        System.out.println(Config.getAllTypesPaths().keySet() + " *** " + Config.getAllTypesPaths().values());

        //Load the TGFDs.
        TGFDGenerator generator = new TGFDGenerator(Config.patternPath);
        List<TGFD> allTGFDs=generator.getTGFDs();

        String name="";
        for (TGFD tgfd:allTGFDs)
            name+=tgfd.getName() + "_";

        if(!name.equals(""))
            name=name.substring(0, name.length() - 1);
        else
            name="noSpecificTGFDs";

        System.out.println("Generating the diff files for the TGFD: " + name);
        DBPediaLoader first, second=null;
        List<Change> allChanges;
        int t1,t2=0;
        for (int i=0;i<Util.graphs.size();i+=2) {

            System.out.println("===========Snapshot (" + i + ")===========");
            long startTime = System.currentTimeMillis();

            t1=i;

            first = (DBPediaLoader) Util.graphs.get(i);
//            first = new DBPediaLoader(allTGFDs, Config.getAllTypesPaths().get((int) ids[i]),
//                    Config.getAllDataPaths().get((int) ids[i]));

            printWithTime("Load graph (" + i + ")", System.currentTimeMillis() - startTime);

            //
            if(second!=null)
            {
                ChangeFinder cFinder=new ChangeFinder(second,first,allTGFDs);
                allChanges= cFinder.findAllChanged();

                analyzeChanges(allChanges,allTGFDs,second.getGraphSize(),cFinder.getNumberOfEffectiveChanges(),t2,t1,name, Config.getDiffCaps());
            }

            if(i+1>=Util.graphs.size())
                break;

            System.out.println("===========Snapshot (" + i+1 + ")===========");
            startTime = System.currentTimeMillis();

            t2=i+1;
            second = (DBPediaLoader) Util.graphs.get(i+1);
//            second = new DBPediaLoader(allTGFDs, Config.getAllTypesPaths().get((int) ids[i+1]),
//                    Config.getAllDataPaths().get((int) ids[i+1]));

            printWithTime("Load graph (" + i+1 + ")", System.currentTimeMillis() - startTime);

            //
            ChangeFinder cFinder=new ChangeFinder(first,second,allTGFDs);
            allChanges= cFinder.findAllChanged();

            analyzeChanges(allChanges,allTGFDs,first.getGraphSize(),cFinder.getNumberOfEffectiveChanges(),t1,t2,name, Config.getDiffCaps());
        }
    }

    private static void analyzeChanges(List<Change> allChanges, List<TGFD> allTGFDs, int graphSize,
                                       int changeSize, int timestamp1, int timestamp2, String TGFDsName, ArrayList <Double> diffCaps)
    {
        ChangeTrimmer trimmer=new ChangeTrimmer(allChanges,allTGFDs);
        for (double i:diffCaps)
        {
            int allowedNumberOfChanges= (int) (i*graphSize);
            if (allowedNumberOfChanges<changeSize)
            {
                List<Change> trimmedChanges=trimmer.trimChanges(allowedNumberOfChanges);
                saveChanges(trimmedChanges,timestamp1,timestamp2,TGFDsName + "_" + i);
            }
            else
            {
                saveChanges(allChanges,timestamp1,timestamp2,TGFDsName + "_full");
                return;
            }
        }
    }

    private static void printWithTime(String message, long runTimeInMS)
    {
        System.out.println(message + " time: " + runTimeInMS + "(ms) ** " +
                TimeUnit.MILLISECONDS.toSeconds(runTimeInMS) + "(sec) ** " +
                TimeUnit.MILLISECONDS.toMinutes(runTimeInMS) +  "(min)");
    }

    private static void saveChanges(List<Change> allChanges, int t1, int t2, String tgfdName)
    {
        System.out.println("Printing the changes: " + t1 +" -> " + t2);
        int insertChangeEdge=0;
        int insertChangeVertex=0;
        int insertChangeAttribute=0;
        int deleteChangeEdge=0;
        int deleteChangeVertex=0;
        int deleteChangeAttribute=0;
        int changeAttributeValue=0;

        for (Change c:allChanges) {
            if(c instanceof EdgeChange)
            {
                if(c.getTypeOfChange()== ChangeType.deleteEdge)
                    deleteChangeEdge++;
                else if(c.getTypeOfChange()== ChangeType.insertEdge)
                    insertChangeEdge++;
            }
            else if(c instanceof VertexChange)
            {
                if(c.getTypeOfChange()== ChangeType.deleteVertex)
                    deleteChangeVertex++;
                else if(c.getTypeOfChange()== ChangeType.insertVertex)
                    insertChangeVertex++;
            }
            else if(c instanceof AttributeChange)
            {
                if(c.getTypeOfChange()== ChangeType.deleteAttr)
                    deleteChangeAttribute++;
                else if(c.getTypeOfChange()== ChangeType.insertAttr)
                    insertChangeAttribute++;
                else
                    changeAttributeValue++;
            }
        }


//        System.out.println("Size: " + allChanges.size());
//        ArrayList<List<Change>> newSmallerChanges = new ArrayList<>();
//        List<Change> t = new ArrayList<>();
//        for (int i=0;i<allChanges.size()/4;i++)
//            t.add(allChanges.get(i));
//        newSmallerChanges.add(t);
//        t = new ArrayList<>();
//        for (int i=allChanges.size()/4 ; i<((allChanges.size()*2)/4) ; i++)
//            t.add(allChanges.get(i));
//        newSmallerChanges.add(t);
//        t = new ArrayList<>();
//        for (int i=((allChanges.size()*2)/4) ; i<((allChanges.size()*3)/4) ; i++)
//            t.add(allChanges.get(i));
//        newSmallerChanges.add(t);
//        t = new ArrayList<>();
//        for (int i=((allChanges.size()*3)/4);i<allChanges.size();i++)
//            t.add(allChanges.get(i));
//        newSmallerChanges.add(t);
//        int i=1;
//        for (List<Change> small:newSmallerChanges) {
            try {
                final StringWriter sw =new StringWriter();
                final ObjectMapper mapper = new ObjectMapper();
                mapper.writeValue(sw, allChanges);
                FileWriter file = new FileWriter("./changes_t" + t1 + "_t" + t2 + "_" + tgfdName  + ".json");
                file.write(sw.toString());
                file.close();
                System.out.println("Successfully wrote to the file.");
                sw.close();
//                i++;
            } catch (IOException e) {
                e.printStackTrace();
            }
//        }

        System.out.println("Total number of changes: " + allChanges.size());
        System.out.println("Edges: +" + insertChangeEdge + " ** -" + deleteChangeEdge);
        System.out.println("Vertices: +" + insertChangeVertex + " ** -" + deleteChangeVertex);
        System.out.println("Attributes: +" + insertChangeAttribute + " ** -" + deleteChangeAttribute +" ** updates: "+ changeAttributeValue);
    }
}
