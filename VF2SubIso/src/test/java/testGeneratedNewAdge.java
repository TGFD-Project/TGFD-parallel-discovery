import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.iterators.IteratorEnumeration;
import org.apache.jena.ext.com.google.common.collect.Sets;

import java.io.*;
import java.util.*;

public class testGeneratedNewAdge {

    public static final String matchFilesPath = "/Users/roy/IdeaProjects/TGFD-parallel-discovery/";
    public static final String dataPath = "/Users/roy/Desktop/TGFD/dbpedia-50000";
    public static final Map<String, String> villageCountry = Maps.newHashMap();
    public static final Map<String, Set<String>> villageSettlementMap = Maps.newHashMap();
    public static final Map<String, String> settlementCountryMap = Maps.newHashMap();

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        pairVillageCountryAndVillageSettlement();
        createSettlementCountryMap();
        addCountry2Settlement();


        System.out.println("");
    }

    public static void pairVillageCountryAndVillageSettlement() {
        BufferedReader br = null;
        try {
            File file1 = new File(dataPath + "/2014/2014-50000.ttl");
            File file2 = new File(dataPath + "/2015/2015-50000.ttl");
            File file3 = new File(dataPath + "/2016/2016-50000.ttl");
            File file4 = new File(dataPath + "/2017/2017-50000.ttl");
            FileInputStream f1 = new FileInputStream(file1);
            FileInputStream f2 = new FileInputStream(file2);
            FileInputStream f3 = new FileInputStream(file3);
            FileInputStream f4 = new FileInputStream(file4);

            SequenceInputStream sis = new SequenceInputStream(new IteratorEnumeration<>(Arrays.asList(f1, f2, f3, f4).iterator()));
            br = new BufferedReader(new InputStreamReader(sis));

            String line = br.readLine().toLowerCase();
            while (line != null) {
                if (line.startsWith("<http://dbpedia.org/resource/")) {
                    String nodeURI = line.toLowerCase().substring(line.lastIndexOf("/") + 1, line.lastIndexOf(">"));
                    line = br.readLine().toLowerCase();
                    boolean isVillage = line.toLowerCase().substring(line.lastIndexOf("/") + 1, line.lastIndexOf(">")).equals("village");
                    if (isVillage) {
                        line = br.readLine().toLowerCase();
                        // Extract Country from data
                        // Return (village, country)
                        boolean isCountry = line.toLowerCase().substring(line.lastIndexOf("/") + 1, line.lastIndexOf(">")).equals("country");
                        if (isCountry) {
                            line = br.readLine();
                            String country = line.substring(line.lastIndexOf("/") + 1, line.lastIndexOf(">"));
                            villageCountry.put(nodeURI, country);

                            line = br.readLine();
                            // Extract Settlement from data
                            // Return (village, Sets of Settlements)
                            boolean isPartOf = line.substring(line.lastIndexOf("/") + 1, line.lastIndexOf(">")).equals("isPartOf");
                            if (isPartOf) {
                                line = br.readLine();
                                Set<String> sets = Sets.newHashSet();
                                String[] dataSets = line.replaceAll(" ", "").replaceAll(";", "").split(">");
                                for (String originData : dataSets) {
                                    String settlement = originData.substring(originData.lastIndexOf("/") + 1);
                                    sets.add(settlement);
                                }
                                villageSettlementMap.put(nodeURI, sets);
                            }
                        }
                    }
                }
                line = br.readLine().toLowerCase();
            }
            System.out.println("Pair of (Village->Country) Size: " + villageCountry.size());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    public static void addCountry2Settlement() {
        BufferedReader br = null;
        BufferedWriter bw = null;
        try {
            br = new BufferedReader(new FileReader(dataPath + "/2017/2017-50000.ttl"));
            bw = new BufferedWriter(new FileWriter(dataPath + "/new/2017-50000.ttl"));

            String line = br.readLine().toLowerCase();
            bw.write(line);
            bw.newLine();
            bw.flush();
            while (line != null) {
                if (line.startsWith("<http://dbpedia.org/resource/")) {
                    boolean countryExist = false;
                    String nodeURI = line.toLowerCase().substring(line.lastIndexOf("/") + 1, line.lastIndexOf(">"));
                    line = br.readLine().toLowerCase();
                    bw.write(line);
                    bw.newLine();
                    bw.flush();
                    boolean isSettlement = line.toLowerCase().substring(line.lastIndexOf("/") + 1, line.lastIndexOf(">")).equals("settlement");
                    if (isSettlement) {
                        line = br.readLine();
                        bw.write(line);
                        bw.newLine();
                        bw.flush();
                        while (!line.replaceAll(" ", "").equals("<http://xmlns.com/foaf/0.1/name>")) {
                            if (line.replaceAll(" ", "").equals("<http://dbpedia.org/ontology/country>")) {
                                countryExist = true;
                            }
                            line = br.readLine();
                            bw.write(line);
                            bw.newLine();
                            bw.flush();
                        }
                        // 出来后加一行
                        line = br.readLine();
                        String country = settlementCountryMap.getOrDefault(nodeURI, "");
                        if (!countryExist) {
                            if (StringUtils.isNotBlank(country)) {
                                line = line.replace(".", ";");
                            }
                        }
                        bw.write(line);
                        bw.newLine();
                        bw.flush();
                        if (!countryExist) {
                            // insert country to settlement
//                            String country = settlementCountryMap.getOrDefault(nodeURI, "");
                            if (StringUtils.isNotBlank(country)) {
                                System.out.println("Settlement " + nodeURI);
                                System.out.println("<http://dbpedia.org/ontology/country>");
                                System.out.println("<http://dbpedia.org/resource/" + country + "> ;");
                                System.out.println("==========================================");
                                bw.write("<http://dbpedia.org/ontology/country>");
                                bw.newLine();
                                bw.write("<http://dbpedia.org/resource/" + country + "> .");
                                bw.newLine();
                                bw.flush();
                            }
                        }
                    }
                }
                line = br.readLine();
                bw.write(line);
                bw.newLine();
                bw.flush();
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
                if (bw != null) {
                    bw.close();
                }
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
        }
    }

//    public static void addCountry2Settlement() {
//        BufferedReader br = null;
//        try {
//            File file1 = new File(dataPath + "/2014/2014-50000.ttl");
//            File file2 = new File(dataPath + "/2015/2015-50000.ttl");
//            File file3 = new File(dataPath + "/2016/2016-50000.ttl");
//            File file4 = new File(dataPath + "/2017/2017-50000.ttl");
//            FileInputStream f1 = new FileInputStream(file1);
//            FileInputStream f2 = new FileInputStream(file2);
//            FileInputStream f3 = new FileInputStream(file3);
//            FileInputStream f4 = new FileInputStream(file4);
//
//            SequenceInputStream sis = new SequenceInputStream(new IteratorEnumeration<>(Arrays.asList(f1, f2, f3, f4).iterator()));
//            br = new BufferedReader(new InputStreamReader(sis));
//
//            String line = br.readLine().toLowerCase();
//            while (line != null) {
//                if (line.startsWith("<http://dbpedia.org/resource/")) {
//                    boolean countryExist = false;
//                    String nodeURI = line.toLowerCase().substring(line.lastIndexOf("/") + 1, line.lastIndexOf(">"));
//                    line = br.readLine().toLowerCase();
//                    boolean isSettlement = line.toLowerCase().substring(line.lastIndexOf("/") + 1, line.lastIndexOf(">")).equals("settlement");
//                    if (isSettlement) {
//                        line = br.readLine();
//                        while (!line.replaceAll(" ", "").equals("<http://xmlns.com/foaf/0.1/name>")) {
//                            if (line.replaceAll(" ","").equals("<http://dbpedia.org/ontology/country>")) {
//                                countryExist = true;
//                            }
//                            line = br.readLine();
//                        }
//                        // 出来后加一行
//                        line = br.readLine();
//                        if (!countryExist) {
//                            // insert country to settlement
//                            String country = settlementCountryMap.getOrDefault(nodeURI, "");
//                            if (StringUtils.isNotBlank(country)) {
//                                System.out.println("Settlement " + nodeURI);
//                                System.out.println("<http://dbpedia.org/ontology/country>");
//                                System.out.println("<http://dbpedia.org/resource/" + country + "> ;");
//                                System.out.println("==========================================");
//                            }
//                        }
//                    }
//                }
//                line = br.readLine();
//            }
//        } catch (Exception e) {
//            System.out.println(e.getMessage());
//        } finally {
//            try {
//                if (br != null) {
//                    br.close();
//                }
//            } catch (IOException e) {
//                System.out.println(e.getMessage());
//            }
//        }
//    }

    public static void createSettlementCountryMap() {
        villageSettlementMap.forEach((k, v) -> {
            String country = villageCountry.getOrDefault(k, "");
            if (StringUtils.isNotBlank(country)) {
                for (String settlement : v) {
                    settlementCountryMap.putIfAbsent(settlement.toLowerCase(), country);
                }
            }
        });
    }
}
