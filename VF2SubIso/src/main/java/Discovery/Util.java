package Discovery;

import ChangeExploration.AttributeChange;
import ChangeExploration.Change;
import ChangeExploration.ChangeType;
import ChangeExploration.TypeChange;
import ICs.TGFD;
import Infra.*;
import Loader.GraphLoader;
import org.apache.commons.cli.*;
import org.apache.commons.math3.util.CombinatoricsUtils;
import org.apache.jena.rdf.model.Model;
import org.jetbrains.annotations.NotNull;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Util {

    //region --[Static Fields: Public]---------------------------------------

    public static final String TYPE_PREDICATE_URI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    public static final int DEFAULT_NUM_OF_SNAPSHOTS = 3;
    public static final String NO_REUSE_MATCHES_PARAMETER_TEXT = "noReuseMatches";
    public static final String CHANGEFILE_PARAMETER_TEXT = "changefile";
    public static final int DEFAULT_MAX_LITERALS_NUM = 0;
    public static final String FREQUENT_SIZE_SET_PARAM = "f";
    public static final String MAX_LIT_PARAM = "maxLit";
    public static final String PATH_PARAM = "path";
    public static String changefilePath = ".";
    public static int INDIVIDUAL_SUPER_VERTEX_INDEGREE_FLOOR = 25;
    public static double MEDIAN_SUPER_VERTEX_TYPE_INDEGREE_FLOOR = 25.0;
    public static final double DEFAULT_MAX_SUPER_VERTEX_DEGREE = 1500.0;
    public static final double DEFAULT_AVG_SUPER_VERTEX_DEGREE = 30.0;
    public static boolean fastMatching = true;
    public static int maxNumOfLiterals = DEFAULT_MAX_LITERALS_NUM;
    public static int T = DEFAULT_NUM_OF_SNAPSHOTS;
    public static boolean dissolveSuperVerticesBasedOnCount = false;
    public static double superVertexDegree = MEDIAN_SUPER_VERTEX_TYPE_INDEGREE_FLOOR;
    public static boolean useOptChangeFile = true;
    public static boolean dissolveSuperVertexTypes = false;
    public static boolean validationSearch = false;
    public static String path = ".";
    public static int numOfSnapshots;
    public static final int DEFAULT_FREQUENT_SIZE_SET = 25;
    public static final int DEFAULT_GAMMA = 20;
    public static final int DEFAULT_K = 3;
    public static final double DEFAULT_TGFD_THETA = 0.25;
    public static final double DEFAULT_PATTERN_THETA = 0.05;
    public static boolean reUseMatches = true;
    public static boolean generatek0Tgfds = false;
    public static boolean skipK1 = false;
    public static Integer numOfEdgesInAllGraphs = 0;
    public static int numOfVerticesInAllGraphs = 0;
    public static Map<String, Set<String>> vertexTypesToActiveAttributesMap; // freq attributes come from here
    public static PatternTree patternTree;
    public static boolean hasMinimalityPruning = true;
    public static String graphSize = null;
    public static boolean onlyInterestingTGFDs = true;
    public static int k = DEFAULT_K;
    public static double tgfdTheta = DEFAULT_TGFD_THETA;
    public static double patternTheta = DEFAULT_PATTERN_THETA;
    public static int gamma = DEFAULT_GAMMA;
    public static int frequentSetSize = DEFAULT_FREQUENT_SIZE_SET;
    public static Set<String> activeAttributesSet = null;
    public static int previousLevelNodeIndex = 0;
    public static int candidateEdgeIndex = 0;
    public static int currentVSpawnLevel = 0;
    public static ArrayList<ArrayList<TGFD>> discoveredTgfds;
    public static long discoveryStartTime;
    public static final ArrayList<Long> kRuntimes = new ArrayList<>();
    public static String experimentStartTimeAndDateStamp = null;
    public static boolean kExperiment = false;
    public static boolean useChangeFile = false;
    public static List<Map.Entry<String, Integer>> sortedVertexHistogram; // freq nodes come from here
    public static List<Map.Entry<String, Integer>> sortedFrequentEdgesHistogram = null; // freq edges come from here
    public static Map<String, Integer> vertexHistogram;
    public static boolean hasSupportPruning = true;
    public static final List<Double> medianPatternSupportsList = new ArrayList<>();
    public static ArrayList<Double> patternSupportsListForThisSnapshot = new ArrayList<>();
    public static final List<Double> medianConstantTgfdSupportsList = new ArrayList<>();
    public static ArrayList<Double> constantTgfdSupportsListForThisSnapshot = new ArrayList<>();
    public static final List<Double> medianGeneralTgfdSupportsList = new ArrayList<>();
    public static ArrayList<Double> generalTgfdSupportsListForThisSnapshot = new ArrayList<>();
    public static final ArrayList<Double> vertexFrequenciesList = new ArrayList<>();
    public static final ArrayList<Double> edgeFrequenciesList = new ArrayList<>();
    public static final List<Long> totalSupergraphCheckingTime = new ArrayList<>();
    public static final List<Long> totalVisitedPathCheckingTime = new ArrayList<>();
    // TODO: Replace with RuntimeList
    public static final List<Long> totalMatchingTime = new ArrayList<>();
    public static final List<Long> totalSupersetPathCheckingTime = new ArrayList<>();
    public static final List<Long> totalFindEntitiesTime = new ArrayList<>();
    public static final List<Long> totalVSpawnTime = new ArrayList<>();
    public static final List<Long> totalDiscoverConstantTGFDsTime = new ArrayList<>();
    public static final List<Long> totalDiscoverGeneralTGFDTime = new ArrayList<>();
    public static String experimentName;
    public static String loader;
    public static List<Map.Entry<String, List<String>>> timestampToFilesMap = new ArrayList<>();
    public static HashMap<String, JSONArray> changeFilesMap = null;
    public static List<GraphLoader> graphs;
    public static boolean isStoreInMemory = true;
    public static Map<String, Double> vertexTypesToAvgInDegreeMap = new HashMap<>();
    public static Model firstSnapshotTypeModel = null;
    public static Model firstSnapshotDataModel = null;
    public static long totalHistogramTime = 0;
    public static final Set<String> interestLabelsSet = new HashSet<>();
    public static final List<Integer> rhsInconsistencies = new ArrayList<>();
    public static int numOfConsistentRHS = 0;
    public static PrintStream logStream = null;
    public static PrintStream summaryStream = null;
    public static boolean printToLogFile = false;
    public static int numOfCandidateGeneralTGFDs = 0;
    public static Map<String, Set<String>> typeChangeURIs;
    public static boolean isIncremental = false;

    //endregion

    public static void config(String []args)
    {
        String timeAndDateStamp = ZonedDateTime.now(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern("uuuu.MM.dd.HH.mm.ss"));
        experimentStartTimeAndDateStamp = timeAndDateStamp;
        discoveryStartTime = System.currentTimeMillis();

        Options options = initializeCmdOptions();
        CommandLine cmd = parseArgs(options, args);

        if (cmd.hasOption(PATH_PARAM)) {
            path = cmd.getOptionValue(PATH_PARAM).replaceFirst("^~", System.getProperty("user.home"));
            if (!Files.isDirectory(Paths.get(path))) {
                System.out.println("Dataset path " + Paths.get(path) + " is not a valid directory.");
                System.exit(1);
            }
            graphSize = Paths.get(path).getFileName().toString();
        }

        if (!cmd.hasOption("loader")) {
            System.out.println("No loader is specified.");
            System.exit(1);
        } else {
            loader = cmd.getOptionValue("loader");
        }

        isStoreInMemory = !cmd.hasOption("dontStore");

        if (cmd.hasOption("name")) {
            experimentName = cmd.getOptionValue("name");
        } else  {
            experimentName = "experiment";
        }

        printToLogFile = !cmd.hasOption("console");
        divertOutputToLogFile();

        hasMinimalityPruning = !cmd.hasOption("noMinimalityPruning");
        hasSupportPruning = (!cmd.hasOption("noSupportPruning"));
        onlyInterestingTGFDs = (!cmd.hasOption("uninteresting"));
        useChangeFile = (false);
        boolean validationSearchTemp = false;
        boolean reUseMatchesTemp = !cmd.hasOption(NO_REUSE_MATCHES_PARAMETER_TEXT);
        if (cmd.hasOption("validation")) {
            validationSearchTemp = true;
            reUseMatchesTemp = false;
        }
        if (cmd.hasOption(CHANGEFILE_PARAMETER_TEXT)) {
            useChangeFile = (true);
            if (cmd.getOptionValue(CHANGEFILE_PARAMETER_TEXT).equalsIgnoreCase("all"))
                useOptChangeFile = (false);
            else if (cmd.getOptionValue(CHANGEFILE_PARAMETER_TEXT).equalsIgnoreCase("opt"))
                useOptChangeFile = (true);
            else {
                System.out.println("Invalid option \"" + cmd.getOptionValue(CHANGEFILE_PARAMETER_TEXT) + "\" for changefile.");
                System.exit(1);
            }
            if (cmd.hasOption("changefilePath")) {
                changefilePath = cmd.getOptionValue("changefilePath").replaceFirst("^~", System.getProperty("user.home"));
                if (!Files.isDirectory(Paths.get(path))) {
                    System.out.println("Changefiles path " + Paths.get(path) + " is not a valid directory.");
                    System.exit(1);
                }
            } else {
                System.out.println("Changefiles path not specified.");
                System.exit(1);
            }
        }
        if (cmd.hasOption("incremental"))
            isIncremental = (true);

        reUseMatches = (reUseMatchesTemp);
        validationSearch = (validationSearchTemp);

        generatek0Tgfds = (cmd.hasOption("k0"));
        skipK1 = (cmd.hasOption("skipK1"));

        T = (cmd.getOptionValue("t") == null ? Util.DEFAULT_NUM_OF_SNAPSHOTS : Integer.parseInt(cmd.getOptionValue("t")));
        numOfSnapshots = (T);
        gamma = (cmd.getOptionValue("a") == null ? Util.DEFAULT_GAMMA : Integer.parseInt(cmd.getOptionValue("a")));
        maxNumOfLiterals = (cmd.getOptionValue(MAX_LIT_PARAM) == null ? DEFAULT_MAX_LITERALS_NUM : Integer.parseInt(cmd.getOptionValue(MAX_LIT_PARAM)));
        tgfdTheta = (cmd.getOptionValue("theta") == null ? Util.DEFAULT_TGFD_THETA : Double.parseDouble(cmd.getOptionValue("theta")));
        patternTheta = (cmd.getOptionValue("pTheta") == null ? tgfdTheta : Double.parseDouble(cmd.getOptionValue("pTheta")));
        k = (cmd.getOptionValue("k") == null ? Util.DEFAULT_K : Integer.parseInt(cmd.getOptionValue("k")));
        frequentSetSize = (cmd.getOptionValue(FREQUENT_SIZE_SET_PARAM) == null ? Util.DEFAULT_FREQUENT_SIZE_SET : Integer.parseInt(cmd.getOptionValue(FREQUENT_SIZE_SET_PARAM)));

        initializeTgfdLists();

        if (cmd.hasOption("K")) {
            experimentName = "vary-k";
            kExperiment = (true);
        }

        if (cmd.hasOption("simplifySuperVertexTypes")) {
            MEDIAN_SUPER_VERTEX_TYPE_INDEGREE_FLOOR = Double.parseDouble(cmd.getOptionValue("simplifySuperVertexTypes"));
            dissolveSuperVertexTypes = true;
        } else if (cmd.hasOption("simplifySuperVertex")) {
            INDIVIDUAL_SUPER_VERTEX_INDEGREE_FLOOR = Integer.parseInt(cmd.getOptionValue("simplifySuperVertex"));
            dissolveSuperVerticesBasedOnCount = (true);
        }

        switch (loader.toLowerCase()) {
            case "dbpedia": setDBpediaTimestampsAndFilePaths(path); break;
            case "citation": setCitationTimestampsAndFilePaths(); break;
            case "imdb": setImdbTimestampToFilesMapFromPath(path); break;
            case "synthetic": setSyntheticTimestampToFilesMapFromPath(path); break;
            default: throw new IllegalArgumentException("No valid loader specified.");
        }

        if (cmd.hasOption("interestLabels")) {
            String[] interestLabels = cmd.getOptionValue("interestLabels").split(",");
            interestLabelsSet.addAll(Arrays.asList(interestLabels));
        }

        if (cmd.hasOption("slow"))
            fastMatching = (false);
    }

    private static Options initializeCmdOptions() {
        Options options = new Options();
        options.addOption("name", true, "output files will be given the specified name");
        options.addOption("console", false, "print to console");
        options.addOption("noMinimalityPruning", false, "run algorithm without minimality pruning");
        options.addOption("noSupportPruning", false, "run algorithm without support pruning");
        options.addOption("uninteresting", false, "run algorithm and also consider uninteresting TGFDs");
        options.addOption("g", true, "run experiment on a specific graph size");
        options.addOption("t", true, "run experiment using t number of snapshots");
        options.addOption("k", true, "run experiment for k iterations");
        options.addOption("a", true, "run experiment for specified active attribute set size");
        options.addOption("theta", true, "run experiment using a specific support threshold");
        options.addOption("pTheta", true, "run experiment using a specific pattern support threshold");
        options.addOption("K", false, "print runtime to file after each level i, where 0 <= i <= k");
        options.addOption(FREQUENT_SIZE_SET_PARAM, true, "run experiment using frequent set of p vertices and p edges");
        options.addOption(CHANGEFILE_PARAMETER_TEXT, true, "run experiment using changefiles instead of snapshots");
        options.addOption(NO_REUSE_MATCHES_PARAMETER_TEXT, false, "run experiment without reusing matches between levels");
        options.addOption("k0", false, "run experiment and generate tgfds for single-node patterns");
        options.addOption("loader", true, "run experiment using specified loader");
        options.addOption(PATH_PARAM, true, "path to dataset");
        options.addOption("changefilePath", true, "path to changefiles");
        options.addOption("skipK1", false, "run experiment and generate tgfds for k > 1");
        options.addOption("validation", false, "Run experiment without any pattern matching optimization. This is very slow. Only use for validation testing.");
        options.addOption("simplifySuperVertex", true, "run experiment by collapsing super vertices");
        options.addOption("simplifySuperVertexTypes", true, "run experiment by collapsing super vertex types");
        options.addOption("dontStore", false, "run experiment without storing changefiles in memory, read from disk");
        options.addOption(MAX_LIT_PARAM, true, "run experiment that outputs TGFDs with up n literals");
        options.addOption("slow", false, "run experiment without using fast matching");
        options.addOption("incremental", false, "run experiment using incremental matching");
        options.addOption("interestLabels", true, "run experiment using frequent sets of vertices and edges that contain labels of interest");
        return options;
    }

    public static void addToTotalFindEntitiesTime(long findEntitiesTime) {
        addToValueInListAtIndex(totalFindEntitiesTime, currentVSpawnLevel, findEntitiesTime);
    }

    private static void addToValueInListAtIndex(List<Long> list, int index, long valueToAdd) {
        while (list.size() <= index)
            list.add(0L);
        list.set(index, list.get(index)+valueToAdd);
    }

    private static CommandLine parseArgs(Options options, String[] args) {
        CommandLineParser parser = new BasicParser();// DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return cmd;
    }


    private static void setDBpediaTimestampsAndFilePaths(String path) {
        Map<String, List<String>> timestampToFilesMap = generateDbpediaTimestampToFilesMap(path);
        setTimestampToFilesMap(new ArrayList<>(timestampToFilesMap.entrySet()));
    }

    private static void setCitationTimestampsAndFilePaths() {
        ArrayList<String> filePaths = new ArrayList<>();
        filePaths.add("dblp_papers_v11.txt");
        filePaths.add("dblp.v12.json");
        filePaths.add("dblpv13.json");
        Map<String,List<String>> timestampstoFilePathsMap = new HashMap<>();
        int timestampName = 11;
        for (String filePath: filePaths) {
            timestampstoFilePathsMap.put(String.valueOf(timestampName), Collections.singletonList(filePath));
        }
        Util.setTimestampToFilesMap(new ArrayList<>(timestampstoFilePathsMap.entrySet()));
    }

    public static void setImdbTimestampToFilesMapFromPath(String path) {
        HashMap<String, List<String>> timestampToFilesMap = generateImdbTimestampToFilesMapFromPath(path);
        Util.setTimestampToFilesMap(new ArrayList<>(timestampToFilesMap.entrySet()));
    }

    @NotNull
    public static HashMap<String, List<String>> generateImdbTimestampToFilesMapFromPath(String path) {
        System.out.println("Searching for IMDB snapshots in path: "+ path);
        List<File> allFilesInDirectory = new ArrayList<>(Arrays.asList(Objects.requireNonNull(new File(path).listFiles(File::isFile))));
        System.out.println("Found files: "+allFilesInDirectory);
        List<File> ntFilesInDirectory = new ArrayList<>();
        for (File ntFile: allFilesInDirectory) {
            System.out.println("Is this an .nt file? "+ntFile.getName());
            if (ntFile.getName().endsWith(".nt")) {
                System.out.println("Found .nt file: "+ntFile.getPath());
                ntFilesInDirectory.add(ntFile);
            }
        }
        ntFilesInDirectory.sort(Comparator.comparing(File::getName));
        System.out.println("Found .nt files: "+ntFilesInDirectory);
        HashMap<String,List<String>> timestampToFilesMap = new HashMap<>();
        for (File ntFile: ntFilesInDirectory) {
            String regex = "^imdb-([0-9]+)\\.nt$";
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(ntFile.getName());
            if (matcher.find()) {
                String timestamp = matcher.group(1);
                timestampToFilesMap.putIfAbsent(timestamp, new ArrayList<>());
                timestampToFilesMap.get(timestamp).add(ntFile.getPath());
            }
        }
        System.out.println("TimestampToFilesMap...");
        System.out.println(timestampToFilesMap);
        return timestampToFilesMap;
    }

    public static void setSyntheticTimestampToFilesMapFromPath(String path) {
        HashMap<String, List<String>> timestampToFilesMap = generateSyntheticTimestampToFilesMapFromPath(path);
        Util.setTimestampToFilesMap(new ArrayList<>(timestampToFilesMap.entrySet()));
    }


    public static void setCurrentVSpawnLevel(int currentVSpawnLevel) {
        Util.currentVSpawnLevel = currentVSpawnLevel;
        System.out.println("VSpawn level " + Util.currentVSpawnLevel);
    }

    private static Long getTotalVSpawnTime(int index) {
        return index < totalVSpawnTime.size() ? totalVSpawnTime.get(index) : (long)0;
    }

    public static void addToTotalVSpawnTime(long vSpawnTime) {
        printWithTime("vSpawn", vSpawnTime);
        addToValueInListAtIndex(Util.totalVSpawnTime, Util.currentVSpawnLevel, vSpawnTime);
    }

    private static Long getTotalMatchingTime(int index) {
        return index < totalMatchingTime.size() ? totalMatchingTime.get(index) : (long)0;
    }

    public static void addToTotalMatchingTime(long matchingTime) {
        addToValueInListAtIndex(Util.totalMatchingTime, Util.currentVSpawnLevel, matchingTime);
    }

    public static void initializeTgfdLists() {
        discoveredTgfds = new ArrayList<>();
        for (int vSpawnLevel = 0; vSpawnLevel <= k; vSpawnLevel++) {
            discoveredTgfds.add(new ArrayList<>());
        }
    }
    public static JSONArray readJsonArrayFromFile(String changeFilePath) {
        System.out.println("Reading JSON array from file "+changeFilePath);
        JSONParser parser = new JSONParser();
        Object json;
        JSONArray jsonArray = null;
        try {
            json = parser.parse(new FileReader(changeFilePath));
            jsonArray = (JSONArray) json;
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        return jsonArray;
    }

    public static void sortChanges(List<Change> changes) {
        System.out.println("Number of changes: "+changes.size());
        HashMap<ChangeType, Integer> map = new HashMap<>();
        map.put(ChangeType.deleteAttr, 1);
        map.put(ChangeType.insertAttr, 3);
        map.put(ChangeType.changeAttr, 1);
        map.put(ChangeType.deleteEdge, 0);
        map.put(ChangeType.insertEdge, 3);
        map.put(ChangeType.changeType, 1);
        map.put(ChangeType.deleteVertex, 1);
        map.put(ChangeType.insertVertex, 2);
        changes.sort(new Comparator<Change>() {
            @Override
            public int compare(Change o1, Change o2) {
                return map.get(o1.getTypeOfChange()).compareTo(map.get(o2.getTypeOfChange()));
            }
        });
        System.out.println("Sorted changes.");
    }

    @NotNull
    public static List<Map.Entry<Integer, HashSet<Change>>> getSortedChanges(HashMap<Integer, HashSet<Change>> newChanges) {
        List<Map.Entry<Integer,HashSet<Change>>> sortedChanges = new ArrayList<>(newChanges.entrySet());
        HashMap<ChangeType, Integer> map = new HashMap<>();
        map.put(ChangeType.deleteAttr, 2);
        map.put(ChangeType.insertAttr, 4);
        map.put(ChangeType.changeAttr, 4);
        map.put(ChangeType.deleteEdge, 0);
        map.put(ChangeType.insertEdge, 5);
        map.put(ChangeType.changeType, 3);
        map.put(ChangeType.deleteVertex, 1);
        map.put(ChangeType.insertVertex, 1);
        sortedChanges.sort(new Comparator<Map.Entry<Integer, HashSet<Change>>>() {
            @Override
            public int compare(Map.Entry<Integer, HashSet<Change>> o1, Map.Entry<Integer, HashSet<Change>> o2) {
                if ((o1.getValue().iterator().next() instanceof AttributeChange || o1.getValue().iterator().next() instanceof TypeChange)
                        && (o2.getValue().iterator().next() instanceof AttributeChange || o2.getValue().iterator().next() instanceof TypeChange)) {
                    boolean o1containsTypeChange = false;
                    for (Change c: o1.getValue()) {
                        if (c instanceof TypeChange) {
                            o1containsTypeChange = true;
                            break;
                        }
                    }
                    boolean o2containsTypeChange = false;
                    for (Change c: o2.getValue()) {
                        if (c instanceof TypeChange) {
                            o2containsTypeChange = true;
                            break;
                        }
                    }
                    if (o1containsTypeChange && !o2containsTypeChange) {
                        return -1;
                    } else if (!o1containsTypeChange && o2containsTypeChange) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
                return map.get(o1.getValue().iterator().next().getTypeOfChange()).compareTo(map.get(o2.getValue().iterator().next().getTypeOfChange()));
            }
        });
        return sortedChanges;
    }


    public static void setTimestampToFilesMap(List<Map.Entry<String, List<String>>> timestampToFilesMap) {
        timestampToFilesMap.sort(Map.Entry.comparingByKey());
        Util.timestampToFilesMap = timestampToFilesMap.subList(0,Math.min(timestampToFilesMap.size(),T));
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }


    public String getGraphSize() {
        return graphSize;
    }

    public void setGraphSize(String graphSize) {
        this.graphSize = graphSize;
    }


    private static Long getTotalSupergraphCheckingTime(int index) {
        return returnLongAtIndexIfExistsElseZero(totalSupergraphCheckingTime, index);
    }

    public static void addToTotalSupergraphCheckingTime(long supergraphCheckingTime) {
        addToValueInListAtIndex(Util.totalSupergraphCheckingTime, Util.currentVSpawnLevel, supergraphCheckingTime);
    }

    private static Long getTotalVisitedPathCheckingTime(int index) {
        return returnLongAtIndexIfExistsElseZero(totalVisitedPathCheckingTime, index);
    }

    public static void addToTotalVisitedPathCheckingTime(long visitedPathCheckingTime) {
        addToValueInListAtIndex(totalVisitedPathCheckingTime, currentVSpawnLevel, visitedPathCheckingTime);
    }

    public static void setValueInListAtIndex(List<Double> list, int index, Double value) {
        while (list.size() <= index) {
            list.add(0.0);
        }
        list.set(index, value);
    }

    private static Long getTotalSupersetPathCheckingTime(int index) {
        return returnLongAtIndexIfExistsElseZero(totalSupersetPathCheckingTime, index);
    }

    public static void addToTotalSupersetPathCheckingTime(long supersetPathCheckingTime) {
        addToValueInListAtIndex(totalSupersetPathCheckingTime, currentVSpawnLevel, supersetPathCheckingTime);
    }


    private static Long getTotalFindEntitiesTime(int index) {
        return returnLongAtIndexIfExistsElseZero(totalFindEntitiesTime, index);
    }


    private static Long getTotalDiscoverConstantTGFDsTime(int index) {
        return returnLongAtIndexIfExistsElseZero(totalDiscoverConstantTGFDsTime, index);
    }

    public static void addToTotalDiscoverConstantTGFDsTime(long discoverConstantTGFDsTime) {
        addToValueInListAtIndex(totalDiscoverConstantTGFDsTime, currentVSpawnLevel, discoverConstantTGFDsTime);
    }


    private static Long getTotalDiscoverGeneralTGFDTime(int index) {
        return returnLongAtIndexIfExistsElseZero(Util.totalDiscoverGeneralTGFDTime, index);
    }

    public static void addToTotalDiscoverGeneralTGFDTime(long discoverGeneralTGFDTime) {
        addToValueInListAtIndex(totalDiscoverGeneralTGFDTime, currentVSpawnLevel, discoverGeneralTGFDTime);
    }

    public static Long returnLongAtIndexIfExistsElseZero(List<Long> list, int index) {
        return index < list.size() ? list.get(index) : (long)0;
    }

    public static Double getMedianPatternSupportsList(int index) {
        return returnDoubleAtIndexIfExistsElseZero(Util.medianPatternSupportsList, index);
    }

    private static void addToMedianPatternSupportsList(double medianPatternSupport) {
        setValueInListAtIndex(Util.medianPatternSupportsList, Util.currentVSpawnLevel, medianPatternSupport);
    }

    public static Double getMedianConstantTgfdSupportsList(int index) {
        return returnDoubleAtIndexIfExistsElseZero(Util.medianConstantTgfdSupportsList, index);
    }

    private static void addToMedianConstantTgfdSupportsList(double medianConstantTgfdSupport) {
        setValueInListAtIndex(Util.medianConstantTgfdSupportsList, Util.currentVSpawnLevel, medianConstantTgfdSupport);
    }

    public static Double getMedianGeneralTgfdSupportsList(int index) {
        return returnDoubleAtIndexIfExistsElseZero(Util.medianGeneralTgfdSupportsList, index);
    }

    private static void addToMedianGeneralTgfdSupportsList(double medianGeneralTgfdSupport) {
        setValueInListAtIndex(Util.medianGeneralTgfdSupportsList, Util.currentVSpawnLevel, medianGeneralTgfdSupport);
    }

    public static Double returnDoubleAtIndexIfExistsElseZero(List<Double> list, int index) {
        return index < list.size() ? list.get(index) : (double)0;
    }


    public static Map<String, Set<String>> getVertexTypesToActiveAttributesMap() {
        return vertexTypesToActiveAttributesMap;
    }

    public static void printTgfdsToFile(String experimentName, ArrayList<TGFD> tgfds) {
        tgfds.sort(new Comparator<TGFD>() {
            @Override
            public int compare(TGFD o1, TGFD o2) {
                return o2.getSupport().compareTo(o1.getSupport());
            }
        });
        System.out.println("Printing TGFDs to file for k = " + currentVSpawnLevel);
        try {
            PrintStream printStream = new PrintStream(experimentName + "-tgfds" + experimentStartTimeAndDateStamp + ".txt");
            printStream.println("k = " + currentVSpawnLevel);
            printStream.println("# of TGFDs generated = " + tgfds.size());
            for (TGFD tgfd : tgfds) {
                printStream.println(tgfd);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void printExperimentRuntimestoFile() {
        try {
            PrintStream printStream = new PrintStream(Util.experimentName + "-runtimes-" + Util.experimentStartTimeAndDateStamp + ".txt");
            for (int i  = 0; i < Util.kRuntimes.size(); i++) {
                printStream.print("k = " + i);
                printStream.println(", execution time = " + Util.kRuntimes.get(i));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }


    public static void printSupportStatisticsForThisSnapshot(int level) {
        divertOutputToSummaryFile();
        System.out.println("----------------Support Statistics for vSpawn level "+level+"-----------------");
        System.out.println("Median Pattern Support: " + Util.getMedianPatternSupportsList(level));
        System.out.println("Median Constant TGFD Support: " + Util.getMedianConstantTgfdSupportsList(level));
        System.out.println("Median General TGFD Support: " + Util.getMedianGeneralTgfdSupportsList(level));
        divertOutputToLogFile();
    }

    public static void printSupportStatisticsForThisSnapshot() {
        divertOutputToSummaryFile();
        System.out.println("----------------Support Statistics for vSpawn level "+currentVSpawnLevel+"-----------------");

        Collections.sort(Util.patternSupportsListForThisSnapshot);
        Collections.sort(Util.constantTgfdSupportsListForThisSnapshot);
        Collections.sort(Util.generalTgfdSupportsListForThisSnapshot);

        double medianPatternSupport = 0;
        if (Util.patternSupportsListForThisSnapshot.size() > 0) {
            medianPatternSupport = Util.patternSupportsListForThisSnapshot.size() % 2 != 0 ? Util.patternSupportsListForThisSnapshot.get(Util.patternSupportsListForThisSnapshot.size() / 2) : ((Util.patternSupportsListForThisSnapshot.get(Util.patternSupportsListForThisSnapshot.size() / 2) + Util.patternSupportsListForThisSnapshot.get(Util.patternSupportsListForThisSnapshot.size() / 2 - 1)) / 2);
        }
        Util.addToMedianPatternSupportsList(medianPatternSupport);
        double medianConstantTgfdSupport = 0;
        if (Util.constantTgfdSupportsListForThisSnapshot.size() > 0) {
            medianConstantTgfdSupport = Util.constantTgfdSupportsListForThisSnapshot.size() % 2 != 0 ? Util.constantTgfdSupportsListForThisSnapshot.get(Util.constantTgfdSupportsListForThisSnapshot.size() / 2) : ((Util.constantTgfdSupportsListForThisSnapshot.get(Util.constantTgfdSupportsListForThisSnapshot.size() / 2) + Util.constantTgfdSupportsListForThisSnapshot.get(Util.constantTgfdSupportsListForThisSnapshot.size() / 2 - 1)) / 2);
        }
        Util.addToMedianConstantTgfdSupportsList(medianConstantTgfdSupport);
        double medianGeneralTgfdSupport = 0;
        if (Util.generalTgfdSupportsListForThisSnapshot.size() > 0) {
            medianGeneralTgfdSupport = Util.generalTgfdSupportsListForThisSnapshot.size() % 2 != 0 ? Util.generalTgfdSupportsListForThisSnapshot.get(Util.generalTgfdSupportsListForThisSnapshot.size() / 2) : ((Util.generalTgfdSupportsListForThisSnapshot.get(Util.generalTgfdSupportsListForThisSnapshot.size() / 2) + Util.generalTgfdSupportsListForThisSnapshot.get(Util.generalTgfdSupportsListForThisSnapshot.size() / 2 - 1)) / 2);
        }
        Util.addToMedianGeneralTgfdSupportsList(medianGeneralTgfdSupport);

        System.out.println("Median Pattern Support: " + medianPatternSupport);
        System.out.println("Median Constant TGFD Support: " + medianConstantTgfdSupport);
        System.out.println("Median General TGFD Support: " + medianGeneralTgfdSupport);
        // Reset for each level of vSpawn
        Util.patternSupportsListForThisSnapshot = new ArrayList<>();
        Util.constantTgfdSupportsListForThisSnapshot = new ArrayList<>();
        Util.generalTgfdSupportsListForThisSnapshot = new ArrayList<>();
        divertOutputToLogFile();
    }

    public static void printTimeStatisticsForThisSnapshot(int level) {
        divertOutputToSummaryFile();
        System.out.println("----------------Time Statistics for vSpawn level "+level+"-----------------");
        printWithTime("Total vSpawn", Util.getTotalVSpawnTime(level)-Util.getTotalSupergraphCheckingTime(level));
        printWithTime("Total Supergraph Checking", Util.getTotalSupergraphCheckingTime(level));
        printWithTime("Total Matching", Util.getTotalMatchingTime(level));
        printWithTime("Total Visited Path Checking", Util.getTotalVisitedPathCheckingTime(level));
        printWithTime("Total Superset Path Checking", Util.getTotalSupersetPathCheckingTime(level));
        printWithTime("Total Find Entities", Util.getTotalFindEntitiesTime(level));
        printWithTime("Total Discover Constant TGFDs", Util.getTotalDiscoverConstantTGFDsTime(level));
        printWithTime("Total Discover General TGFD", Util.getTotalDiscoverGeneralTGFDTime(level));
        printWithTime("As of k = " + level + ", execution", Util.kRuntimes.get(level));
        divertOutputToLogFile();
    }

    public static void printTimeStatistics() {
        divertOutputToSummaryFile();
        System.out.println("----------------Total Time Statistics-----------------");
        printWithTime("Total Histogram", Util.totalHistogramTime);
        printWithTime("Total vSpawn", Util.totalVSpawnTime.stream().reduce(0L, Long::sum)-Util.totalSupergraphCheckingTime.stream().reduce(0L, Long::sum));
        printWithTime("Total Supergraph Checking", Util.totalSupergraphCheckingTime.stream().reduce(0L, Long::sum));
        printWithTime("Total Matching", Util.totalMatchingTime.stream().reduce(0L, Long::sum));
        printWithTime("Total Visited Path Checking", Util.totalVisitedPathCheckingTime.stream().reduce(0L, Long::sum));
        printWithTime("Total Superset Path Checking", Util.totalSupersetPathCheckingTime.stream().reduce(0L, Long::sum));
        printWithTime("Total Find Entities", Util.totalFindEntitiesTime.stream().reduce(0L, Long::sum));
        printWithTime("Total Discover Constant TGFDs", Util.totalDiscoverConstantTGFDsTime.stream().reduce(0L, Long::sum));
        printWithTime("Total Discover General TGFD", Util.totalDiscoverGeneralTGFDTime.stream().reduce(0L, Long::sum));
        System.out.println("----------------Additional Statistics-----------------");
        System.out.println("Number of candidate constant TGFDs: "+(Util.numOfConsistentRHS+Util.rhsInconsistencies.size()));
        System.out.println("Number of consistent candidate constant TGFDs: "+Util.numOfConsistentRHS);
        System.out.println("Number of inconsistent candidate constant TGFDs: "+Util.rhsInconsistencies.size());
        System.out.println("Average number of inconsistencies per inconsistent candidate constant TGFD: "+((double)Util.rhsInconsistencies.stream().reduce(0, Integer::sum) / (double)Util.rhsInconsistencies.size()));
        System.out.println("Number of candidate general TGFDs: "+Util.numOfCandidateGeneralTGFDs);
        List<Integer> intervalWidths = Util.discoveredTgfds.stream().map(list -> list.stream().map(tgfd -> tgfd.getDelta() == null ? 0 : tgfd.getDelta().getIntervalWidth()).collect(Collectors.toList())).flatMap(List::stream).sorted(Comparator.naturalOrder()).collect(Collectors.toList());
        if (intervalWidths.size() > 0) {
            System.out.println("Minimum delta interval width: " + intervalWidths.get(0));
            System.out.println("Maximum delta interval width: " + intervalWidths.get(intervalWidths.size() - 1));
            System.out.println("Average delta interval width: "+intervalWidths.stream().reduce(0, Integer::sum)/intervalWidths.size());
        } else {
            System.out.println("Cannot report statistics on delta interval widths. No TGFDs found in dataset");
        }
    }

    public static void divertOutputToSummaryFile() {
        if (printToLogFile) {
            String fileName = "tgfd-discovery-summary-" + experimentStartTimeAndDateStamp + ".txt";
            if (Util.summaryStream == null) {
                try {
                    Util.summaryStream = new PrintStream(fileName);
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
            }
            divertOutputToStream(Util.summaryStream);
        }
    }

    public static void divertOutputToLogFile() {
        if (Util.printToLogFile) {
            String fileName = "tgfd-discovery-log-" + Util.experimentStartTimeAndDateStamp + ".txt";
            if (Util.logStream == null) {
                try {
                    Util.logStream = new PrintStream(fileName);
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
            }
            divertOutputToStream(Util.logStream);
        }
    }

    public static void divertOutputToStream(PrintStream stream) {
        System.setOut(stream);
    }


    public static HashSet<ConstantLiteral> getActiveAttributesInPattern(Set<Vertex> vertexSet, boolean considerURI) {
        HashMap<String, HashSet<String>> patternVerticesAttributes = new HashMap<>();
        for (Vertex vertex : vertexSet) {
            for (String vertexType : vertex.getTypes()) {
                patternVerticesAttributes.put(vertexType, new HashSet<>());
                Set<String> attrNameSet = Util.getVertexTypesToActiveAttributesMap().get(vertexType);
                for (String attrName : attrNameSet) {
                    patternVerticesAttributes.get(vertexType).add(attrName);
                }
            }
        }
        HashSet<ConstantLiteral> literals = new HashSet<>();
        for (String vertexType : patternVerticesAttributes.keySet()) {
            if (considerURI) literals.add(new ConstantLiteral(vertexType,"uri",null));
            for (String attrName : patternVerticesAttributes.get(vertexType)) {
                ConstantLiteral literal = new ConstantLiteral(vertexType, attrName, null);
                literals.add(literal);
            }
        }
        return literals;
    }

    public static boolean literalPathIsMissingTypesInPattern(ArrayList<ConstantLiteral> parentsPathToRoot, Set<Vertex> patternVertexSet) {
        for (Vertex v : patternVertexSet) {
            boolean missingType = true;
            for (ConstantLiteral literal : parentsPathToRoot) {
                if (literal.getVertexType().equals(v.getTypes().iterator().next())) {
                    missingType = false;
                }
            }
            if (missingType) return true;
        }
        return false;
    }

    public static boolean isUsedVertexType(String vertexType, ArrayList<ConstantLiteral> parentsPathToRoot) {
        for (ConstantLiteral literal : parentsPathToRoot) {
            if (literal.getVertexType().equals(vertexType)) {
                System.out.println("Skip. Literal has a vertex type that is already part of interesting dependency.");
                return true;
            }
        }
        return false;
    }

    public static List<Integer> createEmptyArrayListOfSize(int size) {
        List<Integer> emptyArray = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            emptyArray.add(0);
        }
        return emptyArray;
    }

    public static void printWithTime(String message, long runTimeInMS)
    {
        System.out.println(message + " time: " + runTimeInMS + "(ms) ** " +
                TimeUnit.MILLISECONDS.toSeconds(runTimeInMS) + "(sec) ** " +
                TimeUnit.MILLISECONDS.toMinutes(runTimeInMS) +  "(min)");
    }

    public static double calculatePatternSupport(Map<String, List<Integer>> entityURIs, double S, int T) {
//		System.out.println("Calculating pattern support...");
//		String centerVertexType = patternTreeNode.getPattern().getCenterVertexType();
//		System.out.println("Center vertex type: " + centerVertexType);
        int numOfPossiblePairs = 0;
        for (Map.Entry<String, List<Integer>> entityUriEntry : entityURIs.entrySet()) {
            int numberOfAcrossMatchesOfEntity = (int) entityUriEntry.getValue().stream().filter(x -> x > 0).count();
            int k = 2;
            if (numberOfAcrossMatchesOfEntity >= k)
                numOfPossiblePairs += CombinatoricsUtils.binomialCoefficient(numberOfAcrossMatchesOfEntity, k);

            int numberOfWithinMatchesOfEntity = (int) entityUriEntry.getValue().stream().filter(x -> x > 1).count();
            numOfPossiblePairs += numberOfWithinMatchesOfEntity;
        }
//		int S = this.vertexHistogram.get(centerVertexType);
// 		patternTreeNode.calculatePatternSupport(patternSupport);
        return calculateSupport(numOfPossiblePairs, S, T);
    }

    public static double calculateSupport(double numerator, double S, int T) {
        System.out.println("S = "+S);
        double denominator = S * CombinatoricsUtils.binomialCoefficient(T+1,2);
        System.out.print("Support: " + numerator + " / " + denominator + " = ");
        if (numerator > denominator)
            throw new IllegalArgumentException("numerator > denominator");
        double support = numerator / denominator;
        System.out.println(support);
        return support;
    }

    public static boolean isDuplicateEdge(VF2PatternGraph pattern, String edgeType, String sourceType, String targetType) {
        for (RelationshipEdge edge : pattern.getPattern().edgeSet()) {
            if (edge.getLabel().equalsIgnoreCase(edgeType)) {
                if (edge.getSource().getTypes().contains(sourceType) && edge.getTarget().getTypes().contains(targetType)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean isMultipleEdge(VF2PatternGraph pattern, String sourceType, String targetType) {
        for (RelationshipEdge edge : pattern.getPattern().edgeSet()) {
            if (edge.getSource().getTypes().contains(sourceType) && edge.getTarget().getTypes().contains(targetType)) {
                return true;
            } else if (edge.getSource().getTypes().contains(targetType) && edge.getTarget().getTypes().contains(sourceType)) {
                return true;
            }
        }
        return false;
    }

    @NotNull
    public static HashMap<String, List<String>> generateSyntheticTimestampToFilesMapFromPath(String path) {
        List<File> allFilesInDirectory = new ArrayList<>(Arrays.asList(Objects.requireNonNull(new File(path).listFiles(File::isFile))));
        allFilesInDirectory.sort(Comparator.comparing(File::getName));
        HashMap<String,List<String>> timestampToFilesMap = new HashMap<>();
        for (File ntFile: allFilesInDirectory) {
            String regex = "^graph([0-9]+)\\.txt$";
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(ntFile.getName());
            if (matcher.find()) {
                String timestamp = matcher.group(1);
                timestampToFilesMap.putIfAbsent(timestamp, new ArrayList<>());
                timestampToFilesMap.get(timestamp).add(ntFile.getPath());
            }
        }
        return timestampToFilesMap;
    }

    // TODO: Can this be merged with the code in histogram?
    public static void dissolveSuperVerticesBasedOnCount(GraphLoader graph, int superVertexDegree) {
        System.out.println("Dissolving super vertices based on count...");
        System.out.println("Initial edge count of first snapshot: "+graph.getGraph().getGraph().edgeSet().size());
        for (Vertex v: graph.getGraph().getGraph().vertexSet()) {
            int inDegree = graph.getGraph().getGraph().incomingEdgesOf(v).size(); // TODO: Should we use general degree instead of in-degree?
            if (inDegree > superVertexDegree) {
                List<RelationshipEdge> edgesToDelete = new ArrayList<>(graph.getGraph().getGraph().incomingEdgesOf(v));
                for (RelationshipEdge e : edgesToDelete) {
                    Vertex sourceVertex = e.getSource();
                    Map<String, Attribute> sourceVertexAttrMap = sourceVertex.getAllAttributesHashMap();
                    String newAttrName = e.getLabel();
                    if (sourceVertexAttrMap.containsKey(newAttrName)) {
                        newAttrName = e.getLabel() + "value";
                        if (!sourceVertexAttrMap.containsKey(newAttrName)) {
                            sourceVertex.putAttributeIfAbsent(new Attribute(newAttrName, v.getAttributeValueByName("uri")));
                        }
                    }
                    graph.getGraph().getGraph().removeEdge(e);
                }
            }
        }
        System.out.println("Updated edge count of first snapshot: "+graph.getGraph().getGraph().edgeSet().size());
    }


    @NotNull
    public static Map<String, List<String>> generateDbpediaTimestampToFilesMap(String path) {
        ArrayList<File> directories = new ArrayList<>(Arrays.asList(Objects.requireNonNull(new File(path).listFiles(File::isDirectory))));
        directories.sort(Comparator.comparing(File::getName));
        Map<String, List<String>> timestampToFilesMap = new HashMap<>();
        for (File directory: directories) {
            ArrayList<File> files = new ArrayList<>(Arrays.asList(Objects.requireNonNull(new File(directory.getPath()).listFiles(File::isFile))));
            List<String> paths = files.stream().map(File::getPath).collect(Collectors.toList());
            timestampToFilesMap.put(directory.getName(),paths);
        }
        return timestampToFilesMap;
    }


    public static class Pair implements Comparable<Util.Pair> {
        private final Integer min;
        private final Integer max;

        public Pair(int min, int max) {
            this.min = min;
            this.max = max;
        }

        public Integer min() {
            return min;
        }

        public Integer max() {
            return max;
        }

        @Override
        public int compareTo(@NotNull Util.Pair o) {
            if (this.min.equals(o.min)) {
                return this.max.compareTo(o.max);
            } else {
                return this.min.compareTo(o.min);
            }
        }

        @Override
        public String toString() {
            return "(" + min +
                    ", " + max +
                    ')';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Util.Pair pair = (Util.Pair) o;
            return min.equals(pair.min) && max.equals(pair.max);
        }

        @Override
        public int hashCode() {
            return Objects.hash(min, max);
        }
    }


}
