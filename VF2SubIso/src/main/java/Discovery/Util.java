package Discovery;

import ICs.TGFD;
import Infra.PatternTree;
import Loader.GraphLoader;
import org.apache.commons.cli.*;
import org.apache.jena.rdf.model.Model;
import org.jetbrains.annotations.NotNull;
import org.json.simple.JSONArray;

import java.io.File;
import java.io.FileNotFoundException;
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


    public static void Config(String []args)
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

        T = (cmd.getOptionValue("t") == null ? TGFDDiscovery.DEFAULT_NUM_OF_SNAPSHOTS : Integer.parseInt(cmd.getOptionValue("t")));
        numOfSnapshots = (T);
        gamma = (cmd.getOptionValue("a") == null ? TGFDDiscovery.DEFAULT_GAMMA : Integer.parseInt(cmd.getOptionValue("a")));
        maxNumOfLiterals = (cmd.getOptionValue(MAX_LIT_PARAM) == null ? DEFAULT_MAX_LITERALS_NUM : Integer.parseInt(cmd.getOptionValue(MAX_LIT_PARAM)));
        tgfdTheta = (cmd.getOptionValue("theta") == null ? TGFDDiscovery.DEFAULT_TGFD_THETA : Double.parseDouble(cmd.getOptionValue("theta")));
        patternTheta = (cmd.getOptionValue("pTheta") == null ? tgfdTheta : Double.parseDouble(cmd.getOptionValue("pTheta")));
        k = (cmd.getOptionValue("k") == null ? TGFDDiscovery.DEFAULT_K : Integer.parseInt(cmd.getOptionValue("k")));
        frequentSetSize = (cmd.getOptionValue(FREQUENT_SIZE_SET_PARAM) == null ? TGFDDiscovery.DEFAULT_FREQUENT_SIZE_SET : Integer.parseInt(cmd.getOptionValue(FREQUENT_SIZE_SET_PARAM)));

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

    public static void divertOutputToLogFile() {
        if (printToLogFile) {
            String fileName = "tgfd-discovery-log-" + experimentStartTimeAndDateStamp + ".txt";
            if (logStream == null) {
                try {
                    logStream = new PrintStream(fileName);
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
            }
            divertOutputToStream(logStream);
        }
    }

    private static void divertOutputToStream(PrintStream stream) {
        System.setOut(stream);
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

    @NotNull
    private static Map<String, List<String>> generateDbpediaTimestampToFilesMap(String path) {
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
        setTimestampToFilesMap(new ArrayList<>(timestampstoFilePathsMap.entrySet()));
    }

    private static void setImdbTimestampToFilesMapFromPath(String path) {
        HashMap<String, List<String>> timestampToFilesMap = generateImdbTimestampToFilesMapFromPath(path);
        setTimestampToFilesMap(new ArrayList<>(timestampToFilesMap.entrySet()));
    }

    @NotNull
    private static HashMap<String, List<String>> generateImdbTimestampToFilesMapFromPath(String path) {
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

    private static void setSyntheticTimestampToFilesMapFromPath(String path) {
        HashMap<String, List<String>> timestampToFilesMap = generateSyntheticTimestampToFilesMapFromPath(path);
        setTimestampToFilesMap(new ArrayList<>(timestampToFilesMap.entrySet()));
    }

    @NotNull
    private static HashMap<String, List<String>> generateSyntheticTimestampToFilesMapFromPath(String path) {
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

    public void setCurrentVSpawnLevel(int currentVSpawnLevel) {
        this.currentVSpawnLevel = currentVSpawnLevel;
        System.out.println("VSpawn level " + this.currentVSpawnLevel);
    }

    public int getK() {
        return k;
    }

    public int getNumOfSnapshots() {
        return numOfSnapshots;
    }

    public double getTgfdTheta() {
        return tgfdTheta;
    }

    public List<Long> getTotalVSpawnTime() {
        return totalVSpawnTime;
    }

    public Long getTotalVSpawnTime(int index) {
        return index < this.getTotalVSpawnTime().size() ? this.getTotalVSpawnTime().get(index) : (long)0;
    }

    public void addToTotalVSpawnTime(long vSpawnTime) {
        TGFDDiscovery.printWithTime("vSpawn", vSpawnTime);
        addToValueInListAtIndex(this.getTotalVSpawnTime(), this.currentVSpawnLevel, vSpawnTime);
    }

    public List<Long> getTotalMatchingTime() {
        return totalMatchingTime;
    }

    public Long getTotalMatchingTime(int index) {
        return index < this.getTotalMatchingTime().size() ? this.getTotalMatchingTime().get(index) : (long)0;
    }

    public void addToTotalMatchingTime(long matchingTime) {
        addToValueInListAtIndex(this.getTotalMatchingTime(), this.currentVSpawnLevel, matchingTime);
    }

    public boolean hasSupportPruning() {
        return hasSupportPruning;
    }

    public void setSupportPruning(boolean hasSupportPruning) {
        this.hasSupportPruning = hasSupportPruning;
    }

    public boolean isSkipK1() {
        return skipK1;
    }

    public ArrayList<ArrayList<TGFD>> getDiscoveredTgfds() {
        return discoveredTgfds;
    }

    public static void initializeTgfdLists() {
        discoveredTgfds = new ArrayList<>();
        for (int vSpawnLevel = 0; vSpawnLevel <= k; vSpawnLevel++) {
            discoveredTgfds.add(new ArrayList<>());
        }
    }

    public boolean reUseMatches() {
        return reUseMatches;
    }

    public boolean isGeneratek0Tgfds() {
        return generatek0Tgfds;
    }

    public boolean useChangeFile() {
        return useChangeFile;
    }

    public void setUseChangeFile(boolean useChangeFile) {
        this.useChangeFile = useChangeFile;
    }

    public int getPreviousLevelNodeIndex() {
        return previousLevelNodeIndex;
    }

    public void setPreviousLevelNodeIndex(int previousLevelNodeIndex) {
        this.previousLevelNodeIndex = previousLevelNodeIndex;
    }

    public int getCandidateEdgeIndex() {
        return candidateEdgeIndex;
    }

    public void setCandidateEdgeIndex(int candidateEdgeIndex) {
        this.candidateEdgeIndex = candidateEdgeIndex;
    }

    public void findAndSetNumOfSnapshots() {
        if (this.useChangeFile()) {
            this.setNumOfSnapshots(this.getChangeFilesMap().entrySet().size() + 1);
//			this.setNumOfSnapshots(this.getTimestampToFilesMap().size());
        } else {
            this.setNumOfSnapshots(this.getGraphs().size());
        }
        System.out.println("Number of "+this.getLoader()+" snapshots: "+this.getNumOfSnapshots());
    }

    public String getLoader() {
        return loader;
    }

    public void setLoader(String loader) {
        this.loader = loader;
    }

    public List<Map.Entry<String, List<String>>> getTimestampToFilesMap() {
        return timestampToFilesMap;
    }

    private static void setTimestampToFilesMap(List<Map.Entry<String, List<String>>> timestampToFilesMap) {
        timestampToFilesMap.sort(Map.Entry.comparingByKey());
        timestampToFilesMap = timestampToFilesMap.subList(0,Math.min(timestampToFilesMap.size(),T));
    }

    public int getT() {
        return T;
    }

    public void setT(int t) {
        this.T = t;
    }


    public boolean isValidationSearch() {
        return validationSearch;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public void setReUseMatches(boolean reUseMatches) {
        this.reUseMatches = reUseMatches;
    }

    public void setValidationSearch(boolean validationSearch) {
        this.validationSearch = validationSearch;
    }

    public long getDiscoveryStartTime() {
        return discoveryStartTime;
    }

    public void setDiscoveryStartTime(long discoveryStartTime) {
        this.discoveryStartTime = discoveryStartTime;
    }

    public boolean isDissolveSuperVertexTypes() {
        return dissolveSuperVertexTypes;
    }

    public void setDissolveSuperVertexTypes(boolean dissolveSuperVertexTypes) {
        this.dissolveSuperVertexTypes = dissolveSuperVertexTypes;
    }

    public boolean hasMinimalityPruning() {
        return hasMinimalityPruning;
    }

    public void setMinimalityPruning(boolean hasMinimalityPruning) {
        this.hasMinimalityPruning = hasMinimalityPruning;
    }

    public void setGeneratek0Tgfds(boolean generatek0Tgfds) {
        this.generatek0Tgfds = generatek0Tgfds;
    }

    public void setSkipK1(boolean skipK1) {
        this.skipK1 = skipK1;
    }

    public boolean isOnlyInterestingTGFDs() {
        return onlyInterestingTGFDs;
    }

    public void setOnlyInterestingTGFDs(boolean onlyInterestingTGFDs) {
        this.onlyInterestingTGFDs = onlyInterestingTGFDs;
    }

    public boolean iskExperiment() {
        return kExperiment;
    }

    public void setkExperiment(boolean kExperiment) {
        this.kExperiment = kExperiment;
    }

    public List<GraphLoader> getGraphs() {
        return graphs;
    }

    public void setGraphs(List<GraphLoader> graphs) {
        this.graphs = graphs;
    }

    public boolean isStoreInMemory() {
        return isStoreInMemory;
    }

    public void setStoreInMemory(boolean storeInMemory) {
        isStoreInMemory = storeInMemory;
    }

    public String getExperimentName() {
        return experimentName;
    }

    public void setExperimentName(String experimentName) {
        this.experimentName = experimentName;
    }

    public void setK(int k) {
        this.k = k;
    }

    public void setTgfdTheta(double tgfdTheta) {
        this.tgfdTheta = tgfdTheta;
    }

    public int getGamma() {
        return gamma;
    }

    public void setGamma(int gamma) {
        this.gamma = gamma;
    }

    public int getFrequentSetSize() {
        return frequentSetSize;
    }

    public void setFrequentSetSize(int frequentSetSize) {
        this.frequentSetSize = frequentSetSize;
    }

    public void setNumOfSnapshots(int numOfSnapshots) {
        this.numOfSnapshots = numOfSnapshots;
    }

    public HashMap<String, JSONArray> getChangeFilesMap() {
        return changeFilesMap;
    }

    public void setChangeFilesMap(HashMap<String, JSONArray> changeFilesMap) {
        this.changeFilesMap = changeFilesMap;
    }

    public String getGraphSize() {
        return graphSize;
    }

    public void setGraphSize(String graphSize) {
        this.graphSize = graphSize;
    }

    public ArrayList<Long> getkRuntimes() {
        return kRuntimes;
    }

    public String getExperimentStartTimeAndDateStamp() {
        return experimentStartTimeAndDateStamp;
    }

    public void setExperimentStartTimeAndDateStamp(String experimentStartTimeAndDateStamp) {
        this.experimentStartTimeAndDateStamp = experimentStartTimeAndDateStamp;
    }

    public boolean isUseOptChangeFile() {
        return useOptChangeFile;
    }

    public void setUseOptChangeFile(boolean useOptChangeFile) {
        this.useOptChangeFile = useOptChangeFile;
    }

    public List<Map.Entry<String, Integer>> getSortedVertexHistogram() {
        return sortedVertexHistogram;
    }

    public Integer getNumOfEdgesInAllGraphs() {
        return numOfEdgesInAllGraphs;
    }

    public int getNumOfVerticesInAllGraphs() {
        return numOfVerticesInAllGraphs;
    }

    public double getSuperVertexDegree() {
        return superVertexDegree;
    }

    public void setSuperVertexDegree(double superVertexDegree) {
        this.superVertexDegree = superVertexDegree;
    }

    public Map<String, Double> getVertexTypesToAvgInDegreeMap() {
        return vertexTypesToAvgInDegreeMap;
    }

    public void setVertexTypesToAvgInDegreeMap(Map<String, Double> vertexTypesToAvgInDegreeMap) {
        this.vertexTypesToAvgInDegreeMap = vertexTypesToAvgInDegreeMap;
    }

    public boolean isDissolveSuperVerticesBasedOnCount() {
        return dissolveSuperVerticesBasedOnCount;
    }

    public void setDissolveSuperVerticesBasedOnCount(boolean dissolveSuperVerticesBasedOnCount) {
        this.dissolveSuperVerticesBasedOnCount = dissolveSuperVerticesBasedOnCount;
    }

    public Model getFirstSnapshotTypeModel() {
        return firstSnapshotTypeModel;
    }

    public void setFirstSnapshotTypeModel(Model firstSnapshotTypeModel) {
        this.firstSnapshotTypeModel = firstSnapshotTypeModel;
    }

    public Model getFirstSnapshotDataModel() {
        return firstSnapshotDataModel;
    }

    public void setFirstSnapshotDataModel(Model firstSnapshotDataModel) {
        this.firstSnapshotDataModel = firstSnapshotDataModel;
    }

    public int getMaxNumOfLiterals() {
        return maxNumOfLiterals;
    }

    public void setMaxNumOfLiterals(int maxNumOfLiterals) {
        this.maxNumOfLiterals = maxNumOfLiterals;
    }

    public List<Long> getTotalSupergraphCheckingTime() {
        return totalSupergraphCheckingTime;
    }

    public Long getTotalSupergraphCheckingTime(int index) {
        return returnLongAtIndexIfExistsElseZero(this.getTotalSupergraphCheckingTime(), index);
    }

    public void addToTotalSupergraphCheckingTime(long supergraphCheckingTime) {
        addToValueInListAtIndex(this.getTotalSupergraphCheckingTime(), this.currentVSpawnLevel, supergraphCheckingTime);
    }

    public List<Long> getTotalVisitedPathCheckingTime() {
        return totalVisitedPathCheckingTime;
    }

    public Long getTotalVisitedPathCheckingTime(int index) {
        return returnLongAtIndexIfExistsElseZero(totalVisitedPathCheckingTime, index);
    }

    public void addToTotalVisitedPathCheckingTime(long visitedPathCheckingTime) {
        addToValueInListAtIndex(totalVisitedPathCheckingTime, currentVSpawnLevel, visitedPathCheckingTime);
    }

    public static void setValueInListAtIndex(List<Double> list, int index, Double value) {
        while (list.size() <= index) {
            list.add(0.0);
        }
        list.set(index, value);
    }

    public List<Long> getTotalSupersetPathCheckingTime() {
        return totalSupersetPathCheckingTime;
    }

    public Long getTotalSupersetPathCheckingTime(int index) {
        return returnLongAtIndexIfExistsElseZero(this.getTotalSupersetPathCheckingTime(), index);
    }

    public static void addToTotalSupersetPathCheckingTime(long supersetPathCheckingTime) {
        addToValueInListAtIndex(totalSupersetPathCheckingTime, currentVSpawnLevel, supersetPathCheckingTime);
    }

    public List<Long> getTotalFindEntitiesTime() {
        return totalFindEntitiesTime;
    }

    public Long getTotalFindEntitiesTime(int index) {
        return returnLongAtIndexIfExistsElseZero(totalFindEntitiesTime, index);
    }

    public List<Long> getTotalDiscoverConstantTGFDsTime() {
        return totalDiscoverConstantTGFDsTime;
    }

    public Long getTotalDiscoverConstantTGFDsTime(int index) {
        return returnLongAtIndexIfExistsElseZero(totalDiscoverConstantTGFDsTime, index);
    }

    public static void addToTotalDiscoverConstantTGFDsTime(long discoverConstantTGFDsTime) {
        addToValueInListAtIndex(totalDiscoverConstantTGFDsTime, currentVSpawnLevel, discoverConstantTGFDsTime);
    }

    public List<Long> getTotalDiscoverGeneralTGFDTime() {
        return totalDiscoverGeneralTGFDTime;
    }

    public Long getTotalDiscoverGeneralTGFDTime(int index) {
        return returnLongAtIndexIfExistsElseZero(this.totalDiscoverGeneralTGFDTime, index);
    }

    public static void addToTotalDiscoverGeneralTGFDTime(long discoverGeneralTGFDTime) {
        addToValueInListAtIndex(totalDiscoverGeneralTGFDTime, currentVSpawnLevel, discoverGeneralTGFDTime);
    }

    public static Long returnLongAtIndexIfExistsElseZero(List<Long> list, int index) {
        return index < list.size() ? list.get(index) : (long)0;
    }

    public double getPatternTheta() {
        return patternTheta;
    }

    public void setPatternTheta(double patternTheta) {
        this.patternTheta = patternTheta;
    }

    public long getTotalHistogramTime() {
        return totalHistogramTime;
    }

    public void setTotalHistogramTime(long totalHistogramTime) {
        this.totalHistogramTime = totalHistogramTime;
    }

    public boolean isFastMatching() {
        return fastMatching;
    }

    public void setFastMatching(boolean fastMatching) {
        this.fastMatching = fastMatching;
    }

    public Double getMedianPatternSupportsList(int index) {
        return returnDoubleAtIndexIfExistsElseZero(this.medianPatternSupportsList, index);
    }

    public void addToMedianPatternSupportsList(double medianPatternSupport) {
        setValueInListAtIndex(this.medianPatternSupportsList, this.currentVSpawnLevel, medianPatternSupport);
    }

    public Double getMedianConstantTgfdSupportsList(int index) {
        return returnDoubleAtIndexIfExistsElseZero(this.medianConstantTgfdSupportsList, index);
    }

    public void addToMedianConstantTgfdSupportsList(double medianConstantTgfdSupport) {
        setValueInListAtIndex(this.medianConstantTgfdSupportsList, this.currentVSpawnLevel, medianConstantTgfdSupport);
    }

    public Double getMedianGeneralTgfdSupportsList(int index) {
        return returnDoubleAtIndexIfExistsElseZero(this.medianGeneralTgfdSupportsList, index);
    }

    public void addToMedianGeneralTgfdSupportsList(double medianGeneralTgfdSupport) {
        setValueInListAtIndex(this.medianGeneralTgfdSupportsList, this.currentVSpawnLevel, medianGeneralTgfdSupport);
    }

    public static Double returnDoubleAtIndexIfExistsElseZero(List<Double> list, int index) {
        return index < list.size() ? list.get(index) : (double)0;
    }

    public Set<String> getInterestLabelsSet() {
        return interestLabelsSet;
    }

    public Set<String> getActiveAttributesSet() {
        return activeAttributesSet;
    }

    public void setActiveAttributesSet(Set<String> activeAttributesSet) {
        this.activeAttributesSet = activeAttributesSet;
    }

    public Map<String, Integer> getVertexHistogram() {
        return vertexHistogram;
    }

    public Map<String, Set<String>> getVertexTypesToActiveAttributesMap() {
        return vertexTypesToActiveAttributesMap;
    }

    public List<Map.Entry<String, Integer>> getSortedFrequentEdgesHistogram() {
        return sortedFrequentEdgesHistogram;
    }

    public void setSortedFrequentEdgesHistogram(List<Map.Entry<String, Integer>> sortedFrequentEdgesHistogram) {
        this.sortedFrequentEdgesHistogram = sortedFrequentEdgesHistogram;
    }

    public void setSortedVertexHistogram(List<Map.Entry<String, Integer>> sortedVertexHistogram) {
        this.sortedVertexHistogram = sortedVertexHistogram;
    }

    public void setVertexTypesToActiveAttributesMap(Map<String, Set<String>> vertexTypesToActiveAttributesMap) {
        this.vertexTypesToActiveAttributesMap = vertexTypesToActiveAttributesMap;
    }

    public void setVertexHistogram(Map<String, Integer> vertexHistogram) {
        this.vertexHistogram = vertexHistogram;
    }

    public boolean isPrintToLogFile() {
        return printToLogFile;
    }

    public void setPrintToLogFile(boolean printToLogFile) {
        this.printToLogFile = printToLogFile;
    }

    public void setTypeChangeURIs(Map<String, Set<String>> typeChangeURIs) {
        this.typeChangeURIs = typeChangeURIs;
    }

    public Map<String, Set<String>> getTypeChangeURIs() {
        return typeChangeURIs;
    }

    public boolean isIncremental() {
        return isIncremental;
    }

    public void setIncremental(boolean incremental) {
        isIncremental = incremental;
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
