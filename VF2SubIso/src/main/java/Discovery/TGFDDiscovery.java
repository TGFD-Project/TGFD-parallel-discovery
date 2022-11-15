package Discovery;

import ICs.TGFD;
import IncrementalRunner.IncUpdates;
import IncrementalRunner.IncrementalChange;
import Infra.*;
import VF2Runner.*;
import ChangeExploration.*;
import Loader.DBPediaLoader;
import Loader.GraphLoader;
import Loader.IMDBLoader;
import Loader.SyntheticLoader;
import org.apache.commons.cli.*;
import org.apache.commons.math3.util.CombinatoricsUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jgrapht.GraphMapping;
import org.jgrapht.alg.isomorphism.VF2AbstractIsomorphismInspector;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TGFDDiscovery {

	public TGFDDiscovery() {
		Util.discoveryStartTime = System.currentTimeMillis();

		printInfo();

		Util.initializeTgfdLists();
	}

	public TGFDDiscovery(String[] args) {

		Util.config(args);
		printInfo();
	}

	protected void divertOutputToLogFile() {
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

	protected void divertOutputToSummaryFile() {
		if (Util.printToLogFile) {
			String fileName = "tgfd-discovery-summary-" + Util.experimentStartTimeAndDateStamp + ".txt";
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

	private void divertOutputToStream(PrintStream stream) {
		System.setOut(stream);
	}

	protected void printInfo() {
		this.divertOutputToSummaryFile();

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

		System.out.println(String.join(", ", info));

		this.divertOutputToLogFile();
	}

	public static CommandLine parseArgs(Options options, String[] args) {
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

	public void initialize() {

		vSpawnInit();

		if (Util.generatek0Tgfds) {
			Util.printTgfdsToFile(Util.experimentName, Util.discoveredTgfds.get(Util.currentVSpawnLevel));
		}
		Util.kRuntimes.add(System.currentTimeMillis() - Util.discoveryStartTime);
		Util.printSupportStatisticsForThisSnapshot();
		Util.printTimeStatisticsForThisSnapshot(Util.currentVSpawnLevel);
		Util.patternTree.addLevel();
		Util.setCurrentVSpawnLevel(Util.currentVSpawnLevel + 1);
	}

	@Override
	public String toString() {
		String[] infoArray = {"G"+Util.graphSize
				, "t" + Util.T
				, "k" + Util.currentVSpawnLevel
				, Util.maxNumOfLiterals > 0 ? Util.MAX_LIT_PARAM + Util.maxNumOfLiterals : ""
				, Util.patternTheta != Util.tgfdTheta ? "pTheta" + Util.patternTheta : ""
				, "theta" + Util.tgfdTheta
				, "gamma" + Util.gamma
				, "freqSet" + (Util.frequentSetSize == Integer.MAX_VALUE ? "All" : Util.frequentSetSize)
				, Util.interestLabelsSet.size() > 0 ? "interestLabels" : ""
				, (Util.fastMatching ? "fast" : "")
				, (Util.validationSearch? "validation" : "")
				, (Util.useChangeFile ? "changefile"+(Util.useOptChangeFile?"Type":"All") : "")
				, (Util.isIncremental ? "incremental" : "")
				, (!Util.isStoreInMemory ? "dontStore" : "")
				, (!Util.reUseMatches ? "noMatchesReUsed" : "")
				, (!Util.onlyInterestingTGFDs ? "uninteresting" : "")
				, (!Util.hasMinimalityPruning ? "noMinimalityPruning" : "")
				, (!Util.hasSupportPruning ? "noSupportPruning" : "")
				, (Util.dissolveSuperVertexTypes ? "simplifySuperTypes"+(Util.superVertexDegree) : "")
				, (Util.dissolveSuperVerticesBasedOnCount ? "simplifySuperNodes"+(Util.INDIVIDUAL_SUPER_VERTEX_INDEGREE_FLOOR) : "")
				, (Util.experimentStartTimeAndDateStamp == null ? "" : Util.experimentStartTimeAndDateStamp)
		};
		List<String> list = Stream.of(infoArray).filter(s -> !s.equals("")).collect(Collectors.toList());
		return String.join("-", list);
	}

	public void start()
	{
		final long startTime = System.currentTimeMillis();
		loadGraphsAndComputeHistogram2();
		initialize();
	}

	public static void main(String[] args) {
		final long startTime = System.currentTimeMillis();

		TGFDDiscovery tgfdDiscovery = new TGFDDiscovery(args);

		tgfdDiscovery.loadGraphsAndComputeHistogram2();
//        tgfdDiscovery.loadGraphsAndComputeHistogram(tgfdDiscovery.getTimestampToFilesMap());

        tgfdDiscovery.initialize();
		while (Util.currentVSpawnLevel <= Util.k) {

			PatternTreeNode patternTreeNode = null;
			while (patternTreeNode == null && Util.currentVSpawnLevel <= Util.k)
				patternTreeNode = tgfdDiscovery.vSpawn();

			if (Util.currentVSpawnLevel > Util.k)
				break;

			if (patternTreeNode == null)
				throw new NullPointerException("patternTreeNode == null");

			List<Set<Set<ConstantLiteral>>> matchesPerTimestamps;
			long matchingTime = System.currentTimeMillis();
			if (Util.validationSearch)
				matchesPerTimestamps = tgfdDiscovery.getMatchesForPatternUsingVF2(patternTreeNode);
			else if (Util.isIncremental)
				matchesPerTimestamps = tgfdDiscovery.getMatchesUsingIncrementalMatching(patternTreeNode);
			else if (Util.useChangeFile)
				matchesPerTimestamps = tgfdDiscovery.getMatchesUsingChangeFiles(patternTreeNode);
			else
				matchesPerTimestamps = tgfdDiscovery.findMatchesUsingCenterVertices2(Util.graphs, patternTreeNode);

			matchingTime = System.currentTimeMillis() - matchingTime;
			TGFDDiscovery.printWithTime("Pattern matching", (matchingTime));
			Util.addToTotalMatchingTime(matchingTime);

			if (tgfdDiscovery.doesNotSatisfyTheta(patternTreeNode)) {
				System.out.println("Mark as pruned. Real pattern support too low for pattern " + patternTreeNode.getPattern());
				if (Util.hasSupportPruning)
					patternTreeNode.setIsPruned();
				continue;
			}

			if (Util.skipK1 && Util.currentVSpawnLevel == 1)
				continue;

			final long hSpawnStartTime = System.currentTimeMillis();
			HSpawn hspawn = new HSpawn(patternTreeNode, matchesPerTimestamps);
			ArrayList<TGFD> tgfds = hspawn.performHSPawn();
			TGFDDiscovery.printWithTime("hSpawn", (System.currentTimeMillis() - hSpawnStartTime));
			Util.discoveredTgfds.get(Util.currentVSpawnLevel).addAll(tgfds);
		}

		tgfdDiscovery.divertOutputToSummaryFile();
		System.out.println("---------------------------------------------------------------");
		System.out.println("                          Summary                              ");
		System.out.println("---------------------------------------------------------------");
		Util.printTimeStatistics();
		System.out.println("Total execution time: "+(System.currentTimeMillis() - startTime));
	}

	protected void countTotalNumberOfMatchesFound(List<Set<Set<ConstantLiteral>>> matchesPerTimestamps) {
		int numberOfMatchesFound = 0;
		for (Set<Set<ConstantLiteral>> matchesInOneTimestamp : matchesPerTimestamps) {
			numberOfMatchesFound += matchesInOneTimestamp.size();
		}
		System.out.println("Total number of matches found across all snapshots: " + numberOfMatchesFound);
	}

	public List<Set<Set<ConstantLiteral>>> findMatchesUsingCenterVertices2(List<GraphLoader> graphs, PatternTreeNode patternTreeNode) {

		LocalizedVF2Matching localizedVF2Matching;
		if (Util.fastMatching)
			if (patternTreeNode.getPattern().getPatternType() == PatternType.Windmill)
				localizedVF2Matching = new WindmillMatching(patternTreeNode.getPattern(), patternTreeNode.getCenterVertexParent(), Util.T, Util.onlyInterestingTGFDs, Util.getVertexTypesToActiveAttributesMap(), Util.reUseMatches);
			else
				localizedVF2Matching = new FastMatching(patternTreeNode.getPattern(), patternTreeNode.getCenterVertexParent(), Util.T, Util.onlyInterestingTGFDs, Util.getVertexTypesToActiveAttributesMap(), Util.reUseMatches);
		else
			localizedVF2Matching = new LocalizedVF2Matching(patternTreeNode.getPattern(), patternTreeNode.getCenterVertexParent(), Util.T, Util.onlyInterestingTGFDs, Util.getVertexTypesToActiveAttributesMap(), Util.reUseMatches);

		// TODO: Should we also have a method that does not require graphs to be stored in memory?
		localizedVF2Matching.findMatches(graphs, Util.T);

		List<Set<Set<ConstantLiteral>>> matchesPerTimestamp = localizedVF2Matching.getMatchesPerTimestamp();
		this.countTotalNumberOfMatchesFound(matchesPerTimestamp);

		Map<String, List<Integer>> entityURIs = localizedVF2Matching.getEntityURIs();
		if (Util.reUseMatches)
			patternTreeNode.setEntityURIs(entityURIs);

		localizedVF2Matching.printEntityURIs();

		double S = Util.vertexHistogram.get(patternTreeNode.getPattern().getCenterVertexType());
		double patternSupport = TGFDDiscovery.calculatePatternSupport(entityURIs, S, Util.T);
		Util.patternSupportsListForThisSnapshot.add(patternSupport);
		patternTreeNode.setPatternSupport(patternSupport);

		return matchesPerTimestamp;
	}

	private void calculateAverageInDegree(Map<String, List<Integer>> vertexTypesToInDegreesMap) {
		System.out.println("Average in-degrees of vertex types...");
		List<Double> avgInDegrees = new ArrayList<>();
		for (Entry<String, List<Integer>> entry: vertexTypesToInDegreesMap.entrySet()) {
			if (entry.getValue().size() == 0) continue;
			entry.getValue().sort(Comparator.naturalOrder());
			double avgInDegree = (double) entry.getValue().stream().mapToInt(Integer::intValue).sum() / (double) entry.getValue().size();
			System.out.println(entry.getKey()+": "+avgInDegree);
			avgInDegrees.add(avgInDegree);
			Util.vertexTypesToAvgInDegreeMap.put(entry.getKey(), avgInDegree);
		}
//		double avgInDegree = avgInDegrees.stream().mapToDouble(Double::doubleValue).sum() / (double) avgInDegrees.size();
		double avgInDegree = this.getHighOutlierThreshold(avgInDegrees);
		Util.superVertexDegree = (Math.max(avgInDegree, Util.DEFAULT_AVG_SUPER_VERTEX_DEGREE));
		System.out.println("Super vertex degree is "+ Util.superVertexDegree);
	}

	private void calculateMaxInDegree(Map<String, List<Integer>> vertexTypesToInDegreesMap) {
		System.out.println("Max in-degrees of vertex types...");
		List<Double> maxInDegrees = new ArrayList<>();
		for (Entry<String, List<Integer>> entry: vertexTypesToInDegreesMap.entrySet()) {
			if (entry.getValue().size() == 0) continue;
			double maxInDegree = Collections.max(entry.getValue()).doubleValue();
			System.out.println(entry.getKey()+": "+maxInDegree);
			maxInDegrees.add(maxInDegree);
			Util.vertexTypesToAvgInDegreeMap.put(entry.getKey(), maxInDegree);
		}
		double maxInDegree = getHighOutlierThreshold(maxInDegrees);
		System.out.println("Based on histogram, high outlier threshold for in-degree is "+maxInDegree);
		Util.superVertexDegree = (Math.max(maxInDegree, Util.DEFAULT_MAX_SUPER_VERTEX_DEGREE));
		System.out.println("Super vertex degree is "+ Util.superVertexDegree);
	}

	private double getHighOutlierThreshold(List<Double> listOfDegrees) {
		listOfDegrees.sort(Comparator.naturalOrder());
		if (listOfDegrees.size() == 1) return listOfDegrees.get(0);
		double q1, q3;
		if (listOfDegrees.size() % 2 == 0) {
			int halfSize = listOfDegrees.size()/2;
			q1 = listOfDegrees.get(halfSize/2);
			q3 = listOfDegrees.get((halfSize+ listOfDegrees.size())/2);
		} else {
			int middleIndex = listOfDegrees.size()/2;
			List<Double> firstHalf = listOfDegrees.subList(0,middleIndex);
			q1 = firstHalf.get(firstHalf.size()/2);
			List<Double> secondHalf = listOfDegrees.subList(middleIndex, listOfDegrees.size());
			q3 = secondHalf.get(secondHalf.size()/2);
		}
		double iqr = q3 - q1;
		return q3 + (9 * iqr);
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

	private void calculateSuperVertexDegreeThreshold(Map<String, List<Integer>> vertexTypesToInDegreesMap) {
		List<Long> listOfAverageDegreesAbove1 = new ArrayList<>();
		System.out.println("Average in-degree of each vertex type...");
		for (Entry<String, List<Integer>> entry: vertexTypesToInDegreesMap.entrySet()) {
			long averageDegree = Math.round(entry.getValue().stream().reduce(0, Integer::sum).doubleValue() / (double) entry.getValue().size());
			System.out.println(entry.getKey()+":"+averageDegree);
			listOfAverageDegreesAbove1.add(averageDegree);
		}
		Util.superVertexDegree = (Math.max(Util.superVertexDegree, Math.round(listOfAverageDegreesAbove1.stream().reduce(0L, Long::sum).doubleValue() / (double) listOfAverageDegreesAbove1.size())));
	}

	public void loadGraphsAndComputeHistogram2() {
		this.divertOutputToSummaryFile();
		System.out.println("Computing Histogram...");
		Histogram histogram = new Histogram(Util.T, Util.timestampToFilesMap, Util.loader, Util.frequentSetSize, Util.gamma, Util.interestLabelsSet);
		Integer superVertexDegree = Util.dissolveSuperVerticesBasedOnCount ? Util.INDIVIDUAL_SUPER_VERTEX_INDEGREE_FLOOR : null;
		if (Util.useChangeFile || Util.isIncremental) {
			List<String> changefilePaths = new ArrayList<>();
			for (int t = 1; t < Util.T; t++) {
				String changeFilePath = Util.changefilePath+"/changes_t" + t + "_t" + (t + 1) + "_" + Util.graphSize + ".json";
				changefilePaths.add(changeFilePath);
			}
			histogram.computeHistogramUsingChangefilesAll(changefilePaths, Util.isStoreInMemory, superVertexDegree, Util.dissolveSuperVertexTypes);
			if (Util.isStoreInMemory)
				Util.changeFilesMap = (histogram.getChangefilesToJsonArrayMap());
		} else {
			histogram.computeHistogramByReadingGraphsFromFile(Util.isStoreInMemory, superVertexDegree);
			if (Util.isStoreInMemory)
				Util.graphs = (histogram.getGraphs());
		}

		storeRelevantInformationFromHistogram(histogram);
		this.divertOutputToLogFile();
	}

	protected void storeRelevantInformationFromHistogram(Histogram histogram) {
		Util.vertexTypesToAvgInDegreeMap = histogram.getVertexTypesToMedianInDegreeMap();
		Util.activeAttributesSet = histogram.getActiveAttributesSet();
		Util.vertexTypesToActiveAttributesMap = histogram.getVertexTypesToActiveAttributesMap();
		Util.sortedFrequentEdgesHistogram = histogram.getSortedFrequentEdgesHistogram();
		Util.sortedVertexHistogram = histogram.getSortedVertexTypesHistogram();
		Util.vertexHistogram = histogram.getVertexHistogram();
		Util.totalHistogramTime = histogram.getTotalHistogramTime();
		Util.typeChangeURIs = histogram.getTypeChangesURIs();
	}

	public HashSet<ConstantLiteral> getActiveAttributesInPattern(Set<Vertex> vertexSet, boolean considerURI) {
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

	public ArrayList<TGFD> getDummyVertexTypeTGFDs() {
		ArrayList<TGFD> dummyTGFDs = new ArrayList<>();
		for (Entry<String,Integer> frequentVertexTypeEntry : Util.sortedVertexHistogram) {
			String frequentVertexType = frequentVertexTypeEntry.getKey();
			VF2PatternGraph patternGraph = new VF2PatternGraph();
			PatternVertex patternVertex = new PatternVertex(frequentVertexType);
			patternGraph.addVertex(patternVertex);
//			HashSet<ConstantLiteral> activeAttributes = getActiveAttributesInPattern(patternGraph.getPattern().vertexSet());
//			for (ConstantLiteral activeAttribute: activeAttributes) {
				TGFD dummyTGFD = new TGFD();
				dummyTGFD.setName(frequentVertexType);
				dummyTGFD.setPattern(patternGraph);
//				Dependency dependency = new Dependency();
//				dependency.addLiteralToY(activeAttribute);
				dummyTGFDs.add(dummyTGFD);
//			}
		}
		return dummyTGFDs;
	}

	public ArrayList<TGFD> getDummyEdgeTypeTGFDs() {
		ArrayList<TGFD> dummyTGFDs = new ArrayList<>();

		for (Entry<String,Integer> frequentEdgeEntry : Util.sortedFrequentEdgesHistogram) {
			String frequentEdge = frequentEdgeEntry.getKey();
			String[] info = frequentEdge.split(" ");
			String sourceVertexType = info[0];
			String edgeType = info[1];
			String targetVertexType = info[2];
			VF2PatternGraph patternGraph = new VF2PatternGraph();
			PatternVertex sourceVertex = new PatternVertex(sourceVertexType);
			patternGraph.addVertex(sourceVertex);
			PatternVertex targetVertex = new PatternVertex(targetVertexType);
			patternGraph.addVertex(targetVertex);
			RelationshipEdge edge = new RelationshipEdge(edgeType);
			patternGraph.addEdge(sourceVertex, targetVertex, edge);
//			HashSet<ConstantLiteral> activeAttributes = getActiveAttributesInPattern(patternGraph.getPattern().vertexSet());
//			for (ConstantLiteral activeAttribute: activeAttributes) {
				TGFD dummyTGFD = new TGFD();
				dummyTGFD.setName(frequentEdge.replaceAll(" ","_"));
				dummyTGFD.setPattern(patternGraph);
//				Dependency dependency = new Dependency();
//				dependency.addLiteralToY(activeAttribute);
				dummyTGFDs.add(dummyTGFD);
//			}
		}
		return dummyTGFDs;
	}

	public void setSyntheticTimestampToFilesMapFromPath(String path) {
		HashMap<String, List<String>> timestampToFilesMap = generateSyntheticTimestampToFilesMapFromPath(path);
		Util.setTimestampToFilesMap(new ArrayList<>(timestampToFilesMap.entrySet()));
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

	public void setImdbTimestampToFilesMapFromPath(String path) {
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

	protected void loadChangeFilesIntoMemory() {
		HashMap<String, JSONArray> changeFilesMap = new HashMap<>();
		if (Util.useOptChangeFile) { // TODO: Deprecate type changefiles?
			for (Entry<String,Integer> frequentVertexTypeEntry : Util.vertexHistogram.entrySet()) {
				for (int i = 0; i < Util.T - 1; i++) {
					System.out.println("-----------Snapshot (" + (i + 2) + ")-----------");
					String changeFilePath = "changes_t" + (i + 1) + "_t" + (i + 2) + "_" + frequentVertexTypeEntry.getKey() + ".json";
					JSONArray jsonArray = readJsonArrayFromFile(changeFilePath);
					System.out.println("Storing " + changeFilePath + " in memory");
					changeFilesMap.put(changeFilePath, jsonArray);
				}
			}
		} else {
			for (int i = 0; i < Util.T - 1; i++) {
				System.out.println("-----------Snapshot (" + (i + 2) + ")-----------");
				String changeFilePath = "changes_t" + (i + 1) + "_t" + (i + 2) + "_" + Util.graphSize + ".json";
				JSONArray jsonArray = readJsonArrayFromFile(changeFilePath);
				System.out.println("Storing " + changeFilePath + " in memory");
				changeFilesMap.put(changeFilePath, jsonArray);
			}
		}
		Util.changeFilesMap = (changeFilesMap);
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

	public void setDBpediaTimestampsAndFilePaths(String path) {
		Map<String, List<String>> timestampToFilesMap = generateDbpediaTimestampToFilesMap(path);
		Util.setTimestampToFilesMap(new ArrayList<>(timestampToFilesMap.entrySet()));
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

	public void setCitationTimestampsAndFilePaths() {
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

	protected static boolean isUsedVertexType(String vertexType, ArrayList<ConstantLiteral> parentsPathToRoot) {
		for (ConstantLiteral literal : parentsPathToRoot) {
			if (literal.getVertexType().equals(vertexType)) {
				System.out.println("Skip. Literal has a vertex type that is already part of interesting dependency.");
				return true;
			}
		}
		return false;
	}

	protected static boolean literalPathIsMissingTypesInPattern(ArrayList<ConstantLiteral> parentsPathToRoot, Set<Vertex> patternVertexSet) {
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



	public void vSpawnInit() {
		Util.patternTree = new PatternTree();
		Util.patternTree.addLevel();

		System.out.println("VSpawn Level 0");
		for (int i = 0; i < Util.sortedVertexHistogram.size(); i++) {
			long vSpawnTime = System.currentTimeMillis();
			System.out.println("VSpawnInit with single-node pattern " + (i+1) + "/" + Util.sortedVertexHistogram.size());
			String patternVertexType = Util.sortedVertexHistogram.get(i).getKey();

			if (Util.getVertexTypesToActiveAttributesMap().get(patternVertexType).size() == 0)
				continue; // TODO: Should these frequent types without active attribute be filtered out much earlier?

			System.out.println("Vertex type: "+patternVertexType);
			VF2PatternGraph candidatePattern = new VF2PatternGraph();
			PatternVertex patternVertex = new PatternVertex(patternVertexType);
			candidatePattern.addVertex(patternVertex);
			candidatePattern.getCenterVertexType();
			System.out.println("VSpawnInit with single-node pattern " + (i+1) + "/" + Util.sortedVertexHistogram.size() + ": " + candidatePattern.getPattern().vertexSet());

			PatternTreeNode patternTreeNode;
			patternTreeNode = Util.patternTree.createNodeAtLevel(Util.currentVSpawnLevel, candidatePattern);

			final long finalVspawnTime = System.currentTimeMillis() - vSpawnTime;
			Util.addToTotalVSpawnTime(finalVspawnTime);
			TGFDDiscovery.printWithTime("vSpawn", finalVspawnTime);

			final long matchingStartTime = System.currentTimeMillis();
			if (!Util.generatek0Tgfds) {
				if (Util.validationSearch)
					this.getMatchesForPatternUsingVF2(patternTreeNode);
				else if (Util.isIncremental)
					this.getMatchesUsingIncrementalMatching(patternTreeNode);
				else if (Util.useChangeFile)
					this.getMatchesUsingChangeFiles(patternTreeNode);
				else {
					Map<String, List<Integer>> entityURIs = new HashMap<>();
					LocalizedVF2Matching localizedVF2Matching = new LocalizedVF2Matching(patternTreeNode.getPattern(), patternTreeNode.getCenterVertexParent(), Util.T, Util.onlyInterestingTGFDs, Util.getVertexTypesToActiveAttributesMap(), Util.reUseMatches);
					for (int t = 0; t < Util.T; t++)
						localizedVF2Matching.extractListOfCenterVerticesInSnapshot(patternTreeNode.getPattern().getCenterVertexType(), entityURIs, t, Util.graphs.get(t));

					System.out.println("Number of center vertex URIs found containing active attributes: " + entityURIs.size());
					for (Entry<String, List<Integer>> entry: entityURIs.entrySet()) {
						System.out.println(entry);
					}
					if (Util.reUseMatches)
						patternTreeNode.setEntityURIs(entityURIs);
					double S = Util.vertexHistogram.get(patternTreeNode.getPattern().getCenterVertexType());
					double patternSupport = TGFDDiscovery.calculatePatternSupport(entityURIs, S, Util.T);
					Util.patternSupportsListForThisSnapshot.add(patternSupport);
					patternTreeNode.setPatternSupport(patternSupport);
				}
				final long matchingEndTime = System.currentTimeMillis() - matchingStartTime;
				printWithTime("matchingTime", matchingEndTime);
				Util.addToTotalMatchingTime(matchingEndTime);

				if (doesNotSatisfyTheta(patternTreeNode)) {
					patternTreeNode.setIsPruned();
				}
			} else {
				List<Set<Set<ConstantLiteral>>> matchesPerTimestamps;
				if (Util.validationSearch)
					matchesPerTimestamps = this.getMatchesForPatternUsingVF2(patternTreeNode);
				else if (Util.isIncremental)
					matchesPerTimestamps = this.getMatchesUsingIncrementalMatching(patternTreeNode);
				else if (Util.useChangeFile)
					matchesPerTimestamps = this.getMatchesUsingChangeFiles(patternTreeNode);
				else {
					LocalizedVF2Matching localizedVF2Matching;
					if (Util.fastMatching)
						localizedVF2Matching = new FastMatching(patternTreeNode.getPattern(), patternTreeNode.getCenterVertexParent(), Util.T, Util.onlyInterestingTGFDs, Util.getVertexTypesToActiveAttributesMap(), Util.reUseMatches);
					else
						localizedVF2Matching = new LocalizedVF2Matching(patternTreeNode.getPattern(), patternTreeNode.getCenterVertexParent(), Util.T, Util.onlyInterestingTGFDs, Util.getVertexTypesToActiveAttributesMap(), Util.reUseMatches);

					localizedVF2Matching.findMatches(Util.graphs, Util.T);

					Map<String, List<Integer>> entityURIs = localizedVF2Matching.getEntityURIs();
					localizedVF2Matching.printEntityURIs();
					if (Util.reUseMatches)
						patternTreeNode.setEntityURIs(entityURIs);

					double S = Util.vertexHistogram.get(patternTreeNode.getPattern().getCenterVertexType());
					double patternSupport = TGFDDiscovery.calculatePatternSupport(entityURIs, S, Util.T);
					Util.patternSupportsListForThisSnapshot.add(patternSupport);
					patternTreeNode.setPatternSupport(patternSupport);
					matchesPerTimestamps = localizedVF2Matching.getMatchesPerTimestamp();
				}

				final long matchingEndTime = System.currentTimeMillis() - matchingStartTime;
				printWithTime("matchingTime", matchingEndTime);
				Util.addToTotalMatchingTime(matchingEndTime);

				if (doesNotSatisfyTheta(patternTreeNode))
					patternTreeNode.setIsPruned();
				else {
					final long hSpawnStartTime = System.currentTimeMillis();
					HSpawn hspawn = new HSpawn(patternTreeNode, matchesPerTimestamps);
					ArrayList<TGFD> tgfds = hspawn.performHSPawn();
					printWithTime("hSpawn", (System.currentTimeMillis() - hSpawnStartTime));
					Util.discoveredTgfds.get(0).addAll(tgfds);
				}
			}
		}
		System.out.println("GenTree Level " + Util.currentVSpawnLevel + " size: " + Util.patternTree.getLevel(Util.currentVSpawnLevel).size());
		for (PatternTreeNode node : Util.patternTree.getLevel(Util.currentVSpawnLevel)) {
			System.out.println("Pattern: " + node.getPattern());
//			System.out.println("Pattern Support: " + node.getPatternSupport());
//			System.out.println("Dependency: " + node.getDependenciesSets());
		}

	}

	public static double calculatePatternSupport(Map<String, List<Integer>> entityURIs, double S, int T) {
//		System.out.println("Calculating pattern support...");
//		String centerVertexType = patternTreeNode.getPattern().getCenterVertexType();
//		System.out.println("Center vertex type: " + centerVertexType);
		int numOfPossiblePairs = 0;
		for (Entry<String, List<Integer>> entityUriEntry : entityURIs.entrySet()) {
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

	protected static double calculateSupport(double numerator, double S, int T) {
		System.out.println("S = "+S);
		double denominator = S * CombinatoricsUtils.binomialCoefficient(T+1,2);
		System.out.print("Support: " + numerator + " / " + denominator + " = ");
		if (numerator > denominator)
			throw new IllegalArgumentException("numerator > denominator");
		double support = numerator / denominator;
		System.out.println(support);
		return support;
	}

	protected boolean doesNotSatisfyTheta(PatternTreeNode patternTreeNode) {
		if (patternTreeNode.getPatternSupport() == null)
			throw new IllegalArgumentException("patternTreeNode.getPatternSupport() == null");
		return patternTreeNode.getPatternSupport() < Util.patternTheta;
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

	public PatternTreeNode vSpawn() {

		long vSpawnTime = System.currentTimeMillis();

		if (Util.candidateEdgeIndex > Util.sortedFrequentEdgesHistogram.size()-1) {
			Util.candidateEdgeIndex = 0;
			Util.previousLevelNodeIndex  = Util.previousLevelNodeIndex + 1;
		}

		if (Util.previousLevelNodeIndex >= Util.patternTree.getLevel(Util.currentVSpawnLevel-1).size()) {
			Util.kRuntimes.add(System.currentTimeMillis() - Util.discoveryStartTime);
			Util.printTgfdsToFile(Util.experimentName, Util.discoveredTgfds.get(Util.currentVSpawnLevel));
			if (Util.kExperiment) Util.printExperimentRuntimestoFile();
			Util.printSupportStatisticsForThisSnapshot();
			Util.printTimeStatisticsForThisSnapshot(Util.currentVSpawnLevel);
			Util.addToTotalVSpawnTime(System.currentTimeMillis()-vSpawnTime);
			Util.setCurrentVSpawnLevel(Util.currentVSpawnLevel + 1);
			vSpawnTime = System.currentTimeMillis();
			if (Util.currentVSpawnLevel > Util.k) {
				Util.addToTotalVSpawnTime(System.currentTimeMillis()-vSpawnTime);
				return null;
			}
			Util.patternTree.addLevel();
			Util.previousLevelNodeIndex = 0;
			Util.candidateEdgeIndex = 0;
		}

		System.out.println("Performing VSpawn");
		System.out.println("VSpawn Level " + Util.currentVSpawnLevel);

		ArrayList<PatternTreeNode> previousLevel = Util.patternTree.getLevel(Util.currentVSpawnLevel - 1);
		if (previousLevel.size() == 0) {
			System.out.println("Previous level of vSpawn contains no pattern nodes.");
			Util.previousLevelNodeIndex = (Util.previousLevelNodeIndex + 1);
			Util.addToTotalVSpawnTime(System.currentTimeMillis()-vSpawnTime);
			return null;
		}
		PatternTreeNode previousLevelNode = previousLevel.get(Util.previousLevelNodeIndex);
		System.out.println("Processing previous level node " + Util.previousLevelNodeIndex + "/" + (previousLevel.size()-1));
		System.out.println("Performing VSpawn on pattern: " + previousLevelNode.getPattern());

		System.out.println("Level " + (Util.currentVSpawnLevel - 1) + " pattern: " + previousLevelNode.getPattern());
		if (Util.hasSupportPruning && previousLevelNode.isPruned()) {
			System.out.println("Marked as pruned. Skip.");
			Util.previousLevelNodeIndex = (Util.previousLevelNodeIndex + 1);
			Util.addToTotalVSpawnTime(System.currentTimeMillis()-vSpawnTime);
			return null;
		}

		System.out.println("Processing candidate edge " + Util.candidateEdgeIndex + "/" + (Util.sortedFrequentEdgesHistogram.size()-1));
		Entry<String, Integer> candidateEdge = Util.sortedFrequentEdgesHistogram.get(Util.candidateEdgeIndex);
		String candidateEdgeString = candidateEdge.getKey();
		System.out.println("Candidate edge:" + candidateEdgeString);


		String sourceVertexType = candidateEdgeString.split(" ")[0];
		String targetVertexType = candidateEdgeString.split(" ")[2];

		if (Util.getVertexTypesToActiveAttributesMap().get(targetVertexType).size() == 0) {
			System.out.println("Target vertex in candidate edge does not contain active attributes");
			Util.candidateEdgeIndex = (Util.candidateEdgeIndex + 1);
			return null;
		}

		// TODO: We should add support for duplicate vertex types in the future
		if (sourceVertexType.equals(targetVertexType)) {
			System.out.println("Candidate edge contains duplicate vertex types. Skip.");
			Util.candidateEdgeIndex = (Util.candidateEdgeIndex + 1);
			Util.addToTotalVSpawnTime(System.currentTimeMillis()-vSpawnTime);
			return null;
		}
		String edgeType = candidateEdgeString.split(" ")[1];

		// Check if candidate edge already exists in pattern
		if (isDuplicateEdge(previousLevelNode.getPattern(), edgeType, sourceVertexType, targetVertexType)) {
			System.out.println("Candidate edge: " + candidateEdge.getKey());
			System.out.println("already exists in pattern");
			Util.candidateEdgeIndex = (Util.candidateEdgeIndex + 1);
			Util.addToTotalVSpawnTime(System.currentTimeMillis()-vSpawnTime);
			return null;
		}

		if (isMultipleEdge(previousLevelNode.getPattern(), sourceVertexType, targetVertexType)) {
			System.out.println("We do not support multiple edges between existing vertices.");
			Util.candidateEdgeIndex = (Util.candidateEdgeIndex + 1);
			Util.addToTotalVSpawnTime(System.currentTimeMillis()-vSpawnTime);
			return null;
		}

		// Checks if candidate edge extends pattern
		PatternVertex sourceVertex = isDuplicateVertex(previousLevelNode.getPattern(), sourceVertexType);
		PatternVertex targetVertex = isDuplicateVertex(previousLevelNode.getPattern(), targetVertexType);
		if (sourceVertex == null && targetVertex == null) {
			System.out.println("Candidate edge: " + candidateEdge.getKey());
			System.out.println("does not extend from pattern");
			Util.candidateEdgeIndex = (Util.candidateEdgeIndex + 1);
			Util.addToTotalVSpawnTime(System.currentTimeMillis()-vSpawnTime);
			return null;
		}

		PatternTreeNode patternTreeNode = null;
		// TODO: FIX label conflict. What if an edge has same vertex type on both sides?
		for (Vertex v : previousLevelNode.getGraph().vertexSet()) {
			System.out.println("Looking to add candidate edge to vertex: " + v.getTypes());
			PatternVertex pv = (PatternVertex) v;
			if (pv.isMarked()) {
				System.out.println("Skip vertex. Already added candidate edge to vertex: " + pv.getTypes());
				continue;
			}
			if (!pv.getTypes().contains(sourceVertexType) && !pv.getTypes().contains(targetVertexType)) {
				System.out.println("Skip vertex. Candidate edge does not connect to vertex: " + pv.getTypes());
				pv.setMarked(true);
				continue;
			}

			// Create unmarked copy of k-1 pattern
			VF2PatternGraph newPattern = previousLevelNode.getPattern().copy();
			if (targetVertex == null) {
				targetVertex = new PatternVertex(targetVertexType);
				newPattern.addVertex(targetVertex);
			} else {
				for (Vertex vertex : newPattern.getPattern().vertexSet()) {
					if (vertex.getTypes().contains(targetVertexType)) {
						targetVertex.setMarked(true);
						targetVertex = (PatternVertex) vertex;
						break;
					}
				}
			}
			RelationshipEdge newEdge = new RelationshipEdge(edgeType);
			if (sourceVertex == null) {
				sourceVertex = new PatternVertex(sourceVertexType);
				newPattern.addVertex(sourceVertex);
			} else {
				for (Vertex vertex : newPattern.getPattern().vertexSet()) {
					if (vertex.getTypes().contains(sourceVertexType)) {
						sourceVertex.setMarked(true);
						sourceVertex = (PatternVertex) vertex;
						break;
					}
				}
			}
			newPattern.addEdge(sourceVertex, targetVertex, newEdge);

			System.out.println("Created new pattern: " + newPattern);

			// TODO: Debug - Why does this work with strings but not subgraph isomorphism???
			if (isIsomorphicPattern(newPattern, Util.patternTree)) {
				pv.setMarked(true);
				System.out.println("Skip. Candidate pattern is an isomorph of existing pattern");
				continue;
			}

			if (Util.hasSupportPruning && isSuperGraphOfPrunedPattern(newPattern, Util.patternTree)) {
				pv.setMarked(true);
				System.out.println("Skip. Candidate pattern is a supergraph of pruned pattern");
				continue;
			}
			if (Util.currentVSpawnLevel == 1) {
				newPattern.assignOptimalCenterVertex(Util.vertexTypesToAvgInDegreeMap, Util.fastMatching);
				patternTreeNode = new PatternTreeNode(newPattern, previousLevelNode, candidateEdgeString);
				Util.patternTree.getTree().get(Util.currentVSpawnLevel).add(patternTreeNode);
				Util.patternTree.findSubgraphParents(Util.currentVSpawnLevel-1, patternTreeNode);
				Util.patternTree.findCenterVertexParent(Util.currentVSpawnLevel-1, patternTreeNode, true);
			} else {
				newPattern.assignOptimalCenterVertex(Util.vertexTypesToAvgInDegreeMap, Util.fastMatching);
				boolean considerAlternativeParents = true;
				if (Util.fastMatching && Util.currentVSpawnLevel > 2) {
					if (newPattern.getPatternType() == PatternType.Line) {
						considerAlternativeParents = false;
					}
				}
				patternTreeNode = Util.patternTree.createNodeAtLevel(Util.currentVSpawnLevel, newPattern, previousLevelNode, candidateEdgeString, considerAlternativeParents);
			}
			System.out.println("Marking vertex " + pv.getTypes() + "as expanded.");
			break;
		}
		if (patternTreeNode == null) {
			for (Vertex v : previousLevelNode.getGraph().vertexSet()) {
				System.out.println("Unmarking all vertices in current pattern for the next candidate edge");
				((PatternVertex)v).setMarked(false);
			}
			Util.candidateEdgeIndex = (Util.candidateEdgeIndex + 1);
		}
		Util.addToTotalVSpawnTime(System.currentTimeMillis()-vSpawnTime);
		return patternTreeNode;
	}

	private boolean isIsomorphicPattern(VF2PatternGraph newPattern, PatternTree patternTree) {
		final long isIsomorphicPatternCheckStartTime = System.currentTimeMillis();
	    System.out.println("Checking if the pattern is isomorphic...");
	    ArrayList<String> newPatternEdges = new ArrayList<>();
        newPattern.getPattern().edgeSet().forEach((edge) -> {newPatternEdges.add(edge.toString());});
        boolean isIsomorphic = false;
		for (PatternTreeNode otherPattern: patternTree.getLevel(Util.currentVSpawnLevel)) {
            ArrayList<String> otherPatternEdges = new ArrayList<>();
            otherPattern.getGraph().edgeSet().forEach((edge) -> {otherPatternEdges.add(edge.toString());});
            if (newPatternEdges.containsAll(otherPatternEdges)) {
				System.out.println("Candidate pattern: " + newPattern);
				System.out.println("is an isomorph of current VSpawn level pattern: " + otherPattern.getPattern());
				isIsomorphic = true;
				break;
			}
		}
		final long isomorphicCheckingTime = System.currentTimeMillis() - isIsomorphicPatternCheckStartTime;
		printWithTime("isIsomorphicPatternCheck", isomorphicCheckingTime);
		Util.addToTotalSupergraphCheckingTime(isomorphicCheckingTime);
		return isIsomorphic;
	}

	// TODO: Should this be done using real subgraph isomorphism instead of strings?
	private boolean isSuperGraphOfPrunedPattern(VF2PatternGraph newPattern, PatternTree patternTree) {
		final long supergraphCheckingStartTime = System.currentTimeMillis();
        ArrayList<String> newPatternEdges = new ArrayList<>();
        newPattern.getPattern().edgeSet().forEach((edge) -> {newPatternEdges.add(edge.toString());});
		int i = Util.currentVSpawnLevel;
		boolean isSupergraph = false;
		while (i >= 0) {
			for (PatternTreeNode treeNode : patternTree.getLevel(i)) {
				if (treeNode.isPruned()) {
					if (treeNode.getPattern().getCenterVertexType().equals(newPattern.getCenterVertexType())) {
						if (i == 0) {
							isSupergraph = true;
						} else {
							ArrayList<String> otherPatternEdges = new ArrayList<>();
							treeNode.getGraph().edgeSet().forEach((edge) -> {
								otherPatternEdges.add(edge.toString());
							});
							if (newPatternEdges.containsAll(otherPatternEdges)) {
								isSupergraph = true;
							}
						}
						if (isSupergraph) {
							System.out.println("Candidate pattern: " + newPattern);
							System.out.println("is a supergraph of pruned subgraph pattern: " + treeNode.getPattern());
							break;
						}
					}
				}
			}
			if (isSupergraph) break;
			i--;
		}
		final long supergraphCheckingTime = System.currentTimeMillis() - supergraphCheckingStartTime;
		printWithTime("Supergraph checking", supergraphCheckingTime);
		Util.addToTotalSupergraphCheckingTime(supergraphCheckingTime);
		return isSupergraph;
	}

	private PatternVertex isDuplicateVertex(VF2PatternGraph newPattern, String vertexType) {
		for (Vertex v: newPattern.getPattern().vertexSet()) {
			if (v.getTypes().contains(vertexType)) {
				return (PatternVertex) v;
			}
		}
		return null;
	}

	public static List<Integer> createEmptyArrayListOfSize(int size) {
		List<Integer> emptyArray = new ArrayList<>();
		for (int i = 0; i < size; i++) {
			emptyArray.add(0);
		}
		return emptyArray;
	}

	public List<Set<Set<ConstantLiteral>>> getMatchesForPatternUsingVF2(PatternTreeNode patternTreeNode) {
		// TODO: Potential speed up for single-edge/single-node patterns. Iterate through all edges/nodes in graph.
		Map<String, List<Integer>> entityURIs = new HashMap<>();
		List<Set<Set<ConstantLiteral>>> matchesPerTimestamps = new ArrayList<>();
		for (int timestamp = 0; timestamp < Util.numOfSnapshots; timestamp++) {
			matchesPerTimestamps.add(new HashSet<>());
		}

		patternTreeNode.getPattern().getCenterVertexType();

		for (int year = 0; year < Util.numOfSnapshots; year++) {
			long searchStartTime = System.currentTimeMillis();
			ArrayList<HashSet<ConstantLiteral>> matches = new ArrayList<>();
			int numOfMatchesInTimestamp = 0;
			VF2AbstractIsomorphismInspector<Vertex, RelationshipEdge> results = new VF2SubgraphIsomorphism().execute2(Util.graphs.get(year).getGraph(), patternTreeNode.getPattern(), false);
			if (results.isomorphismExists()) {
				numOfMatchesInTimestamp = extractMatches(results.getMappings(), matches, patternTreeNode, entityURIs, year);
			}
			System.out.println("Number of matches found: " + numOfMatchesInTimestamp);
			System.out.println("Number of matches found that contain active attributes: " + matches.size());
			matchesPerTimestamps.get(year).addAll(matches);
			printWithTime("Search Cost", (System.currentTimeMillis() - searchStartTime));
		}

		// TODO: Should we implement pattern support here to weed out patterns with few matches in later iterations?
		// Is there an ideal pattern support threshold after which very few TGFDs are discovered?
		// How much does the real pattern differ from the estimate?
		int numberOfMatchesFound = 0;
		for (Set<Set<ConstantLiteral>> matchesInOneTimestamp : matchesPerTimestamps) {
			numberOfMatchesFound += matchesInOneTimestamp.size();
		}
		System.out.println("Total number of matches found across all snapshots:" + numberOfMatchesFound);

		for (Entry<String, List<Integer>> entry: entityURIs.entrySet()) {
			System.out.println(entry);
		}
		double S = Util.vertexHistogram.get(patternTreeNode.getPattern().getCenterVertexType());
		double patternSupport = TGFDDiscovery.calculatePatternSupport(entityURIs, S, Util.T);
		Util.patternSupportsListForThisSnapshot.add(patternSupport);
		patternTreeNode.setPatternSupport(patternSupport);

		return matchesPerTimestamps;
	}

	// TODO: Merge with other extractMatch method?
	private String extractMatch(GraphMapping<Vertex, RelationshipEdge> result, PatternTreeNode patternTreeNode, HashSet<ConstantLiteral> match, Map<String, Integer> interestingnessMap) {
		String entityURI = null;
		for (Vertex v : patternTreeNode.getGraph().vertexSet()) {
			Vertex currentMatchedVertex = result.getVertexCorrespondence(v, false);
			if (currentMatchedVertex == null) continue;
			String patternVertexType = v.getTypes().iterator().next();
			if (entityURI == null) {
				entityURI = extractAttributes(patternTreeNode, patternVertexType, match, currentMatchedVertex, interestingnessMap);
			} else {
				extractAttributes(patternTreeNode, patternVertexType, match, currentMatchedVertex, interestingnessMap);
			}
		}
		return entityURI;
	}

	private String extractAttributes(PatternTreeNode patternTreeNode, String patternVertexType, HashSet<ConstantLiteral> match, Vertex currentMatchedVertex, Map<String, Integer> interestingnessMap) {
		String entityURI = null;
		String centerVertexType = patternTreeNode.getPattern().getCenterVertexType();
		Set<String> matchedVertexTypes = currentMatchedVertex.getTypes();
		for (ConstantLiteral activeAttribute : getActiveAttributesInPattern(patternTreeNode.getGraph().vertexSet(),true)) {
			if (!matchedVertexTypes.contains(activeAttribute.getVertexType())) continue;
			for (String matchedAttrName : currentMatchedVertex.getAllAttributesNames()) {
				if (matchedVertexTypes.contains(centerVertexType) && matchedAttrName.equals("uri")) {
					entityURI = currentMatchedVertex.getAttributeValueByName(matchedAttrName);
				}
				if (!activeAttribute.getAttrName().equals(matchedAttrName)) continue;
				String matchedAttrValue = currentMatchedVertex.getAttributeValueByName(matchedAttrName);
				ConstantLiteral xLiteral = new ConstantLiteral(patternVertexType, matchedAttrName, matchedAttrValue);
				interestingnessMap.merge(patternVertexType, 1, Integer::sum);
				match.add(xLiteral);
			}
		}
		return entityURI;
	}

	protected int extractMatches(Iterator<GraphMapping<Vertex, RelationshipEdge>> iterator, ArrayList<HashSet<ConstantLiteral>> matches, PatternTreeNode patternTreeNode, Map<String, List<Integer>> entityURIs, int timestamp) {
		int numOfMatches = 0;
		while (iterator.hasNext()) {
			numOfMatches++;
			GraphMapping<Vertex, RelationshipEdge> result = iterator.next();
			HashSet<ConstantLiteral> literalsInMatch = new HashSet<>();
			Map<String, Integer> interestingnessMap = new HashMap<>();
			String entityURI = extractMatch(result, patternTreeNode, literalsInMatch, interestingnessMap);
			// ensures that the match is not empty and contains more than just the uri attribute
			if (Util.onlyInterestingTGFDs && interestingnessMap.values().stream().anyMatch(n -> n < 2)) {
				continue;
			} else if (!Util.onlyInterestingTGFDs && literalsInMatch.size() < patternTreeNode.getGraph().vertexSet().size()) {
				continue;
			}
			if (entityURI != null) {
				entityURIs.putIfAbsent(entityURI, createEmptyArrayListOfSize(Util.numOfSnapshots));
				entityURIs.get(entityURI).set(timestamp, entityURIs.get(entityURI).get(timestamp)+1);
			}
			matches.add(literalsInMatch);
		}
		matches.sort(new Comparator<HashSet<ConstantLiteral>>() {
			@Override
			public int compare(HashSet<ConstantLiteral> o1, HashSet<ConstantLiteral> o2) {
				return o1.size() - o2.size();
			}
		});
		return numOfMatches;
	}

	public List<Set<Set<ConstantLiteral>>> getMatchesUsingChangeFiles(PatternTreeNode patternTreeNode) {
		Set<String> vertexSets = patternTreeNode.getGraph().vertexSet().stream().map(vertex -> vertex.getTypes().iterator().next()).collect(Collectors.toSet());
		LocalizedVF2Matching localizedVF2Matching;
		if (Util.fastMatching)
			if (patternTreeNode.getPattern().getPatternType() == PatternType.Windmill)
				localizedVF2Matching = new WindmillMatching(patternTreeNode.getPattern(), patternTreeNode.getCenterVertexParent(), Util.T, Util.onlyInterestingTGFDs, Util.getVertexTypesToActiveAttributesMap(), Util.reUseMatches);
			else
				localizedVF2Matching = new FastMatching(patternTreeNode.getPattern(), patternTreeNode.getCenterVertexParent(), Util.T, Util.onlyInterestingTGFDs, Util.getVertexTypesToActiveAttributesMap(), Util.reUseMatches);
		else
			localizedVF2Matching = new LocalizedVF2Matching(patternTreeNode.getPattern(), patternTreeNode.getCenterVertexParent(), Util.T, Util.onlyInterestingTGFDs, Util.getVertexTypesToActiveAttributesMap(), Util.reUseMatches);

		GraphLoader graph = loadFirstSnapshot();
		localizedVF2Matching.findMatchesInSnapshot(graph, 0);
		for (int t = 1; t < Util.T; t++) {
			if (Util.useOptChangeFile)
				updateGraphUsingChangefiles(graph, t, vertexSets);
			else
				updateGraphUsingChangefiles(graph, t, null);

			localizedVF2Matching.findMatchesInSnapshot(graph, t);
		}
		Map<String, List<Integer>> entityURIs = localizedVF2Matching.getEntityURIs();
		if (Util.reUseMatches)
			patternTreeNode.setEntityURIs(entityURIs);

		System.out.println("-------------------------------------");
		System.out.println("Number of entity URIs found: "+entityURIs.size());
		localizedVF2Matching.printEntityURIs();

		double S = Util.vertexHistogram.get(patternTreeNode.getPattern().getCenterVertexType());
		double patternSupport = TGFDDiscovery.calculatePatternSupport(entityURIs, S, Util.T);
		Util.patternSupportsListForThisSnapshot.add(patternSupport);
		patternTreeNode.setPatternSupport(patternSupport);

		return localizedVF2Matching.getMatchesPerTimestamp();
	}

	protected void updateGraphUsingChangefiles(GraphLoader graph, int t, Set<String> vertexSets) {
		System.out.println("-----------Snapshot (" + (t + 1) + ")-----------");
		String changeFilePath = Util.changefilePath+"/changes_t" + t + "_t" + (t + 1) + "_" + Util.graphSize + ".json";
		JSONArray jsonArray = Util.isStoreInMemory ? Util.changeFilesMap.get(changeFilePath) : readJsonArrayFromFile(changeFilePath);
		updateGraphUsingChanges(new ChangeLoader(jsonArray, vertexSets, (Util.useOptChangeFile ? Util.typeChangeURIs : null), true), graph);
	}

	private void updateGraphUsingChanges(ChangeLoader jsonArray, GraphLoader graph) {
		ChangeLoader changeLoader = jsonArray;
		IncUpdates incUpdatesOnDBpedia = new IncUpdates(graph.getGraph(), new ArrayList<>());
		sortChanges(changeLoader.getAllChanges());
		incUpdatesOnDBpedia.updateEntireGraph(changeLoader.getAllChanges());
		if (Util.dissolveSuperVerticesBasedOnCount)
			dissolveSuperVerticesBasedOnCount(graph, Util.INDIVIDUAL_SUPER_VERTEX_INDEGREE_FLOOR);
	}

	public List<Set<Set<ConstantLiteral>>> getMatchesUsingIncrementalMatching(PatternTreeNode patternTreeNode) {
		// TODO: Should we use changefiles based on freq types??

//		List<Set<Set<ConstantLiteral>>> matchesPerTimestamps = new ArrayList<>();

//		patternTreeNode.getPattern().setDiameter(this.getCurrentVSpawnLevel());

		TGFD dummyTgfd = new TGFD();
		if (Util.currentVSpawnLevel == 0)
			dummyTgfd.setName(patternTreeNode.getGraph().vertexSet().toString());
		else
			dummyTgfd.setName(patternTreeNode.getAllEdgeStrings().toString());

		dummyTgfd.setPattern(patternTreeNode.getPattern());

		System.out.println("-----------Snapshot (1)-----------");
		List<TGFD> tgfds = Collections.singletonList(dummyTgfd);
		int numberOfMatchesFound = 0;

//		GraphLoader graph = loadFirstSnapshot();

		// Now, we need to find the matches for each snapshot.
		// Finding the matches...
//		Map<String, List<Integer>> entityURIs = new HashMap<>();

//		for (TGFD tgfd : tgfds) {
//			System.out.println("\n###########" + tgfd.getName() + "###########");

		//Retrieving and storing the matches of each timestamp.
		final long matchingStartTime = System.currentTimeMillis();
//		if (this.getCurrentVSpawnLevel() < 1) {
//			if (patternTreeNode.getGraph().vertexSet().size() != 1)
//				throw new IllegalArgumentException("For vSpawn level 0, patternTreeNode.getGraph().vertexSet().size() != 1");
//			String patternVertexType = patternTreeNode.getGraph().vertexSet().iterator().next().getTypes().iterator().next();
//			this.findMatchesUsingCenterVerticesForVSpawnInit(Collections.singletonList(graph), patternVertexType, patternTreeNode, matchesPerTimestamps, null, entityURIs);
//		} else {
//			this.extractMatchesAcrossSnapshots(Collections.singletonList(graph), patternTreeNode, matchesPerTimestamps, entityURIs);
//		}
		LocalizedVF2Matching localizedVF2Matching;
		if (Util.fastMatching)
			if (patternTreeNode.getPattern().getPatternType() == PatternType.Windmill)
				localizedVF2Matching = new WindmillMatching(patternTreeNode.getPattern(), patternTreeNode.getCenterVertexParent(), Util.T, Util.onlyInterestingTGFDs, Util.getVertexTypesToActiveAttributesMap(), Util.reUseMatches);
			else
				localizedVF2Matching = new FastMatching(patternTreeNode.getPattern(), patternTreeNode.getCenterVertexParent(), Util.T, Util.onlyInterestingTGFDs, Util.getVertexTypesToActiveAttributesMap(), Util.reUseMatches);
		else
			localizedVF2Matching = new LocalizedVF2Matching(patternTreeNode.getPattern(), patternTreeNode.getCenterVertexParent(), Util.T, Util.onlyInterestingTGFDs, Util.getVertexTypesToActiveAttributesMap(), Util.reUseMatches);

		GraphLoader graph = loadFirstSnapshot();
		localizedVF2Matching.findMatchesInSnapshot(graph, 0);
		List<Set<Set<ConstantLiteral>>> matchesPerTimestamps = localizedVF2Matching.getMatchesPerTimestamp();
		Map<String, List<Integer>> entityURIs = localizedVF2Matching.getEntityURIs();
		if (Util.reUseMatches)
			patternTreeNode.setEntityURIs(entityURIs);
		final long totalMatchingTime = System.currentTimeMillis() - matchingStartTime;
		printWithTime("Snapshot 1 matching", totalMatchingTime);
		Util.addToTotalMatchingTime(totalMatchingTime);
		numberOfMatchesFound += matchesPerTimestamps.get(0).size();
//		}

		//Load the change files
		for (int i = 0; i < Util.T-1; i++) {
			System.out.println("-----------Snapshot (" + (i+2) + ")-----------");

			final long loadChangefileStartTime = System.currentTimeMillis();
			String changeFilePath = "changes_t" + (i+1) + "_t" + (i + 2) + "_" + Util.graphSize + ".json";
			JSONArray jsonArray = Util.isStoreInMemory ? Util.changeFilesMap.get(changeFilePath) : readJsonArrayFromFile(changeFilePath);
			Set<String> vertexSets = patternTreeNode.getGraph().vertexSet().stream().map(vertex -> vertex.getTypes().iterator().next()).collect(Collectors.toSet());
			ChangeLoader changeLoader = new ChangeLoader(jsonArray, (Util.useOptChangeFile ? vertexSets : null), (Util.useOptChangeFile ? Util.typeChangeURIs : null), Util.currentVSpawnLevel != 0);

			HashMap<Integer,HashSet<Change>> newChanges = changeLoader.getAllGroupedChanges();
			System.out.println("Total number of changes in changefile: " + newChanges.size());

			List<Entry<Integer, HashSet<Change>>> sortedChanges = getSortedChanges(newChanges);
			List<List<Entry<Integer,HashSet<Change>>>> changefiles = new ArrayList<>();
			changefiles.add(sortedChanges);

			long totalLoadChangefileTime = System.currentTimeMillis() - loadChangefileStartTime;
			printWithTime("Load changes for Snapshot (" + (i+2) + ")", totalLoadChangefileTime);
			// TODO: Should we add this time to some tally?

			System.out.println("Total number of changefiles: " + changefiles.size());

			// Now, we need to find the matches for each snapshot.
			// Finding the matches...

			final long graphUpdateAndMatchTime = System.currentTimeMillis();
			System.out.println("Updating the graph...");
			// TODO: Do we need to update the subgraphWithinDiameter method used in IncUpdates?
			IncUpdates incUpdatesOnDBpedia = new IncUpdates(graph.getGraph(), tgfds);
			if (Util.currentVSpawnLevel > 0) {
				incUpdatesOnDBpedia.AddNewVertices(changeLoader.getAllChanges());
				System.out.println("Added new vertices.");
			}

			HashMap<String, TGFD> tgfdsByName = new HashMap<>();
			for (TGFD tgfd : tgfds) {
				tgfdsByName.put(tgfd.getName(), tgfd);
			}
			Map<String, Set<ConstantLiteral>> newMatches = new HashMap<>();
			Map<String, Set<ConstantLiteral>> removedMatches = new HashMap<>();
			int numOfNewMatchesFoundInSnapshot = 0;
			for (int changefileIndex = 0; changefileIndex < changefiles.size(); changefileIndex++) {
				List<Entry<Integer, HashSet<Change>>> changesInChangefile = changefiles.get(changefileIndex);
				for (int changeIndex = 0; changeIndex < changesInChangefile.size(); changeIndex++) {
					Entry<Integer, HashSet<Change>> groupedChangeEntry = changesInChangefile.get(changeIndex);
					HashMap<String, IncrementalChange> incrementalChangeHashMap = incUpdatesOnDBpedia.updateGraphByGroupOfChanges(groupedChangeEntry.getValue(), tgfdsByName, Util.currentVSpawnLevel == 0);
					if (incrementalChangeHashMap == null)
						continue;
					for (String tgfdName : incrementalChangeHashMap.keySet()) {
						for (Entry<String, Set<ConstantLiteral>> allLiteralsInNewMatchEntry : incrementalChangeHashMap.get(tgfdName).getNewMatchesInConstantLiteralFormat().entrySet()) {
							numOfNewMatchesFoundInSnapshot++;
							HashSet<ConstantLiteral> match = new HashSet<>();
							Map<String, Integer> interestingnessMap = new HashMap<>();
							for (ConstantLiteral matchedLiteral: allLiteralsInNewMatchEntry.getValue()) {
								for (ConstantLiteral activeAttribute : getActiveAttributesInPattern(patternTreeNode.getGraph().vertexSet(), true)) {
									if (!matchedLiteral.getVertexType().equals(activeAttribute.getVertexType())) continue;
									if (!matchedLiteral.getAttrName().equals(activeAttribute.getAttrName())) continue;
									ConstantLiteral xLiteral = new ConstantLiteral(matchedLiteral.getVertexType(), matchedLiteral.getAttrName(), matchedLiteral.getAttrValue());
									interestingnessMap.merge(matchedLiteral.getVertexType(), 1, Integer::sum);
									match.add(xLiteral);
								}
							}
							if (Util.onlyInterestingTGFDs && interestingnessMap.values().stream().anyMatch(n -> n < 2)) {
								continue;
							} else if (!Util.onlyInterestingTGFDs && match.size() < patternTreeNode.getGraph().vertexSet().size()) {
								continue;
							}
							newMatches.put(allLiteralsInNewMatchEntry.getKey(), match);
						}

						for (Entry<String, Set<ConstantLiteral>> allLiteralsInRemovedMatchesEntry : incrementalChangeHashMap.get(tgfdName).getRemovedMatches().entrySet()) {
							HashSet<ConstantLiteral> match = new HashSet<>();
							Map<String, Integer> interestingnessMap = new HashMap<>();
							for (ConstantLiteral matchedLiteral: allLiteralsInRemovedMatchesEntry.getValue()) {
								for (ConstantLiteral activeAttribute : getActiveAttributesInPattern(patternTreeNode.getGraph().vertexSet(), true)) {
									if (!matchedLiteral.getVertexType().equals(activeAttribute.getVertexType())) continue;
									if (!matchedLiteral.getAttrName().equals(activeAttribute.getAttrName())) continue;
									ConstantLiteral xLiteral = new ConstantLiteral(matchedLiteral.getVertexType(), matchedLiteral.getAttrName(), matchedLiteral.getAttrValue());
									interestingnessMap.merge(matchedLiteral.getVertexType(), 1, Integer::sum);
									match.add(xLiteral);
								}
							}
							if (Util.onlyInterestingTGFDs && interestingnessMap.values().stream().anyMatch(n -> n < 2)) {
								continue;
							} else if (!Util.onlyInterestingTGFDs && match.size() < patternTreeNode.getGraph().vertexSet().size()) {
								continue;
							}
							removedMatches.putIfAbsent(allLiteralsInRemovedMatchesEntry.getKey(), match);
						}
					}
					if (changeIndex % 100000 == 0) System.out.println("Processed changes "+changeIndex+"/"+changesInChangefile.size());
				}
				System.out.println("Processed changefile "+changefileIndex+"/"+changefiles.size());
			}
			System.out.println("Number of new matches found: " + numOfNewMatchesFoundInSnapshot);
			System.out.println("Number of new matches found that contain active attributes: " + newMatches.size());
			System.out.println("Number of removed matched: " + removedMatches.size());

			int processedMatches = 0;
			matchesPerTimestamps.add(new HashSet<>());
			for (Set<ConstantLiteral> newMatch: newMatches.values()) {
				for (ConstantLiteral l: newMatch) {
					if (l.getVertexType().equals(patternTreeNode.getPattern().getCenterVertexType())
							&& l.getAttrName().equals("uri")) {
						String entityURI = l.getAttrValue();
						entityURIs.putIfAbsent(entityURI, createEmptyArrayListOfSize(Util.numOfSnapshots));
						entityURIs.get(entityURI).set(i+1, entityURIs.get(entityURI).get(i+1)+1);
						break;
					}
				}
				matchesPerTimestamps.get(i+1).add(newMatch);
				processedMatches++;
				if (processedMatches % 100000 == 0) System.out.println("Processed 100000 matches");
			}
			System.out.println("Processed "+ (processedMatches % 100000) + " matches");

			int numOfOldMatchesFoundInSnapshot = 0;
			processedMatches = 0;
			Set<Set<ConstantLiteral>> removedMatchesSet = new HashSet<>(removedMatches.values());
			Set<Set<ConstantLiteral>> newMatchesSet = new HashSet<>(newMatches.values());
			for (Set<ConstantLiteral> previousMatch : matchesPerTimestamps.get(i)) {
				processedMatches++; if (processedMatches % 100000 == 0) System.out.println("Processed 100000 matches");
				String centerVertexType = patternTreeNode.getPattern().getCenterVertexType();
				String entityURI = null;
				for (ConstantLiteral l: previousMatch) {
					if (l.getVertexType().equals(centerVertexType) && l.getAttrName().equals("uri")) {
						entityURI = l.getAttrValue();
						break;
					}
				}
				if (removedMatchesSet.contains(previousMatch))
					continue;
				if (newMatchesSet.contains(previousMatch))
					continue;
				if (entityURI != null) {
					entityURIs.putIfAbsent(entityURI, createEmptyArrayListOfSize(Util.numOfSnapshots));
					entityURIs.get(entityURI).set(i + 1, entityURIs.get(entityURI).get(i + 1) + 1);
				}
				matchesPerTimestamps.get(i+1).add(previousMatch);
				numOfOldMatchesFoundInSnapshot++;
			}
			System.out.println("Processed "+ (processedMatches % 100000) + " matches");

			System.out.println("Number of valid old matches that are not new or removed: " + numOfOldMatchesFoundInSnapshot);
			System.out.println("Total number of matches with active attributes found in this snapshot: " + matchesPerTimestamps.get(i+1).size());

			numberOfMatchesFound += matchesPerTimestamps.get(i+1).size();

			if (Util.currentVSpawnLevel > 0)
				incUpdatesOnDBpedia.deleteVertices(changeLoader.getAllChanges());

			final long finalGraphUpdateAndMatchTime = System.currentTimeMillis() - graphUpdateAndMatchTime;
			printWithTime("Update graph and retrieve matches", finalGraphUpdateAndMatchTime);
			Util.addToTotalMatchingTime(finalGraphUpdateAndMatchTime);
		}

		System.out.println("-------------------------------------");
		System.out.println("Number of entity URIs found: "+entityURIs.size());
		for (Entry<String, List<Integer>> entry: entityURIs.entrySet()) {
			System.out.println(entry);
		}
		System.out.println("Total number of matches found in all snapshots: " + numberOfMatchesFound);
		double S = Util.vertexHistogram.get(patternTreeNode.getPattern().getCenterVertexType());
		double patternSupport = TGFDDiscovery.calculatePatternSupport(entityURIs, S, Util.T);
		Util.patternSupportsListForThisSnapshot.add(patternSupport);
		patternTreeNode.setPatternSupport(patternSupport);

		return matchesPerTimestamps;
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
	private static List<Entry<Integer, HashSet<Change>>> getSortedChanges(HashMap<Integer, HashSet<Change>> newChanges) {
		List<Entry<Integer,HashSet<Change>>> sortedChanges = new ArrayList<>(newChanges.entrySet());
		HashMap<ChangeType, Integer> map = new HashMap<>();
		map.put(ChangeType.deleteAttr, 2);
		map.put(ChangeType.insertAttr, 4);
		map.put(ChangeType.changeAttr, 4);
		map.put(ChangeType.deleteEdge, 0);
		map.put(ChangeType.insertEdge, 5);
		map.put(ChangeType.changeType, 3);
		map.put(ChangeType.deleteVertex, 1);
		map.put(ChangeType.insertVertex, 1);
		sortedChanges.sort(new Comparator<Entry<Integer, HashSet<Change>>>() {
			@Override
			public int compare(Entry<Integer, HashSet<Change>> o1, Entry<Integer, HashSet<Change>> o2) {
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

	@NotNull
	protected GraphLoader loadFirstSnapshot() {
		final long startTime = System.currentTimeMillis();
		GraphLoader graph;
		if (Util.firstSnapshotTypeModel == null && Util.firstSnapshotDataModel == null) {
			for (String path : Util.timestampToFilesMap.get(0).getValue()) {
				if (!path.toLowerCase().endsWith(".ttl") && !path.toLowerCase().endsWith(".nt"))
					continue;
				if (path.toLowerCase().contains("literals") || path.toLowerCase().contains("objects"))
					continue;
				Path input= Paths.get(path);
				Model model = ModelFactory.createDefaultModel();
				System.out.println("Reading Node Types: " + path);
				model.read(input.toUri().toString());
				Util.firstSnapshotTypeModel = (model);
			}
			Model dataModel = ModelFactory.createDefaultModel();
			for (String path: Util.timestampToFilesMap.get(0).getValue()) {
				if (!path.toLowerCase().endsWith(".ttl") && !path.toLowerCase().endsWith(".nt"))
					continue;
				if (path.toLowerCase().contains("types"))
					continue;
				Path input= Paths.get(path);
				System.out.println("Reading data graph: "+path);
				dataModel.read(input.toUri().toString());
				Util.firstSnapshotDataModel = (dataModel);
			}
		}
//		Config.optimizedLoadingBasedOnTGFD = true; // TODO: Does enabling optimized loading cause problems with TypeChange?
		if (Util.loader.equals("dbpedia")) {
			if (Util.firstSnapshotTypeModel == null || Util.firstSnapshotDataModel == null)
				throw new NullPointerException("this.getFirstSnapshotTypeModel() == null || this.getFirstSnapshotDataModel() == null");
			graph = new DBPediaLoader(new ArrayList<>(), Collections.singletonList(Util.firstSnapshotTypeModel), Collections.singletonList(Util.firstSnapshotDataModel));
		} else if (Util.loader.equals("synthetic")) {
			graph = new SyntheticLoader(new ArrayList<>(), Util.timestampToFilesMap.get(0).getValue());
		} else {
			if (Util.firstSnapshotDataModel == null)
				throw new NullPointerException("this.getFirstSnapshotDataModel() == null");
			graph = new IMDBLoader(new ArrayList<>(), Collections.singletonList(Util.firstSnapshotDataModel));
		}
//		Config.optimizedLoadingBasedOnTGFD = false;

		if (Util.dissolveSuperVerticesBasedOnCount)
			dissolveSuperVerticesBasedOnCount(graph, Util.INDIVIDUAL_SUPER_VERTEX_INDEGREE_FLOOR);

		printWithTime("Load graph (1)", System.currentTimeMillis()- startTime);
		return graph;
	}

	public static void printWithTime(String message, long runTimeInMS)
	{
		System.out.println(message + " time: " + runTimeInMS + "(ms) ** " +
				TimeUnit.MILLISECONDS.toSeconds(runTimeInMS) + "(sec) ** " +
				TimeUnit.MILLISECONDS.toMinutes(runTimeInMS) +  "(min)");
	}
}

