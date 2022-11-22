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
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.jetbrains.annotations.NotNull;
import org.jgrapht.GraphMapping;
import org.jgrapht.alg.isomorphism.VF2AbstractIsomorphismInspector;
import org.json.simple.JSONArray;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TGFDDiscovery {


	private VSpawn vSpawn;

	public TGFDDiscovery() {
		Util.discoveryStartTime = System.currentTimeMillis();

		printInfo();

		Util.initializeTgfdLists();

		vSpawn = new VSpawn();
	}

	public TGFDDiscovery(String[] args) {

		Util.config(args);
		printInfo();

		vSpawn = new VSpawn();
	}

	protected void printInfo() {
		Util.divertOutputToSummaryFile();

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

		Util.divertOutputToLogFile();
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

		vSpawnInit();

		if (Util.generatek0Tgfds) {
			Util.printTgfdsToFile(Util.experimentName, Util.discoveredTgfds.get(Util.currentVSpawnLevel));
		}
		Util.kRuntimes.add(System.currentTimeMillis() - Util.discoveryStartTime);
		Util.printSupportStatisticsForThisSnapshot();
		Util.printTimeStatisticsForThisSnapshot(Util.currentVSpawnLevel);
		Util.patternTree.addLevel();
		Util.setCurrentVSpawnLevel(Util.currentVSpawnLevel + 1);

		while (Util.currentVSpawnLevel <= Util.k) {

			PatternTreeNode patternTreeNode = null;
			while (patternTreeNode == null && Util.currentVSpawnLevel <= Util.k)
				patternTreeNode = vSpawn.perform();

			if (Util.currentVSpawnLevel > Util.k)
				break;

			List<Set<Set<ConstantLiteral>>> matchesPerTimestamps;
			long matchingTime = System.currentTimeMillis();
			if (Util.validationSearch)
				matchesPerTimestamps = getMatchesForPatternUsingVF2(patternTreeNode);
			else if (Util.isIncremental)
				matchesPerTimestamps = getMatchesUsingIncrementalMatching(patternTreeNode);
			else if (Util.useChangeFile)
				matchesPerTimestamps = getMatchesUsingChangeFiles(patternTreeNode);
			else
				matchesPerTimestamps = findMatchesUsingCenterVertices2(Util.graphs, patternTreeNode);

			matchingTime = System.currentTimeMillis() - matchingTime;
			Util.printWithTime("Pattern matching", (matchingTime));
			Util.addToTotalMatchingTime(matchingTime);

			if (doesNotSatisfyTheta(patternTreeNode)) {
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
			Util.printWithTime("hSpawn", (System.currentTimeMillis() - hSpawnStartTime));
			Util.discoveredTgfds.get(Util.currentVSpawnLevel).addAll(tgfds);
		}

		Util.divertOutputToSummaryFile();
		System.out.println("---------------------------------------------------------------");
		System.out.println("                          Summary                              ");
		System.out.println("---------------------------------------------------------------");
		Util.printTimeStatistics();
		System.out.println("Total execution time: "+(System.currentTimeMillis() - startTime));
	}

	public List<PatternTreeNode> vSpawnSinglePatternTreeNode() {
		Util.patternTree = new PatternTree();
		Util.patternTree.addLevel();

		List<PatternTreeNode> singleNodePatternTreeNodes = new ArrayList<>();

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
			Util.printWithTime("vSpawn", finalVspawnTime);

			singleNodePatternTreeNodes.add(patternTreeNode);
		}
		System.out.println("GenTree Level " + Util.currentVSpawnLevel + " size: " + Util.patternTree.getLevel(Util.currentVSpawnLevel).size());
		for (PatternTreeNode node : Util.patternTree.getLevel(Util.currentVSpawnLevel)) {
			System.out.println("Pattern: " + node.getPattern());
		}

		return singleNodePatternTreeNodes;
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
				patternTreeNode = new VSpawn().perform();

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
			Util.printWithTime("Pattern matching", (matchingTime));
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
			Util.printWithTime("hSpawn", (System.currentTimeMillis() - hSpawnStartTime));
			Util.discoveredTgfds.get(Util.currentVSpawnLevel).addAll(tgfds);
		}

		Util.divertOutputToSummaryFile();
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
		double patternSupport = Util.calculatePatternSupport(entityURIs, S, Util.T);
		Util.patternSupportsListForThisSnapshot.add(patternSupport);
		patternTreeNode.setPatternSupport(patternSupport);

		return matchesPerTimestamp;
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


	public void loadGraphsAndComputeHistogram2() {
		Util.divertOutputToSummaryFile();
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
		Util.divertOutputToLogFile();
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

	public void vSpawnInitialTreeSets()
	{
		Util.patternTree = new PatternTree();
		Util.patternTree.addLevel();

		System.out.println("VSpawn Level 0");
		for (int i = 0; i < Util.sortedVertexHistogram.size(); i++) {
			long vSpawnTime = System.currentTimeMillis();
			System.out.println("VSpawnInit with single-node pattern " + (i + 1) + "/" + Util.sortedVertexHistogram.size());
			String patternVertexType = Util.sortedVertexHistogram.get(i).getKey();

			if (Util.getVertexTypesToActiveAttributesMap().get(patternVertexType).size() == 0)
				continue; // TODO: Should these frequent types without active attribute be filtered out much earlier?

			System.out.println("Vertex type: " + patternVertexType);
			VF2PatternGraph candidatePattern = new VF2PatternGraph();
			PatternVertex patternVertex = new PatternVertex(patternVertexType);
			candidatePattern.addVertex(patternVertex);
			candidatePattern.getCenterVertexType();
			System.out.println("VSpawnInit with single-node pattern " + (i + 1) + "/" + Util.sortedVertexHistogram.size() + ": " + candidatePattern.getPattern().vertexSet());

			PatternTreeNode patternTreeNode;
			patternTreeNode = Util.patternTree.createNodeAtLevel(Util.currentVSpawnLevel, candidatePattern);

			final long finalVspawnTime = System.currentTimeMillis() - vSpawnTime;
			Util.addToTotalVSpawnTime(finalVspawnTime);
			Util.printWithTime("vSpawn", finalVspawnTime);
		}
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
			Util.printWithTime("vSpawn", finalVspawnTime);

			final long matchingStartTime = System.currentTimeMillis();
			if (!Util.generatek0Tgfds)
			{
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
					double patternSupport = Util.calculatePatternSupport(entityURIs, S, Util.T);
					Util.patternSupportsListForThisSnapshot.add(patternSupport);
					patternTreeNode.setPatternSupport(patternSupport);
				}
				final long matchingEndTime = System.currentTimeMillis() - matchingStartTime;
				Util.printWithTime("matchingTime", matchingEndTime);
				Util.addToTotalMatchingTime(matchingEndTime);

				if (doesNotSatisfyTheta(patternTreeNode)) {
					patternTreeNode.setIsPruned();
				}
			}
			else
			{
				List<Set<Set<ConstantLiteral>>> matchesPerTimestamps;
				if (Util.validationSearch)
					matchesPerTimestamps = this.getMatchesForPatternUsingVF2(patternTreeNode);
				else if (Util.isIncremental)
					matchesPerTimestamps = this.getMatchesUsingIncrementalMatching(patternTreeNode);
				else if (Util.useChangeFile)
					matchesPerTimestamps = this.getMatchesUsingChangeFiles(patternTreeNode);
				else
				{
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
					double patternSupport = Util.calculatePatternSupport(entityURIs, S, Util.T);
					Util.patternSupportsListForThisSnapshot.add(patternSupport);
					patternTreeNode.setPatternSupport(patternSupport);
					matchesPerTimestamps = localizedVF2Matching.getMatchesPerTimestamp();
				}

				final long matchingEndTime = System.currentTimeMillis() - matchingStartTime;
				Util.printWithTime("matchingTime", matchingEndTime);
				Util.addToTotalMatchingTime(matchingEndTime);

				if (doesNotSatisfyTheta(patternTreeNode))
					patternTreeNode.setIsPruned();
				else {
					final long hSpawnStartTime = System.currentTimeMillis();
					HSpawn hspawn = new HSpawn(patternTreeNode, matchesPerTimestamps);
					ArrayList<TGFD> tgfds = hspawn.performHSPawn();
					Util.printWithTime("hSpawn", (System.currentTimeMillis() - hSpawnStartTime));
					Util.discoveredTgfds.get(0).addAll(tgfds);
				}
			}
		}
		System.out.println("GenTree Level " + Util.currentVSpawnLevel + " size: " + Util.patternTree.getLevel(Util.currentVSpawnLevel).size());
		for (PatternTreeNode node : Util.patternTree.getLevel(Util.currentVSpawnLevel)) {
			System.out.println("Pattern: " + node.getPattern());
		}

	}

	public void vSpawnInit(List<PatternTreeNode> singlePatternTreeNodes) {
		Util.patternTree = new PatternTree();
		Util.patternTree.addLevel();

		System.out.println("Received singlePatternTreeNodes from the coordinator for VSpawn Level 0");

		for (PatternTreeNode patternTreeNode:singlePatternTreeNodes) {
			Util.patternTree.createNodeAtLevel(Util.currentVSpawnLevel, patternTreeNode.getPattern());

			final long matchingStartTime = System.currentTimeMillis();
			if (!Util.generatek0Tgfds)
			{
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
					double patternSupport = Util.calculatePatternSupport(entityURIs, S, Util.T);
					Util.patternSupportsListForThisSnapshot.add(patternSupport);
					patternTreeNode.setPatternSupport(patternSupport);
				}
				final long matchingEndTime = System.currentTimeMillis() - matchingStartTime;
				Util.printWithTime("matchingTime", matchingEndTime);
				Util.addToTotalMatchingTime(matchingEndTime);

				if (doesNotSatisfyTheta(patternTreeNode)) {
					patternTreeNode.setIsPruned();
				}
			}
			else
			{
				List<Set<Set<ConstantLiteral>>> matchesPerTimestamps;
				if (Util.validationSearch)
					matchesPerTimestamps = this.getMatchesForPatternUsingVF2(patternTreeNode);
				else if (Util.isIncremental)
					matchesPerTimestamps = this.getMatchesUsingIncrementalMatching(patternTreeNode);
				else if (Util.useChangeFile)
					matchesPerTimestamps = this.getMatchesUsingChangeFiles(patternTreeNode);
				else
				{
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
					double patternSupport = Util.calculatePatternSupport(entityURIs, S, Util.T);
					Util.patternSupportsListForThisSnapshot.add(patternSupport);
					patternTreeNode.setPatternSupport(patternSupport);
					matchesPerTimestamps = localizedVF2Matching.getMatchesPerTimestamp();
				}

				final long matchingEndTime = System.currentTimeMillis() - matchingStartTime;
				Util.printWithTime("matchingTime", matchingEndTime);
				Util.addToTotalMatchingTime(matchingEndTime);

				if (doesNotSatisfyTheta(patternTreeNode))
					patternTreeNode.setIsPruned();
				else {
					final long hSpawnStartTime = System.currentTimeMillis();
					HSpawn hspawn = new HSpawn(patternTreeNode, matchesPerTimestamps);
					ArrayList<TGFD> tgfds = hspawn.performHSPawn();
					Util.printWithTime("hSpawn", (System.currentTimeMillis() - hSpawnStartTime));
					Util.discoveredTgfds.get(0).addAll(tgfds);
				}
			}
		}
		System.out.println("GenTree Level " + Util.currentVSpawnLevel + " size: " + Util.patternTree.getLevel(Util.currentVSpawnLevel).size());
		for (PatternTreeNode node : Util.patternTree.getLevel(Util.currentVSpawnLevel)) {
			System.out.println("Pattern: " + node.getPattern());
		}

	}


	protected boolean doesNotSatisfyTheta(PatternTreeNode patternTreeNode) {
		if (patternTreeNode.getPatternSupport() == null)
			throw new IllegalArgumentException("patternTreeNode.getPatternSupport() == null");
		return patternTreeNode.getPatternSupport() < Util.patternTheta;
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
			Util.printWithTime("Search Cost", (System.currentTimeMillis() - searchStartTime));
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
		double patternSupport = Util.calculatePatternSupport(entityURIs, S, Util.T);
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
				entityURIs.putIfAbsent(entityURI, Util.createEmptyArrayListOfSize(Util.numOfSnapshots));
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
		double patternSupport = Util.calculatePatternSupport(entityURIs, S, Util.T);
		Util.patternSupportsListForThisSnapshot.add(patternSupport);
		patternTreeNode.setPatternSupport(patternSupport);

		return localizedVF2Matching.getMatchesPerTimestamp();
	}

	protected void updateGraphUsingChangefiles(GraphLoader graph, int t, Set<String> vertexSets) {
		System.out.println("-----------Snapshot (" + (t + 1) + ")-----------");
		String changeFilePath = Util.changefilePath+"/changes_t" + t + "_t" + (t + 1) + "_" + Util.graphSize + ".json";
		JSONArray jsonArray = Util.isStoreInMemory ? Util.changeFilesMap.get(changeFilePath) : Util.readJsonArrayFromFile(changeFilePath);
		updateGraphUsingChanges(new ChangeLoader(jsonArray, vertexSets, (Util.useOptChangeFile ? Util.typeChangeURIs : null), true), graph);
	}

	private void updateGraphUsingChanges(ChangeLoader jsonArray, GraphLoader graph) {
		ChangeLoader changeLoader = jsonArray;
		IncUpdates incUpdatesOnDBpedia = new IncUpdates(graph.getGraph(), new ArrayList<>());
		Util.sortChanges(changeLoader.getAllChanges());
		incUpdatesOnDBpedia.updateEntireGraph(changeLoader.getAllChanges());
		if (Util.dissolveSuperVerticesBasedOnCount)
			Util.dissolveSuperVerticesBasedOnCount(graph, Util.INDIVIDUAL_SUPER_VERTEX_INDEGREE_FLOOR);
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
		Util.printWithTime("Snapshot 1 matching", totalMatchingTime);
		Util.addToTotalMatchingTime(totalMatchingTime);
		numberOfMatchesFound += matchesPerTimestamps.get(0).size();
//		}

		//Load the change files
		for (int i = 0; i < Util.T-1; i++) {
			System.out.println("-----------Snapshot (" + (i+2) + ")-----------");

			final long loadChangefileStartTime = System.currentTimeMillis();
			String changeFilePath = "changes_t" + (i+1) + "_t" + (i + 2) + "_" + Util.graphSize + ".json";
			JSONArray jsonArray = Util.isStoreInMemory ? Util.changeFilesMap.get(changeFilePath) : Util.readJsonArrayFromFile(changeFilePath);
			Set<String> vertexSets = patternTreeNode.getGraph().vertexSet().stream().map(vertex -> vertex.getTypes().iterator().next()).collect(Collectors.toSet());
			ChangeLoader changeLoader = new ChangeLoader(jsonArray, (Util.useOptChangeFile ? vertexSets : null), (Util.useOptChangeFile ? Util.typeChangeURIs : null), Util.currentVSpawnLevel != 0);

			HashMap<Integer,HashSet<Change>> newChanges = changeLoader.getAllGroupedChanges();
			System.out.println("Total number of changes in changefile: " + newChanges.size());

			List<Entry<Integer, HashSet<Change>>> sortedChanges = Util.getSortedChanges(newChanges);
			List<List<Entry<Integer,HashSet<Change>>>> changefiles = new ArrayList<>();
			changefiles.add(sortedChanges);

			long totalLoadChangefileTime = System.currentTimeMillis() - loadChangefileStartTime;
			Util.printWithTime("Load changes for Snapshot (" + (i+2) + ")", totalLoadChangefileTime);
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
						entityURIs.putIfAbsent(entityURI, Util.createEmptyArrayListOfSize(Util.numOfSnapshots));
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
					entityURIs.putIfAbsent(entityURI, Util.createEmptyArrayListOfSize(Util.numOfSnapshots));
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
			Util.printWithTime("Update graph and retrieve matches", finalGraphUpdateAndMatchTime);
			Util.addToTotalMatchingTime(finalGraphUpdateAndMatchTime);
		}

		System.out.println("-------------------------------------");
		System.out.println("Number of entity URIs found: "+entityURIs.size());
		for (Entry<String, List<Integer>> entry: entityURIs.entrySet()) {
			System.out.println(entry);
		}
		System.out.println("Total number of matches found in all snapshots: " + numberOfMatchesFound);
		double S = Util.vertexHistogram.get(patternTreeNode.getPattern().getCenterVertexType());
		double patternSupport = Util.calculatePatternSupport(entityURIs, S, Util.T);
		Util.patternSupportsListForThisSnapshot.add(patternSupport);
		patternTreeNode.setPatternSupport(patternSupport);

		return matchesPerTimestamps;
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
			Util.dissolveSuperVerticesBasedOnCount(graph, Util.INDIVIDUAL_SUPER_VERTEX_INDEGREE_FLOOR);

		Util.printWithTime("Load graph (1)", System.currentTimeMillis()- startTime);
		return graph;
	}
}

