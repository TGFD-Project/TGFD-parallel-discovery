// Databricks notebook source
// MAGIC %md
// MAGIC # Summary
// MAGIC 
// MAGIC Notebook for testing matching on synthetic graphs.

// COMMAND ----------

import edu.drexel.cs.dbgroup.portal._
import edu.drexel.cs.dbgroup.portal.ProgramContext
import edu.drexel.cs.dbgroup.portal.representations.VEGraph
import edu.drexel.cs.dbgroup.portal.TGraph
import edu.drexel.cs.dbgroup.portal.util.TempGraphOps
import java.sql.Date
import java.time.LocalDate
import org.apache.spark.graphx._
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Time
import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map, SynchronizedMap, HashMap}
import scala.reflect.ClassTag
import scala.util.Random
import scala.util.control.Breaks._

ProgramContext.setContext(sc)

// COMMAND ----------

// MAGIC %md
// MAGIC # Schema

// COMMAND ----------

// MAGIC %md
// MAGIC ## Vertex schema

// COMMAND ----------

// Define custom vertex factory.
type CustomVertexAttribute = (/*vtype*/String, List[(/*key*/String, /*value*/String)])
type CustomVertex = (VertexId, (Interval, CustomVertexAttribute))
def createCustomVertex(vid: Long, start: String, end: String, vtype: String, keyValues: List[(String, String)]) : CustomVertex = {
  (vid, (Interval(LocalDate.parse(start), LocalDate.parse(end)), (vtype, keyValues)))
}

// TGraph construction needs a default value for vertices
val DefaultCustomVertexAttribute = ("", List[(String, String)]())

// Define schema for converting vertices to Dataframe.
val vertexSchema = StructType(
  StructField("VID"         , LongType  , false) ::
  StructField("Start"       , DateType  , false) ::
  StructField("End"         , DateType  , false) ::
  StructField("VType"       , StringType, false) ::
  StructField("KeyValueList", StringType, false) :: Nil
)

// Create Dataframe from vertices.
val dfFromVertices = (vertices: RDD[CustomVertex]) =>
  sqlContext.createDataFrame(
    vertices.map{ case (vid, (interval, (vtype, keyValues))) => Row(vid, Date.valueOf(interval.start), Date.valueOf(interval.end), vtype, keyValues.mkString(",")) },
    vertexSchema)

// Display vertices in Databricks table.
def displayVertices(vertices: RDD[CustomVertex]) = {
  display(dfFromVertices(vertices).limit(1000).sort("VID", "Start"))  
}

// COMMAND ----------

// MAGIC %md
// MAGIC ## Edge schema

// COMMAND ----------

// Define custom edge factory.
type CustomEdgeAttribute = String
type CustomEdge = TEdge[CustomEdgeAttribute]
def createCustomEdge(eid: Long, srcId: Long, dstId: Long, start: String, end: String, attribute: String) = {
  TEdge[CustomEdgeAttribute](eid, srcId, dstId, Interval(LocalDate.parse(start), LocalDate.parse(end)), attribute)
}

// Define schema for converting vertices to Dataframe.
val edgeSchema = StructType(
  StructField("EID"      , LongType  , false) ::
  StructField("SrcId"    , LongType  , false) ::
  StructField("DstId"    , LongType  , false) ::
  StructField("Start"    , DateType  , false) ::
  StructField("End"      , DateType  , false) ::
  StructField("Attribute", StringType, false) :: Nil
)

// Create Dataframe from edges.
val dfFromEdges = (edges: RDD[CustomEdge]) =>
  sqlContext.createDataFrame(
    edges.map{ edge => Row(edge.eId, edge.srcId, edge.dstId, Date.valueOf(edge.interval.start), Date.valueOf(edge.interval.end), edge.attr) },
    edgeSchema)

// Display edges in Databricks table.
def displayEdges(edges: RDD[CustomEdge]) = {
  display(dfFromEdges(edges).limit(1000).sort("EID", "Start"))  
}

// COMMAND ----------

// MAGIC %md
// MAGIC # TGraphBuilder

// COMMAND ----------

object TGraphBuilder 
{
  /**
    * @param pattern List of edges of srcVertexType to dstVertexType.
    * @param instances Number of instances of the patterns to create.
    * @param intervals Number of intervals (in years).
    * @param start Start interval.
    * @param change Percentage chance of change between each interval between 0 and 1 (by randomly changing the vertex attribute).
    * @param verbose Print the graph as it changes.
    */
  def apply(
    pattern: List[(String, String)],
    instances: Int,
    intervals: Int,
    start: LocalDate,
    change: Double,
    verbose: Boolean = false) = { // TGraph[CustomVertexAttribute,String] = {
    val (vertices, edges) = createInitalSnapshot(pattern, instances, start, start.plusYears(1))
    
    var graph = VEGraph.fromRDDs(
      sc.parallelize(vertices.toSeq),
      sc.parallelize(edges.toSeq),
      DefaultCustomVertexAttribute, StorageLevel.MEMORY_ONLY_SER)
    printGraph(graph, verbose, start, start.plusYears(1))
    
    for (i <- 1 to (intervals - 1)) {
      graph = appendNextSnapshot(graph, vertices, edges, start.plusYears(i), start.plusYears(i + 1), change)
      printGraph(graph, verbose, start.plusYears(i), start.plusYears(i + 1))
    }
    
    graph
  }
  
  // Create the initial graph based on the pattern, number of instances, and start date.
  private def createInitalSnapshot(pattern: List[(String, String)], instances: Int, start: LocalDate, end: LocalDate)
    : (ListBuffer[CustomVertex], ListBuffer[CustomEdge]) = {
    var vertices = ListBuffer[CustomVertex]()
    var edges = ListBuffer[CustomEdge]()

    val vtypes = getVtypes(pattern)
    var nextVertexId = 1L
    var nextEdgeId = 1L
    for (i <- 1 to instances) {
      // Create type to vertex map for the current pattern to easily create edges
      var idByVtype = Map[String, VertexId]()
      
      for (vtype <- vtypes) {
        vertices += createCustomVertex(nextVertexId, start.toString, end.toString, vtype, List(("name", Random.alphanumeric.take(3).mkString)))
        idByVtype += (vtype -> nextVertexId)
        nextVertexId += 1
      }
      
      for (edgePattern <- pattern) {
        val srcId = idByVtype.getOrElse(edgePattern._1, 0L)
        val dstId = idByVtype.getOrElse(edgePattern._2, 0L)
        edges += createCustomEdge(nextEdgeId, srcId, dstId, start.toString, end.toString, edgePattern._1 + edgePattern._2)
        nextEdgeId += 1
      }
    }
    
    (vertices, edges)
  }
  
  /** Appends the next snapshot interval with randomly changed attributes. 
    * @param graph The graph to append to.
    * @param prevVertices The vertices in the initial snapshot (used to randomly pick vertice attributes to change)
    * @param prevEdges The edges in the initial snapshot (used to randomly pick vertice attributes to change)
    * @param start Start of the next snapshot.
    * @param end End of the next snapshot.
    * @param change Percentage chance of change between each interval (by randomly changing the vertex attribute).
    */
  private def appendNextSnapshot(graph: VEGraph[CustomVertexAttribute, CustomEdgeAttribute], initVertices: ListBuffer[CustomVertex], initEdges: ListBuffer[CustomEdge], start: LocalDate, end: LocalDate, change: Double)
    : VEGraph[CustomVertexAttribute, CustomEdgeAttribute] = {  
    val vertices = initVertices.map{
      case(vid, (intv, (vtype, keyValues))) =>
        createCustomVertex(
          vid, start.toString, end.toString, vtype,
          if (Random.nextDouble <= change) List(("name", Random.alphanumeric.take(3).mkString)) else keyValues)
    }
    val edges = initEdges.map(e =>
      createCustomEdge(e.eId, e.srcId, e.dstId, start.toString, end.toString, e.attr))

    val nextSnapshot = VEGraph.fromRDDs(
      sc.parallelize(vertices.toSeq),
      sc.parallelize(edges.toSeq),
      DefaultCustomVertexAttribute, StorageLevel.MEMORY_ONLY_SER)
    graph.union(nextSnapshot, (vattr1, vattr2) => vattr1, (eattr1, eattr2) => eattr1)
  }
  
  // Extract the vtypes defined in the pattern.
  private def getVtypes(pattern: List[(String, String)]): List[String] = {
    pattern
      .map(e => List(e._1, e._2))
      .flatten
      .distinct
  }
  
  private def printGraph(graph: VEGraph[CustomVertexAttribute, CustomEdgeAttribute], verbose: Boolean, start: LocalDate, end: LocalDate) = {
    if (verbose) {  
      println("-- from " + start.toString + " to " + end.toString)
      println("vertices:")
      graph.vertices.collect.sortBy(v => (v._1, v._2._1)).foreach(println(_))
      println("edges:")
      graph.edges.collect.sortBy(e => (e.eId, e.interval)).foreach(println(_))
    }
  }
}

val testTGraphBuilder = false
if (testTGraphBuilder) {
TGraphBuilder.apply(
  pattern = ("A", "B") :: ("B", "C") :: Nil,
  instances = 1,
  intervals = 2,
  start = LocalDate.parse("2000-01-01"),
  change = 0.5,
  verbose = true)
}

// COMMAND ----------

// MAGIC %md
// MAGIC # Graphs

// COMMAND ----------

// MAGIC %md
// MAGIC ## Graph Alpha  
// MAGIC 
// MAGIC **Pattern of vertices**:
// MAGIC - A
// MAGIC - B
// MAGIC 
// MAGIC **Pattern of edges**:
// MAGIC - A to B

// COMMAND ----------

val graphAlpha = TGraphBuilder.apply(
  pattern = ("A", "B") :: Nil,
  instances = 100,
  intervals = 10,
  start = LocalDate.parse("2000-01-01"),
  change = 0.5,
  verbose = false)

// COMMAND ----------

val predAlpha = (id: VertexId, attr: CustomVertexAttribute, interval: Interval) => attr._1 == "A" || attr._1 == "B"
val subgraphAlpha = graphAlpha.vsubgraph(predAlpha)

// COMMAND ----------

 displayVertices(graphAlpha.vertices)

// COMMAND ----------

displayEdges(graphAlpha.edges)

// COMMAND ----------

case class TPattern(
  var srcPred: (CustomVertexAttribute) => Boolean,
  var dstPred: (CustomVertexAttribute) => Boolean)
  extends Serializable {}

def extractMatches() = {
  
}

def extractMatches(
  graph: VEGraph[CustomVertexAttribute, CustomEdgeAttribute],
  pred: TPattern,
  chainingSrcId: VertexId = -1) = //: VEGraph[CustomVertexAttribute, CustomEdgeAttribute] =
{
 
  
  
}

// COMMAND ----------

 displayVertices(subgraphAlpha.vertices)

// COMMAND ----------

displayEdges(subgraphAlpha.edges)

// COMMAND ----------

/*case class TPattern[VD: ClassTag](
  var srcPred: (VD) => Boolean,
  var dstPred: (VD) => Boolean)
  extends Serializable {}

// TODO: create outer recursive function to use this inner function (make use of a pred Transversable[(TPattern[VD]) => Boolean]) [2021-01-21]
def findMatches[VD: ClassTag, ED: ClassTag](
  vertices: RDD[(VertexId, (Interval, VD))],
  edges: RDD[TEdge[ED]],
  pred: TPattern[VD],
  chainingSrcId: VertexId = -1): List[(RDD[(VertexId, (Interval, VD))], RDD[TEdge[ED]])] =
{
  // CONSIDER: use vsubgraph with a complex predicate filtering for all desired vertex types which will also filter for valid edges [2021-01-29]
  // e.g. val pred = (id: VertexId, attr: CustomVertexAttribute, interval: Interval) => attr._1 == "A" || attr._1 == "B" || attr._1 == "C"
  
  // TODO: handle multiple src vertices e.g. (A->B, A-C) by grouping preds by pred.srcPred [2021-01-22]
  val srcVerts: RDD[(VertexId, (Interval, VD))] = vertices
    .filter{ case (vid, (intv, attrs)) => pred.srcPred(attrs) }
  val dstVerts: RDD[(VertexId, (Interval, VD))] = vertices
    .filter{ case (vid, (intv, attrs)) => pred.dstPred(attrs) }
  
  val srcVertsById = srcVerts.groupBy{ case (vid, (intv, attrs)) => vid }
  
  // CONSIDER: constrain edges before extracting (if not using vsubgraph) [2021-01-22]
  //val newVerts = sc.union(srcVerts, dstVerts)
  //val newEdges = weakConstrainEdges(newVerts, graph.edges)
  
  val result = new ListBuffer[(RDD[(VertexId, (Interval, VD))], RDD[TEdge[ED]])]()
  
  for (srcVertsOfId <- srcVertsById.collect) { // CONSIDER: avoid collect [2021-01-22]
    if (chainingSrcId != -1) {
      // TODO: check if srcVertsOfId is a dstId in the edges if chainingSrcId != -1 [2021-01-22]  
    }
    
    // Filter for edges that have a valid source vertex
    val newEdges = edges.filter{ e => e.srcId == srcVertsOfId._1 }
    
    // Filter for destination vertices that have a valid edge pointing to it
    val newDstEdgeIds = newEdges.map(e => (e.dstId, true)).collectAsMap
    val newDstVerts = dstVerts.filter{ case (vid, (intv, attrs)) => newDstEdgeIds.contains(vid) }
    
    // Add back the edges on the destination vertices (to be used for chaining)
    // TODO: add edges [2021-01-22]
    
    val newVerts = sc.union(sc.parallelize(srcVertsOfId._2.toSeq), newDstVerts)
    result += ((newVerts, newEdges))
  }
  
  result.toList
}

val func = () =>
{
  val patternA = TPattern[CustomVertexAttribute](
    (attr: CustomVertexAttribute) => attr._1 == "A", (attr: CustomVertexAttribute) => attr._1 == "B")
  findMatches(graphAlpha.vertices, graphAlpha.edges, patternA, -1)  
}
val matchesA = func()
*/

// COMMAND ----------

// MAGIC %md
// MAGIC ## Graph A
// MAGIC 
// MAGIC Pattern of edges:
// MAGIC - A to B, A to C
// MAGIC - B to D, B to E
// MAGIC - E to D, E to G
// MAGIC - G to H
// MAGIC - H to D

// COMMAND ----------

val graphA = TGraphBuilder.apply(
  pattern =
    ("A", "B") :: ("A", "C") ::
    ("B", "D") :: ("B", "E") ::
    ("D", "G") ::
    ("E", "D") :: ("E", "G") ::
    ("G", "H") ::
    ("H", "D") :: Nil,
  instances = 100,
  intervals = 10,
  start = LocalDate.parse("2000-01-01"),
  change = 0.5,
  verbose = false)

// COMMAND ----------

val predA = (id: VertexId, attr: CustomVertexAttribute, interval: Interval) =>
  attr._1 == "A" ||
  attr._1 == "B" ||
  attr._1 == "C" ||
  attr._1 == "D" ||
  attr._1 == "E" ||
  attr._1 == "G" ||
  attr._1 == "H"
val subgraphA = graphA.vsubgraph(predA)

// COMMAND ----------

 displayVertices(graphA.vertices)

// COMMAND ----------

displayEdges(graphA.edges)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Graph B
// MAGIC 
// MAGIC Pattern of edges:
// MAGIC - A to B
// MAGIC - A to C
// MAGIC - A to D
// MAGIC - A to E

// COMMAND ----------

val graphB = TGraphBuilder.apply(
  pattern =
    ("A", "B") :: 
    ("A", "C") ::
    ("A", "D") ::
    ("A", "E") ::
    ("A", "F") :: Nil,
  instances = 100,
  intervals = 10,
  start = LocalDate.parse("2000-01-01"),
  change = 0.5,
  verbose = false)

// COMMAND ----------

val predB = (id: VertexId, attr: CustomVertexAttribute, interval: Interval) =>
  attr._1 == "A" ||
  attr._1 == "B" ||
  attr._1 == "C" ||
  attr._1 == "D" ||
  attr._1 == "E" ||
  attr._1 == "F"
val subgraphB = graphB.vsubgraph(predB)

// COMMAND ----------

 displayVertices(graphB.vertices)

// COMMAND ----------

displayEdges(graphB.edges)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Graph C
// MAGIC 
// MAGIC Pattern of edges:
// MAGIC - A to B
// MAGIC - B to C
// MAGIC - C to D

// COMMAND ----------

val graphC = TGraphBuilder.apply(
  pattern =
    ("A", "B") :: 
    ("B", "C") ::
    ("C", "D") :: Nil,
  instances = 100,
  intervals = 50,
  start = LocalDate.parse("2000-01-01"),
  change = 0.1,
  verbose = false)

// COMMAND ----------

val predC = (id: VertexId, attr: CustomVertexAttribute, interval: Interval) =>
  attr._1 == "A" ||
  attr._1 == "B" ||
  attr._1 == "C" ||
  attr._1 == "D"
val subgraphC = graphC.vsubgraph(predC)

// COMMAND ----------

 displayVertices(graphC.vertices)

// COMMAND ----------

displayEdges(graphC.edges)
