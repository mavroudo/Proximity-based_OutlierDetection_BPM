package Indexing.BallTree

import java.io.{BufferedWriter, File, FileWriter}
import Utils.Preprocess
import Utils.Utils
import au.com.bytecode.opencsv.CSVWriter
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import oultierDetectionAlgorithms.Structs.Trace_Vector
import weka.core.converters.CSVLoader
import weka.core.{EuclideanDistance, Instance, Instances}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
class BallTree(traces: RDD[Trace_Vector]) {
  private var wekaBallTree: BallTree_Weka = _
  private val output: String = "output/ball_tree.csv"
  private var data: Instances = _

  def buildTree(): Unit = {
    //first we got to write traces to csv file
    this.writeToCsv(this.output, traces)
    //load instances from csv file back
    this.data = this.getInstances(this.output)
    this.wekaBallTree = new BallTree_Weka(this.data)
    val distance: EuclideanDistance = new EuclideanDistance(this.data)
    distance.setDontNormalize(true)
    distance.setAttributeIndices("2-last") //this can be change, depending on the distance we want to calculate
    this.wekaBallTree.setEuclideanDistanceFunction(distance)
    this.wekaBallTree.buildTree()
  }

  def getInstance(i: Int): Instance = {
    this.data.get(i)
  }

  def kNearestNeighbors(index: Int, k: Int): List[(Int, Double)] = {
    val nn = this.wekaBallTree.kNearestNeighbours(data.get(index), k)
    val distances = this.wekaBallTree.getDistances
    distances.zipWithIndex.map(d => {
      (nn.get(d._2).value(0).toInt, d._1)
    }).toList
  }

  private def writeToCsv(filePath: String, traces: RDD[Trace_Vector]): Unit = {
    val outputFile = new BufferedWriter(new FileWriter(filePath))
    val csvWriter = new CSVWriter(outputFile)
    var data: ListBuffer[Array[String]] = new ListBuffer[Array[String]]
    var schema: ArrayBuffer[String] = new ArrayBuffer[String]()
    schema += "id"
    for (i <- 1 until traces.first().elements.length + 1) {
      schema += "el_" + i.toString
    }
    data += schema.toArray
    traces.collect.map(x => {
      x.id.toDouble +: x.elements
    }).foreach(x => {
      var l: ArrayBuffer[String] = new ArrayBuffer[String]()
      x.foreach(n => l += n.toString)
      data += l.toArray
    })

    val jList: java.util.List[Array[String]] = data.asJava
    csvWriter.writeAll(jList)
    outputFile.close()
  }

  private def getInstances(filePath: String): Instances = {
    val loader: CSVLoader = new CSVLoader()
    loader.setSource(new File(filePath))
    loader.getDataSet
  }


}

object Main {


  def main(args: Array[String]): Unit = {
    val filename = "input/financial_log.xes"
    val dims = 10
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("Temporal trace anomaly detection")
      .master("local[*]")
      .getOrCreate()
    println(s"Starting Spark version ${spark.version}")
    val log = Utils.read_xes(filename)
    val rddTransformed_whole = Preprocess.preprocess(log, dims, Utils.convert_to_vector_both_duration_repetitions)


    val ballTree: BallTree = new BallTree(rddTransformed_whole)
    ballTree.buildTree()
    ballTree.kNearestNeighbors(0, 20).foreach(println)


  }
}
