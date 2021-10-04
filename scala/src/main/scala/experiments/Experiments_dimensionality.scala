package experiments

import Utils.Results
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import oultierDetectionAlgorithms.OurMethod

import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable.ListBuffer

object Experiments_dimensionality {

//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)
//    val spark = SparkSession.builder()
//      .appName("Temporal trace anomaly detection")
//      .master("local[*]")
//      .getOrCreate()
//    println(s"Starting Spark version ${spark.version}")
//    val k = 50
//    val dims = List(3, 5, 10, 15, 20, 24)
////    val files = List("30_activities_3k_0.01", "30_activities_3k_0.05", "30_activities_3k_0.1", "financial_log_0.01", "financial_log_0.05", "financial_log_0.1")
//    val files = List( "30_activities_3k_0.1")
//    //    our method as it is
//    for (dataset <- files) {
//      println(dataset)
//      val output = "output/" + dataset + "_dimensions"
//      val filename = "input/outliers_" + dataset + ".xes"
//      val log = Utils.Utils.read_xes(filename)
//      val results_file = "input/results_" + dataset+"_description"
//      val results = Results.read_with_description(results_file)
//      var exp = new ListBuffer[String]()
//
//      val t1 = System.nanoTime
//      val outliers = OurMethod.assignOutlyingFactor(log, k,Utils.Utils.convert_to_vector_only_durations,Utils.Utils.distanceEuclidean).slice(0, results.size)
//      val found = outliers.count(i => results.map(_._2).contains(i._1)).toDouble / results.size
//      val duration = (System.nanoTime - t1) / 1e9d
//      exp += "Pure," + dataset + "," + k.toString + "," + duration.toString + "," + found.toString + "\n"
//
//      for(dim <- dims){
//        println(dataset,dim)
//        val t2 = System.nanoTime
//        val outliers2 = OurMethod.assignOutlyingFactorWithPCA(log, k,dim,Utils.Utils.convert_to_vector_only_durations,Utils.Utils.distanceEuclidean).slice(0, results.size)
//        val found2 = outliers2.count(i => results.map(_._2).contains(i._1)).toDouble / results.size
//        val duration2 = (System.nanoTime - t2) / 1e9d
//        exp += "PCA," + dataset + "," + dim.toString + "," + duration2.toString + "," + found2.toString + "\n"
//      }
//
//      val t3=System.nanoTime
//      val ballTree = OurMethod.createBallTree(log,Utils.Utils.convert_to_vector_only_durations)
//      val durationTree = (System.nanoTime - t3) / 1e9d
//      exp += "Create Ball Tree," + dataset + "," + durationTree.toString + "\n"
//
//      val t4 = System.nanoTime
//      val outliers3 = OurMethod.assignOutlyingFactorWithBallTree(log,ballTree, k).slice(0, results.size)
//      val found3 = outliers3.count(i => results.map(_._2).contains(i._1)).toDouble / results.size
//      val duration4 = (System.nanoTime - t4) / 1e9d
//      exp += "BallTree," + dataset + "," + k.toString + "," + duration4.toString + "," + found3.toString + "\n"
//      val file = new File(output)
//      val bw = new BufferedWriter(new FileWriter(file))
//      exp.toList.foreach(line=>{
//        bw.write(line)
//      })
//      bw.close()
//
//    }
//
//
//  }
}
