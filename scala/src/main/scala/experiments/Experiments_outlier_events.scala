package experiments

import java.io.{BufferedWriter, File, FileWriter}

import Utils.Results
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import oultierDetectionAlgorithms.EventPairPerActivity

import scala.collection.mutable.ListBuffer

object Experiments_outlier_events {

//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)
//    val spark = SparkSession.builder()
//      .appName("Temporal trace anomaly detection")
//      .master("local[*]")
//      .getOrCreate()
//    println(s"Starting Spark version ${spark.version}")
//
//    val ks = List(1, 3, 10, 20, 50, 75, 100)
//    val rs = List(0.01, 0.05, 0.1, 0.15, 0.2)
//    val files = List("30_activities_10k_0.01", "30_activities_10k_0.05", "30_activities_10k_0.1")
//
//    for (dataset <- files) {
//      val filename = "input/outliers_" + dataset + ".xes"
//      val log = Utils.Utils.read_xes(filename)
//      val results_file = "input/results_" + dataset + "_description"
//      val results = Results.read_with_description(results_file)
//      var exp = new ListBuffer[String]()
//
//      //    check for different ks
//      for (k <- ks) {
//        println(k)
//        val r = 0.01
//        val groundTruth = results.map(x => (x._2, x._3))
//        val outlierEvents = EventPairPerActivity.getOutlierEvents(log, k, r).collect()
//        val found = outlierEvents
//          .map(x => (x.trace_id.toInt, x.event_pos))
//          .count(x => groundTruth.contains(x))
//        val precision = found.toDouble / outlierEvents.length
//        val recall = found.toDouble / results.length
//        exp += k.toString + "," + r.toString + "," + precision.toString + "," + recall.toString + "\n"
//      }
//
//      //    check for different rs
//      for (r <- rs) {
//        println(r)
//        val k = 50
//        val groundTruth = results.map(x => (x._2, x._3))
//        val outlierEvents = EventPairPerActivity.getOutlierEvents(log, k, r).collect()
//        val found = outlierEvents
//          .map(x => (x.trace_id.toInt, x.event_pos))
//          .count(x => groundTruth.contains(x))
//        val precision = found.toDouble / outlierEvents.length
//        val recall = found.toDouble / results.length
//        exp += k.toString + "," + r.toString + "," + precision.toString + "," + recall.toString + "\n"
//      }
//
//      val output = "output/" + dataset + "_precision_recall"
//      val file = new File(output)
//      val bw = new BufferedWriter(new FileWriter(file))
//      exp.toList.foreach(line => {
//        bw.write(line)
//      })
//      bw.close()
//
//    }
//  }

}
