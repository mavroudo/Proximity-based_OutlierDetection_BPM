package experiments.eventOutliers

import Utils.Results
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import oultierDetectionAlgorithms.{LOF_core, OurMethodEventOutlier}

import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable.ListBuffer

object RunAll {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("Temporal trace anomaly detection")
      .master("local[*]")
      .getOrCreate()
    println(s"Starting Spark version ${spark.version}")
    val ks = List(5, 10, 15, 25, 50, 75, 100, 200,300,400,500)
    val zetas = List(10,20,50,75,100,125,150,200,250,300)
    val ns = List(0.5, 1, 1.25, 1.5, 1.75, 2, 2.5, 2.75, 3, 3.25, 3.5, 3.75, 4)
    val rs = List(0.01, 0.02, 0.03, 0.05, 0.07, 0.09, 0.1, 0.15, 0.2)
    val files = List("30_activities_10k_0.005", "30_activities_10k_0.01", "30_activities_10k_0.05", "30_activities_10k_0.1")
//    val ks =List(5)
//    val zetas =List(10)
//    val ns = List(1)
//    val rs = List(0.1)
//    val files = List("test_0.01")

    val log =Utils.Utils.read_synthetic("input/synthetic_dataset_4.txt")
    val results = Results.read_synthetic("input/resuts_dataset_4.txt")

//    for (dataset <- files) {
//      val output = "output/" + dataset + "_lof_trace"
//      val filename = "input/outliers_" + dataset + ".xes"
//      val log = Utils.Utils.read_xes(filename)



//      val results_file = "input/results_" + dataset + "_description"
//      val results = Results.read_with_description(results_file)
      var exp = new ListBuffer[String]()

      //run LOF on events for k = 50 and zeta = result size (for constant)
      for (k <- ks) {
//        println("LOF", dataset, k)
        println("LOF", k)
        val zeta = results.size
        val t = System.nanoTime
        val outliers = LOF_core.findOutlierEvents(log, k, zeta)
        val duration = (System.nanoTime - t) / 1e9d
        val groundTruth = results.map(x => (x._2, x._3))
        val found = outliers.count(x => groundTruth.contains((x._1.trace_index.toInt, x._1.event_pos.toInt)))
//        exp += "LOF," + dataset + "," + k.toString + "," + zeta.toString + "," + duration.toString + "," + found.toString + "," + outliers.length.toString+"\n"
        exp += "LOF," + k.toString + "," + zeta.toString + "," + duration.toString + "," + found.toString + "," + outliers.length.toString+"\n"
      }
      for (zeta <- zetas) {
//        println("LOF", dataset, zeta)
        println("LOF", zeta)
        val k = 200
        val t = System.nanoTime
        val outliers = LOF_core.findOutlierEvents(log, k, zeta)
        val duration = (System.nanoTime - t) / 1e9d
        val groundTruth = results.map(x => (x._2, x._3))
        val found = outliers.count(x => groundTruth.contains((x._1.trace_index.toInt, x._1.event_pos.toInt)))
//        exp += "LOF," + dataset + "," + k.toString + "," + zeta.toString + "," + duration.toString + "," + found.toString + "," + outliers.length.toString+"\n"
        exp += "LOF," + k.toString + "," + zeta.toString + "," + duration.toString + "," + found.toString + "," + outliers.length.toString+"\n"
      }
      //run Distance with k and zeta, k = 50 and zeta = result size (for constant)
      for (k <- ks) {
//        println("OursFactor", dataset, k)
        println("OursFactor", k)
        val zeta = results.size
        val t = System.nanoTime
        val outliers = OurMethodEventOutlier.findOutlierEventsZeta(log, k, zeta)
        val duration = (System.nanoTime - t) / 1e9d
        val groundTruth = results.map(x => (x._2, x._3))
        val found = outliers.count(x => groundTruth.contains((x._1.trace_index.toInt, x._1.event_pos.toInt)))
//        exp += "OursFactor," + dataset + "," + k.toString + "," + zeta.toString + "," + duration.toString + "," + found.toString + "," + outliers.length.toString+"\n"
        exp += "OursFactor," + k.toString + "," + zeta.toString + "," + duration.toString + "," + found.toString + "," + outliers.length.toString+"\n"
      }
      for (zeta <- zetas) {
//        println("OursFactor", dataset, zeta)
        println("OursFactor", zeta)
        val k = 200
        val t = System.nanoTime
        val outliers = LOF_core.findOutlierEvents(log, k, zeta)
        val duration = (System.nanoTime - t) / 1e9d
        val groundTruth = results.map(x => (x._2, x._3))
        val found = outliers.count(x => groundTruth.contains((x._1.trace_index.toInt, x._1.event_pos.toInt)))
//        exp += "OursFactor," + dataset + "," + k.toString + "," + zeta.toString + "," + duration.toString + "," + found.toString + "," + outliers.length.toString+"\n"
        exp += "OursFactor," + k.toString + "," + zeta.toString + "," + duration.toString + "," + found.toString + "," + outliers.length.toString+"\n"
      }
//      //run Distance with k and n, k=50 and n = 2
      for (n <- ns) {
//        println("OursStatistical", dataset, n)
        println("OursStatistical", n)
        val k = 50
        val t = System.nanoTime
        val outliers = OurMethodEventOutlier.findOutlierEvents(log, k, n)
        val duration = (System.nanoTime - t) / 1e9d
        val groundTruth = results.map(x => (x._2, x._3))
        val found = outliers.count(x => groundTruth.contains((x._1.trace_index.toInt, x._1.event_pos.toInt)))
//        exp += "OursStatistical," + dataset + "," + k.toString + "," + n.toString + "," + duration.toString + "," + found.toString + "," + outliers.length.toString+"\n"
        exp += "OursStatistical," + k.toString + "," + n.toString + "," + duration.toString + "," + found.toString + "," + outliers.length.toString+"\n"
      }
      for (k <- ks) {
//        println("OursStatistical", dataset, k)
        println("OursStatistical", k)
        val n = 2
        val t = System.nanoTime
        val outliers = OurMethodEventOutlier.findOutlierEvents(log, k, n)
        val duration = (System.nanoTime - t) / 1e9d
        val groundTruth = results.map(x => (x._2, x._3))
        val found = outliers.count(x => groundTruth.contains((x._1.trace_index.toInt, x._1.event_pos.toInt)))
//        exp += "OursStatistical," + dataset + "," + k.toString + "," + n.toString + "," + duration.toString + "," + found.toString + "," + outliers.length.toString+"\n"
        exp += "OursStatistical," + k.toString + "," + n.toString + "," + duration.toString + "," + found.toString + "," + outliers.length.toString+"\n"
      }
      //run Distance with k and r, k=50 and r = 0.05
      for (k <- ks) {
//        println("OursTraditional", dataset, k)
        println("OursTraditional", k)
        val r = 0.05
        val t = System.nanoTime
        val outliers = OurMethodEventOutlier.findOutlierEventsTraditional(log, k, r)
        val duration = (System.nanoTime - t) / 1e9d
        val groundTruth = results.map(x => (x._2, x._3))
        val found = outliers.count(x => groundTruth.contains((x._1.trace_index.toInt, x._1.event_pos.toInt)))
//        exp += "OursTraditional," + dataset + "," + k.toString + "," + r.toString + "," + duration.toString + "," + found.toString + "," + outliers.length.toString+"\n"
        exp += "OursTraditional," + k.toString + "," + r.toString + "," + duration.toString + "," + found.toString + "," + outliers.length.toString+"\n"
      }
      for (r <- rs) {
//        println("OursTraditional", dataset, r)
        println("OursTraditional", r)
        val k = 50
        val t = System.nanoTime
        val outliers = OurMethodEventOutlier.findOutlierEventsTraditional(log, k, r)
        val duration = (System.nanoTime - t) / 1e9d
        val groundTruth = results.map(x => (x._2, x._3))
        val found = outliers.count(x => groundTruth.contains((x._1.trace_index.toInt, x._1.event_pos.toInt)))
//        exp += "OursTraditional," + dataset + "," + k.toString + "," + r.toString + "," + duration.toString + "," + found.toString + "," + outliers.length.toString+"\n"
        exp += "OursTraditional," + k.toString + "," + r.toString + "," + duration.toString + "," + found.toString + "," + outliers.length.toString+"\n"
      }
      val output="output/synthetic_events"
      val file = new File(output)
      val bw = new BufferedWriter(new FileWriter(file))
      exp.toList.foreach(line => {
        bw.write(line)
      })
      bw.close()

//    }


  }


}
