package experiments.traceOutliers

import Utils.Results
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import oultierDetectionAlgorithms.{ODAL, OurMethod, Structs}

import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable.ListBuffer

object ODALonTrace {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("Temporal trace anomaly detection")
      .master("local[*]")
      .getOrCreate()
    println(s"Starting Spark version ${spark.version}")

    val ks = List(2, 3, 5, 10, 15, 25, 50, 75, 100, 200)
    val ns = List(0.5, 1, 1.25, 1.5, 1.75, 2, 2.5, 2.75, 3, 3.25, 3.5, 3.75, 4)

    //        val ks =List(5)
    //    val ns = List(4)

    //    val files = List("30_activities_10k_0.1")
    val files = List("30_activities_10k_0.005", "30_activities_10k_0.01", "30_activities_10k_0.05", "30_activities_10k_0.1")
    val converter: Structs.Log => RDD[Structs.Trace_Vector] = Utils.Utils.convert_to_vector_only_durations


    for (dataset <- files) {
      val output = "output/" + dataset + "_odal3_trace"
      val filename = "input/outliers_" + dataset + ".xes"
      val log = Utils.Utils.read_xes(filename)
      val results_file = "input/results_" + dataset + "_description"
      val results = Results.read_with_description(results_file)
      var exp = new ListBuffer[String]()

      for (k <- ks) {
        val n = 2
        println(dataset, k)
        val t2 = System.nanoTime
        val zeta = results.size
        val outliers2 = ODAL.find_outliers_admition(log, k, n)

        val duration2 = (System.nanoTime - t2) / 1e9d
        val found2 = outliers2.count(i => results.map(_._2).contains(i)).toDouble / outliers2.length
        exp += dataset + "," + k.toString + "," + n.toString + "," + duration2.toString + "," + found2.toString + "," + outliers2.length.toString + "\n"
      }
      for (n <- ns) {
        val k = 50
        val t2 = System.nanoTime
        val outliers2 = ODAL.find_outliers_admition(log, k, n.toFloat)
        val duration2 = (System.nanoTime - t2) / 1e9d
        println(dataset, n)
        val found2 = outliers2.count(i => results.map(_._2).contains(i)).toDouble / outliers2.length
        exp += dataset + "," + k.toString + "," + n.toString + "," + duration2.toString + "," + found2.toString + "," + outliers2.length.toString + "\n"

      }

      val file = new File(output)
      val bw = new BufferedWriter(new FileWriter(file))
      exp.toList.foreach(line => {
        bw.write(line)
      })
      bw.close()
    }
  }

}
