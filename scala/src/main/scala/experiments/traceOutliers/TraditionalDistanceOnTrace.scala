package experiments.traceOutliers

import Utils.Results
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import oultierDetectionAlgorithms.{OurMethod, Structs}

import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable.ListBuffer

object TraditionalDistanceOnTrace {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("Temporal trace anomaly detection")
      .master("local[*]")
      .getOrCreate()
    println(s"Starting Spark version ${spark.version}")

    val ks = List(5, 10, 15, 25, 50, 75, 100, 200)
    val rs = List(0.01, 0.02, 0.03, 0.05, 0.07, 0.09, 0.1, 0.15, 0.2, 0.3)
    //
    //    val ks =List(5)
    //    val rs = List(5)

    val distances: Array[(Structs.Trace_Vector, Structs.Trace_Vector) => Double] = Array(Utils.Utils.distance, Utils.Utils.distanceEuclidean, Utils.Utils.distanceMinkowski, Utils.Utils.distance)
    val names = Array("rmse", "euclidean", "minkowski", "mahalanobis")

    //    val files = List("test_0.01")
    val files = List("30_activities_10k_0.005", "30_activities_10k_0.01", "30_activities_10k_0.05", "30_activities_10k_0.1")
    val converter: Structs.Log => RDD[Structs.Trace_Vector] = Utils.Utils.convert_to_vector_only_durations


    for (dataset <- files) {
      val output = "output/" + dataset + "_oursTraditional_trace"
      val filename = "input/outliers_" + dataset + ".xes"
      val log = Utils.Utils.read_xes(filename)
      val results_file = "input/results_" + dataset + "_description"
      val results = Results.read_with_description(results_file)
      var exp = new ListBuffer[String]()

      for (distance <- names.indices) {
        for (k <- ks) {
          val r = 0.05
          println(dataset, k, names(distance))
          val t2 = System.nanoTime
          val zeta = results.size
          val outliers2 = OurMethod.findOutliers(log, k, r, converter, distances(distance), names(distance))
          val duration2 = (System.nanoTime - t2) / 1e9d
          val found2 = outliers2.count(i => results.map(_._2).contains(i)).toDouble / outliers2.length
          exp += names(distance) + "," + dataset + "," + k.toString + "," + r.toString + "," + duration2.toString + "," + found2.toString + "," + outliers2.length.toString + "\n"
        }
        for (r <- rs) {
          val k = 50
          val t2 = System.nanoTime
          val outliers2 = OurMethod.findOutliers(log, k, r, converter, distances(distance), names(distance))
          val duration2 = (System.nanoTime - t2) / 1e9d
          println(dataset, r, names(distance))
          val found2 = outliers2.count(i => results.map(_._2).contains(i)).toDouble / outliers2.length
          exp += names(distance) + "," + dataset + "," + k.toString + "," + r.toString + "," + duration2.toString + "," + found2.toString + "," + outliers2.length.toString + "\n"

        }
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
