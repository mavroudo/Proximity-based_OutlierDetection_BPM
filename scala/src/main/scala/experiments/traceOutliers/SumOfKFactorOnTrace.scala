package experiments.traceOutliers

import Utils.Results
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import oultierDetectionAlgorithms.{LOF_core, OurMethod, Structs}

import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable.ListBuffer

object SumOfKFactorOnTrace {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("Temporal trace anomaly detection")
      .master("local[*]")
      .getOrCreate()
    println(s"Starting Spark version ${spark.version}")

    val ks = List(5, 10, 15, 25, 50, 75, 100, 200)
    val zetas = List(10, 25, 50, 75, 100, 250, 500, 750, 1000, 2000)
    //    val ks =List(5) //test
    //    val zetas = List(10) //test
    val distances: Array[(Structs.Trace_Vector, Structs.Trace_Vector) => Double] = Array(Utils.Utils.distance, Utils.Utils.distanceEuclidean, Utils.Utils.distanceMinkowski, Utils.Utils.distance)
    val names = Array("rmse", "euclidean", "minkowski", "mahalanobis")

    val files = List("30_activities_10k_0.005", "30_activities_10k_0.01", "30_activities_10k_0.05", "30_activities_10k_0.1")
    //    val files = List("test_0.01")
    val converter: Structs.Log => RDD[Structs.Trace_Vector] = Utils.Utils.convert_to_vector_only_durations


    for (dataset <- files) {
      val output = "output/" + dataset + "_oursFactor_trace"
      val filename = "input/outliers_" + dataset + ".xes"
      val log = Utils.Utils.read_xes(filename)
      val results_file = "input/results_" + dataset + "_description"
      val results = Results.read_with_description(results_file)
      var exp = new ListBuffer[String]()

      for (distance <- names.indices) {
        for (k <- ks) {
          println(dataset, k, names(distance))
          val t2 = System.nanoTime
          val zeta = results.size
          val outliers2 = OurMethod.assignOutlyingFactor(log, k, converter, distances(distance), names(distance)).slice(0, zeta)
          val duration2 = (System.nanoTime - t2) / 1e9d
          val found2 = outliers2.count(i => results.map(_._2).contains(i._1)).toDouble / zeta
          exp += names(distance) + "," + dataset + "," + k.toString + "," + zeta.toString + "," + duration2.toString + "," + found2.toString + "\n"
        }
        val k = 50
        val t2 = System.nanoTime
        val outliers2 = OurMethod.assignOutlyingFactor(log, k, converter, distances(distance), names(distance))
        val duration2 = (System.nanoTime - t2) / 1e9d
        for (zeta <- zetas) {
          println(dataset, zeta, names(distance))
          val found2 = outliers2.slice(0, zeta).count(i => results.map(_._2).contains(i._1)).toDouble / zeta
          exp += names(distance) + "," + dataset + "," + k.toString + "," + zeta.toString + "," + duration2.toString + "," + found2.toString + "\n"
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
