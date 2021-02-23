package experiments.ResponseDistances

import Utils.Results
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import oultierDetectionAlgorithms.{LOF_core, OurMethod, Structs}
import java.io.{BufferedWriter, File, FileWriter}

import scala.collection.mutable.ListBuffer


object ResponseDistance {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("Temporal trace anomaly detection")
      .master("local[*]")
      .getOrCreate()
    println(s"Starting Spark version ${spark.version}")

    val ks = List(5, 10, 15, 25, 50, 75, 100, 200)
    val fracs = List(0.01,0.05,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,0.99)

    val distances: Array[(Structs.Trace_Vector, Structs.Trace_Vector) => Double] = Array(Utils.Utils.distance, Utils.Utils.distanceEuclidean, Utils.Utils.distanceMinkowski, Utils.Utils.distance)
    val names = Array("rmse", "euclidean", "minkowski", "mahalanobis")
    val file = "financial_log"
    val converter: Structs.Log => RDD[Structs.Trace_Vector] = Utils.Utils.convert_to_vector_only_durations

    val output = "output/" + file + "_distances_response_lof"
//    val filename = "input/outliers_" + file + ".xes"
    val filename = "input/" + file + ".xes"
    val log = Utils.Utils.read_xes(filename)
//    val results_file = "input/results_" + file + "_description"
//    val results = Results.read_with_description(results_file)
    var exp = new ListBuffer[String]()
    for (distance <- names.indices) {
      for (k <- ks) {
        println(k, names(distance))
        val t = System.nanoTime
//        val zeta = results.size
//        val outliers2 = OurMethod.assignOutlyingFactor(log, k, converter, distances(distance), names(distance))
        val outliers2 = LOF_core.assignOutlierFactor(log,k,converter,distances(distance),names(distance))
        val duration = (System.nanoTime - t) / 1e9d
        exp += names(distance) + "," + file + "," + k.toString + ",10000," + duration.toString + "\n"
      }
      for(frac <- fracs){
        println(frac, names(distance))
        val k = 50
        val newLog = Structs.Log(log.traces.sample(withReplacement = false,frac),log.activities)
        val t2 = System.nanoTime
//        val outliers2 = OurMethod.assignOutlyingFactor(newLog, k, converter, distances(distance), names(distance))
        val outliers2 = LOF_core.assignOutlierFactor(newLog,k,converter,distances(distance),names(distance))
        val duration2 = (System.nanoTime - t2) / 1e9d
        exp += names(distance) + "," + file + "," + k.toString + ","+newLog.traces.count().toString+"," + duration2.toString + "\n"
      }
    }

    val fileW = new File(output)
    val bw = new BufferedWriter(new FileWriter(fileW))
    exp.toList.foreach(line => {
      bw.write(line)
    })
    bw.close()


  }
}
