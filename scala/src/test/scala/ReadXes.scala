import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import oultierDetectionAlgorithms.{LOF_core, OurMethod, Structs}

import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable.ListBuffer

class ReadXes extends FunSuite {

  test("Read xes files to Structs.Log") {

    val spark = SparkSession.builder()
      .appName("Temporal trace anomaly detection")
      .master("local[*]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    val f1 = "input/bpi_2015.xes" //http://doi.org/10.4121/uuid:a0addfda-2044-4541-a450-fdcc9fe16d17
    val f2 = "input/bpi_2011.xes" //http://doi.org/10.4121/uuid:d9769f3d-0ab0-4fb8-803b-0d1120ffcf54
    val f3 = "input/bpi_2012.xes" //http://doi.org/10.4121/uuid:3926db30-f712-4394-aebc-75976070e91f
    val f4 = "input/bpi_2017.xes" //https://doi.org/10.4121/uuid:5f3067df-f10b-45da-b98b-86ae4c7a310b

    val sl1 = Utils.Utils.read_xes(f1)
    val events1 = sl1.traces.map(x => x.events.length)
    val sl2 = Utils.Utils.read_xes(f2)
    val events2 = sl2.traces.map(x => x.events.length)
    val sl3 = Utils.Utils.read_xes(f3)
    val sl4 = Utils.Utils.read_xes(f4)
    println("hello")

  }

  test("Test execution time per different k and different distances") {
    val spark = SparkSession.builder()
      .appName("Temporal trace anomaly detection")
      .master("local[*]")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    val converter: Structs.Log => RDD[Structs.Trace_Vector] = Utils.Utils.convert_to_vector_only_durations
    //    val datasets = List("bpi_2012")
    val datasets = List("bpi_2012", "bpi_2011", "bpi_2015", "bpi_2017")
    //    val distances: Array[(Structs.Trace_Vector, Structs.Trace_Vector) => Double] = Array(Utils.Utils.distance)
    val distances: Array[(Structs.Trace_Vector, Structs.Trace_Vector) => Double] = Array(Utils.Utils.distance,Utils.Utils.distanceEuclidean, Utils.Utils.distanceMinkowski, Utils.Utils.distance)
    //    val names = Array("rmse")
    val names = Array("rmse","euclidean", "minkowski", "mahalanobis")
//    val ks = List(5)
    val ks = List(50)
    for (file <- datasets) {
      var exp = new ListBuffer[String]()
      val log = Utils.Utils.read_xes("input/" + file + ".xes")
      for (d <- names.indices) {
        for (k <- ks) {
          println(file, names(d), k)
          val t1 = System.nanoTime
          val outliers = OurMethod.assignOutlyingFactor(log, k, converter, distances(d), names(d))
          val duration1 = (System.nanoTime - t1) / 1e9d
          exp += "Topz," + names(d) + "," + k.toString + "," + duration1.toString + "\n"
          val t2 = System.nanoTime
          val outliers2 = LOF_core.assignOutlierFactor(log, k, converter, distances(d), names(d))
          val duration2 = (System.nanoTime - t2) / 1e9d
          exp += "LOF," + names(d) + "," + k.toString + "," + duration2.toString + "\n"
        }
      }
      val outfile = new File("output/" + file + "execution_time.txt")
      val bw = new BufferedWriter(new FileWriter(outfile))
      exp.toList.foreach(line => {
        bw.write(line)
      })
      bw.close()
    }

  }

}
