import Utils.Results
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import oultierDetectionAlgorithms.{LOF_core, ODAL, OurMethod, Structs}

import scala.collection.mutable.ListBuffer

class StatisticalTest extends FunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var results: List[(String, Int, Int)] = _
  private var log: Structs.Log = _
  private val distances: Array[(Structs.Trace_Vector, Structs.Trace_Vector) => Double] = Array(Utils.Utils.distance, Utils.Utils.distanceEuclidean, Utils.Utils.distanceMinkowski, Utils.Utils.distance)
  private val distance_names = Array("rmse", "euclidean", "minkowski", "mahalanobis")

  override def beforeAll(): Unit = {
    val filename = "input/outliers_30_activities_10k_0.1.xes"
    val results_file = "input/results_30_activities_10k_0.1_description"
    results = Results.read_with_description(results_file)
    Logger.getLogger("org").setLevel(Level.ERROR)
    spark = SparkSession.builder()
      .appName("Temporal trace anomaly detection")
      .master("local[*]")
      .getOrCreate()
    println(s"Starting Spark version ${spark.version}")
    log = Utils.Utils.read_xes(filename)
  }

  test("run on sample topz") {
    def meanList(l: List[Double]): Double = l.sum / l.length

    val k = 100
    for ((distance, distance_name) <- distances.zip(distance_names)) {
      println(distance_name)
      val precision = new ListBuffer[Double]
      val recall = new ListBuffer[Double]
      val f1 = new ListBuffer[Double]
      for (i <- 1 to 10) {
        println(i)
        val sampled_traces = log.traces.takeSample(false, 5000)
        val new_log = Structs.Log(spark.sparkContext.parallelize(sampled_traces), log.activities)
        val outliers = results.filter(x => sampled_traces.map(_.id).contains(x._2))
        val scores = OurMethod.assignOutlyingFactor(new_log, k, Utils.Utils.convert_to_vector_only_durationsMean, distance, distance_name)
        val found = scores.slice(0, outliers.size + 1).count(i => outliers.map(_._2).contains(i._1.toInt))
        val p = found.toDouble / outliers.size.toDouble
        val r = found.toDouble / outliers.size.toDouble
        precision += p
        recall += r
        f1 += 2 * (p * r) / (p + r)
      }
      println(distance_name + ": Precision: ", meanList(precision.toList), "Recall: ", meanList(recall.toList), "F1: ", meanList(f1.toList))
    }
  }

  test("run on sample lof") {
    def meanList(l: List[Double]): Double = l.sum / l.length

    val k = 50
    for ((distance, distance_name) <- distances.zip(distance_names)) {
      println(distance_name)
      val precision = new ListBuffer[Double]
      val recall = new ListBuffer[Double]
      val f1 = new ListBuffer[Double]
      for (i <- 1 to 10) {
        val sampled_traces = log.traces.takeSample(false, 5000)
        val new_log = Structs.Log(spark.sparkContext.parallelize(sampled_traces), log.activities)
        val outliers = results.filter(x => sampled_traces.map(_.id).contains(x._2))

        val scores = LOF_core.assignOutlierFactor(new_log, k, Utils.Utils.convert_to_vector_only_durationsMean, distance, distance_name)
        val found = scores.slice(0, outliers.size + 1).count(i => outliers.map(_._2).contains(i._1.toInt))
        val p = found.toDouble / outliers.size.toDouble
        val r = found.toDouble / outliers.size.toDouble
        precision += p
        recall += r
        f1 += 2 * (p * r) / (p + r)
      }
      println(distance_name + ": Precision: ", meanList(precision.toList), "Recall: ", meanList(recall.toList), "F1: ", meanList(f1.toList))
    }
  }

  test("run on sample statistic") {
    def meanList(l: List[Double]): Double = l.sum / l.length
    val k = 50
    val n = 2
    val filenames = List("input/outliers_30_activities_10k_0.005.xes","input/outliers_30_activities_10k_0.01.xes","input/outliers_30_activities_10k_0.05.xes","input/outliers_30_activities_10k_0.1.xes")
    val result_files = List("input/results_30_activities_10k_0.005_description","input/results_30_activities_10k_0.01_description","input/results_30_activities_10k_0.05_description","input/results_30_activities_10k_0.1_description")
    for ((fn,rn) <-filenames.zip(result_files)){
      println(fn)
      results = Results.read_with_description(rn)
      log = Utils.Utils.read_xes(fn)
      for ((distance, distance_name) <- distances.zip(distance_names)) {
        println(distance_name)
        val precision = new ListBuffer[Double]
        val recall = new ListBuffer[Double]
        val f1 = new ListBuffer[Double]
        for (i <- 1 to 10) {
          val sampled_traces = log.traces.takeSample(false, 5000)
          val new_log = Structs.Log(spark.sparkContext.parallelize(sampled_traces), log.activities)
          val outliers = results.filter(x => sampled_traces.map(_.id).contains(x._2))

          val foundOutliers = OurMethod.findOutliersStatistical(new_log, k, n, Utils.Utils.convert_to_vector_only_durationsMean, distance, distance_name)
          val found = foundOutliers.count(i => outliers.map(_._2).contains(i._1)).toDouble

          val p = found / foundOutliers.size.toDouble
          val r = found / outliers.size.toDouble
          precision += p
          recall += r
          f1 += 2 * (p * r) / (p + r)
        }
        println(distance_name + ": Precision: ", meanList(precision.toList), "Recall: ", meanList(recall.toList), "F1: ", meanList(f1.toList))
      }
    }
  }

  test("run on sample traditional") {
    def meanList(l: List[Double]): Double = l.sum / l.length
    val k = 10
    val radius:Double = 0.05
    val filenames = List("input/outliers_30_activities_10k_0.005.xes","input/outliers_30_activities_10k_0.01.xes","input/outliers_30_activities_10k_0.05.xes","input/outliers_30_activities_10k_0.1.xes")
    val result_files = List("input/results_30_activities_10k_0.005_description","input/results_30_activities_10k_0.01_description","input/results_30_activities_10k_0.05_description","input/results_30_activities_10k_0.1_description")
    for ((fn,rn) <-filenames.zip(result_files)){
      println(fn)
      results = Results.read_with_description(rn)
      log = Utils.Utils.read_xes(fn)
      for ((distance, distance_name) <- distances.zip(distance_names)) {
        println(distance_name)
        val precision = new ListBuffer[Double]
        val recall = new ListBuffer[Double]
        val f1 = new ListBuffer[Double]
        for (i <- 1 to 10) {
          val sampled_traces = log.traces.takeSample(false, 5000)
          val new_log = Structs.Log(spark.sparkContext.parallelize(sampled_traces), log.activities)
          val outliers = results.filter(x => sampled_traces.map(_.id).contains(x._2))

          val foundOutliers = OurMethod.findOutliers(new_log, k, radius, Utils.Utils.convert_to_vector_only_durationsMean, distance, distance_name)
          val found = foundOutliers.count(i => outliers.map(_._2).contains(i)).toDouble

          val p = found / foundOutliers.length.toDouble
          val r = found / outliers.size.toDouble
          precision += p
          recall += r
          f1 += 2 * (p * r) / (p + r)
        }
        println(distance_name + ": Precision: ", meanList(precision.toList), "Recall: ", meanList(recall.toList), "F1: ", meanList(f1.toList))
      }
    }
  }





  test("run on sample odal") {
    val ns = List(0.3)
    val ks = List(1)
    //    val k=10
    for (k <- ks) {
      for (n <- ns) {
        for (i <- 1 to 10) {
          println(i, n, k)
          val sampled_traces = log.traces.takeSample(false, 5000)
          val new_log = Structs.Log(spark.sparkContext.parallelize(sampled_traces), log.activities)
          val outliers = results.filter(x => sampled_traces.map(_.id).contains(x._2))
          val outliers2 = ODAL.find_outliers_admition(new_log, k, n.toFloat)
          val found = outliers2.count(i => results.map(_._2).contains(i)).toDouble
          val prec = found / outliers2.length.toDouble
          val rec = found / outliers.size.toDouble
          val f1 = 2 * ((prec * rec) / (prec + rec))
          println("Precision: ", prec, "Recall: ", rec, "F1:", f1)
        }
      }
    }

  }


}
