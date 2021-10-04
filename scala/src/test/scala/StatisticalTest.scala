import Utils.Results
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import oultierDetectionAlgorithms.{ODAL, OurMethod, Structs}

class StatisticalTest extends FunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var results: List[(String, Int, Int)] = _
  private var log: Structs.Log = _

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

    val k = 100
    for (i <- 1 to 10) {
      println(i)
      val sampled_traces = log.traces.takeSample(false, 5000)
      val new_log = Structs.Log(spark.sparkContext.parallelize(sampled_traces), log.activities)
      val outliers = results.filter(x => sampled_traces.map(_.id).contains(x._2))
      val scores = OurMethod.assignOutlyingFactor(new_log, k, Utils.Utils.convert_to_vector_only_durationsMean, Utils.Utils.distance, "mahalanobis")
      val found = scores.slice(0, outliers.size + 1).count(i => outliers.map(_._2).contains(i._1.toInt))
      println("Precision: ", found.toDouble / outliers.size.toDouble, "Recall: ", found.toDouble / outliers.size.toDouble)
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
