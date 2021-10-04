import Utils.Results
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import oultierDetectionAlgorithms.ODAL

class ODALTest extends FunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private var results: List[(String,Int, Int)] = _
  val filename = "input/outliers_30_activities_3k_0.1.xes"
  val results_file = "input/results_30_activities_3k_0.1_description"

  override def beforeAll(): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    spark = SparkSession.builder()
      .appName("Temporal trace anomaly detection")
      .master("local[*]")
      .getOrCreate()
    println(s"Starting Spark version ${spark.version}")
    results = Results.read_with_description(results_file)
  }

  test("ODAL Algorithm with Signatures") {
    val k = 50
    val log = Utils.Utils.read_xes(filename)
    spark.time({
      val outliers = ODAL.find_outliers(log, k, 3)
      val found = outliers.count(i => results.map(_._2).contains(i))
      println(found.toDouble / results.size)
    })
  }

  test("ODAL Algorithm with Alignment") {
    val k = 50
    val log = Utils.Utils.read_xes(filename)
    spark.time({
      val outliers = ODAL.findOutliersAlignment(log, k, 2)
      val found = outliers.count(i => results.map(_._2).contains(i))
      println(found.toDouble / results.size)
    })
  }

}
