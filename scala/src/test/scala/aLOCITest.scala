import Utils.Results
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import oultierDetectionAlgorithms.ALOCIElki

class aLOCITest extends FunSuite{


  test("aLoci test"){
    val filename = "input/outliers_delay_7_activities.xes"
    val results_file="input/results_7_activities"

    val results=Results.read(results_file)
    val spark = SparkSession.builder()
      .appName("Temporal trace anomaly detection")
      .master("local[*]")
      .getOrCreate()
    println(s"Starting Spark version ${spark.version}")
    val log = Utils.Utils.read_xes(filename)
    val x=ALOCIElki.assignScore(log).take(results.size)
    val found=x.count(i => results.map(_._1).contains(i._1))
    println((found.toDouble/results.size))
  }

}
