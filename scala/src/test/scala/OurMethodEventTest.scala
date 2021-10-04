import Utils.Results
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import oultierDetectionAlgorithms.{LOF_core, OurMethodEventOutlier, Structs}

class OurMethodEventTest extends FunSuite with BeforeAndAfterAll{
  private var spark: SparkSession = _
  private var results:List[(String,Int,Int)] =_
  private var log:Structs.Log=_

  override def beforeAll(): Unit = {
    val filename = "input/outliers_30_activities_10k_0.1.xes"
    val results_file = "input/results_30_activities_10k_0.1_description"
    results=Results.read_with_description(results_file)
    Logger.getLogger("org").setLevel(Level.ERROR)
    spark = SparkSession.builder()
      .appName("Temporal trace anomaly detection")
      .master("local[*]")
      .getOrCreate()
    println(s"Starting Spark version ${spark.version}")
    log = Utils.Utils.read_xes(filename)
  }

  test("Distance based on Event level, keeping top zeta "){
    val k=50
    val zeta =1000
    val outliers= OurMethodEventOutlier.findOutlierEventsZeta(log,k,zeta)
    val groundTruth= results.map(x=>(x._2,x._3))
    val found = outliers.count(x=>groundTruth.contains((x._1.trace_index.toInt,x._1.event_pos.toInt)))
    println(found.toDouble/results.size)
  }

  test("Distance based on Event level, keeping those that deviates more than n times the stdev from mean value"){
    val k=50
    val n =2.5
    val outliers= OurMethodEventOutlier.findOutlierEvents(log,k,n)
    val groundTruth= results.map(x=>(x._2,x._3))
    val found = outliers.count(x=>groundTruth.contains((x._1.trace_index.toInt,x._1.event_pos.toInt)))
    println(found.toDouble/results.size)
  }

  test("Distance based on Event level, keeping those that have less than k neighbors in radius smaller than r"){
    // the durations are normalized in distance 0-1 in order to have effective r in every activity
    val k=50
    val r =0.02
    val outliers= OurMethodEventOutlier.findOutlierEventsTraditional(log,k,r)
    val groundTruth= results.map(x=>(x._2,x._3))
    val found = outliers.count(x=>groundTruth.contains((x._1.trace_index.toInt,x._1.event_pos.toInt)))
    println(found.toDouble/results.size)

  }
}
