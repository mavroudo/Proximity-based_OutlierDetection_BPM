import Utils.Preprocess
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import oultierDetectionAlgorithms.{Distances, InitializeNeighbors, LOF_Spark}

object TemporalTraceAnomalies {

  def main(args: Array[String]): Unit = {
    val filename = "input/financial_log.xes"
    val k = 50 //number of nearest neighbors
    val dims = 10
    val zeta = 10
    val n =4
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("Temporal trace anomaly detection")
      .master("local[*]")
      .getOrCreate()
    println(s"Starting Spark version ${spark.version}")
    val log = Utils.Utils.read_xes(filename)

    val rddTransformed_whole=Preprocess.preprocess(log,dims,Utils.Utils.convert_to_vector_both_duration_repetitions)
    val rddTransformed=rddTransformed_whole.sample(withReplacement = false,0.01)
    val lof=new LOF_Spark(rddTransformed_whole,k,Distances.distanceRMSE)

    spark.time{
      lof.initialize(InitializeNeighbors.init_naive)
    }
    spark.time{
      val n = lof.assignOutlierFactor()
      n.foreach(x=>println(x._1,x._2))
    }


//    spark.time {
//      val distances = NaiveMethod.initializeDistances(rddTransformed,k)
//      println("size: ",distances.first().distances.size)
//      val sortedByOutlyingFactor=OutlierDetection.assignOutlyingFactor(distances, k)
//      sortedByOutlyingFactor.persist(StorageLevel.MEMORY_AND_DISK)
//      val outliers:Array[(Long,Double)]=sortedByOutlyingFactor.collect().sortBy(_._2).slice(0,zeta+1) //top zeta elements
//      println("Outliers based on top zeta reported")
//      outliers.foreach(println)
//      println("Outliers based on deviating more than n times stdev from mean")
//      val outliers_stats:RDD[(Long,Double)]=OutlierDetection.outlierBasedOnDeviation(sortedByOutlyingFactor,n)
//      outliers_stats.foreach(println)
//
//    }
    spark.stop()
  }
}
