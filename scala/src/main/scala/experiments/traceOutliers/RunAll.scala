package experiments.traceOutliers

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import oultierDetectionAlgorithms.{LOF_core, ODAL, OurMethod}

object RunAll {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("Temporal trace anomaly detection")
      .master("local[*]")
      .getOrCreate()
    println(s"Starting Spark version ${spark.version}")

    //    println("LOF")
    //    LOFonTrace.main(args)
    //    println("sum of k neighbors factor")
    //    SumOfKFactorOnTrace.main(args)
    //    println("sum of k neighbors statistic")
    //    SumOfKStatisticOnTrace.main(args)
    //    println("traditional distances")
//        TraditionalDistanceOnTrace.main(args)
        println("ODAL")
        ODALonTrace.main(args)


//    val log = Utils.Utils.read_xes("input/financial_log.xes")
//    val log =Utils.Utils.read_xes("input/bpi_2017.xes")
//    val log =Utils.Utils.read_xes("input/outliers_30_activities_10k_0.1.xes")
//    val outliers= ODAL.find_outliers_admition(log,10,4.0.toFloat)

    spark.time({
//      val scores = OurMethod.assignOutlyingFactor(log, 10, Utils.Utils.convert_to_vector_only_durationsMean, Utils.Utils.distance, "rmse")
//            val scores = LOF_core.assignOutlierFactor(log, 10, Utils.Utils.convert_to_vector_only_durationsMean, Utils.Utils.distance, "rmse")
//          val outliers = OurMethod.findOutliers(log,10,1,Utils.Utils.convert_to_vector_only_durationsMean, Utils.Utils.distance, "rmse")

    })


  }

}
