package experiments

import java.io.{BufferedWriter, File, FileWriter}

import Utils.Results
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import oultierDetectionAlgorithms.EventPairPerActivity

import scala.collection.mutable.ListBuffer

object Experiments_measurements {

//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)
//    val spark = SparkSession.builder()
//      .appName("Temporal trace anomaly detection")
//      .master("local[*]")
//      .getOrCreate()
//    println(s"Starting Spark version ${spark.version}")
//    val dataset = "30_activities_5k_0.1"
//
//    val filename = "input/outliers_" + dataset + ".xes"
//    val log = Utils.Utils.read_xes(filename)
//    val results_file = "input/results_" + dataset + "_description"
//    val results = Results.read_with_description(results_file)
//    var exp = new ListBuffer[String]()
//
//    val k=50
//    val r=0.01
//    val ts = List(0.01,0.1, 0.2,0.3,0.5,0.6,0.7,0.8,0.9,1.0,1.2,1.4,1.6,1.8,2,2.2,2.5)
//    val outlierPairs = EventPairPerActivity.getOutlierPairs(log, k, r)
//    val gtm = results.filter(_._1=="measurement").map(x => (x._2, x._3))
//    for(t<-ts) {
//      println(t)
//      val determineMeasurements = EventPairPerActivity.getMeasurementError(log, outlierPairs,t).filter(_._3)
//        .map(pair=>((pair._1.trace_id.toInt,pair._1.event_pos),(pair._2.trace_id.toInt,pair._2.event_pos)))
//      val found=determineMeasurements
//        .count(pair=> gtm.contains(pair._1) || gtm.contains(pair._2))
//        .toDouble
//      val precision = found.toDouble / determineMeasurements.length
//      val recall = found.toDouble / gtm.length
//      exp += t.toString + "," + precision.toString + "," + recall.toString + "\n"
//    }
//
//    val output = "output/" + dataset + "_measurements_"+k.toString+"_"+r.toString
//    val file = new File(output)
//    val bw = new BufferedWriter(new FileWriter(file))
//    exp.toList.foreach(line => {
//      bw.write(line)
//    })
//    bw.close()
//
//  }

}
