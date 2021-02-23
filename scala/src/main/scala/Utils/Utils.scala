package Utils

import java.io.{File, FileInputStream}
import java.text.SimpleDateFormat
import java.time.ZonedDateTime
import java.util.Date
import org.apache.spark.mllib.linalg.Vectors.zeros
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.deckfour.xes.in.XParserRegistry
import org.deckfour.xes.model.XLog
import oultierDetectionAlgorithms.Structs
import oultierDetectionAlgorithms.Structs.{Event, Sequence}

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable.ListBuffer
import scala.io.Source

object Utils {


  def read_synthetic(filename:String):Structs.Log={
    val file=Source.fromFile(filename)
    val lines = file.getLines().toList

    val seq=lines.zipWithIndex.map(line=>{
      val events=line._1.split(",").map(ev=>{
        val e = ev.split("_")
        (e(0),e(1).toDouble)
      })
      (line._2,events)
    })
    val uniqueActivities=seq.flatMap(_._2).map(_._1).distinct

    val tracesList:List[Sequence]=seq.map(x=>{
      val events:List[Event]=x._2.map(e=>{
        Structs.Event(e._1,"",e._2.toLong)
      }).toList
      Structs.Sequence(x._1,events)
    })
    val spark = SparkSession.builder().getOrCreate()
    val traces = spark.sparkContext.parallelize(tracesList)
    val log= Structs.Log(traces,uniqueActivities)
    file.close()
    log
  }

  def read_xes(filename: String): Structs.Log = {
    val spark = SparkSession.builder().getOrCreate()
    val fileObject = new File(filename)
    var parsed_logs: List[XLog] = null
    val parsers_iterator = XParserRegistry.instance().getAvailable.iterator()
    while (parsers_iterator.hasNext) { // Finds available parser for this file
      val p = parsers_iterator.next
      if (p.canParse(fileObject)) {
        parsed_logs = p.parse(new FileInputStream(fileObject)).toList
      }
    }

    //    val df = new SimpleDateFormat("MMM d, yyyy HH:mm:ss a") //read this pattern from xes
    //    Event(name, df2.format(df.parse(timestamp))

    val df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") // transform it to this patter
    var activities: Set[String] = Set()
    val data = parsed_logs.head.zipWithIndex.map(x => { // sequence
      val alist: ListBuffer[Event] = new ListBuffer[Event]()
      var previous_time: Date = new Date()
      for (i <- 0 until x._1.size()) {
        if (i == 0) {
//          previous_time = Date.from(ZonedDateTime.parse(x._1.getAttributes.get("REG_DATE").toString).toInstant)
          val event = x._1(i)
          val timestamp = event.getAttributes.get("time:timestamp").toString
          val date: Date = Date.from(ZonedDateTime.parse(timestamp).toInstant)
        }
        val event = x._1(i)
        val name = event.getAttributes.get("concept:name").toString
        activities += name
        val timestamp = event.getAttributes.get("time:timestamp").toString
        val date: Date = Date.from(ZonedDateTime.parse(timestamp).toInstant)
        val duration = this.duration(previous_time, date)
        previous_time = date
        alist += Event(name, df2.format(date), duration)
      }
      Sequence(x._2.toLong, alist.toList)
    }).toList
    val par: RDD[Sequence] = spark.sparkContext.parallelize(data)
    Structs.Log(par, activities.toList)
  }

  def convert_to_vector_only_repetitions(log: Structs.Log): RDD[Structs.Trace_Vector] = {
    log.traces.map(trace => {
      val v: Array[Double] = zeros(log.activities.size).toArray
      trace.events.foreach(event => {
        v(log.activities.indexOf(event.task)) += 1
      })

      Structs.Trace_Vector(trace.id, v)
    })
  }

  def convert_to_vector_only_durations(log: Structs.Log): RDD[Structs.Trace_Vector] = {
    log.traces.map(trace => {
      val v: Array[Double] = zeros(log.activities.size).toArray
      trace.events.foreach(event => {
        v(log.activities.indexOf(event.task)) += event.duration
      })
      Structs.Trace_Vector(trace.id, v)
    })
  }

  def convert_to_vector_only_durationsMean(log: Structs.Log): RDD[Structs.Trace_Vector] = {
    log.traces.map(trace => {
      val v: Array[Double] = zeros(log.activities.size).toArray
      val t:Array[Double] = zeros(log.activities.size).toArray
      trace.events.foreach(event => {
        v(log.activities.indexOf(event.task)) += event.duration
        t(log.activities.indexOf(event.task)) += 1
      })
      val l = v.zip(t).map(x=>{
        if(x._2==0){
          0
        }else{
          x._1/x._2
        }
      })
      Structs.Trace_Vector(trace.id, l)
    })
  }

  def convert_to_vector_both_duration_repetitions(log: Structs.Log): RDD[Structs.Trace_Vector] = {
    log.traces.map(trace => {
      val v: Array[Double] = zeros(2 * log.activities.size).toArray
      trace.events.foreach(event => {
        val index = log.activities.indexOf(event.task)
        v(index) += 1
        v(index + log.activities.size) += event.duration
      })
      Structs.Trace_Vector(trace.id, v)
    })
  }

  def duration(previous: Date, now: Date): Long = {
    now.getTime - previous.getTime
  }

  def distance(v1: Structs.Trace_Vector, v2: Structs.Trace_Vector): Double = {
    var d: Double = 0
    v1.elements.zip(v2.elements).foreach(x => {
      d += math.pow(x._1 - x._2, 2)
    })
    math.sqrt(d / v1.elements.length)
  }

  def distanceEuclidean(v1: Structs.Trace_Vector, v2: Structs.Trace_Vector): Double={
    var d: Double = 0
    v1.elements.zip(v2.elements).foreach(x => {
      d += math.pow(x._1 - x._2, 2)
    })
    math.sqrt(d)
  }

  def distanceMinkowski(v1: Structs.Trace_Vector, v2: Structs.Trace_Vector): Double={
    v1.elements.zip(v2.elements)
      .map(x=>Math.abs(x._1-x._2))
      .max
  }

  import Numeric.Implicits._

  def mean[T: Numeric](xs: Iterable[T]): Double = xs.sum.toDouble / xs.size

  def variance[T: Numeric](xs: Iterable[T]): Double = {
    val avg = mean(xs)

    xs.map(_.toDouble).map(a => math.pow(a - avg, 2)).sum / xs.size
  }

  def stdDev[T: Numeric](xs: Iterable[T]): Double = math.sqrt(variance(xs))

  def min_max_normalization(value:Double,min:Double,max:Double):Double={
    (value-min)/(max-min)
  }

}
