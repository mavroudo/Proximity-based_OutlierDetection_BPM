package oultierDetectionAlgorithms

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

case class Event(trace_id: Long, event_name: String, event_pos: Int, duration: Double)

object EventPairPerActivity {

  //An event pair will be consider outlier, if one or both of the events have less than k neighbors in a radius = r
  def getOutlierPairs(log: Structs.Log, k: Int, r: Double): Array[(Event,Event)] = {
    val spark = SparkSession.builder().getOrCreate()
    val outlier_events=this.getOutlierEvents(log,k,r)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val outlier_events_collected = outlier_events.collect()
    val broadcastOutlierTraceIds=spark.sparkContext.broadcast(outlier_events_collected.map(_.trace_id))
    val outlying_traces=this.preprocess(log)
      .groupBy(_.event_name)
      .map(x => {
        (x._1, this.min_max_normalization(x._2))
      })
      .flatMap(_._2)
      .filter(event=>broadcastOutlierTraceIds.value.contains(event.trace_id))
      .groupBy(_.trace_id)
      .map(events=>{
        val listEvents=events._2.toList
          .sortWith((ea,eb)=>ea.event_pos<eb.event_pos)
        (events._1,listEvents)
      }).collect()

    val outlierPairs=outlier_events_collected.flatMap(out_event=>{
      val trace=outlying_traces.filter(_._1==out_event.trace_id).head._2
      val pairs=ListBuffer[(Event,Event)]()
      if (out_event.event_pos<=trace.length-2){
        val nextEvent=trace(out_event.event_pos+1)
        pairs+=((out_event,Event(out_event.trace_id,nextEvent.event_name,out_event.event_pos+1,nextEvent.duration)))
      }
      if (out_event.event_pos>=1){
        val nextEvent=trace(out_event.event_pos-1)
        pairs+=((Event(out_event.trace_id,nextEvent.event_name,out_event.event_pos-1,nextEvent.duration),out_event))
      }
      pairs.toList
    })
    outlier_events.unpersist()
    outlierPairs
  }

  def getOutlierEvents(log: Structs.Log, k: Int, r: Double):RDD[Event]={
    val preprocessedLog=this.preprocess(log)
    val outlier_events = preprocessedLog
      .groupBy(_.event_name)
      .map(x => {
        (x._1, this.min_max_normalization(x._2))
      })
      .map(activity => { //short events by their duration
        (activity._1, activity._2.toList.sortWith((x, y) => x.duration < y.duration))
      })
      .flatMap(activity => {
        activity._2.zipWithIndex.flatMap(event => {
          val fromIndex = Math.max(0, event._2 - k)
          val toIndex = Math.min(activity._2.length - 1, event._2 + k)
          var neighbors = ListBuffer[(Event, Int)]()
          for (i <- fromIndex to toIndex) {
            if (Math.abs(activity._2(i).duration - event._1.duration) < r) {
              neighbors += ((event._1, 1))
            }
          }
          neighbors.toList
        })
      })
      .groupBy(_._1)
      .map(event => {
        (event._1, event._2.map(_._2).sum)
      })
      .filter(_._2 < k)
      .map(_._1)
    outlier_events
  }


  def getMeasurementError(log:Structs.Log,pairs:Array[(Event,Event)],t:Double):Array[(Event,Event,Boolean)]={
    val means=this.preprocess(log)
      .groupBy(_.event_name)
      .map(x => {
        (x._1, this.min_max_normalization(x._2))
      })
      .map(x=>{
        val mean_value=x._2.map(_.duration).sum/x._2.size
        (x._1,mean_value)
      }).collect()
    pairs.map(pair=>{
      val meanA=means.filter(_._1==pair._1.event_name).head._2
      val meanB=means.filter(_._1==pair._2.event_name).head._2
      val isMeasurement= Math.abs((pair._1.duration-meanA)-(pair._2.duration-meanB))<=t
      (pair._1,pair._2,isMeasurement)
    })

  }


  def preprocess(log: Structs.Log): RDD[Event] = {
    log.traces.zipWithIndex.flatMap(trace => {
      val trace_index = trace._2
      trace._1.events.zipWithIndex.map(event => {
        val event_pos = event._2
        Event(trace_index, event._1.task, event_pos, event._1.duration)
      })
    })
  }

  def min_max_normalization(events: Iterable[Event]): Iterable[Event] = {
    val max = events.map(_.duration).max
    val min = events.map(_.duration).min
    events.map(event => {
      val new_duration = (event.duration - min) / (max - min)
      Event(event.trace_id, event.event_name, event.event_pos, new_duration)

    })
  }


}
