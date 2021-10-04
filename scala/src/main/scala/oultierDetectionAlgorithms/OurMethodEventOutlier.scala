package oultierDetectionAlgorithms

import org.apache.spark.rdd.RDD

object OurMethodEventOutlier {

  case class Event(trace_index: Long, task: String, duration: Double, event_pos: Long)

  def findOutlierEventsZeta(log:Structs.Log,k:Int,zeta:Int):Array[(Event,Double)]={
    //split per activity
    perActivity(log)
    //assign score per activity and normalize to min max
      .flatMap(x=>{
        val outlierScore = this.assignOutlierScore(x._2,k)
        //normalization
        val maxScore=outlierScore.map(_._2).max
        val minScore=outlierScore.map(_._2).min
        outlierScore.map(x=>(x._1,Utils.Utils.min_max_normalization(x._2,minScore,maxScore)))
      })
    //keep the top zeta events
      .sortBy(_._2, ascending = false)
      .take(zeta)
  }

  def findOutlierEvents(log:Structs.Log,k:Int,n:Double):Array[(Event,Double)]={
    perActivity(log)
      .flatMap(x=>{
        val outlierScore = assignOutlierScore(x._2,k)
//        keep those that deviates more than n*stdev from mean value
        val meanScore =Utils.Utils.mean(outlierScore.map(_._2))
        val stdev = Utils.Utils.stdDev(outlierScore.map(_._2))
        outlierScore.filter(x=>x._2>meanScore+n*stdev)
      }).collect()
  }

  def findOutlierEventsTraditional(log:Structs.Log,k:Int,r:Double):Array[(Event,Double)]={
    perActivity(log)
      .map(x=>{
        (x._1,this.normalizeDurationInActivity(x._2))
      })
      .flatMap(x=>{ //for every activity
        x._2.map(e1=>{
          val distance_from_k = x._2.map(e2=>{
            Math.abs(e1.duration-e2.duration)
          }).toList.sortWith((x,y)=>x<y).take(k+1).last
          (e1,distance_from_k)
        })
          .filter(_._2>r)
      })
      .collect()
  }

  private def normalizeDurationInActivity(iterable: Iterable[Event]):Iterable[Event]={
    val maxDuration = iterable.map(_.duration).max
    val minDuration = iterable.map(_.duration).min
    iterable.map(x=>Event(x.trace_index,x.task,Utils.Utils.min_max_normalization(x.duration,minDuration,maxDuration),x.event_pos))
  }

  private def assignOutlierScore(events: Iterable[Event], k: Int):List[(Event,Double)]={
    //score = sum of k closest neighbors
    events.map(e1=>{
      val sumOfClosestNeighbors=events.map(e2=>{
        Math.abs(e1.duration-e2.duration)
      }).toList.sortWith((x,y)=>x<y).take(k+1).sum
      (e1,sumOfClosestNeighbors)
    }).toList
  }

  private def perActivity(log:Structs.Log): RDD[(String, Iterable[Event])] ={
    log.traces.zipWithIndex.flatMap(trace => {
      val trace_index = trace._2
      trace._1.events.zipWithIndex.map(event => {
        val event_pos = event._2
        Event(trace_index, event._1.task, event._1.duration, event_pos)
      })
    }).groupBy(_.task)
  }



}
