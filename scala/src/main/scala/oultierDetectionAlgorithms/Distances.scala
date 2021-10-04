package oultierDetectionAlgorithms

object Distances {

  def distanceRMSE(v1:Structs.Trace_Vector,v2:Structs.Trace_Vector):Double={
    var d: Double = 0
    v1.elements.zip(v2.elements).foreach(x => {
      d += math.pow(x._1 - x._2, 2)
    })
    math.sqrt(d / v1.elements.length)
  }
}
