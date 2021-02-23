package Utils

import breeze.linalg.{DenseMatrix, DenseVector}
import oultierDetectionAlgorithms.Structs

class Mahalanobis(val invCovariance:DenseMatrix[Double]) extends Serializable {

  def distance(v1:Structs.Trace_Vector,v2:Structs.Trace_Vector):Double={
    val d=DenseVector(v1.elements.zip(v2.elements).map(x=>x._1-x._2))
    math.sqrt(d.t * invCovariance * d)
  }

}
