import java.util.Random

import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

import breeze.linalg.{squaredDistance, DenseVector, Vector}
/**
 * K-means clustering.
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.ml.clustering.KMeans.
 */
object Kmeans {
  val N = 1000
  val R = 1000    // Scaling factor
  val D = 10
  val K = 10
  val convergeDist = 0.001
  val rand = new Random(42)

  def generateData: Array[DenseVector[Double]] = {
    def generatePoint(i: Int): DenseVector[Double] = {
      DenseVector.fill(D) {rand.nextDouble * R}
    }
    Array.tabulate(N)(generatePoint)
  }

  def closestPoint(p: Vector[Double], centers: HashMap[Int, Vector[Double]]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    for (i <- 1 to centers.size) {
      val vCurr = centers(i)
      val tempDist = squaredDistance(p, vCurr)
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }

    bestIndex
  }

  def showWarning(): Unit = {
    System.err.println(
      """WARN: This is a naive implementation of KMeans Clustering and is given as an example!
        |Please use org.apache.spark.ml.clustering.KMeans
        |for more conventional use.
      """.stripMargin)
  }

  def main(args: Array[String]): Unit = {

    showWarning()

    val data = generateData
    val points = new HashSet[Vector[Double]]
    val kPoints = new HashMap[Int, Vector[Double]]
    var tempDist = 1.0

    while (points.size < K) {
      points.add(data(rand.nextInt(N)))
    }

    val iter = points.iterator
    for (i <- 1 to points.size) {
      kPoints.put(i, iter.next())
    }

    println(s"Initial centers: $kPoints")

    while(tempDist > convergeDist) {
      val closest = data.map (p => (closestPoint(p, kPoints), (p, 1)))

      val mappings = closest.groupBy[Int] (x => x._1)

      val pointStats = mappings.map { pair =>
        pair._2.reduceLeft [(Int, (Vector[Double], Int))] {
          case ((id1, (p1, c1)), (id2, (p2, c2))) => (id1, (p1 + p2, c1 + c2))
        }
      }

      var newPoints = pointStats.map {mapping =>
        (mapping._1, mapping._2._1 * (1.0 / mapping._2._2))}

      tempDist = 0.0
      for (mapping <- newPoints) {
        tempDist += squaredDistance(kPoints(mapping._1), mapping._2)
      }

      for (newP <- newPoints) {
        kPoints.put(newP._1, newP._2)
      }
    }

    println(s"Final centers: $kPoints")
  }
}
