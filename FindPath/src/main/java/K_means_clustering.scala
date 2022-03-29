import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.annotation.tailrec

/** A raw posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, parentId: Option[Int], score: Int, tags: Option[String]) extends Serializable

/** The main class */
object Assignment2 extends Assignment2 {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("K_means_clustering")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  //sc.setLogLevel("WARN")

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines   = sc.textFile(args(0))
    val raw     = rawPostings(lines)

    val grouped = groupedPostings(raw)
    val scored  = scoredPostings(grouped)
    val vectors = vectorPostings(scored)
    counter = 0
    val means   = kmeans(sampleVectors(vectors), vectors, debug = true)
    val results = clusterResults(means, vectors)

    printResults(results)
  }
}


/** The parsing and kmeans methods */
class Assignment2 extends Serializable {

  /** Languages */
  val Domains =
    List(
      "Machine-Learning", "Compute-Science", "Algorithm", "Big-Data", "Data-Analysis", "Security", "Silicon Valley", "Computer-Systems",
      "Deep-learning", "Internet-Service-Providers", "Programming-Language", "Cloud-services", "Software-Engineering", "Embedded-System", "Architecture")


  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def DomainSpread = 50000
  assert(DomainSpread > 0)

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria, if changes of all centriods < kmeansEta, stop*/
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120


  //
  //
  // Parsing utilities:
  //
  //

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType =    arr(0).toInt,
        id =             arr(1).toInt,
        parentId =       if (arr(2) == "") None else Some(arr(2).toInt),
        score =          arr(3).toInt,
        tags =           if (arr.length >= 5) Some(arr(4).intern()) else None)
    })


  /** Group the questions and answers together */
  def groupedPostings(input: RDD[Posting]): RDD[(Int,Iterable[(Int, Option[String])])] = {
    // Filter the questions and answers separately
    // Prepare them for a join operation by extracting the QID value in the first element of a tuple.
    input.map(line=> if (line.postingType == 1) (line.id,(line.score,line.tags)) else (line.parentId.get,(line.score,line.tags))).groupByKey()
  }


  /** Compute the maximum score for each posting */
  def scoredPostings(input: RDD[(Int,Iterable[(Int, Option[String])])]): RDD[(Int,(Int,Option[String]))] = {
    input.map(line=> (line._1,(line._2.max)))
  }


  /** Compute the vectors for the kmeans */
  def vectorPostings(input: RDD[(Int,(Int,Option[String]))]): RDD[(Int,Int)] = {
    input.map(line=> (Domains.indexOf(line._2._2.getOrElse(-1))*DomainSpread,line._2._1)).filter(_._1!= -1*DomainSpread)
  }

  //  Kmeans method:

  var counter = 0

  /** Main kmeans computation */
  @tailrec final def kmeans(sampleArray: Array[(Int,Int)], vectors: RDD[(Int,Int)], debug:Boolean): Array[(Int,Int)] = {
    val cluster = vectors.map(p=> (sampleArray(findClosest(p,sampleArray)),p)).groupByKey()
    val oldCentersArray = cluster.map(line=> line._1).collect()
    val newCentersArray = cluster.map(line=> averageVectors(line._2)).collect()
    val distance = euclideanDistance(oldCentersArray, newCentersArray)
    if (converged(distance)){
      newCentersArray
    }
    else if (counter >= kmeansMaxIterations){
      newCentersArray
    }
    else{
      counter = counter + 1
      kmeans(newCentersArray,vectors,debug)
    }

  }

  /** consolidate results **/
  def clusterResults(mean: Array[(Int,Int)],vectors:RDD[(Int,Int)]): Array[((Int,Int),Int,Int,Int)] = {
    val scores = vectors.map(p=> (mean(findClosest(p,mean)),p)).groupByKey()
    val result = scores.map(s=> (s._1, s._2.size, computeMedian(s._2),s._1._2)).collect()
    result
  }

  //  Kmeans utilities (Just some cases, you can implement your own utilities.)


  /** Create sample vectors for use as initial centroids*/
  def sampleVectors(input: RDD[(Int,Int)]): Array[(Int,Int)] ={
    input.takeSample(withReplacement = true, kmeansKernels)
  }

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double) = distance < kmeansEta


  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  /** Return the euclidean distance between two points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while(idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Return the closest point */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }


  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }


  def computeMedian(a: Iterable[(Int, Int)]) = {
    val s = a.map(x => x._2).toArray
    val length = s.length
    val (lower, upper) = s.sortWith(_<_).splitAt(length / 2)
    if (length % 2 == 0) (lower.last + upper.head) / 2 else upper.head
  }

  //  Displaying results:

  def printResults(result: Array[((Int,Int),Int,Int,Int)]) = {
    for ( n <- 0 to result.length-1){
      println(result(n).toString())
    }
  }
}
