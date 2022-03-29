import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession

object Main {
  def splitString(input: String): Array[String] ={
    input.split(" ")
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    /** read into dataframe **/
    val df = spark.read.csv(args(0))
    val df2 = df.select("_c5")

    /** read stopwords file into array of strings **/
    val stopwords = spark.read.text(args(1)).collect().map(_.toSeq).flatten


    /** remove trailing and additional spaces and some punctuations including making every character lowercase */
    val trim = df2.map(row => row.mkString(" ").toLowerCase().replaceAll("([.]|[,]|[?]|[!])","").trim.replaceAll(" +"," ")).toDF()

    /** make array */
    val stringArray = trim.map(row => splitString(row.mkString(" ")))

    /** remove stop words */
    val removed = stringArray.map(arrayrow => arrayrow.filter(!stopwords.contains(_))).toDF()

    /** convert words to vectors **/
    val word2vec = new Word2Vec().setInputCol("value").setOutputCol("features").setMinCount(2)
    val model = word2vec.fit(removed)
    val vectors = model.transform(removed)

    /** Kmeans clustering **/
    val numClusters = 100
    val numIterations = 120
    val kmeans = new KMeans().setK(numClusters).setMaxIter(numIterations)
    val kmeansmodel = kmeans.fit(vectors)

    kmeansmodel.clusterCenters.foreach(println)

    /** silhouette score **/
    val predictions = kmeansmodel.transform(vectors)
    val evaluator = new ClusteringEvaluator()
    val silhouette = evaluator.evaluate(predictions)
    println(silhouette)

    /** to find most common words */
      println("start values")
    val values = predictions.select("prediction","value")
    //println("start newdf")
    values.show()
    val newdf = values.rdd.flatMap(f => f.getSeq[String](1).map((f.getInt(0),_))).toDF("prediction","value") //explode each word from tweet such that (cluster, word)
    //println("start groupByCount")
    newdf.show()
    val groupByCount = newdf.repartition(2).groupBy("prediction","value").count().cache() //count number of words in each cluster
    //println("start maxDF")
    //groupByCount.show()
    val maxDF = groupByCount.repartition(2).groupBy("prediction").max("count").cache() //select row with the highest count for each cluster
    //println("start newestDF")
    maxDF.show()
    val newestDF = maxDF.repartition(2).join(groupByCount,"prediction").where($"max(count)"===$"count").select("prediction","value") //join maxDF and groupByCount to find the most frequent word
    newestDF.show()
    //println("printingnewestDF")
    newestDF.collect().foreach(println) //print most frequent words




    spark.stop()
  }
}
