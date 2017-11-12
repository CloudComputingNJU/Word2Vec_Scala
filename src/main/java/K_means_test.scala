import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

object K_means_test {
  Logger.getLogger("org").setLevel(Level.ERROR)
  System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin-master");
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("K_means_test").setMaster("local[2]");
    val sc = new SparkContext(sparkConf)

    val rawTrainingData=sc.textFile("kms_train.txt");
    val parsedTrainingData =
      rawTrainingData.filter(!isColumnNameLine(_)).map(line => {
        val cc=line.split(",").map(_.trim).filter(!"".equals(_)).map(_.toDouble);
        //println("length="+cc.length)
        Vectors.dense(line.split(",").map(_.trim).filter(!"".equals(_)).map(_.toDouble))
      }).cache()



    val numClusters = 200
    val numIterations = 30
    val runTimes = 3
    var clusterIndex: Int = 0

    /*val clusters: KMeansModel =
      KMeans.train(parsedTrainingData, numIterations, runTimes)*/
    val clusters: KMeansModel =
      KMeans.train(parsedTrainingData, numClusters, numIterations, runTimes)
    val cost=clusters.computeCost(parsedTrainingData);
    println("k="+numClusters+"cost="+cost);
    /*val kmeans=new KMeans();
    val clusters:KMeansModel=kmeans.run(parsedTrainingData);*/

    println("Cluster Number:" + clusters.clusterCenters.length)

    println("Cluster Centers Information Overview:")
    clusters.clusterCenters.foreach(
      x => {
        println("Center Point of Cluster " + clusterIndex + ":")
        println(x)
        clusterIndex += 1
      })

    val rawTestData = sc.textFile("kms_test.txt")
    val parsedTestData = rawTestData.map(line => {
      Vectors.dense(line.split(",").map(_.trim).filter(!"".equals(_)).map(_.toDouble))
    })
    parsedTestData.collect().foreach(testDataLine => {
      val predictedClusterIndex:
        Int = clusters.predict(testDataLine)
      println("The data " + testDataLine.toString + " belongs to cluster " +
        predictedClusterIndex)
    })

    println("Spark MLlib K-means clustering test finished.")
    //begin to check which cluster each test data belongs to based on the clustering result
  }
  private def isColumnNameLine(line: String): Boolean = {
    if (line != null && line.contains("Channel")) true
    else false
  }
}
