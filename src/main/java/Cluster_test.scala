import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

object Cluster_test {
  Logger.getLogger("org").setLevel(Level.ERROR)
  System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin-master");

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("K_means_test").setMaster("local[2]");
    val sc = new SparkContext(sparkConf)

    val rawTrainingData = sc.textFile("kms_train.txt");
    val parsedTrainingData =
      rawTrainingData.filter(!isColumnNameLine(_)).map(line => {
        val cc = line.split(",").map(_.trim).filter(!"".equals(_)).map(_.toDouble);
        //println("length=" + cc.length)
        Vectors.dense(line.split(",").map(_.trim).filter(!"".equals(_)).map(_.toDouble))
      }).cache()

    val numClusters = 8
    val numIterations = 30
    val runTimes = 3
    var clusterIndex: Int = 0

    /*for (num <- 1 to 10) {
      val model: KMeansModel =
      KMeans.train(parsedTrainingData, num, numIterations, runTimes)
      val ssd=model.computeCost(parsedTrainingData);
      println("sum of squared distances of points to their nearest center when k=" + num + " -> "+ ssd)
    }*/
    val ks:Array[Int] = Array(3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)

    val num1=320;
    val model1:KMeansModel = KMeans.train(parsedTrainingData,num1,30,1)
    val ssd1= model1.computeCost(parsedTrainingData)
    println("sum of squared distances of points to their nearest center when k=" + num1 + " -> "+ ssd1)

    val num2=325;
    val model2:KMeansModel = KMeans.train(parsedTrainingData,num2,30,1)
    val ssd2 = model2.computeCost(parsedTrainingData)
    println("sum of squared distances of points to their nearest center when k=" + num2 + " -> "+ ssd2)


    println("Spark MLlib K-means clustering test finished.")
    //begin to check which cluster each test data belongs to based on the clustering result
  }
  private def isColumnNameLine(line: String): Boolean = {
    if (line != null && line.contains("Channel")) true
    else false
  }
}
