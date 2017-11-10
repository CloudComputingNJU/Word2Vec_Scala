import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.{SparkConf, SparkContext}

object WordTest2 {
  def main(args: Array[String]): Unit = {
    /*var sparkConf=new SparkConf().setAppName("WordTest2")
      .setMaster("local[2]")
      .set("spark.driver.host", "localhost")
      .set("spark.mongodb.input.uri", "mongodb://114.212.243.98:27017/all_words")

    val sc = new SparkContext(sparkConf)
    val readConfig = ReadConfig(
      Map(
        "database" -> "jd",
        "collection" -> "all_words",
        "spark.mongodb.input.uri"->"mongodb://114.212.243.98:27017/all_words"), Some(ReadConfig(sc)))
    val wordsMongoRDD = MongoSpark.load(sc, readConfig)
    wordsMongoRDD.foreach(s=>{
      println(s);
    })*/

    val sparkConf = new SparkConf()
      .setAppName("WordTest2")
      .setMaster("local[2]")
      .set("spark.mongodb.input.uri", "mongodb://114.212.243.98:27017/jd.all_words")
    val sc = new SparkContext(sparkConf)
    val readConfig = ReadConfig(
      Map(
        "database" -> "jd",
        "collection" -> "all_words"), Some(ReadConfig(sc)))
    var count=0;
    val wordsMongoRDD = MongoSpark.load(sc, readConfig)
    /*wordsMongoRDD.take(100).foreach(s=>{
      println(s.get("words"));

    })*/
    //val wordsLineRDD=wordsMongoRDD.map(s=>s.get("words"));
    /*val wordsLineRDD=wordsMongoRDD.take(10).map(s=>{
      val arr=s.get("words");
      println("arr="+arr.toString);

    });*/
    val wordsLineRDD=wordsMongoRDD.take(10).map(s=>s.get("words").toString());
    wordsLineRDD.foreach(s=>{
      println(s);
    })

    val word2vec = new Word2Vec();
    //val model = word2vec.fit(wordsMongoRDD);


  }


}
