import java.io.{File, PrintWriter}

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/*
author:hc
100000 input 7944 output vector
the number of i&o dont match
mongodb->rdd->txt->rdd->model
havent find a way to fit rdd directly

 */
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
      .set("spark.mongodb.input.uri", "mongodb://localhost:27017/jd.all_words")
    val sc = new SparkContext(sparkConf)
    val readConfig = ReadConfig(
      Map(
        "database" -> "jd",
        "collection" -> "all_words"), Some(ReadConfig(sc)))
    var count=0;
    val wordsMongoRDD = MongoSpark.load(sc, readConfig)

    val filePath="words.txt";
    /*wordsMongoRDD.take(100).foreach(s=>{
      println(s.get("words"));

    })*/
    //val wordsLineRDD=wordsMongoRDD.map(s=>s.get("words"));
    /*val wordsLineRDD=wordsMongoRDD.take(10).map(s=>{
      val arr=s.get("words");
      println("arr="+arr.toString);

    });*/
    /*val wordsLineRDD=wordsMongoRDD.take(10).map(s=>s.get("words").toString()).map(s=>{
      val s2=s.substring(1,s.length()-1).replace(","," ")
      println(s2);
      println("***************************");
      s2;
    });*/

    /*wordsLineRDD.foreach(s=>{
      val s2=s.substring(1,s.length()-1).replace(","," ")
      println(s2);
      println("***************************");
    })*/

    val wordsLineRDD=wordsMongoRDD.take(100000).map(s=>s.get("words").toString()).map(s=>s.substring(1,s.length()-1).replace(","," "))
    val wordsRDD=wordsLineRDD.flatMap(s=>s.split(" ").seq);
    writeWordToTxt(wordsRDD,filePath);


    val input = sc.textFile(filePath).map(line => line.split(" ").toSeq)

    val word2vec = new Word2Vec();

    val model=word2vec.fit(input)
    val vector_map=model.getVectors;

    listWrite(vector_map,"kms_test.txt");


    //println("mapSize="+vector_map.size);
    /*val it=vector_map.iterator;
    while(it.hasNext){
      val vector_item=it.next();
      val v1=vector_item._1;
      val v2=vector_item._2;
      print(v1+"|");
      listPrint(v2);
      listWrite(v2,"kms_train.txt");
      println()
    }*/
    /*val synonyms = model.findSynonyms("赞", 5)
    for((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }*/
    //val model = word2vec.fit(wordsMongoRDD);


  }
  def writeWordToTxt(strs:Array[String],fPath:String): Unit ={
    val writer=new PrintWriter(new File(fPath))
    for(str <- strs)
      writer.print(str+" ")
    writer.close();
  }
  def listPrint(arr:Array[Float]): Unit ={
    for(f<-arr) print(f);
  }

  def listWrite(map: Map[String,Array[Float]],filePath:String): Unit ={
    val writer=new PrintWriter(filePath);
    var it=map.iterator;
    while(it.hasNext) {
      var arr=it.next()._2;
      println("arr的长度是"+arr.length);
      for (f <- arr) {
        writer.print(f + ",")
      }
      writer.print("\n")
    }

    writer.close();
  }
}
