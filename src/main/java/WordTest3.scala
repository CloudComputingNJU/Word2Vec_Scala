import java.io.{File, PrintWriter}

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.log4j.{Level, Logger}
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
object WordTest3 {
  Logger.getLogger("org").setLevel(Level.ERROR)
  System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin-master");

  def main(args: Array[String]): Unit = {
    makeVectors("all_words","words.txt","kms_train.txt","kms_test.txt")
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


  /*
  vectorsWrite:
  将word vector写入kms的训练文件和测试文件
  map:word和array[double]的映射
  trainPath:训练文件路径
  testPath:测试文件路径
   */
  def vectorsWrite(map: Map[String,Array[Float]],trainPath:String,testPath:String): Unit ={
    val train_writer=new PrintWriter(trainPath);
    val test_writer=new PrintWriter(testPath);

    var it=map.iterator;

    var count = 0;
    var writer=test_writer;
    while(it.hasNext) {
      count+=1
      if(count>100){
        writer=train_writer
      }
      var arr=it.next()._2;
      for (f <- arr) {
        writer.print(f + ",")
      }
      writer.print("\n")
    }

    train_writer.close();
    test_writer.close();
  }
  /*
  makeVectors（）
 读取MongoDB，Collection中分词数据
 生成words.txt,词文件
 kms_*_path:存储词向量的文件
   */
  def makeVectors(cPath:String,wordPath:String,kms_train_path:String,kms_test_path:String): Unit ={
    val sparkConf = new SparkConf()
      .setAppName("WordTest2")
      .setMaster("local[2]")
      .set("spark.mongodb.input.uri", "mongodb://localhost:27017/jd."+cPath)
    val sc = new SparkContext(sparkConf)
    val readConfig = ReadConfig(
      Map(
        "database" -> "jd",
        "collection" -> "all_words"), Some(ReadConfig(sc)))
    var count=0;
    val wordsMongoRDD = MongoSpark.load(sc, readConfig)

    val wordsLineRDD=wordsMongoRDD.take(200000).map(s=>s.get("words").toString()).map(s=>s.substring(1,s.length()-1).replace(","," "))
    val wordsRDD=wordsLineRDD.flatMap(s=>s.split(" ").seq);

    val len=wordsRDD.length;
    println("共有"+len+"个词")

    writeWordToTxt(wordsRDD,wordPath);
    //词语写入txt

    val input = sc.textFile(wordPath).map(line => line.split(" ").toSeq)
    val word2vec = new Word2Vec();
    val model=word2vec.fit(input)
    val vector_map=model.getVectors;
    //将word转为vector

    vectorsWrite(vector_map,kms_train_path,kms_test_path);
    println("vectors write finished");
    //写入vectors到train_txt
  }
}
