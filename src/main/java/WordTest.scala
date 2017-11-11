import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.Word2Vec


object WordTest {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Word2vec Application")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    val input = sc.textFile("/home/hc/tmp/words.txt").map(line => line.split(" ").toSeq)

    input.foreach(s=>{
      println("string="+s);
    })
    val word2vec = new Word2Vec()
    val model = word2vec.fit(input)


    val vector_map=model.getVectors;
    println("mapSize="+vector_map.size);
    val it=vector_map.iterator;
    while(it.hasNext){
      val vector_item=it.next();
      val v1=vector_item._1;
      val v2=vector_item._2;
      println(v1+"|"+v2);
    }
    val synonyms = model.findSynonyms("赞", 5)
    for((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }
  }



}
