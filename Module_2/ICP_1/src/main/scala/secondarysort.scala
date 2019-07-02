import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext._

object secondarysort {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","D:\\winutils" )
    val conf = new SparkConf().setAppName("wordcount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val personRDD = sc.textFile("D:\\Source code\\Source code\\wordcount\\input\\secsort.txt")
    val pairsRDD = personRDD.map(_.split(",")).map { k => ((k(0), k(1)), k(2)) }
    println("pairsRDD")
    pairsRDD.foreach { println }
    val numReducers = 2;

    val listRDD = pairsRDD.groupByKey(numReducers).flatMapValues(_.toList.combinations(2))
    listRDD.foreach { println }
    val rdd4 = listRDD.mapValues { case (elems: List[(String, Integer)]) => ((elems(0), elems(1))) }

    println("listRDD")
    rdd4.foreach { println }

    rdd4.saveAsTextFile("D:\\Source code\\Source code\\wordcount\\output_sec2")

  }
}
