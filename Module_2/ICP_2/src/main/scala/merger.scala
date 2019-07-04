import org.apache.spark._
import org.apache.spark.SparkContext._

object merger {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\winutils")
    val conf = new SparkConf().setAppName("Breadthfirst").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val lists=Array(5,8,3,6,2,1,4,7)
    val data=sc.parallelize(lists).sortBy(x=>x)
    data.foreach(println)
  }
}
