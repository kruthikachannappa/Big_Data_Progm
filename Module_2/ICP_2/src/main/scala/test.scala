import org.apache.spark.{SparkConf, SparkContext}

object test {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\winutils")
    val conf = new SparkConf().setAppName("Breadthfirst").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val list = List(5,8,6,3,1,2,4,7)


  }
}
