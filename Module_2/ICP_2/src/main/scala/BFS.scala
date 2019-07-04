import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.immutable.Queue
object BFS {
  def breadth_first_traverse[Node](node: Node, f: Node => Queue[Node]): Stream[Node] = {
    def recurse(q: Queue[Node]): Stream[Node] = {
      if (q.isEmpty) {
        Stream.Empty
      } else {
        val (node, tail) = q.dequeue
        node #:: recurse(tail ++ f(node))
      }
    }

    node #:: recurse(Queue.empty ++ f(node))
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Winutils");

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    //breadth_first_traverse()

  }

}
