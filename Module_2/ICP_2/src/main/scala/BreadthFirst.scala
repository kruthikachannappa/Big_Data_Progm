import org.apache.spark.{SparkConf, SparkContext}

object BreadthFirst {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\winutils")
    val conf = new SparkConf().setAppName("Breadthfirst").setMaster("local[*]")
    val sc = new SparkContext(conf)
    type Vertex = Int
    type Graph = Map[Vertex, List[Vertex]]
    val g: Graph = Map(1 -> List(2,3,5,6,7), 2 -> List(1,3,4,6,7), 3 -> List(1,2), 4 -> List(2,5,7),5 -> List(1,6,7),6 -> List(1,2,5,7),7 -> List(1,2,4,5,6))
    //example graph meant to represent
    //  1---2
    //  |   |
    //  4---3

    //I want this to return results in the different layers that it finds them (hence the list of list of vertex)
    def BFS(start: Vertex, g: Graph): List[List[Vertex]] = {

      def BFS0(elems: List[Vertex],visited: List[List[Vertex]]): List[List[Vertex]] = {
        val newNeighbors = elems.flatMap(g(_)).filterNot(visited.flatten.contains).distinct
        if (newNeighbors.isEmpty)
          visited
        else
          BFS0(newNeighbors, newNeighbors :: visited)
      }
      BFS0(List(start),List(List(start))).reverse
    }



    def DFS(start: Vertex, g: Graph): List[Vertex] = {

      def DFS0(v: Vertex, visited: List[Vertex]): List[Vertex] = {
        if (visited.contains(v))
          visited
        else {
          val neighbours:List[Vertex] = g(v) filterNot visited.contains
          neighbours.foldLeft(v :: visited)((b,a) => DFS0(a,b))
        }
      }
      DFS0(start,List()).reverse
    }

    val bfsresult=BFS(1,g)
    val dfsresult=DFS(1,g )
   // val result = sc.parallelize(1,Seq(Map(1 -> List(2,3,5,6,7), 2 -> List(1,3,4,6,7), 3 -> List(1,2), 4 -> List(2,5,7),5 -> List(1,6,7),6 -> List(1,2,5,7),7 -> List(1,2,4,5,6)))).map(BFS)
   println(bfsresult.mkString(","))
   println(dfsresult.mkString(","))

  }


}