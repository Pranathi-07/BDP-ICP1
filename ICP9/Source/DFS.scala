import org.apache.spark.{SparkConf, SparkContext}

object DFS {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\Spark\\spark-3.0.0-preview2-bin-hadoop2.7")

    //Spark Context
    val conf = new SparkConf().setAppName("merge_sort").setMaster("local");
    val sc = new SparkContext(conf);
    type Vertex = Int
//    initializing the map
    type Graph = Map[Vertex, List[Vertex]]
    val g: Graph = Map(1 -> List(2, 3, 4), 2 -> List(1, 3, 5), 3 -> List(1, 2), 4 -> List(2, 6), 5 -> List(1, 6, 7), 6 -> List(1, 2, 5, 7), 7 -> List(1, 2, 4, 5, 6))
//defining the DFS function with start vertex and graph as parameters
    def DFS(start: Vertex, g: Graph): List[Vertex] = {

      def DFS0(v: Vertex, visited: List[Vertex]): List[Vertex] = {
//        checking if the vertex from the list is visited
        if (visited.contains(v))
          visited
        else {
//          filtering the neighbouring nodes which are not visited
          val neighbours: List[Vertex] = g(v) filterNot visited.contains
//          adding node to visited and applying dfs for its neighbouring nodes
          neighbours.foldLeft(v :: visited)((b, a) => DFS0(a, b))
        }
      }
//      backtracks to least depth possible
      DFS0(start, List()).reverse
    }

    val dfsresult6 = DFS(6, g)

    println("Dfs start node: 6:", dfsresult6.mkString(","))

  }
}
