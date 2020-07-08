import org.apache.spark._

object Secondarysort {

    def main(args: Array[String]) {

      System.setProperty("hadoop.home.dir","D:\\Spark\\spark-3.0.0-preview2-bin-hadoop2.7")
      val conf = new SparkConf().setAppName("Spark - Secondary Sort").setMaster("local[*]")
      val sc = new SparkContext(conf)

      val input_data = sc.textFile("input\\secondary_input")
      val input_pairs = input_data.map(_.split(",")).map { k => ((k(0) + "-" + k(1)),k(3)) }

      val numReducers = 2;

      // val listRDD = pairsRDD.groupByKey(1).mapValues(iter => iter.toList.sortBy(r => r))
      val input_lists = input_pairs.groupByKey(1)
        .mapValues(iter => "[" + iter.toArray.sortBy(r => r).reverse.mkString(",") + "]")
      input_lists.foreach {
        println
      }

      val x = input_lists.partitionBy(new HashPartitioner(2))

      x.saveAsTextFile("secondary_output")
      //x.foreach(println)
    }
  }

