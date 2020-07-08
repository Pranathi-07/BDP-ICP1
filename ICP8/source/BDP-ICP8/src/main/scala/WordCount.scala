import org.apache.spark._
object WordCount {
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","D:\\Spark\\spark-3.0.0-preview2-bin-hadoop2.7" )
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    // Load our input data.
    val input = sc.textFile("input\\input")
    // Split up into words.
    val words = input.flatMap(line => line.split(" "))
    // Transform into word and count.
    val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
    // Save the word count back out to a text file, causing evaluation.
    counts.saveAsTextFile("output")
//    splitting the file and getting the count for word 'are'
    val file = input.flatMap(w => w.split(" ")).filter(value=>value=="are")
println(file.count())
    file.top(2).foreach(println)

  }
}
