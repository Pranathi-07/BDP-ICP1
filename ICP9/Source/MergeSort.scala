import org.apache.spark._

object MergeSort {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\Spark\\spark-3.0.0-preview2-bin-hadoop2.7")

    //Spark Context
    val conf = new SparkConf().setAppName("merge_sort").setMaster("local");
    val sc = new SparkContext(conf);
// array to sort
    val a = Array(38, 27, 43, 3, 9, 82, 10);
//    val b = sc.parallelize(a);

//    val maparray = b.map(x => (x, 1))
//length of the array,initial and last index of the array
    val n = a.length;
    val l = 0;
    val r = n-1;
//displaying array before sorting
    print("\nBefore sorting\n")
    for ( x <- a)
    {
      print(x+"\t");
    }
//applying sort on the array
    sort(a,l,r);
//printing the result array
    print("\nAfter sorting\n")
    for ( y <- a)
    {
      print(y+"\t")
    }

    // Merge sort
    def sort(arr: Array[Int], l: Int, r: Int): Unit = {
      if (l < r) {
        // Finding middle
        val m = (l + r) / 2
        // Sorting first half
        sort(arr, l, m)
        // Sorting second half
        sort(arr, m + 1, r)
        // Merging the both sorted halves
        merge(arr, l, m, r)
      }
    }

    // Merge for Array
    def merge(arr: Array[Int], l: Int, m: Int, r: Int): Unit = {

      // Finding sizes of two subarrays to be merged
      val n1 = m - l + 1
      val n2 = r - m

      // Creating temp arrays
      val L = new Array[Int](n1)
      val R = new Array[Int](n2)

      //Copying data to temp arrays
      var a = 0
      while (a < n1){
        L(a) = arr(l + a);
        a += 1;
      }

      var b = 0
      while (b < n2){
        R(b) = arr(m + 1 + b);
        b += 1;
      }

      // Merge the temp arrays
      // Initial indexes of first and second subarrays
      var i = 0
      var j = 0
      // Initial index of merged subarry array
      var k = l
      while (i < n1 && j < n2) {
        if (L(i) <= R(j)) {
          arr(k) = L(i)
          i += 1
        }
        else {
          arr(k) = R(j)
          j += 1
        }
        k += 1
      }

      // Copy remaining elements of L[] if any
      while (i < n1)
      {
        arr(k) = L(i)
        i += 1
        k += 1
      }

      // Copy remaining elements of R[] if any
      while (j < n2)
      {
        arr(k) = R(j)
        j += 1
        k += 1
      }

    }

  }

}
