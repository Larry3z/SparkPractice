import org.apache.log4j.PropertyConfigurator
import org.apache.spark.{SparkConf, SparkContext}


object RDDActionOperations {
  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure("file/log4j.properties")
    val conf = new SparkConf().setAppName("RDDActionOperations").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.parallelize(List("Hello world", "Hello scala"))
    // map reduce
    lines.flatMap(x => x.split(" ")).map(x => (x, 1))
      .reduceByKey((x, y) => x + y).foreach(println)
    //countByValue word count
    lines.flatMap(x => x.split(" ")).countByValue().foreach(println)
    // collect
    val lines2 = sc.parallelize(List(1, 2, 3, 4, 5))
    lines2.collect().foreach(println)
    // take
    lines2.take(2).foreach(println)
    // top
    lines2.top(2).foreach(println)
  }
}
