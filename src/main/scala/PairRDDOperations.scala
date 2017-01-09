import org.apache.log4j.PropertyConfigurator
import org.apache.spark.{SparkConf, SparkContext}


object PairRDDOperations {
  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure("file/log4j.properties")
    val conf = new SparkConf().setAppName("PairRDDOperations").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("file/test")
    // top N
    lines.flatMap(x => x.split(" "))
      .map(x => (x, 1)).reduceByKey((x, y) => x + y)
      .sortBy(x => x._2, false).take(5).foreach(println)

    val lines2 = sc.textFile("file/README.md")
    // top N
    lines2.filter(x => x != "").flatMap(x => x.split(" "))
      .map(x => (x, 1)).reduceByKey((x, y) => x + y).sortBy(x => x._2, false).take(5).foreach(println)

    //groupByKey
    lines.flatMap(x => x.split(" ")).map(x => (x,1)).groupByKey().foreach(println)


  }
}
