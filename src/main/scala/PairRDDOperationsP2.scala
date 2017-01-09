import org.apache.log4j.PropertyConfigurator
import org.apache.spark.{SparkConf, SparkContext}


object PairRDDOperationsP2 {
  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure("file/log4j.properties")
    val conf = new SparkConf().setAppName("PairRDDOperationsP2").setMaster("local")
    val sc = new SparkContext(conf)

    val set1 = sc.parallelize(List(("Guang", 95),("Xin",90),("Long",99)))
    val set2 = sc.parallelize(List(("Huang",98),("Guang",100)))

    //删掉set1中与set2中相同的元素
    set1.subtractByKey(set2).foreach(println)

    //交集
    set1.join(set2).foreach(println)

    //rightouterjoin 保证括号中的RDD的所有键
    set1.rightOuterJoin(set2).foreach(println)

    //leftouterjoin 保证括号外的RDD的所有键
    set1.leftOuterJoin(set2).foreach(println)

    //两个RDD中具有相同键的分组合并
    set1.cogroup(set2).foreach(println)

  }
}
