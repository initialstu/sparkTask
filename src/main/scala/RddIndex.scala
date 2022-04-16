import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object RddIndex {

  val conf = new SparkConf()
    .setAppName("RddIndex")
//    .setMaster("local[*]")
  val sparkSession: SparkSession = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    val rdd = sparkSession.sparkContext.parallelize(Seq(
      (0, "it is what it is"),
      (1, "what is it"),
      (2, "it is a banana"),
    ))

    rdd.flatMap(row => {
      row._2.split(" ").map(r => ((row._1, r), 1))
    }).reduceByKey(_+_).map(r => {
      (r._1._2, (r._1._1, r._2))
    }).aggregateByKey(collection.mutable.HashSet[(Int, Int)]())((S, e) => {
      S += e
    }, (S1, S2) => {
      S1 ++= S2
    }).foreach(println(_))

    stopJob()
  }

  def stopJob(): Unit = {
    sparkSession.stop()
  }

}