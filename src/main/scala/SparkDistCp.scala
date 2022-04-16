import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.net.URI

object SparkDistCp {

  val conf = new SparkConf()
    .setAppName("SparkDistCp")
//    .setMaster("local[*]")
  val sparkSession: SparkSession = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    // todo 使用new scopt.OptionParser解析参数
    val src = args(0)
    val dst = args(1)

    copyOnHdfs(src, dst)

    stopJob()
  }

  def copyOnHdfs(src: String, dst: String): Unit = {
    val configuration = new Configuration()

    val fsSrc = FileSystem.get(new URI(src), configuration)
    val fsDst = FileSystem.get(new URI(dst), configuration)
    FileUtil.copy(fsSrc, new Path(src), fsDst, new Path(dst), false, configuration)
  }

  def stopJob(): Unit = {
    sparkSession.stop()
  }

}