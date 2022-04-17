package org.bigdata.sparkjob

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.net.URI
import scala.collection.mutable.ArrayBuffer

object SparkDistCp {

  val conf = new SparkConf()
    .setAppName("SparkDistCp")
  //    .setMaster("local[*]")
  val sparkSession: SparkSession = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    val parser = new scopt.OptionParser[SparkDistCpOption]("SparkDistCp") {

      opt[String]("src") action { (x, c) => c.copy(src = x) } text ("source path")
      opt[String]("dst") action { (x, c) => c.copy(dst = x) } text ("destination path")
      opt[Int]("m") action { (x, c) => c.copy(partitionNum = x) } text ("max concurrence")
      opt[Boolean]("i") action { (x, c) => c.copy(ignoreFailure = x) } text ("ignore failures")
    }

    parser.parse(args, SparkDistCpOption()) match {
      case Some(option) =>
        val src = new Path(option.src)
        val dst = new Path(option.dst)

        val fileList = mkDir(sparkSession, src, dst, option)

        fileList.foreach(tuple => println(tuple))

        copy(sparkSession, fileList, option)
      case _ =>
        throw new RuntimeException("Failed to parse arguments")
    }

    stopJob()
  }

  def mkDir(sparkSession: SparkSession, src: Path, dst: Path, option: SparkDistCpOption): ArrayBuffer[(Path, Path)] = {
    val fileList: ArrayBuffer[(Path, Path)] = new ArrayBuffer[(Path, Path)]()

    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    fs.listStatus(src).foreach(curPath => {
      if (curPath.isDirectory) {
        val subPath = curPath.getPath.toString.split(src.toString)(1)
        val nextDstPath = new Path(dst + subPath)
        try {
          fs.mkdirs(nextDstPath)
        } catch {
          case e: Exception =>
            if (option.ignoreFailure) println(e.getMessage) else throw e
        }
        fileList.appendAll(mkDir(sparkSession, curPath.getPath, nextDstPath, option))
      } else {
        fileList.append((curPath.getPath, dst))
      }
    })
    fileList
  }

  def copy(sparkSession: SparkSession, fileList: ArrayBuffer[(Path, Path)], option: SparkDistCpOption): Unit = {
    val sc = sparkSession.sparkContext
    val partitionNum = Some(option.partitionNum).getOrElse(1)
    val pathRdd = sc.makeRDD(fileList, partitionNum)

    pathRdd.foreachPartition(part => {
      val hadoopConf = new Configuration()
      part.foreach(tuple => {
        try {
          FileUtil.copy(tuple._1.getFileSystem(hadoopConf), tuple._1,
            tuple._2.getFileSystem(hadoopConf), tuple._2,
            false, hadoopConf)
        } catch {
          case e: Exception =>
            if (option.ignoreFailure) println(e.getMessage) else throw e
        }
      })
    })
  }

  def stopJob(): Unit = {
    sparkSession.stop()
  }

}
