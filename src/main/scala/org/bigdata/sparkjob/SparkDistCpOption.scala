package org.bigdata.sparkjob

case class SparkDistCpOption(src: String = "", dst: String = "", partitionNum: Int = 1, ignoreFailure: Boolean = true)
