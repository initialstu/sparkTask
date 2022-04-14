# sparkTask

* 题目一：倒排索引RDD

RddIndex.scala核心代码

```scala
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
```
本地执行结果
![a](a.png)
