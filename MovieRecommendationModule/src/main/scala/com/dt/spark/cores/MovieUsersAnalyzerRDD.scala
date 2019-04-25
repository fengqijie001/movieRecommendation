package com.dt.spark.cores

import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashSet

/**
  * Spark商业案例书稿第一章：1.12.3.1 RDD方式案例代码
  *
  * 电影点评系统用户行为分析：用户观看电影和点评电影的所有行为数据的采集、过滤、处理和展示：
  *   数据采集：企业中一般越来越多的喜欢直接把Server中的数据发送给Kafka，因为更加具备实时性；
  *   数据过滤：趋势是直接在Server端进行数据过滤和格式化，当然采用Spark SQL进行数据的过滤也是一种主要形式；
  *   数据处理：
  *     1，一个基本的技巧是，先使用传统的SQL去实现一个下数据处理的业务逻辑（自己可以手动模拟一些数据）；
  *     2，再一次推荐使用DataSet去实现业务功能尤其是统计分析功能；
  *     3，如果你想成为专家级别的顶级Spark人才，请使用RDD实现业务功能，为什么？运行的时候是基于RDD的！
  *
  *  数据：强烈建议大家使用Parquet
  *  1，"ratings.dat"：UserID::MovieID::Rating::Timestamp
  *  2，"users.dat"：UserID::Gender::Age::OccupationID::Zip-code
  *  3，"movies.dat"：MovieID::Title::Genres
  *  4, "occupations.dat"：OccupationID::OccupationName   一般情况下都会以程序中数据结构Hashset的方式存在，是为了做mapjoin
  */
object MovieUsersAnalyzerRDDCase {

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MovieUsersAnalyzerRDDCase")
    val sc: SparkContext = new SparkContext(conf)

    val dataPath: String = "hdfs://hadoop3:8020/input/movieRecom/moviedata/medium/"
    val outputDir: String = "hdfs://hadoop3:8020/out/movieRecom_out"

    val startTime: Long = System.currentTimeMillis();
    /**
      * 读取数据，用什么方式读取数据呢？在这里是使用RDD!
      */
    val usersRDD: RDD[String] = sc.textFile(dataPath + "users.dat")
    val moviesRDD: RDD[String] = sc.textFile(dataPath + "movies.dat")
    val occupationsRDD: RDD[String] = sc.textFile(dataPath + "occupations.dat")
    val ratingsRDD: RDD[String] = sc.textFile(dataPath + "ratings.dat")

    /**
      * 电影点评系统用户行为分析之一：分析具体某部电影观看的用户信息，例如电影ID为1193的
      *   用户信息（用户的ID、Age、Gender、Occupation）
      *   1, "ratings.dat"：UserID::MovieID::Rating::Timestamp
      *   2, "users.dat"：UserID::Gender::Age::OccupationID::Zip-code
      *   3, "movies.dat"：MovieID::Title::Genres
      *   4, "occupations.dat"：OccupationID::OccupationName
      */
    // (OccupationID, (UserID, Gender, Age))
//    val usersBasic: RDD[(String, (String, String, String))] = usersRDD.map(_.split("::"))
//      .map { user => (
//        user(3), (user(0), user(1), user(2))
//      )
//    }

    // (OccupationID, OccupationName)
//    val occupations: RDD[(String, String)] = occupationsRDD.map(_.split("::"))
//      .map(job => (job(0), job(1)))

    // (OccupationID, ((UserID, Gender, Age), OccupationName))
//    val userInformation: RDD[(String, ((String, String, String), String))] = usersBasic.join(occupations)

//    userInformation.cache()

//    for (item <- userInformation.collect()) {
//      // item: (4,((25,M,18),college/grad student))
//      println("item: " + item)
//    }

    // (UserID, MovieID)
//    val targetMovie: RDD[(String, String)] = ratingsRDD.map(_.split("::"))
//      .map(x => (x(0), x(1)))
//      .filter(_._2.equals("1193"))

//    for (movie <- targetMovie.collect()) {
//      // movie: (2,1193)
//      println(s"movie: ${movie}")
//    }

    // (UserID, ((UserID, Gender, Age), OccupationName))
//    val targetUsers: RDD[(String, ((String, String, String), String))] = userInformation
//      .map(x => (x._2._1._1, x._2))

//    for (targetUser <- targetUsers.collect()) {
//      // targetUser: (25,((25,M,18),college/grad student))
//      println("targetUser: " + targetUser)
//    }

    // (UserID, (MovieID, ((UserID, Gender, Age), OccupationName)) )
//    val userInformationForSpecificMovie:
//      RDD[(String, (String, ((String, String, String), String)))] = targetMovie.join(targetUsers)

//    for (item <- userInformationForSpecificMovie.collect().take(10)) {
//      // item: (3492,(1193,((3492,M,35),executive/managerial)))
//      // item: (4286,(1193,((4286,M,35),technician/engineer)))
//      println(s"item: ${item}")
//    }


    /**
      * 电影流行度分析：所有电影中平均得分最高（口碑最好）的电影及观看人数最高的电影（流行度最高）
      * "ratings.dat"：UserID::MovieID::Rating::Timestamp
      * 得分最高的Top10电影实现思路：如果想算总的评分的话一般肯定需要reduceByKey操作或者aggregateByKey操作
      *   第一步：把数据变成Key-Value，大家想一下在这里什么是Key，什么是Value。把MovieID设置成为Key，把Rating设置为Value；
      *   第二步：通过reduceByKey操作或者aggregateByKey实现聚合，然后呢？
      *   第三步：排序，如何做？进行Key和Value的交换
      *
      *  注意：
      *   1，转换数据格式的时候一般都会使用map操作，有时候转换可能特别复杂，需要在map方法中调用第三方jar或者so库；
      *   2，RDD从文件中提取的数据成员默认都是String方式，需要根据实际需要进行转换格式；
      *   3，RDD如果要重复使用，一般都会进行Cache
      *   4，重磅注意事项，RDD的cache操作之后不能直接在跟其他的算子操作，否则在一些版本中cache不生效
      */
    println("所有电影中平均得分最高（口碑最好）的电影:")
    val ratings: RDD[(String, String, String)] = ratingsRDD.map(_.split("::"))
      .map(x => (x(0), x(1), x(2))).cache()
    // (MovieID, 平均评分)
    ratings.map(x => (x._2, (x._3.toDouble, 1)))  // (MovieID, (Rating, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)) // (MovieID, (总评分, 总评分次数))
      .map(x => (x._1, x._2._1.toDouble / x._2._2)) // (MovieID, 平均评分)
      .sortBy(_._2, false) // 对value降序排列
      .take(10)
      .foreach(println)

    val ratingss: RDD[(String, (Double, Int))] = ratingsRDD.map(_.split("::")).map(x => (x(1), (x(2).toDouble, 1)))
    for (elem <- ratingss.collect().take(10)) {
      println(s"elem: ${elem}")
    }
    ratings.map(x => (x._2, (x._3.toDouble, 1)))

    /**
      * 上面的功能计算的是口碑最好的电影，接下来我们分析粉丝或者观看人数最多的电影
      */
    println("所有电影中粉丝或者观看人数最多的电影:")
    ratings.map(x => (x._2, 1)).reduceByKey(_+_).map(x => (x._2, x._1)).sortByKey(false)
      .map(x => (x._2, x._1)).take(10).foreach(println)
//     (MovieID, 总次数)
    ratings.map(x => (x._2, 1)).reduceByKey(_+_).sortBy(_._2, false).take(10).foreach(println)


    /**
      * 今日作业：分析最受男性喜爱的电影Top10和最受女性喜爱的电影Top10
      * 1，"users.dat"：UserID::Gender::Age::OccupationID::Zip-code
      * 2，"ratings.dat"：UserID::MovieID::Rating::Timestamp
      *   分析：单单从ratings中无法计算出最受男性或者女性喜爱的电影Top10,因为该RDD中没有Gender信息，如果我们需要使用
      *     Gender信息来进行Gender的分类，此时一定需要聚合，当然我们力求聚合的使用是mapjoin（分布式计算的Killer
      *     是数据倾斜，map端的join是一定不会数据倾斜），在这里可否使用mapjoin呢？不可以，因为用户的数据非常多！
      *     所以在这里要使用正常的Join，此处的场景不会数据倾斜，因为用户一般都很均匀的分布（但是系统信息搜集端要注意黑客攻击）
      *
      * Tips：
      *   1，因为要再次使用电影数据的RDD，所以复用了前面Cache的ratings数据
      *   2, 在根据性别过滤出数据后关于TopN部分的代码直接复用前面的代码就行了。
      *   3, 要进行join的话需要key-value；
      *   4, 在进行join的时候时刻通过take等方法注意join后的数据格式  (3319,((3319,50,4.5),F))
      *   5, 使用数据冗余来实现代码复用或者更高效的运行，这是企业级项目的一个非常重要的技巧！
      */
    val male = "M"
    val female = "F"
    val ratings2: RDD[(String, (String, String, String))] = ratings.map(x => (x._1, (x._1, x._2, x._3)))
    val usersRDD2: RDD[(String, String)] = usersRDD.map(_.split("::")).map(x => (x(0), x(1)))
    val genderRatings: RDD[(String, ((String, String, String), String))] = ratings2.join(usersRDD2).cache()
//    genderRatings.take(500).foreach(println)

    val maleRatings: RDD[(String, String, String)] = genderRatings.filter(x => x._2._2.equals("M"))
      .map(x => x._2._1)
    val femaleRatings: RDD[(String, String, String)] = genderRatings.filter(x => x._2._2.endsWith("F"))
      .map(x => x._2._1)

    println("所有电影中最受男性喜爱的电影Top10: ")
    maleRatings.map(x => (x._2, (x._3.toDouble, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._1, x._2._1.toDouble / x._2._2))
      .sortBy(_._2, false)
      .take(10)
      .foreach(println)


    println("所有电影中最受女性喜爱的电影Top10: ")
    femaleRatings.map(x => (x._2, (x._3.toDouble, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._1, x._2._1.toDouble / x._2._2))
      .sortBy(_._2, false)
      .take(10)
      .foreach(println)


    /**
      * 最受不同年龄段人员欢迎的电影TopN
      * "users.dat"：UserID::Gender::Age::OccupationID::Zip-code
      * 思路：首先还是计算TopN，但是这里的关注点有两个：
      *   1，不同年龄阶段如何界定，关于这个问题其实是业务的问题，当然，你实际在实现的时候可以使用RDD的filter中的例如
      *     13 < age <18,这样做会导致运行时候大量的计算，因为要进行扫描，所以会非常耗性能。所以，一般情况下，我们都是
      *     在原始数据中直接对要进行分组的年龄段提前进行好ETL, 例如进行ETL后产生以下的数据：
      *     - Gender is denoted by a "M" for male and "F" for female
      *     - Age is chosen from the following ranges:
      *  1:  "Under 18"
      * 18:  "18-24"
      * 25:  "25-34"
      * 35:  "35-44"
      * 45:  "45-49"
      * 50:  "50-55"
      * 56:  "56+"
      *   2，性能问题：
      *     第一点：你实际在实现的时候可以使用RDD的filter中的例如13 < age <18,这样做会导致运行时候大量的计算，因为要进行
      *     扫描，所以会非常耗性能，我们通过提前的ETL把计算发生在Spark业务逻辑运行以前，用空间换时间，当然这些实现也可以
      *     使用Hive，因为Hive语法支持非常强悍且内置了最多的函数；
      *     第二点：在这里要使用mapjoin，原因是targetUsers数据只有UserID，数据量一般不会太多
      */

    val targetQQUsers: RDD[(String, String)] = usersRDD.map(_.split("::")).map(x => (x(0), x(2)))
      .filter(_._2.equals("18"))
    val targetTaobaoUsers: RDD[(String, String)] = usersRDD.map(_.split("::")).map(x => (x(0), x(2)))
      .filter(_._2.equals("25"))

    /**
      * 在Spark中如何实现mapjoin呢，显然是要借助于Broadcast，会把数据广播到Executor级别让该Executor上的所有任务共享
      *  该唯一的数据，而不是每次运行Task的时候都要发送一份数据的拷贝，这显著的降低了网络数据的传输和JVM内存的消耗
      */
    val targetQQUsersSet: HashSet[String] = HashSet() ++ targetQQUsers.map(_._1).collect()
    val targetTaobaoUsersSet: HashSet[String] = HashSet() ++ targetTaobaoUsers.map(_._1).collect()

    val qqUsersBroadcast: Broadcast[HashSet[String]] = sc.broadcast(targetQQUsersSet)
    val taobaoUsersBroadcast: Broadcast[HashSet[String]] = sc.broadcast(targetTaobaoUsersSet)

    /**
      * QQ或者微信核心目标用户最喜爱电影TopN分析
      * (Silence of the Lambs, The (1991),524)
        (Pulp Fiction (1994),513)
        (Forrest Gump (1994),508)
        (Jurassic Park (1993),465)
        (Shawshank Redemption, The (1994),437)
      */
    val tuples: Array[(String, String)] = moviesRDD.map(_.split("::")).map(x => (x(0), x(1))).collect()
    val movieID2Name: Map[String, String] = tuples.toMap
    println("所以电影中QQ或者微信核心目标用户最喜爱电影TopN分析：")
    ratingsRDD.map(_.split("::")).map(x => (x(0), x(1))).filter(x => qqUsersBroadcast.value.contains(x._1))
      .map(x => (x._2, 1)).reduceByKey(_+_).sortBy(_._2, false)
      .take(10)
      .map(x => (movieID2Name.getOrElse(x._1, null), x._2))
      .foreach(println)

    /**
      * taobao核心目标用户最喜爱电影TopN分析
      * (Pulp Fiction (1994),959)
        (Silence of the Lambs, The (1991),949)
        (Forrest Gump (1994),935)
        (Jurassic Park (1993),894)
        (Shawshank Redemption, The (1994),859)
      */
//    println("所有电影中淘宝核心目标用户最喜爱电影TopN分析:")
//    ratingsRDD.map(_.split("::")).map(x => (x(0), x(1))).filter(x => taobaoUsersBroadcast.value.contains(x._1))
//      .map(x => (x._2, 1)).reduceByKey(_ + _).sortBy(_._2, false).take(10)
//      .map(x => (movieID2Name.getOrElse(x._1, null), x._2))
//      .foreach(println)


    /**
      * 对电影评分数据进行二次排序，以Timestamp和Rating两个维度降序排列
      * "ratings.dat"：UserID::MovieID::Rating::Timestamp
      *
      * 完成的功能是最近时间中最新发生的点评信息
      */
//    println("对电影评分数据以Timestamp和Rating两个维度进行二次降序排序:")
//    val pairWithSortkey: RDD[(SecondarySortKey, String)] = ratingsRDD.map(line => {
//      val splited: Array[String] = line.split("::")
//      (new SecondarySortKey(splited(3).toDouble, splited(2).toDouble), line)
//    })
//    val sorted: RDD[(SecondarySortKey, String)] = pairWithSortkey.sortByKey(false)
//    val sortedResult: RDD[String] = sorted.map(sortedline => sortedline._2)
//    sortedResult.take(10).foreach(println)


    val elapsedTime: Long = System.currentTimeMillis() - startTime
    println("Finished in milliseconds: " + elapsedTime / 1000)

  }

}

class SecondarySortKey(val first: Double, val second: Double)
              extends Ordered[SecondarySortKey] with Serializable {
  override def compare(that: SecondarySortKey): Int = {
    if (this.first - that.first != 0) {
      (this.first - that.first).toInt
    } else {
      if (this.second - that.second > 0) {
        // 向上取整，比如：0.5 取 1
        Math.ceil(this.second - that.second).toInt
      } else if (this.second - that.second < 0) {
        // 向下取整，比如：-0.5 取 -1
        Math.floor(this.second - that.second).toInt
      } else {
        (this.second - that.second).toInt
      }
    }
  }
}






























