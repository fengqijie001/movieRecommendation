package com.dt.spark.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object MovieUsersAnalyzerDataSet {

  case class User(UserID: String, Gender: String, Age: String, OccupationID: String, Zip_Code: String)
  case class Rating(UserID: String, MovieID: String, Rating: Double, Timestamp: String)
  case class Movie(MovieID: String, Title: String, Genres: String)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    /**
      * 创建Spark会话上下文SparkSession和集群上下文SparkContext，在SparkConf中可以进行各种依赖和参数的设置等，
      * 大家可以通过SparkSubmit脚本的help去看设置信息，其中SparkSession统一了Spark SQL运行的不同环境。
      */
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MovieUsersAnalyzerDataSet")

    /**
      * SparkSession统一了Spark SQL执行时候的不同的上下文环境，也就是说Spark SQL无论运行在那种环境下我们都可以只使用
      * SparkSession这样一个统一的编程入口来处理DataFrame和DataSet编程，不需要关注底层是否有Hive等。
      */
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //从SparkSession获得的上下文，这是因为我们读原生文件的时候或者实现一些Spark SQL目前还不支持的功能的时候需要使用SparkContext
    val sc: SparkContext = spark.sparkContext

    val dataPath: String = "hdfs://hadoop3:8020/input/movieRecom/moviedata/medium/"
    val outputDir: String = "hdfs://hadoop3:8020/out/movieRecom_out3"
    //    val dataPath = "data/moviedata/medium/"    //数据存放的目录

    val startTime: Long = System.currentTimeMillis();

    import spark.implicits._
    /**
      * 读取数据，用什么方式读取数据呢？在这里是使用RDD!
      */
    val usersRDD: RDD[String] = sc.textFile(dataPath + "users.dat")
    val moviesRDD: RDD[String] = sc.textFile(dataPath + "movies.dat")
    val occupationsRDD: RDD[String] = sc.textFile(dataPath + "occupations.dat")
    //    val ratingsRDD = sc.textFile(dataPath + "ratings.dat")
    val ratingsRDD: RDD[String] = sc.textFile(dataPath + "ratings.dat")

    val ratings: RDD[(String, String, String)] = ratingsRDD.map(_.split("::"))
      .map(x => (x(0), x(1), x(2))).cache()

    /**
      * 功能一：通过DataFrame实现某特定电影观看者中男性和女性不同年龄分别有多少人？
      *   1，从点评数据中获得观看者的信息ID；
      *   2，把ratings和users表进行join操作获得用户的性别信息；
      *   3，使用内置函数（内部包含超过200个内置函数）进行信息统计和分析
      *  在这里我们通过DataFrame来实现：首先通过DataFrame的方式来表现ratings和users的数据，然后进行join和统计操作
      */
    // 使用Struct方式把Users的数据格式化,即在RDD的基础上增加数据的元数据信息
    val schemaforusers: StructType = StructType("UserID::Gender::Age::OccupationID::Zip_Code".split("::")
      .map(column => StructField(column, StringType, true)))
    // 把我们的每一条数据变成以Row为单位的数据
    val usersRDDRows: RDD[Row] = usersRDD.map(_.split("::")).map(line => Row(line(0).trim, line(1).trim,
      line(2).trim, line(3).trim, line(4).trim))
    // 结合Row和StructType的元数据信息基于RDD创建DataFrame，这个时候RDD就有了元数据信息的描述
    val usersDataFrame: DataFrame = spark.createDataFrame(usersRDDRows, schemaforusers)
    val usersDataSet: Dataset[User] = usersDataFrame.as[User]

    val schemaforratings: StructType = StructType("UserID::MovieID".split("::")
      .map(column => StructField(column, StringType, true)))
      .add("Rating", DoubleType, true)
      .add("Timestamp", StringType, true)
    val ratingsRDDRows: RDD[Row] = ratingsRDD.map(_.split("::")).map(line => Row(line(0).trim,
      line(1).trim, line(2).trim.toDouble, line(3).trim))
    val ratingsDataFrame: DataFrame = spark.createDataFrame(ratingsRDDRows, schemaforratings)
    val ratingsDataSet: Dataset[Rating] = ratingsDataFrame.as[Rating]

    val schemaformovies: StructType = StructType("MovieID::Title::Genres".split("::")
      .map(column => StructField(column, StringType, true)))
    val moviesRDDRows: RDD[Row] = moviesRDD.map(_.split("::")).map(line => Row(line(0).trim, line(1).trim,
      line(2).trim))
    val moviesDataFrame: DataFrame = spark.createDataFrame(moviesRDDRows, schemaformovies)
    val moviesDataSet: Dataset[Movie] = moviesDataFrame.as[Movie]

//    println("功能一：通过DataSet实现某特定电影观看者中男性和女性不同年龄分别有多少人？")
//    ratingsDataSet.filter(s" MovieID = 1193") //这里能够直接指定MovieID的原因是DataFrame中有该元数据信息！
//      .join(usersDataSet, "UserID") //Join的时候直接指定基于UserID进行Join，这相对于原生的RDD操作而言更加方便快捷
//      .select("Gender", "Age")  //直接通过元数据信息中的Gender和Age进行数据的筛选
//      .groupBy("Gender", "Age") //直接通过元数据信息中的Gender和Age进行数据的groupBy操作
//      .count()  //基于groupBy分组信息进行count统计操作
//      .show(10) //显示出分组统计后的前10条信息

    /**
      * 功能二：使用DataSet进行电影流行度分析：所有电影中平均得分最高（口碑最好）的电影及观看人数最高的电影（流行度最高）
      * "ratings.dat"：UserID::MovieID::Rating::Timestamp
      * 得分最高的Top10电影实现思路：如果想算总的评分的话一般肯定需要reduceByKey操作或者aggregateByKey操作
      *   第一步：把数据变成Key-Value，大家想一下在这里什么是Key，什么是Value。把MovieID设置成为Key，把Rating设置为Value；
      *   第二步：通过reduceByKey操作或者aggregateByKey实现聚合，然后呢？
      *   第三步：排序，如何做？进行Key和Value的交换
      */
//    println("通过纯粹使用DataSet方式计算所有电影中平均得分最高（口碑最好）的电影TopN:")
//    ratingsDataSet.select("MovieID", "Rating").groupBy("MovieID").
//      avg("Rating").orderBy($"avg(Rating)".desc).show(10)

    /**
      * 上面的功能计算的是口碑最好的电影，接下来我们分析粉丝或者观看人数最多的电影
      */
//    println("纯粹通过DataSet的方式计算最流行电影即所有电影中粉丝或者观看人数最多(最流行电影)的电影TopN:")
//    ratingsDataSet.groupBy("MovieID").count().
//      orderBy($"count".desc).show(10)

    /**
      * 功能三：分析最受男性喜爱的电影Top10和最受女性喜爱的电影Top10
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
    val genderRatingsDataSet: DataFrame = ratingsDataSet.join(usersDataSet, "UserID").cache()
    val maleFilterRatingsDataSet: DataFrame = genderRatingsDataSet.filter(" Gender='M' ").select("MovieID", "Rating")
    val femaleFilterRatingsDataSet: DataFrame = genderRatingsDataSet.filter("Gender= 'F'").select("MovieID", "Rating")

//    println("纯粹使用DataSet实现所有电影中最受男性喜爱的电影Top10:")
//    maleFilterRatingsDataSet.groupBy("MovieID").avg("Rating").orderBy($"avg(Rating)".desc).show(10)

//    println("纯粹使用DataSet实现所有电影中最受女性喜爱的电影Top10:")
//    femaleFilterRatingsDataSet.groupBy("MovieID").avg("Rating").orderBy($"avg(Rating)".desc, $"MovieID".desc).show(10)

    /**
      * 功能五：最受不同年龄段人员欢迎的电影TopN
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
    println("纯粹通过DataSet的方式实现所有电影中QQ或者微信核心目标用户最喜爱电影TopN分析:")
    ratingsDataSet.join(usersDataSet, "UserID").filter("Age = '18'").groupBy("MovieID")
      .count().join(moviesDataSet, "MovieID").select("Title", "count").sort($"count".desc).show(10)

    /**
      * 淘宝核心目标用户最喜爱电影TopN分析
      * (Pulp Fiction (1994),959)
        (Silence of the Lambs, The (1991),949)
        (Forrest Gump (1994),935)
        (Jurassic Park (1993),894)
        (Shawshank Redemption, The (1994),859)
      */
    println("纯粹通过DataSet的方式实现所有电影中淘宝核心目标用户最喜爱电影TopN分析:")
    ratingsDataSet.join(usersDataSet, "UserID").filter("Age = '25'").groupBy("MovieID").
      count().join(moviesDataSet, "MovieID").select("Title", "count").sort($"count".desc).limit(10).show()

    val elapsedTime: Long = System.currentTimeMillis() - startTime
    println("Finished in milliseconds: " + elapsedTime / 1000)























  }

}
