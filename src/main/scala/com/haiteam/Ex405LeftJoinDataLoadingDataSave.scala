package com.haiteam
import org.apache.spark.sql.SparkSession

object Ex405LeftJoinDataLoadingDataSave {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()

    //파일 불러와서 join
    var dataPath = "c:/spark/bin/data/"
    var mainData = "kopo_channel_seasonality_ex.csv"
    var subData = "kopo_product_mst.csv"

    var mainDataDf = spark.read.format("csv").
      option("header", "true").
      load(dataPath + mainData)

    var subDataDf = spark.read.format("csv").
      option("header", "true").
      load(dataPath + subData)

    mainDataDf.createOrReplaceTempView("mainTable")
    subDataDf.createOrReplaceTempView("subTable")

    // sql 로 left join
    spark.sql("select a.regionid, a.productgroup, b.productname, a.yearweek, a.qty " +
      "from mainTable a " +
      "left join subTable b " +
      "on a.PRODUCTGROUP = b.PRODUCTID")



    // 서버 데이터 가져와서 join!
    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb1 = "kopo_channel_seasonality_new"
    var selloutDb2 = "kopo_region_mst"

    // jdbc (java database connectivity) 연결
    val mainDataFromOracle = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb1, "user" -> staticUser, "password" -> staticPw)).load

    val subDataFromOracle2 = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb2, "user" -> staticUser, "password" -> staticPw)).load

    // 메모리 테이블 생성
    mainDataFromOracle.createOrReplaceTempView("mainTable")
    subDataFromOracle2.createOrReplaceTempView("subTable")

    mainDataFromOracle.show()



    // left join  데이터는 무조건 살려야되
    var resultDf = spark.sql("select a.regionid, a.product, b.regionname, a.yearweek, a.qty " +
      "from mainTable a " +
      "left join subTable b " +
      "on a.regionid = b.regionid")



    // 위와 같이 left join 한 data를 데이터베이스 서버로 전송한뒤 컬럼명이 소문자라서 sql 검색이 안되는 문제 발생.
    // 아래 같이 대문자로 컬럼명 바꿔주기

//    mainDataFromOracle.createOrReplaceTempView("mainDataFromOracleTable")
//
//    subDataFromOracle2.createOrReplaceTempView("subDataFromOracle2Table")
//
//    var resultDf = spark.sql("select " +
//      "a.regionid AS REGIONID, " +
//      "a.product AS PRODUCT, " +
//      "a.yearweek AS YEARWEEK, " +
//      "cast(qty as double) AS QTY, " +
//      "b.regionname AS REGIONNAME " +
//      "from mainDataFromOracleTable A " +
//      "left join subDataFromOracle2Table B " +
//      "on a.regionid = b.regionid")


//    // toDF로 대문자로 바꿔줄 수도 있다.
//     resultDf.toDF("REGIONID", "PRODUCT", "REGIONNAME", "YEARWEEK", "QTY")







//
//
//    //오라클 내서버로 join data 저장
//    staticUrl = "jdbc:oracle:thin:@127.0.0.1:1522/XE"
//    var staticUser = "kopoTest"
//    var staticPw = "kopoTest"
//
//    // 데이터 저장
//    val prop = new java.util.Properties
//    prop.setProperty("driver", "oracle.jdbc.OracleDriver")
//    prop.setProperty("user", staticUser)
//    prop.setProperty("password", staticPw)
//    val table = "ssw"
//    //append
//    resultDf.write.mode("overwrite").jdbc(staticUrl, table, prop)
//
//
//
//
    // Dataframe.toDf.....
    ///////////// Oracle Express Usage ////////////////
    //alter system set processes=500 scope=spfile
    //show parameter processes
    //shutdown immediate
    //startup



//  // 파일로 저장할때 한글 깨질때 아래 같이 하면된다
//    resultDf.
//      coalesce(1). // 파일개수
//      write.format("csv").  // 저장포맷
//      mode("overwrite"). // 저장모드 append/overwrite
//      option("charset", "ISO-8859-1").
//      option("header", "true"). // 헤더 유/무
//      save("e:/resultdf2.csv")
//    //ISO-8859-1 ISO-8859-1

//    resultDf.rdd.coalesce(1).map { x =>x.mkString(",") }.saveAsTextFile("e:/test/test10.csv")






  }
}
