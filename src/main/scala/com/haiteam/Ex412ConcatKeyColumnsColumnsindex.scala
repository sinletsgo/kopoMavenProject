package com.haiteam
import org.apache.spark.sql.SparkSession

object Ex412ConcatKeyColumnsColumnsindex {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()


    // RDB(Oracle) 불러오기
    //var [variable name] = spark.read.format(“[format]”).option.load
    ///////////////////////////     Oracle 데이터 로딩 ////////////////////////////////////
    // 접속정보 설정
    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb1 = "kopo_channel_seasonality_new"
    var selloutDb2 = "kopo_product_master"

    // jdbc (java database connectivity) 연결
    val selloutDataFromOracle1= spark.read.format("jdbc").
      options(Map("url" -> staticUrl,"dbtable" -> selloutDb1,"user" -> staticUser, "password" -> staticPw)).load

    val selloutDataFromOracle2= spark.read.format("jdbc").
      options(Map("url" -> staticUrl,"dbtable" -> selloutDb2,"user" -> staticUser, "password" -> staticPw)).load


    // 메모리 테이블 생성
    selloutDataFromOracle1.createOrReplaceTempView("selloutTable1")
    selloutDataFromOracle2.createOrReplaceTempView("selloutTable2")
    selloutDataFromOracle1.show()





    // left join
      spark.sql("select a.regionid, " +
        "a.product, " +
        "a.yearweek, " +
        "a.qty, " +
        "b.productname " +
        "from selloutTable1 a " +
        "left join selloutTable2 b " +
        "on a.product = b.productid")

    //1-1. 분석데이터 키 컬럼 생성
    // concat keycol로 만들고 left join
    var middleResult = spark.sql("select " +
      "concat(a.regionid,'_',a.product) as keycol, " +
      "a.regionid, " +
      "a.product, " +
      "a.yearweek, " +
      "cast(a.qty as double) as qty, " +
      "b.productname " +
      "from selloutTable1 a " +
      "left join selloutTable2 b " +
      "on a.product = b.productid")




//    1-2. 컬럼 인덱스 생성

    var rawData = middleResult
    var rawDataColumns = rawData.columns

    var keyNo = rawDataColumns.indexOf("keycol")
    var accountidNo = rawDataColumns.indexOf("regionid")
    var productNo = rawDataColumns.indexOf("product")
    var yearweekNo = rawDataColumns.indexOf("yearweek")
    //.......




  }


}
