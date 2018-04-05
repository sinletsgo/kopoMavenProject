package com.haiteam
import org.apache.spark.sql.SparkSession;

object ex405 {

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


    //잘 들어갔는지 확인하려면 엑셀에서 vlookup  매치시켜서 데이터 채워주는걸 볼수있음.
    // res29.write로 오라클로 보낸다고?

    //
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
    spark.sql("select a.regionid, a.product, b.regionname, a.yearweek, a.qty " +
      "from mainTable a " +
      "left join subTable b " +
      "on a.regionid = b.regionid")



    // inner join 없는 데이터는 일단 버리자
    spark.sql("select a.regionid, a.product, b.regionname, a.yearweek, a.qty " +
      "from mainTable a " +
      "inner join subTable b " +
      "on a.regionid = b.regionid")

  }
}
