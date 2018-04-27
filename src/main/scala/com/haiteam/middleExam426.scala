package com.haiteam
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object middleExam426 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()





    //! 시험 궁금한것, 알아둘것
    // qty double 0이 많으면 형변환을 해줘야 df 변환 후 출력 오류 안난다
    // qty 1.2 곱하면서 cast 넣으려면?
    // 정제조건 3가지 파라미터로 빼면 더 깔끔하다고, 한곳에 넣으니 좀..
    // df -> rdd 변환시 컬럼명 지정가능. 굳이 대문자 -> 소문자로 변환안했어도 됬는데..
    // 포스트그레 sql 서버로 데이터저장
    // Oracle Data Visualization 조작해보기

    var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "KOPO_CHANNEL_SEASONALITY_NEW"

    // jdbc (java database connectivity) 연결
    val selloutDataFromOracle = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

    // 1번 답: 데이터  불러오고  확인
    selloutDataFromOracle.show(2)



    selloutDataFromOracle.createOrReplaceTempView("selloutTable")
    var qtyResult = spark.sql("select REGIONID," +
      " PRODUCT," +
      " YEARWEEK," +
      " cast(QTY as double)," +
      " QTY * 1.2 AS QTY_NEW from selloutTable")
    // 2번답:  SQL분석
    qtyResult.show()


    qtyResult.createOrReplaceTempView("selloutTable2")
    var qtyResult2 = spark.sql("select REGIONID," +
      " PRODUCT," +
      " YEARWEEK," +
      " cast(QTY as double)," +
      " cast(QTY_NEW as double) from selloutTable2")
    // 죄송합니다. 코드를 줄이지 못했습니다. 2번답:  SQL분석
    qtyResult2.show()




    // 4번답: 정제
    var rawData = qtyResult2
    var rawDataColumns = rawData.columns
    var accountidNo = rawDataColumns.indexOf("REGIONID")
    var productNo = rawDataColumns.indexOf("PRODUCT")
    var yearweekNo = rawDataColumns.indexOf("YEARWEEK")
    var qtyNo1 = rawDataColumns.indexOf("QTY")
    var qtyNo2 = rawDataColumns.indexOf("QTY_NEW")

    var rawRdd = rawData.rdd

    var productArray = Array("PRODUCT1","PRODUCT2")
    var productSet = productArray.toSet
    var filteredRdd = rawRdd.filter(x=>{
      var checkValid = true;
      var productInfo = x.getString(productNo);
      var weekValue = x.getString(yearweekNo).substring(4).toInt
      if ( (weekValue == 52) ||
        (productSet.contains(productInfo) != true) ||
        (x.getString(yearweekNo) < "2016") ) {
        checkValid = false;
      }
      checkValid
    })

    val finalResultDf = spark.createDataFrame(filteredRdd,
      StructType(
        Seq(
            StructField("REGIONID", StringType),
          StructField("PRODUCT", StringType),
          StructField("YEARWEEK", StringType),
          StructField("QTY", DoubleType),
          StructField("QTY_NEW", DoubleType))))

    finalResultDf.createOrReplaceTempView("subDataFromPostgresqlTable")

    var resultDf = spark.sql("select " +
      "REGIONID AS regionid, " +
      "PRODUCT AS product, " +
      "YEARWEEK AS yeakweek, " +
      "QTY AS qty, " +
      "QTY_NEW AS qty_new " +
      "from subDataFromPostgresqlTable")


    var staticUrl = "jdbc:postgresql://192.168.110.111:5432/kopo"
    //staticUrl = "jdbc:oracle:thin:@127.0.0.1:1521/XE"
    var staticUser = "kopo"
    var staticPw = "kopo"

    val prop = new java.util.Properties
    prop.setProperty("driver", "org.postgresql.Driver")
    prop.setProperty("user", staticUser)
    prop.setProperty("password", staticPw)
    val table = "KOPO_ST_RESULT_SSW"

    resultDf.write.mode("overwrite").jdbc(staticUrl, table, prop)




  }
}