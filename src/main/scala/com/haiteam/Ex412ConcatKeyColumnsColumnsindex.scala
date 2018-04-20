package com.haiteam

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.Row

object Ex412ConcatKeyColumnsColumnsindex {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()


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


    // left join
//      spark.sql("select a.regionid, " +
//        "a.product, " +
//        "a.yearweek, " +
//        "a.qty, " +
//        "b.productname " +
//        "from selloutTable1 a " +
//        "left join selloutTable2 b " +
//        "on a.product = b.productid")

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
    var qtyNo = rawDataColumns.indexOf("qty")
    var productnameNo = rawDataColumns.indexOf("productname")

    // Rdd로 변환
    var rawRdd = rawData.rdd




    // 데이터 확인
//    var {RDD변수명}.collect.foreach(println)
//    rawRdd.collect.foreach(println)


    // var {RDD변수명} = {RDD변수명}.filter(x=>{ 필터조건식})
    var rawExRdd = rawRdd.filter(x=>{
      var checkValid = true;    //모든 데이터 true로 먼저 설정
      // 설정 부적합 로직
      if (x.getString(3).length !=6){     // yaerweek가 6자리가 아닌 경우 삭제!
        checkValid = false;
      }
      checkValid
    })





    //디버깅1
//    var rawExRdd = rawRdd;
//    var x =rawExRdd.first
//
//    // spark shell 인터렉티브하게 다음 코드 넣으며 디버깅
//    var checkValid =true
//    x.getString(3)
//    x.getString(4)
//    var yearweek = x.getString(yearweekNo)



  ///
    var filterex2Rdd = rawRdd.filter(x=>{
      (x.getString(yearweekNo) == "201403")
    })
    var x = filterex2Rdd.first


    //디버깅2
//    A60 PRODUCT34 201402 이걸 찾겠다!
    var rawExRdd2 = rawRdd.filter(x=>{
      var checkValid = true;
      if ( (x.getString(accountidNo) == "A06") &&
        (x.getString(productNo) == "PRODUCT34") &&
        (x.getString(yearweekNo) == "201402") ) {
        checkValid = false;
      }
      checkValid
    })
    rawExRdd2.count

    //53주차 제거 로직
    var filteredRdd = rawRdd.filter(x=>{
      var checkValid = true;
      var weekValue = x.getString(yearweekNo).substring(4).toInt
      if (weekValue >=53){
        checkValid = false;
      }
      checkValid
    })

    // product1,2 인 정보만 필터링 1
    var testRdd = rawRdd.filter(x=>{
      var checkValid = false;
      if ( (x.getString(productNo)  == "PRODUCT1") ||
        (x.getString(productNo)  == "PRODUCT2") ){
        checkValid = true;
      }
      checkValid
    })

    // product1,2 인 정보만 필터링 2
    var productArray = Array("PRODUCT1","PRODUCT2")
    var productSet = productArray.toSet
    var resultRdd2 = filteredRdd.filter(x=>{
      var checkValid = false;
      var productInfo = x.getString(productNo);
      if (productSet.contains(productInfo)) {
        checkValid = true;
      }
      checkValid
    })

    // 데이터형 변환 [RDD → Dataframe]
    val finalResultDf = spark.createDataFrame(resultRdd2,
      StructType(
        Seq(
          StructField("KEY", StringType),
          StructField("REGIONID", StringType),
          StructField("PRODUCT", StringType),
          StructField("YEARWEEK", StringType),
          StructField("QTY", DoubleType),
          StructField("PRODUCTNAME", StringType))))




        //Row x
        // key, account, product, yearweek, qty, productname
        //qty가 double이니까, GetString x -> getDouble
        var mapRdd = filteredRdd.map(x=>{

          // 로직 부분
          var qty = x.getDouble(qtyNo)
          var maxValue = 700000

          // 출력 부분
          if(qty > 700000){qty = 700000}
          ( x.getString(keyNo),
            x.getString(yearweekNo),
            x.getString(qtyNo) )

        })

        //Row x를 qty로 바꾸고
        var mapRdd2 = filteredRdd.map(x=>{

          // 로직 부분
          var qty = x.getDouble(qtyNo)
          var maxValue = 700000

          // 출력 부분
          if(qty > 700000){qty = 700000}
          ( x.getString(keyNo),
            x.getString(yearweekNo),
            qty )

        })










//      //Row 로 할시
//     // import org.apache.spark.sql.Row 이거 위에 넣어주고 스파크쉘에서 할때도 넣어주고
//      // key, account, product, yearweek, qty, productname
//      var mapRdd = filteredRdd.map(x=>{
//
////        var qty = x._5  // row이렇게 접근. 어떻게보면 더 직관적일수도..
//        var maxValue = 700000
//
//        if(qty > 700000){qty = 700000}
//        Row( x.getString(keyNo),
//          x.getString(yearweekNo),
//          qty )
//      })






    //filteredRdd 로 할거다!
    // filteredRdd.first 찍어보고 어떤 데이터가 있는지 확인 후 로직을 짜는거다
    // filteredRdd.first = (키정보, 지역정보, 상품정보, 연주차정보, 거래량 정보, 상품이름 정보)
    // 처리로직 : 거래량이 MAXVALUE 이상인건은 MAXVALUE로 치환한다
//    //
//    var MAXVALUE = 700000
//
//    var mapRdd = filteredRdd.map(x=>{
//      //로직구현예정
//      var qty = x.getString(qtyNo)
//    })

    // 디버깅코드
//    var mapRdd = filteredRdd
//    var x = mapRdd.first   //랜덤 가져오기 다시돌리고 또.
    // var x = mapRdd.filter(x=> { x.getDouble(qtyNo) > 700000 }).first   70만아상인건이 잘 동작하는가?
    //x.getDouble(qtyNo)  하면 가져오네?!
    // var org_qty = x.getDouble(qtyNo)
    // var new_qty = org_qty

    var MAXVALUE = 700000

    var mapRdd3 = filteredRdd.map(x=>{
      //로직구현예정
       var org_qty = x.getDouble(qtyNo)
       var new_qty = org_qty
      if (new_qty > MAXVALUE){
        new_qty = MAXVALUE
      }
      //출력 row 키정보, 지역정보, 상품정보, 연주차정보, 거래량 정보, 거래량 정보_new 상품이름정보)
      (x.getString(keyNo),
        x.getString(yearweekNo),
        org_qty,
        new_qty
      )
    })

//    (x.getString(keyNo),
//      x.getString(yearweekNo),
//      org_qty,
//      new_qty
//    )
//    속성이 나오네, res.. _1 하면 가져온다

    //인텔리 j에서 위부터 셋팅 해놔야, x.   했을때 함수목록뜨니, 나중에 더 편리할것


    // row를 찾을 수 없다  import 빼고 올린것일 수도. 구글통해 찾아야 spark Row import
//        Row(x.getString(keyNo),
//          x.getString(yearweekNo),
//          org_qty,
//          new_qty
//        )


    //순서가 뒤바뀌면 인덱스가 꼬이기 시작



    //디버깅을 잘해야!!

    //데이터는 아이템 100개가 있으면 위에 ledtv같은 속성이 있다 또 여러 속성을 아우르는 마스터 프로덕트 계층 속성이 있다. group by시 도움.. 알아만..





  }


}
