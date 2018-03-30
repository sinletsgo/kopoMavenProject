package com.haiteam

import org.apache.spark.sql.SparkSession

object ex322 {
  def main(args: Array[String]): Unit = {
  }

  val spark = SparkSession.builder().appName("hkProject").
    config("spark.master", "local").
    getOrCreate()


  var a = 10;


  var testArr = Array(22, 33, 50, 70, 90, 100)

  testArr.filter(x => {
    x % 10 == 0
  })

  print(testArr)

  // 이런 방법으로도
  var testArray = Array(22, 33, 50, 70, 90, 100)
  var answer = testArray.filter(x => {
    var data = x.toString()
    var dataSize = data.size
    var lastChar = data.substring(dataSize - 1).toString
    lastChar.equalsIgnoreCase("0")})




  var arraySize = answer.size
  for (i <- 0 until arraySize) {
    println(answer(i))
  }



//
//
//
//
//
//
//  //
//  // Postgres / GreenplumDB 데이터 로딩
//  //접속 정보 설정
//  var staticUrl = "jdbc:postgresql://192.168.110.111:5432/kopo"
//  var staticUser = "kopo"
//  var staticPw =  "kopo"
//  var selloutDb = "kopo_channel_seasonality"
//
//    // jdbc (java database connectivity ) 연결
//
//  var selloutDataFromPg = spark.read.format("jdbc").options(Map("url" -> staticUrl,"dbtable"->selloutDb, "user" -> staticUser, "password" -> staticPw)).load
//
//  // 메모리 테이블 생성
//  selloutDataFromPg.createOrReplaceTempView("selloutTable")
//
//
//
//
//
//
//
//  //RDB(MySql) 불러오기
//  //var [variable name] = spark.read.format(“[format]”).option.load
//
//  // 파일설정// 파일설정
//   staticUrl = "jdbc:mysql://192.168.110.112:3306/kopo"
//   staticUser = "root"
//   staticPw = "P@ssw0rd"
//   selloutDb = "KOPO_PRODUCT_VOLUME"
//
//  // jdbc (java database connectivity)
//  val selloutDataFromMysql= spark.read.format("jdbc").
//    options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load
//
//  selloutDataFromMysql.createOrReplaceTempView("selloutTable")
//
//
//
//
//
//
//
////RDB(SqlServer) 불러오기
//// var [variable name] = spark.read.format(“[format]”).option.load
//
//  ///////////////////////////     SqlServer(MySQL) 데이터 로딩 ////////////////////////////////////
//  // 파일설정
//   staticUrl = "jdbc:sqlserver://192.168.110.70:1433;databaseName=kopo"
//   staticUser = "haiteam"
//   staticPw = "haiteam"
//   selloutDb = "dbo.KOPO_PRODUCT_VOLUME"  //dbo는 ms sql 사용하는 것. 자기 자신이다.
//
//  // jdbc (java database connectivity) 연결
//  val selloutDataFromSqlserver= spark.read.format("jdbc").
//    options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load
//
//  // 메모리 테이블 생성
//  selloutDataFromSqlserver.createOrReplaceTempView("selloutTable")
//  // 예전에는 이걸로 사용
//  // selloutDataFromSqlserver.registerTempTable("selloutTable")
//  selloutDataFromSqlserver.show(1)
//
//
//
//
//
//
//
//
//////
//  // RDB(Oracle) 불러오기
//  //var [variable name] = spark.read.format(“[format]”).option.load
//  ///////////////////////////     Oracle 데이터 로딩 ////////////////////////////////////
//  // 접속정보 설정
//  var staticUrl = "jdbc:oracle:thin:@192.168.110.3:1522/XE"
//  var staticUser = "kopoTest"
//  var staticPw = "kopoTest"
//  var selloutDb = "kopo_product_volume"
//
//  // jdbc (java database connectivity) 연결
//  val selloutDataFromOracle= spark.read.format("jdbc").
//    options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load
//
//  // 메모리 테이블 생성
//  selloutDataFromOracle.createOrReplaceTempView("selloutTable")
//  selloutDataFromOracle.show()
//
//
//
//
//
//
//
//
//  //데이터 저장
//
//  //파일로 저장하기
//  //[데이터프레임명].save(“저장파일명”)
//  // 파일저장
//  selloutDataFromOracle.
//    coalesce(1). // 파일개수
//    write.
//    format("csv").  // 저장포맷
//    mode("overwrite"). // 저장모드 append/overwrite
//    option("header", "true"). // 헤더 유/무
//    save("c:/testResult.csv") // 저장파일명
//
//
//
//
//  //데이터베이스에 저장하기
// // [데이터프레임명].write.jdbc(“접속주소”,”테이블명“,”접속정보”)
//
//
//  // 데이터베이스 주소 및 접속정보 설정
//  var staticUrl = "jdbc:oracle:thin:@192.168.110.3:1522/XE"
//  //staticUrl = "jdbc:oracle:thin:@127.0.0.1:1521/XE"
//  var staticUser = "kopoTest"
//  var staticPw = "kopoTest"
//
//  // 데이터 저장
//  val prop = new java.util.Properties
//  prop.setProperty("driver", "oracle.jdbc.OracleDriver")
//  prop.setProperty("user", staticUser)
//  prop.setProperty("password", staticPw)
//  val table = "kopo_channel_seasonality_ssw"
//  //append
//  selloutDataFromOracle.write.mode("overwrite").jdbc(staticUrl, table, prop)
//
//
//
//
//
//
//
//
//




}
