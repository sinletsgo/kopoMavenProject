package com.haiteam

import com.haiteam.ex322.spark
import org.apache.spark.sql.SparkSession

object ex323 {
  def main(args: Array[String]): Unit = {
  }


  val spark = SparkSession.builder().appName("hkProject").
    config("spark.master", "local").
    getOrCreate()



  // 복습.kopo_batch_season_mpara 데이터 가져오기
  var staticUrl = "jdbc:postgresql://192.168.110.111:5432/kopo"
  var staticUser = "kopo"
  var staticPw =  "kopo"
  var selloutDb = "kopo_batch_season_mpara"

  // jdbc (java database connectivity ) 연결

  var selloutDataFromPg = spark.read.format("jdbc").options(Map("url" -> staticUrl,"dbtable"->selloutDb, "user" -> staticUser, "password" -> staticPw)).load

  // 메모리 테이블 생성
  selloutDataFromPg.createOrReplaceTempView("selloutTable")







//   kopo_batch_season_mpara 중 1열에 3번째 데이터만
    selloutDataFromPg.createOrReplaceTempView("testTable")
    spark.sql("select * from testTable where param_value = '3'")



















  //라이브러리 활용하기
  // Calendar 라이브러리 호출
  import java.util.Calendar

  // 라이브러리 사용
  // ex 1주식 데이터 가져온다고 할떄, 오늘 날짜는 가져오니, 전날은 -1 식으로 활용하면 됨
  // ex 2 위도 경도 줄테니 두 지점 거리 구해라~! 라이브러리 지오로케이션 같은 곳에서 제공함. 가져와서 jars 넣고 spark shell통해 사용가능! 일일이 다할 필요 x
  var calendar = Calendar.getInstance()
  var time = calendar.getTime()
  var hour = time.getHours()
  var minutes = time.getMinutes()


  // 현재 시간(분) 에서 1을 뺸 값을 answer변수에 담기
  var answer = calendar.getTime.getMinutes - 1



  // Math 함수로 현재시간 구하기
  Math.round(calendar.getTime.getMinutes-1)







  // 조건 판단하기
  // if (조건문)//조건이 일치하는 경우 실행
  // 조건문 참인경우 실행 }else{
    //조건문 불일치 시 실행 }
    //자바랑 비슷

  // 체크: 입력데이터의 연도 최대 값
  var currentYear = 2018
  var deltaYear = 7
  // 체크: 입력데이터의 연도 최소 값
  var validYear = 2015

  if(deltaYear != 0){
    validYear = currentYear - deltaYear
    print("유효데이터는"+validYear)
  }else{
    validYear
    print("유효데이터는"+validYear)
  }
  //delta 두 값의 차이 같은 뜻. 두 년도에 차이 변수를 저렇게 적어놓음.
  // 7입력하면 차이 만큼의 데이터를 나중에 끌어오거나 할때 활용








  //  디버깅 방법
  // 빨간색 체크하고 런하면 그곳에서 멈춤 f8 단계씩 넘어갈 수 있음
  // 오류잇으면 디버깅 안됨 jas 르 appry 안해서 오류났음

 // if문 사용시 조건에서 && 같은것 사용시 줄 바꿈해주어야 눈에 잘보임


  //반복하기

  //while (조건문){
  // 조건이 참인경우
//}
//조건문이 참이 아닌경우 탈출
  // 반복하기
  var priceData = Array(1000.0,1200.0,1300.0,1500.0,10000.0)
  var promotionRate = 0.2
  var priceDataSize = priceData.size
  var i = 0

  while(i < priceDataSize){
    var promotionEffect = priceData(i) * promotionRate
    priceData(i) = priceData(i) - promotionEffect
    i=i+1
  }


//필터와 달리 map은 변경할수 있다, for 루프 보다 더 속도가 빠름.
  //똑같이 할인된 값을 구하는구나!
priceData.map(x=>{
  var promotionEffect = x*0.2
  x-promotionEffect
})


















}



