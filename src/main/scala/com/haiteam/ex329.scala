package com.haiteam
import org.apache.spark.sql.SparkSession

object ex329 {
  def main(args: Array[String]): Unit = {



  }


    //반복하기
  // for
  // for (i<-0 until {size}){
  // i가 size보다 작을때까지 반복 수행}
  // until 은 size 값 포함하지 않음
  //  to는 size 값 포함함


  var priceData = Array(1000.0,1200.0,1300.0,1500.0,10000.0)
  var promotionRate = 0.2
  var priceDataSize = priceData.size

  for(i <-0 until priceDataSize){   //만약 to로 하면 size가 5까지 하니, array 4번 초과하니 에러 난다
    var promotionEffect = priceData(i) * promotionRate
    priceData(i) = priceData(i) - promotionEffect
  }



  // map vs filter
  var priceData2 = Array(1000.0,1200.0,1300.0,1500.0,10000.0)
  priceData2.map(x=>{
    x  - (x*promotionRate)} //map은 실제 각 요소를 변경 시킨다. 데이터 처리 있어서 플러스 요소 . 분산처리!
  )
  priceData2.filter(x => {
    x > 1000 // 조건에 따라 필터!
  })


  // 자주 사용하는 내용 함수화 하기
  // 생산성 up! 프로젝트할때 함수로 만들면 좋겠지!
  // 함수를 만들던지! 인터넷에서 찾던지!
//  def 함수명(입력변수#1, 입력변수#2,…):
//  return 결과값

  //정의: 가격과 비율을 입력받아 할인 가격을 리턴함
  ////파라미터: 가격, 비율
  ////결과 값: 비율을 적용한 할인가격

  def discountedPrice(price:Double, rate:Double) :Double =  {
    var discount = price * rate
    var returnValue = price - discount
    returnValue //return 써줘도 되고 안써줘도 되고,
  }
  var orgrRate = 0.2
  var orgPrice = 2000
  var newrPrice =
    discountedPrice(orgPrice, orgrRate)











//  자신이 원하는 상품정보 이름을 배열로 정의하고
//    반복분을 돌리면서 상품이름 앞에
//  “kopo_” 이름을 붙이세요^^

  var fruitList = Array("바나나", "포도", "딸기", "수박", "망고", "귤")

  var fruitListSize = fruitList.size
  var i=  0;
  while( i < fruitListSize){
    fruitList(i)  = "kopo" + fruitList(i)
    i = i+1
  }


  var fruitList2 = Array("바나나", "포도", "딸기", "수박", "망고", "귤")

  var fruitListSize2 = fruitList2.size
  var i2=  0;

  for(i2 <-0 until fruitListSize2){
    fruitList2(i2) = "kopo" + fruitList2(i2)
  }

  for (i2 <-0 until fruitListSize2){
    println(fruitList2(i2))
  }









//
//
//
//
////var a = 15.125222
////var b = 15.147218
////var c = 69.72756
//  //  값을 활용하여 1) 반올림 소수점 2자리
//  //  이후 합이 100이되도록 구현하세요
//  // (* a 값에 나머지값은 추가하면됩니다)
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//  //나
//  def roundDef(a:Double, b:Double): Double ={
//    var i1 = Math.pow(10 , b)
//    var i2 = Math.round(a*i1) / i1
//    return i2
//  }
//
//
//  var a = 15.125222
//  var b = 15.147218
//  var c = 69.72756
//
//  var result = roundDef(a, 2)
//  var result2 = roundDef(b, 2)
//  var result3 = roundDef(c, 2)
//
//
// //교수님
//  def hkround(targetValue: Double, sequence:Int): Double = {
//    var multiValue = Math.pow(10, sequence)
//    var returnValue = Math.round(targetValue*multiValue) / multiValue
//    returnValue
//  }
//
//  hkround(12.2222,3)
//  hkround(12.2222,2)
//
//  //나머지 워크 제로에서
//
//
//
//
//










}
