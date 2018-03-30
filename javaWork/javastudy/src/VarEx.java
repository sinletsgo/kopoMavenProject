public class VarEx {

    public static void main(String[] args){
        //변수 + 정수, 실수 리터럴 사용법
        //리터럴? 프로그램에 직접 표현한 값

        int mach; //변수 선언 --> 4byte
        int distance;
        mach = 340; //int mach = 340; 초기화
        distance = mach * 60 * 60;
        System.out.println("1시간 동안 가는 거리 " + distance + "m");

        double radius;
        double area;
        final double PI = 3.14159;  //상수는 final 붙여줌. 변하지 않음

        radius = 10.0;
        area = radius * radius * PI;
        System.out.println("반지름이 " + radius + " 인 원의 넓이 : " + area);radius = 10.0;


        radius = 100.0; //radius 위에서 했지만, 이렇게 값 바꿔서 재사용할 수도 있음.
        area = radius * radius * PI;
        System.out.println("반지름이 " + radius + " 인 원의 넓이 : " + area);




        //문자, 논리
        char ga1 = '가';        //문자 1개
        char ga2 = '\uac00';   //유니코드

        String str = "hello";   //문자 2개 이상

        boolean cham = true;  //참
        boolean geojit = false; //거짓


        System.out.println(ga1);
        System.out.println(ga2);
        System.out.println(str);
        System.out.println(cham + " 가 아니면" + geojit + " 입니다");

        byte fifteen = 0b1111;
        long lightSpeed = 300000L;   //정수 long 타입

        System.out.println(fifteen);
        System.out.println(lightSpeed);




        //타입 변환 응용
        int i;
        double d;
        double d1;
        byte b;


        i = 7 / 4;    // 7나누기 4,  타입변환을 하지 않고 계산 -->손실 생김
        System.out.println(i);


        d =7 / (double) 4; // 큰타입 -> 작은걸로 강제 타입변환 시엔 () 안에 명시적으로 형 변환.
        d1 = 7 / 4.0;   //  두개가 같은 것.
        System.out.println("d 출력결과 :" + d );
        System.out.println("d1 출력결과 :" + d1 );

        double d2 = 1.999999;
        int j = (int) d2;   // d2는 double 타입. 큰타입 -> 작은 int 로 형변환 자료 손실
        System.out.println("j 출력결과 "+ j);

        b = (byte) 300; // byte는 -128 ~ 127 (2에 8  = 256 표현)  300 - 256 = 44가 b에 들어감
        System.out.println("b 출력결과 " + b);



    }
}
