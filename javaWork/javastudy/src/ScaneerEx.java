import java.util.Scanner;

public class ScaneerEx {
    public static void main(String[] args){

        //객체 생성 (Scanner in)
        //클래스 변수 = new 클래스 ();

        Scanner in = new Scanner(System.in); // in  객체 생성 !

        int x = in.nextInt(); //객체사용법: 객체변수명.메소드(); int 타입 입력, 입력 값이 int 변수 x로 들어감
        int y = in.nextInt();


        System.out.printf("%d * %d는 %d입니다 \n", x, y, x*y);


        System.out.println("이름과 나이를 입력하세요");
        String name = in.next(); //string 타입 입력
        int age = in.nextInt();
        System.out.println("당신의 이름은 " + name + " 나이는 " + age + " 입니다");

    //문제 : 직사각형의 가로 세로 값을 입력받아서 넓이를 구하는 실습

        System.out.println("직사각형의 가로를 입력하세요");
        int a = in.nextInt();
        System.out.println("직사각형의 세로를 입력하세요");
        int b = in.nextInt();
        System.out.println("직사각형의 넓이는 " + a * b +" 입니다");


    }

}
