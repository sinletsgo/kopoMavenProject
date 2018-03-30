public class PrintFomat {
    public static void main(String[] args){

        int i = 97;
        String s = "Java";
        double f = 3.14f;

        System.out.printf("정수출력 : %d\n", i);
        System.out.printf("정수출력 : [%10d]\n", i); //[] 괄호는 안넣어도 되지만 위치보려고

        System.out.printf("문자출력 : %s\n", s);
        System.out.printf("문자출력 : %10s\n", s); // 10자리! 빈자리는 공백

        System.out.printf("실수출력 : %f\n", f); //실수 그대로 출력
        System.out.printf("실수출력 : %4.1f\n", f); //5자리. 소수점 1자리
        System.out.printf("실수출력 : %1.2f\n", f); //1자리. 소수점 2자리
        System.out.printf("실수출력 : %04.1f\n", f); //4자리. 소수점 1자리 빈자리0
        System.out.printf("실수출력 : %-4.1f\n", f); // 4자리 소수점 1자리 왼쪽정렬







    }


}
