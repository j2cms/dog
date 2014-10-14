package test;

public class DouleTest {

	public static void main(String args[]){
		String s1 = "10.13";
		double d1 = Double.valueOf(s1);
		String s2 = "10.31111";
		double d2 = Double.valueOf(s2);
		System.out.println(d1);
		
		System.out.println(d1>d2);
		
		double d3 = Double.valueOf("10.00");

		System.out.println((int)d3);
	}
}
