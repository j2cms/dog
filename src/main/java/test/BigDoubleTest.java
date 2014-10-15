package test;

public class BigDoubleTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Double d1 = Double.valueOf("5.643974005005147E8");//合法
		System.out.println(d1);
		
		Double d2 = Double.valueOf("5.643974005005148E8");
		
		System.out.println(d1>d2);
	}

}
