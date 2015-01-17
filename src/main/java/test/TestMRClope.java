package test;

import pclope.PClope;

public class TestMRClope {

	public static void test123() throws Exception{
		String inputFile = "/user/hadoop/clope/test/123.txt";
		String output = "/user/hadoop/clope/test/output";
		double r = 2.0;
		int p = 6;
		int maxIter = 10;
		PClope.buildClusterer(inputFile, output, r, p, maxIter,false);
	}
	
	public static void main(String[] args) throws Exception {
		test123();
	}
}
