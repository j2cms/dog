package main;

public class MRClope {

	public static void testDNS() throws Exception {
		// repulsion = 1.8;
		// int numOfInstance = 29459;
		String basePath = "/user/hadoop/clope/dns";
		String input = basePath + "/input/1.txt";
		Clope.buildClusterer(input, basePath, 1.8, 4,2,false);
	}

	public static void testMushroom() throws Exception {
		String basePath = "/user/hadoop/clope/mushroom";
//		String inputFile = basePath + "/agaricus-lepiota.data";
		String inputFile = basePath + "/split_3";
		double r = 3.1;
		int p = 4;
		int maxIter = 100;
		Clope.buildClusterer(inputFile, basePath, r, p, maxIter,true);
	}

	public static void testPeter() throws Exception{
		String inputFile ="/user/peter/clope/gxy_counsel_kw.txt";
		String output = "/user/peter/clope";
		double r = 1;
		int p = 3;
		int maxIter = 1;
		Clope.buildClusterer(inputFile, output, r, p, maxIter,false);
	}
	
	
	public static void main(String[] args) throws Exception {

		// testPeter();
		// testMushroom();
		// testDNS();

		if ((args.length < 5) || (args[0].equals("-help"))) {
			System.out.println("命令格式:hadoop jar MRClope.jar main.Clope input output repulsion p maxIter number");
			System.exit(-1);
		}
		boolean num = false;
		if (args[5].equals("1") || args[5].equals("true"))
			num = true;
		else if (args[5].equals("0") || args[5].equals("false"))
			num = false;
		Clope.buildClusterer(args[0], args[1], Double.valueOf(args[2]), Integer.valueOf(args[3]), Integer.valueOf(args[4]), num);

	}

}
