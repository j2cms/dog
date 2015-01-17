package sortout;


public class SortOut {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("命令格式:hadoop jar dog.jar sortout.SortOut input ouput k");
			System.exit(2);
		}

		SortOutClusterMapReduce.job(args[0], args[1], Integer.valueOf(args[2]));
	}

}
