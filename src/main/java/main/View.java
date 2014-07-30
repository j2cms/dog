package main;

import mapreduce.ViewClusterMapReduce;

public class View {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("命令格式:hadoop jar MRClope.jar main.View 输入 输出目录 聚类个数");
			System.exit(2);
		}

		ViewClusterMapReduce.job(args[0], args[1], Integer.valueOf(args[2]));
	}

}
