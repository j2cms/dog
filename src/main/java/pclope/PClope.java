package pclope;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.net.InetAddress;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import util.ClusterUtil;
import util.FileUtil;
import util.HDFSUtil;

public class PClope {

	/**
	 * 
	 * @param basePath
	 * @param input
	 * @param p
	 *            排列的参数
	 * @throws Exception
	 */
	public static void buildClusterer(String inputFile, String basePath, double repulsion, int p, int maxIter, boolean number) throws Exception {
		String dateTag = "/r=" + repulsion + "_p=" + p + "_m=" + maxIter + "_" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
		String outputBasePath = basePath + dateTag;
		String input = outputBasePath + "/input";
		String spiltDir = basePath + "/split_" + p;
		// if (!HDFSUtil.exists(spiltDir))// 不存在的话生成一个，如果前面的操作已经生成过了，这里就不做了
		long n = HDFSUtil.spiltToNFile(inputFile, spiltDir, p);
		HDFSUtil.generatePermFile(spiltDir, input);
		String s;
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "yarn");
	    conf.set("yarn.resourcemanager.hostname", InetAddress.getLocalHost().getHostName());
		conf.set("repulsion", String.valueOf(repulsion));
		conf.setLong("n", n);// added 2014.10.14
		conf.setBoolean("number", number);
		conf.setLong("mapred.min.split.size", 1024 * 1024 * 1024);// 1024M
		conf.setLong("mapred.max.split.size", 1024 * 1024 * 1024);
		conf.set("mapred.job.priority", "HIGH");// VERY_HIGH,HIGH,NORMAL
		conf.set("outputBasePath", outputBasePath);
		int iter = 0;
		NumberFormat nf = NumberFormat.getInstance(Locale.CHINA);
		// Phase 1
		long time1 = System.currentTimeMillis();

		FileWriter out = null;
		try {
			// 本地文件
			String outDir = "output/" + basePath.substring(basePath.lastIndexOf("/") + 1);
			FileUtil.mkdirs(outDir);
			out = new FileWriter(outDir + "/" + dateTag + ".txt");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.out.println("the user can't mkdir at current path.");
		}

		BufferedWriter bw = new BufferedWriter(out);
		System.out.print(n);
		bw.write(String.valueOf(n));
		String output = outputBasePath + "/output/" + iter;
		conf.setInt("iter", iter);
		AddToClusterMapReduce.job(conf, new Path(input), new Path(output));
		HDFSUtil.deleteNoUse(output);
		long time2 = System.currentTimeMillis();
		String[] d = ClusterUtil.getMaxtProfit(outputBasePath, iter);
		double k = Double.valueOf(d[1]);
		s = "Phase 1, best clustering is " + d[0] + ", cluster " + (int) (k) + ", profit " + d[2] + ",time cost " + nf.format(time2 - time1) + " ms,  " + "\n";
		System.out.print(s);
		bw.write(s);
		// 策略一 当前使用
		String bestClusterId = d[0].substring(d[0].indexOf("/") + 1);
		inputFile = "part-m-" + bestClusterId.substring(1);// 去掉前面一个0
		// 策略一 可以删除无用的output和clustering
		HDFSUtil.deleteDirExceptFile(output, inputFile);
		HDFSUtil.deleteDirExceptFile(outputBasePath + "/clustering/" + iter, bestClusterId);
		HDFSUtil.deleteDirExceptFile(outputBasePath + "/profit/" + iter, bestClusterId);

		// Phase 2
		boolean moved = false;
		long moveCount = 0;
		long count = 0;
		do {
			iter++;
			if (iter == maxIter)
				break;
			System.out.println("Phase 2,iter=" + iter + "...");
			// 策略一 生成新的输入文件
			inputFile = output + "/" + inputFile;
			spiltDir = outputBasePath + "/split";
			// 分割成p份
			HDFSUtil.spiltToNFile(inputFile, spiltDir, p);
			HDFSUtil.generatePermFile(spiltDir, input);
			conf.set("bestClusterId", bestClusterId);
			// 策略二 　直接将输出作为输入
			// input = output;
			output = outputBasePath + "/output/" + iter;
			conf.setInt("iter", iter);
			long t1 = System.currentTimeMillis();
			MoveToClusterMapReduce.job(conf, new Path(input), new Path(output), iter, count);
			long t2 = System.currentTimeMillis();
			// 删除
			HDFSUtil.deleteNoUse(output);
			d = ClusterUtil.getMaxtProfit(outputBasePath, iter);
			count = Integer.valueOf(d[3]);
			moveCount += count;
			moved = (count > 0 ? true : false);
			k = Double.valueOf(d[1]);
			s = "Phase 2,iter=" + iter + " done, best clustering is " + d[0] + ", NOT empty cluster " + (int) (k) + ", profit " + d[2] + ", moveCount " + count + ", moved = " + moved + ", time cost " + nf.format(t2 - t1) + " ms\n";
			System.out.print(s);
			bw.write(s);

			bestClusterId = d[0].substring(d[0].indexOf("/") + 1);
			inputFile = "part-m-" + bestClusterId.substring(1);// 去掉前面一个0
			// 策略一 可以删除无用的output和clustering
			HDFSUtil.deleteDirExceptFile(output, inputFile);
			HDFSUtil.deleteDirExceptFile(outputBasePath + "/clustering/" + iter, bestClusterId);
			HDFSUtil.deleteDirExceptFile(outputBasePath + "/profit/" + iter, bestClusterId);

			// 删除上一次迭代的结果
			HDFSUtil.deletePath(outputBasePath + "/clustering/" + (iter - 1));
			HDFSUtil.deletePath(outputBasePath + "/output/" + (iter - 1));

		} while (moved);
		long time3 = System.currentTimeMillis();
		s = "Phase 2 total moveCount= " + moveCount + ", total time cost " + nf.format(time3 - time2) + " ms\n";
		System.out.print(s);
		bw.write(s);
		bw.close();
		out.close();
		HDFSUtil.deletePath(outputBasePath + "/clustering");
		HDFSUtil.deletePath(outputBasePath + "/input");
		HDFSUtil.deletePath(outputBasePath + "/split");
		
//		//再把除最后一次的结果删除
//		for(int i =0;i<iter-1;i++){
//			HDFSUtil.deletePath(outputBasePath+"/output/"+i);
//			HDFSUtil.deletePath(outputBasePath+"/profit/"+i);
//		}

		System.out.println("done!");

	}

	public static void testDNS() throws Exception {
		// repulsion = 1.8;
		// int numOfInstance = 29459;
		String basePath = "/user/hadoop/clope/dns";
		String input = basePath + "/input/1.txt";
		PClope.buildClusterer(input, basePath, 1.8, 4, 2, false);
	}

	public static void testMushroom() throws Exception {
		String basePath = "/user/hadoop/clope/mushroom";
		// String inputFile = basePath + "/agaricus-lepiota.data";
		String inputFile = basePath + "/split_3";
		double r = 3.1;
		int p = 4;
		int maxIter = 100;
		PClope.buildClusterer(inputFile, basePath, r, p, maxIter, true);
	}

	public static void testPeter() throws Exception {
		String inputFile = "/user/peter/clope/gxy_counsel_kw.txt";
		String output = "/user/peter/clope";
		double r = 1;
		int p = 3;
		int maxIter = 1;
		PClope.buildClusterer(inputFile, output, r, p, maxIter, false);
	}

	public static void main(String[] args) throws Exception {
		if ((args.length < 6) || (args[0].equals("-help"))) {
			System.out.println("命令格式:hadoop jar dog.jar pclope.PClope input output repulsion p maxIter isNumber");
			System.exit(-1);
		}
		boolean isNumber = false;
		if (args[5].equals("1") || args[5].equals("true"))
			isNumber = true;
		else if (args[5].equals("0") || args[5].equals("false"))
			isNumber = false;

		for (String arg : args)
			System.out.print(arg + "\t");
		System.out.println();
		PClope.buildClusterer(args[0], args[1], Double.valueOf(args[2]), Integer.valueOf(args[3]), Integer.valueOf(args[4]), isNumber);
	}

}
