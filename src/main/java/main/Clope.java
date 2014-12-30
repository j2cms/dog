package main;


import instance.Instance;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import mapreduce.AddToClusterMapReduce;
import mapreduce.MoveToClusterMapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import util.ClusterUtil;
import util.FileUtil;
import util.HDFSUtil;
import cluster.Cluster;
import cluster.ClusterArrayList;

public class Clope {
	public static int addInstanceToBestCluster(Instance inst, List<Cluster> clusters, double repulsion) {
		double delta;
		double deltaMax;
		int clusterId = -1;
		if (clusters.size() > 0) {
			int tempS = 0;
			int tempW = 0;
			for (int i = 0; i < inst.size(); i++) {
				tempS++;
				tempW++;
			}
			deltaMax = tempS / Math.pow(tempW, repulsion);
			for (int id = 0; id < clusters.size(); id++) {
				Cluster tempcluster = clusters.get(id);// clusters.get(id);
				delta = tempcluster.deltaAdd(inst, repulsion);
				if (delta >= deltaMax) {
					deltaMax = delta;
					clusterId = id;
				}
			}
		}
		if (clusterId == -1) {// add
			clusterId = clusters.size();
			inst.setClusterId(clusterId);
			Cluster newCluster = new Cluster();
			newCluster.addInstance(inst);
			clusters.add(newCluster);// clusters.add(newcluster);
		} else {
			inst.setClusterId(clusterId);
			clusters.get(clusterId).addInstance(inst);
		}
		return clusterId;
	}

	public static int moveInstanceToBestCluster(Instance inst, ClusterArrayList clusters, double repulsion) {
		clusters.get(inst.getClusterId()).removeInstance(inst);
		double delta = 0;
		int size = clusters.size();
		int newClusterId = size;
		int tempS = 0;
		int tempW = 0;
		for (int i = 0; i < inst.size(); i++) {
			tempS++;
			tempW++;
		}
		double deltaMax = tempS / Math.pow(tempW, repulsion);
		for (int i = 0; i < size; i++) {
			Cluster c = clusters.get(i);// clusters.get(i);
			delta = c.deltaAdd(inst, repulsion);
			if (delta >= deltaMax) {
				deltaMax = delta;
				newClusterId = i;
			}
		}
		inst.setClusterId(newClusterId);
		if (newClusterId == size) {// new
			System.out.println("产生了新cluster!!!");
			Cluster c = new Cluster();
			c.addInstance(inst);
			clusters.add(c);
		} else {// move
			clusters.get(newClusterId).addInstance(inst);
		}
		return newClusterId;
	}

	/**
	 * 
	 * @param basePath
	 * @param input
	 * @param p
	 *            排列的参数
	 * @throws Exception
	 */
	public static void buildClusterer(String inputFile, String basePath, double repulsion, int p , int maxIter, boolean number) throws Exception {
		String dateTag ="/r=" + repulsion + "_p=" + p + "_m=" + maxIter + "_" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
		String outputBasePath = basePath + dateTag;
		String input = outputBasePath + "/input";
		String spiltDir = basePath + "/split_"+p;
//		if (!HDFSUtil.exists(spiltDir))// 不存在的话生成一个，如果前面的操作已经生成过了，这里就不做了
		long n =	HDFSUtil.spiltToNFile(inputFile, spiltDir, p);
		HDFSUtil.generatePermFile(spiltDir, input);
		String s;
		Configuration conf = new Configuration();
		conf.set("repulsion", String.valueOf(repulsion));
		conf.setLong("n", n);//added 2014.10.14
		conf.setBoolean("number", number);
		conf.setLong("mapred.min.split.size", 1024 * 1024 * 1024);// 1024M
		conf.setLong("mapred.max.split.size", 1024 * 1024 * 1024);
		conf.set("mapred.job.priority", "HIGH");// VERY_HIGH,HIGH,NORMAL
		conf.set("outputBasePath", outputBasePath);
		int iter = 0;
		NumberFormat nf = NumberFormat.getInstance(Locale.CHINA);
		// Phase 1
		long time1 = System.currentTimeMillis();
		// 本地文件
		String outDir = "output/"+basePath.substring(basePath.lastIndexOf("/")+1);
		FileUtil.mkdirs(outDir);
		FileWriter out = new FileWriter(outDir+"/" +dateTag+ ".txt");
		BufferedWriter bw = new BufferedWriter(out);
		System.out.print(n);
		bw.write(String.valueOf(n));
		String output = outputBasePath + "/output/" + iter;
		conf.setInt("iter", iter);
		long timeCostHDFS = AddToClusterMapReduce.job(conf, new Path(input), new Path(output));
		HDFSUtil.deleteNoUse(output);
		long time2 = System.currentTimeMillis();
		String[] d = ClusterUtil.getMaxtProfit(outputBasePath, iter);
		double k =  Double.valueOf(d[1]);
		s = "Phase 1, best clustering is " + d[0] + ", cluster " + (int)(k) + ", profit " + d[2] + ",time cost " + nf.format(time2 - time1) + " ms, HDFS time cost " + nf.format(timeCostHDFS) + " ms,  " + (double) timeCostHDFS / (time2 - time1) + "\n";
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
			if(iter==maxIter)
				break;
			System.out.println("Phase 2,iter=" + iter + "...");
			//策略一 生成新的输入文件
			inputFile = output +"/"+ inputFile;
			spiltDir = outputBasePath + "/split";
			//分割成p份
			HDFSUtil.spiltToNFile(inputFile, spiltDir, p);
			HDFSUtil.generatePermFile(spiltDir,input);
			conf.set("bestClusterId", bestClusterId);
			// 策略二 　直接将输出作为输入
			// input = output;
			output = outputBasePath + "/output/" + iter;
			conf.setInt("iter", iter);
			long t1 = System.currentTimeMillis();
			timeCostHDFS = MoveToClusterMapReduce.job(conf, new Path(input), new Path(output), iter, count);
			long t2 = System.currentTimeMillis();
			// 删除
			HDFSUtil.deleteNoUse(output);
			d = ClusterUtil.getMaxtProfit(outputBasePath, iter);
			count = Integer.valueOf(d[3]);
			moveCount += count;
			moved = (count > 0 ? true : false);
			k =  Double.valueOf(d[1]);
			s = "Phase 2,iter=" + iter + " done, best clustering is " + d[0] + ", NOT empty cluster " + (int)(k) + ", profit " + d[2] + ", moveCount " + count + ", moved = " + moved + ", time cost " + nf.format(t2 - t1) + " ms, HDFS time cost " + nf.format(timeCostHDFS) + " ms, " + (double) timeCostHDFS / (t2 - t1) + "\n";
			System.out.print(s);
			bw.write(s);

			bestClusterId = d[0].substring(d[0].indexOf("/") + 1);
			inputFile = "part-m-" + bestClusterId.substring(1);// 去掉前面一个0
			// 策略一 可以删除无用的output和clustering
			HDFSUtil.deleteDirExceptFile(output, inputFile);
			HDFSUtil.deleteDirExceptFile(outputBasePath + "/clustering/" + iter, bestClusterId);
			HDFSUtil.deleteDirExceptFile(outputBasePath + "/profit/" + iter, bestClusterId);
			
			//删除上一次迭代的结果
			HDFSUtil.deletePath(outputBasePath+"/clustering/" + (iter-1));
			HDFSUtil.deletePath(outputBasePath+"/output/" + (iter-1));
			
		} while (moved);
		long time3 = System.currentTimeMillis();
		s = "Phase 2 total moveCount= " + moveCount + ", total time cost " + nf.format(time3 - time2) + " ms\n";
		System.out.print(s);
		bw.write(s);
		bw.close();
		out.close();
		HDFSUtil.deletePath(outputBasePath+"/clustering");
		HDFSUtil.deletePath(outputBasePath+"/input");
		HDFSUtil.deletePath(outputBasePath+"/split");
		for(int i =0;i<iter;i++){
			HDFSUtil.deletePath(outputBasePath+"/output/"+i);
			HDFSUtil.deletePath(outputBasePath+"/profit/"+i);
		}
		
		System.out.println("done!");

	}

	
	
}
