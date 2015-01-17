package kmodes;

import instance.Instance;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import util.HDFSUtil;

public class KModes {

	public static void run(String input, String outputBasePath, String centerPath, int k, int maxIter) throws Exception {

		// 初始化
		Center.initial(outputBasePath, centerPath);

		int iter = 1;
		System.out.println("iter: " + iter);// 迭代次数
		int threshold = 1;// threshold是阀值
		do {
			if(iter >1)
				input = outputBasePath +"/"+  (iter-1) +"/output/";
			String output = outputBasePath +"/"+  iter +"/output/";
			
			Configuration conf = new Configuration();
//			conf.set("mapred.job.tracker", "master:9001");
			conf.set("outputBasePath", outputBasePath);
			conf.setInt("iter", iter);

		
			
			Job job = new Job(conf, "kmodes-iter:" + iter);
			job.setJarByClass(KModes.class);// 设定作业的启动类
			job.setMapperClass(KMapper.class);// 设定Mapper类
			job.setNumReduceTasks(0);

			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);

			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);

			job.setInputFormatClass(KeyValueTextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			// 解析输入和输出参数，分别作为作业的输入和输出，都是文件
			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
		
			if (job.waitForCompletion(true))
			{
				HDFSUtil.deleteNoUse(output);
				
				ClusterArrayList clusters = Center.renew(outputBasePath, iter,conf);
				
				if(((clusters.d <= threshold) || (iter >= maxIter))){// 当误差小于阈值停止。
					
					StringBuffer sb = new StringBuffer();
					for(Cluster c:clusters)
						sb.append(c.id+"\t"+c.center+"\n");
					
					String path = outputBasePath +"/"+  iter +"/centerPoint";
					HDFSUtil.write(path, sb.toString());
					
					break;
					
				}
				else{
					String path = outputBasePath +"/"+  iter +"/centerPoint";
					HDFSUtil.write(path, clusters);
					iter++;
				}
			}
		} while (true);

	}

	public static int addInsToBestFitCluster(Instance inst, ClusterArrayList clusters) {
		System.out.println("instance:"+inst);
		int min = Integer.MAX_VALUE;// dissimilarity
		int clusterId = 0;
		for (Cluster c : clusters) {
			int d = c.different(inst);
			if (min > d) {
				min = d;
				clusterId = c.id;
			}
			System.out.println("c.id="+c.id+", c.center="+c.center+", d="+d);
		}
		
		System.out.println("min="+min+", clusterId="+clusterId);
		
		clusters.get(clusterId).addInstance(inst);
		return clusterId;
	}

	public static void main(String[] args) throws Exception {
				
		if ((args.length < 5) || (args[0].equals("-help"))) {
			System.out.println("命令格式:hadoop jar dog.jar kmodes.KModes input output center k maxIter");
			System.exit(-1);
		}
		
		for(String arg:args)
			System.out.print(arg+"\t");
		System.out.println();
		
		
//		args = new String[] { "/user/hadoop/kmodes/mushroom/input", "/user/hadoop/kmodes/mushroom/output", "/user/hadoop/kmodes/mushroom/center/c.txt" };
		int k = Integer.valueOf(args[3]);
		int maxIter = Integer.valueOf(args[4]);
		String dateTag = "/k=" + k + "_m=" + maxIter + "_" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
		String outputBasePath = args[1] + dateTag;
		
		HDFSUtil.mkdirs(outputBasePath);
		KModes.run(args[0], outputBasePath, args[2], k, maxIter);
	}

}
